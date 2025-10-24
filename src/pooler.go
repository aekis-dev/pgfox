package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// WildcardPooler manages dynamic database discovery and pooling
type WildcardPooler struct {
	config   Config
	listener net.Listener

	// Unified storage: target_name -> database_name -> username -> manager
	targets   map[string]map[string]map[string]*DatabaseManager
	targetsMu sync.RWMutex

	targetConfigs []*Target // Reference to config targets

	clients                map[net.Conn]*ClientConnection
	clientsMu              sync.RWMutex
	listeners              map[string]map[*ClientConnection]bool
	listenersMu            sync.RWMutex
	notificationMonitors   map[string]*NotificationMonitor
	notificationMonitorsMu sync.RWMutex
	stats                  GlobalStats
	ctx                    context.Context
	cancel                 context.CancelFunc
	wg                     sync.WaitGroup
	logger                 *Logger
}

// DatabaseManager manages connections for a specific database with specific user credentials
type DatabaseManager struct {
	config      DatabaseConfig
	target      *Target // Reference to the target this manager belongs to
	backendPool chan *BackendConnection
	stats       DatabaseStats

	// Client credentials - required for all database managers
	username string
	password string

	mu sync.RWMutex
}

// GlobalStats contains global pooler statistics
type GlobalStats struct {
	TotalClients          int64
	ActiveClients         int64
	TotalDatabases        int64 // Total unique (target, database, user) combinations
	TotalQueries          int64
	NotificationsSent     int64
	IdleConnectionsClosed int64
}

// DatabaseStats contains per-database statistics
type DatabaseStats struct {
	TotalConnections  int64
	ActiveConnections int64
	IdleConnections   int64
	QueriesExecuted   int64
	ErrorCount        int64
	BytesReceived     int64
	BytesSent         int64
}

// DatabaseConfig is used internally to configure a database manager
type DatabaseConfig struct {
	Name           string
	Host           string
	Port           int
	User           string
	Password       string
	SSLMode        string
	SSLCAFile      string
	MaxConnections int
	ConnectTimeout time.Duration
	Parameters     map[string]string
}

// NewWildcardPooler creates a new wildcard pooler
func NewWildcardPooler(config Config, logger *Logger) (*WildcardPooler, error) {
	listener, err := net.Listen("tcp", config.Server.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", config.Server.ListenAddr, err)
	}

	pooler := &WildcardPooler{
		config:        config,
		listener:      listener,
		targets:       make(map[string]map[string]map[string]*DatabaseManager),
		targetConfigs: make([]*Target, 0, len(config.Targets)),
		clients:       make(map[net.Conn]*ClientConnection),
		listeners:     make(map[string]map[*ClientConnection]bool),
		logger:        logger,
	}

	// Initialize target references
	for i := range config.Targets {
		pooler.targetConfigs = append(pooler.targetConfigs, &config.Targets[i])
	}

	return pooler, nil
}

// Start starts the wildcard pooler
func (p *WildcardPooler) Start(ctx context.Context) error {
	// Store context and create cancel function
	p.ctx, p.cancel = context.WithCancel(ctx)

	p.logger.Info("PostgreSQL connection pooler starting",
		"listen_addr", p.config.Server.ListenAddr,
		"targets", len(p.targetConfigs))

	// Start idle connection cleanup worker if configured
	if p.config.Server.IdleTimeout > 0 {
		p.wg.Add(1)
		go p.idleConnectionCleanupWorker(p.ctx)
	}

	// Start metrics server if enabled
	if p.config.Metrics.Enabled {
		p.wg.Add(1)
		go p.startMetricsServer(p.ctx)
	}

	for {
		select {
		case <-p.ctx.Done():
			p.logger.Info("Accept loop stopping due to shutdown signal")
			return p.shutdown()
		default:
			// Set accept deadline to allow periodic context checking
			if listener, ok := p.listener.(*net.TCPListener); ok {
				listener.SetDeadline(time.Now().Add(1 * time.Second))
			}

			conn, err := p.listener.Accept()

			// Clear deadline
			if listener, ok := p.listener.(*net.TCPListener); ok {
				listener.SetDeadline(time.Time{})
			}

			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					// Timeout is expected, continue to check context
					continue
				}
				if p.ctx.Err() != nil {
					return p.shutdown()
				}
				p.logger.WithError(err).Error("Accept error")
				continue
			}

			atomic.AddInt64(&p.stats.TotalClients, 1)
			atomic.AddInt64(&p.stats.ActiveClients, 1)

			p.wg.Add(1)
			go p.handleClient(conn)
		}
	}
}

// shutdown gracefully shuts down the pooler
func (p *WildcardPooler) shutdown() error {
	p.logger.Info("Starting graceful shutdown")

	// Stop accepting new connections
	if p.listener != nil {
		if err := p.listener.Close(); err != nil {
			p.logger.WithError(err).Warn("Error closing listener")
		}
	}

	// Cancel all background workers
	p.cancel()

	// Stop all notification monitors
	p.logger.Info("Stopping notification monitors")
	p.notificationMonitorsMu.Lock()
	for key, monitor := range p.notificationMonitors {
		p.logger.Debug("Stopping notification monitor", "key", key)
		monitor.Stop()
	}
	p.notificationMonitors = nil
	p.notificationMonitorsMu.Unlock()

	// Close all client connections with a brief grace period
	p.logger.Info("Closing client connections")
	p.clientsMu.Lock()
	for conn := range p.clients {
		conn.Close()
	}
	p.clientsMu.Unlock()

	// Close all database managers
	p.logger.Info("Closing database pools")
	p.targetsMu.Lock()
	for _, targetMap := range p.targets {
		for _, dbMap := range targetMap {
			for _, manager := range dbMap {
				p.closeDatabaseManager(manager)
			}
		}
	}
	p.targetsMu.Unlock()

	// Wait for all goroutines to finish (no timeout)
	p.wg.Wait()
	p.logger.Info("Graceful shutdown completed")

	return nil
}

// closeDatabaseManager closes a database manager and its connections
func (p *WildcardPooler) closeDatabaseManager(dbManager *DatabaseManager) {
	// Close all backend connections
	close(dbManager.backendPool)
	for conn := range dbManager.backendPool {
		conn.Close()
	}
}

// handleClient handles a client connection
func (p *WildcardPooler) handleClient(conn net.Conn) {
	defer p.wg.Done()
	defer func() {
		atomic.AddInt64(&p.stats.ActiveClients, -1)
		conn.Close()
	}()

	clientLogger := p.logger.WithClient(conn.RemoteAddr().String())
	client := NewClientConnection(conn, clientLogger)

	p.clientsMu.Lock()
	p.clients[conn] = client
	p.clientsMu.Unlock()

	defer func() {
		p.clientsMu.Lock()
		delete(p.clients, conn)
		p.clientsMu.Unlock()

		p.cleanupClientListeners(client)

		// Only release backend connection if not in session mode
		if backend := client.GetBackendConnection(); backend != nil {
			if !client.ShouldKeepBackendConnection() {
				p.releaseBackendConnection(backend)
			} else {
				// For session mode, close the backend connection directly
				clientLogger.Debug("Closing dedicated backend connection")
				backend.Close()

				// Update statistics
				if dbManager, _ := p.getDatabaseManager(client.GetDatabase(), client.GetUser()); dbManager != nil {
					atomic.AddInt64(&dbManager.stats.TotalConnections, -1)
					atomic.AddInt64(&dbManager.stats.ActiveConnections, -1)
				}
			}
		}

		// Remove client from notification monitors
		p.cleanupClientFromNotificationMonitors(client)
	}()

	if err := p.handleStartupMessage(client); err != nil {
		clientLogger.WithError(err).Error("Startup error")
		return
	}

	for {
		select {
		case <-p.ctx.Done():
			return
		default:
			if err := p.handleClientMessage(client); err != nil {
				if err.Error() != "EOF" {
					clientLogger.WithError(err).Error("Client message error")
				}
				return
			}
		}
	}
}

// Stats returns current pooler statistics
func (p *WildcardPooler) Stats() GlobalStats {
	stats := GlobalStats{
		TotalClients:          atomic.LoadInt64(&p.stats.TotalClients),
		ActiveClients:         atomic.LoadInt64(&p.stats.ActiveClients),
		TotalDatabases:        atomic.LoadInt64(&p.stats.TotalDatabases),
		TotalQueries:          atomic.LoadInt64(&p.stats.TotalQueries),
		NotificationsSent:     atomic.LoadInt64(&p.stats.NotificationsSent),
		IdleConnectionsClosed: atomic.LoadInt64(&p.stats.IdleConnectionsClosed),
	}

	return stats
}

// getSortedTargets returns targets sorted by priority
func (p *WildcardPooler) getSortedTargets() []*Target {
	sorted := make([]*Target, len(p.targetConfigs))
	copy(sorted, p.targetConfigs)

	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Priority == sorted[j].Priority {
			// Same priority, sort by name for consistency
			return sorted[i].Name < sorted[j].Name
		}
		return sorted[i].Priority < sorted[j].Priority
	})

	return sorted
}

// targetServesDatabase checks if a target serves a specific database
func (p *WildcardPooler) targetServesDatabase(target *Target, dbName string) bool {
	// Check include list (if specified, only these databases are allowed)
	if len(target.IncludeDatabases) > 0 {
		found := false
		for _, included := range target.IncludeDatabases {
			if included == dbName {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check target-level exclude list
	for _, excluded := range target.ExcludeDatabases {
		if excluded == dbName {
			return false
		}
	}

	return true
}

// cleanupClientListeners removes a client from all listener registrations
func (p *WildcardPooler) cleanupClientListeners(client *ClientConnection) {
	client.mu.Lock()
	channels := make([]string, 0, len(client.listenChannels))
	for channel := range client.listenChannels {
		channels = append(channels, channel)
	}
	client.mu.Unlock()

	for _, channel := range channels {
		p.unregisterListener(channel, client)
	}
}

// releaseBackendConnection returns a backend connection to its pool
func (p *WildcardPooler) releaseBackendConnection(conn *BackendConnection) {
	if conn == nil {
		return
	}

	logger := p.logger.WithField("backend", conn.RemoteAddr())
	logger.Debug("Attempting to release backend connection")

	// Find the appropriate database manager
	var dbManager *DatabaseManager

	p.targetsMu.RLock()
	targetMap, exists := p.targets[conn.targetName]
	if exists {
		dbMap, exists := targetMap[conn.dbName]
		if exists {
			for _, manager := range dbMap {
				if manager.config.Name == conn.dbName {
					dbManager = manager
					break
				}
			}
		}
	}
	p.targetsMu.RUnlock()

	if dbManager == nil {
		logger.Warn("No database manager found for connection, closing",
			"db", conn.dbName, "target", conn.targetName)
		conn.Close()
		return
	}

	conn.mu.Lock()
	isListening := conn.isListening
	conn.inUse = false
	conn.lastUsedAt = time.Now()
	conn.clientRef = nil
	conn.mu.Unlock()

	if isListening {
		logger.Debug("Not returning listening connection to pool, closing",
			"db", conn.dbName)
		conn.Close()
		atomic.AddInt64(&dbManager.stats.TotalConnections, -1)
		atomic.AddInt64(&dbManager.stats.ActiveConnections, -1)
		return
	}

	// Try to return to pool
	select {
	case dbManager.backendPool <- conn:
		atomic.AddInt64(&dbManager.stats.ActiveConnections, -1)
		atomic.AddInt64(&dbManager.stats.IdleConnections, 1)
		logger.Debug("Successfully returned connection to pool",
			"db", conn.dbName)
	default:
		// Pool is full, close the connection
		logger.Debug("Pool full, closing connection",
			"db", conn.dbName)
		conn.Close()
		atomic.AddInt64(&dbManager.stats.TotalConnections, -1)
		atomic.AddInt64(&dbManager.stats.ActiveConnections, -1)
	}
}

// startMetricsServer starts the web server
func (p *WildcardPooler) startMetricsServer(ctx context.Context) {
	defer p.wg.Done()

	addr := fmt.Sprintf(":%d", p.config.Metrics.Port)
	server := NewWebServer(p, addr, p.logger)

	p.logger.Info("Starting web server", "addr", addr)

	if err := server.Start(ctx); err != nil {
		p.logger.WithError(err).Error("Web server failed")
	}
}

// isSSLSupported checks if SSL is supported by any configured target
func (p *WildcardPooler) isSSLSupported() bool {
	// Check all targets
	for _, target := range p.targetConfigs {
		if target.SSLMode != "disable" {
			return true
		}
	}
	return false
}

// upgradeToTLS upgrades a client connection to TLS
func (p *WildcardPooler) upgradeToTLS(client *ClientConnection) error {
	// Load TLS configuration from config
	if p.config.Server.SSLCertFile == "" || p.config.Server.SSLKeyFile == "" {
		return fmt.Errorf("SSL certificate or key file not configured")
	}

	cert, err := tls.LoadX509KeyPair(p.config.Server.SSLCertFile, p.config.Server.SSLKeyFile)
	if err != nil {
		return fmt.Errorf("failed to load SSL certificate: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// Wrap the connection with TLS
	tlsConn := tls.Server(client.conn, tlsConfig)

	// Perform TLS handshake
	if err := tlsConn.Handshake(); err != nil {
		return fmt.Errorf("TLS handshake failed: %w", err)
	}

	p.logger.Debug("TLS connection established", "client", client.RemoteAddr())

	// Replace the connection, reader, and writer
	client.mu.Lock()
	client.conn = tlsConn
	client.reader = bufio.NewReader(tlsConn)
	client.writer = bufio.NewWriter(tlsConn)
	client.mu.Unlock()

	return nil
}

// idleConnectionCleanupWorker periodically removes idle connections from pools
func (p *WildcardPooler) idleConnectionCleanupWorker(ctx context.Context) {
	defer p.wg.Done()

	logger := p.logger.WithField("component", "idle_cleanup")
	logger.Info("Starting idle connection cleanup worker", "idle_timeout", p.config.Server.IdleTimeout)

	// Run cleanup every 1/4 of the idle timeout period
	interval := p.config.Server.IdleTimeout / 4
	if interval < 30*time.Second {
		interval = 30 * time.Second // Minimum 30 second interval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Idle connection cleanup worker stopping")
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				// Check context before cleanup
				return
			}
			p.cleanupIdleConnections()
		}
	}
}

// cleanupIdleConnections removes idle connections from all database pools
func (p *WildcardPooler) cleanupIdleConnections() {
	logger := p.logger.WithField("component", "idle_cleanup")
	cutoff := time.Now().Add(-p.config.Server.IdleTimeout)

	totalCleaned := 0

	p.targetsMu.RLock()
	defer p.targetsMu.RUnlock()

	// Iterate through all targets -> databases -> users
	for targetName, targetMap := range p.targets {
		for dbName, dbMap := range targetMap {
			for userName, dbManager := range dbMap {
				key := fmt.Sprintf("%s/%s/%s", targetName, dbName, userName)
				cleaned := p.cleanupDatabasePool(dbManager, key, cutoff)
				totalCleaned += cleaned
			}
		}
	}

	if totalCleaned > 0 {
		logger.Info("Idle connection cleanup completed", "connections_closed", totalCleaned)
	} else {
		logger.Debug("Idle connection cleanup completed", "connections_closed", 0)
	}
}

// cleanupDatabasePool cleans up idle connections from a specific database pool
func (p *WildcardPooler) cleanupDatabasePool(dbManager *DatabaseManager, poolKey string, cutoff time.Time) int {
	logger := p.logger.WithField("pool_key", poolKey)

	// Get minimum connections from target config (default to 0 if not set)
	minConnections := 0
	if dbManager.target != nil {
		// You might want to add a MinConnectionsPerDB field to Target struct
		// For now, we'll keep a minimum of 1 connection per pool
		minConnections = 1
	}

	currentTotal := atomic.LoadInt64(&dbManager.stats.TotalConnections)

	if currentTotal <= int64(minConnections) {
		logger.Debug("Skipping cleanup - at or below minimum connections",
			"current", currentTotal,
			"min", minConnections)
		return 0
	}

	cleaned := 0
	poolSize := len(dbManager.backendPool)

	logger.Debug("Checking pool for idle connections",
		"pool_size", poolSize,
		"total_connections", currentTotal,
		"idle_timeout", p.config.Server.IdleTimeout)

	// Check connections in the pool (idle ones)
	// We can't check all at once, so we'll check a limited number per cycle
	maxChecks := poolSize
	if maxChecks > 10 {
		maxChecks = 10 // Check at most 10 connections per cleanup cycle
	}

	for i := 0; i < maxChecks; i++ {
		select {
		case conn := <-dbManager.backendPool:
			lastUsed := conn.LastUsedAt()

			// Check if connection is idle for too long
			if lastUsed.Before(cutoff) && currentTotal > int64(minConnections) {
				logger.Debug("Closing idle connection",
					"last_used", lastUsed,
					"idle_duration", time.Since(lastUsed),
					"backend_addr", conn.RemoteAddr())

				conn.Close()
				atomic.AddInt64(&dbManager.stats.TotalConnections, -1)
				atomic.AddInt64(&dbManager.stats.IdleConnections, -1)
				atomic.AddInt64(&p.stats.IdleConnectionsClosed, 1)
				currentTotal--
				cleaned++
			} else {
				// Connection is not idle or we're at minimum, put it back
				select {
				case dbManager.backendPool <- conn:
					// Successfully returned to pool
				default:
					// Pool is full (shouldn't happen), close the connection
					logger.Warn("Pool full when returning connection, closing")
					conn.Close()
					atomic.AddInt64(&dbManager.stats.TotalConnections, -1)
					atomic.AddInt64(&dbManager.stats.IdleConnections, -1)
					atomic.AddInt64(&p.stats.IdleConnectionsClosed, 1)
					currentTotal--
					cleaned++
				}
			}
		default:
			// No more connections in pool to check
			return cleaned
		}
	}

	if cleaned > 0 {
		logger.Info("Cleaned up idle connections",
			"closed", cleaned,
			"remaining_total", atomic.LoadInt64(&dbManager.stats.TotalConnections),
			"remaining_idle", atomic.LoadInt64(&dbManager.stats.IdleConnections))
	}

	return cleaned
}

// getOrCreateNotificationMonitor gets or creates a notification monitor
func (p *WildcardPooler) getOrCreateNotificationMonitor(dbName, userName string) (*NotificationMonitor, error) {
	key := fmt.Sprintf("%s:%s", dbName, userName)

	p.notificationMonitorsMu.RLock()
	monitor, exists := p.notificationMonitors[key]
	p.notificationMonitorsMu.RUnlock()

	if exists {
		return monitor, nil
	}

	p.notificationMonitorsMu.Lock()
	defer p.notificationMonitorsMu.Unlock()

	// Double-check after acquiring write lock
	if monitor, exists := p.notificationMonitors[key]; exists {
		return monitor, nil
	}

	// Create new monitor
	monitor = NewNotificationMonitor(p.ctx, p, dbName, userName)
	if err := monitor.Start(); err != nil {
		return nil, fmt.Errorf("failed to start notification monitor: %w", err)
	}

	if p.notificationMonitors == nil {
		p.notificationMonitors = make(map[string]*NotificationMonitor)
	}
	p.notificationMonitors[key] = monitor

	p.logger.Info("Created notification monitor", "key", key)

	return monitor, nil
}

func (p *WildcardPooler) cleanupClientFromNotificationMonitors(client *ClientConnection) {
	key := fmt.Sprintf("%s:%s", client.GetDatabase(), client.GetUser())

	p.notificationMonitorsMu.RLock()
	monitor, exists := p.notificationMonitors[key]
	p.notificationMonitorsMu.RUnlock()

	if exists {
		monitor.RemoveClient(client)
	}
}
