package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// WildcardPooler manages dynamic database discovery and pooling
type WildcardPooler struct {
	config           Config
	listener         net.Listener
	staticDatabases  map[string]*DatabaseManager
	dynamicDatabases map[string]*DatabaseManager
	wildcardTargets  []*WildcardTarget
	clients          map[net.Conn]*ClientConnection
	clientsMu        sync.RWMutex
	databasesMu      sync.RWMutex
	listeners        map[string]map[*ClientConnection]bool
	listenersMu      sync.RWMutex
	stats            GlobalStats
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	discoveryCache   map[string]*DatabaseDiscoveryInfo
	discoveryCacheMu sync.RWMutex
	logger           *Logger
}

// DatabaseManager manages connections for a specific database
type DatabaseManager struct {
	config         DatabaseConfig
	wildcardTarget *WildcardTarget
	backendPool    chan *BackendConnection
	healthChecker  *HealthChecker
	stats          DatabaseStats
	lastUsed       time.Time
	isStatic       bool
	mu             sync.RWMutex
}

// DatabaseDiscoveryInfo contains cached database discovery information
type DatabaseDiscoveryInfo struct {
	DatabaseName string
	Exists       bool
	Owner        string
	Size         int64
	LastChecked  time.Time
	Target       *WildcardTarget
}

// GlobalStats contains global pooler statistics
type GlobalStats struct {
	TotalClients        int64
	ActiveClients       int64
	StaticDatabases     int64
	DynamicDatabases    int64
	HealthyDatabases    int64
	TotalQueries        int64
	NotificationsSent   int64
	DatabasesDiscovered int64
	DatabasesCreated    int64
	DatabasesRemoved    int64
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

// NewWildcardPooler creates a new wildcard pooler
func NewWildcardPooler(config Config, logger *Logger) (*WildcardPooler, error) {
	listener, err := net.Listen("tcp", config.Server.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", config.Server.ListenAddr, err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	pooler := &WildcardPooler{
		config:           config,
		listener:         listener,
		staticDatabases:  make(map[string]*DatabaseManager),
		dynamicDatabases: make(map[string]*DatabaseManager),
		clients:          make(map[net.Conn]*ClientConnection),
		listeners:        make(map[string]map[*ClientConnection]bool),
		discoveryCache:   make(map[string]*DatabaseDiscoveryInfo),
		ctx:              ctx,
		cancel:           cancel,
		logger:           logger,
	}

	// Initialize wildcard targets
	for i := range config.WildcardTargets {
		pooler.wildcardTargets = append(pooler.wildcardTargets, &config.WildcardTargets[i])
	}

	// Initialize static databases
	for name, dbConfig := range config.Databases {
		if err := pooler.addStaticDatabase(name, dbConfig); err != nil {
			logger.WithError(err).Warn("Failed to initialize static database", "database", name)
			continue
		}
	}

	// Start database discovery if enabled
	if config.AutoDiscovery.Enabled && !config.AutoDiscovery.CreatePoolsOnDemand {
		pooler.wg.Add(1)
		go pooler.databaseDiscoveryWorker()
	}

	// Start cleanup worker for unused pools
	if config.AutoDiscovery.RemoveUnusedPools {
		pooler.wg.Add(1)
		go pooler.cleanupUnusedPoolsWorker()
	}

	// Start metrics server if enabled
	if config.Metrics.Enabled {
		pooler.wg.Add(1)
		go pooler.startMetricsServer()
	}

	return pooler, nil
}

// Start starts the wildcard pooler
func (p *WildcardPooler) Start(ctx context.Context) error {
	p.logger.Info("PostgreSQL connection pooler starting",
		"listen_addr", p.config.Server.ListenAddr,
		"static_databases", len(p.staticDatabases),
		"wildcard_targets", len(p.wildcardTargets),
		"auto_discovery", p.config.AutoDiscovery.Enabled,
		"on_demand_pools", p.config.AutoDiscovery.CreatePoolsOnDemand)

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Pooler shutting down")
			return p.shutdown()
		default:
			conn, err := p.listener.Accept()
			if err != nil {
				if ctx.Err() != nil {
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
		p.listener.Close()
	}

	// Cancel all background workers
	p.cancel()

	// Close all client connections
	p.clientsMu.Lock()
	for conn := range p.clients {
		conn.Close()
	}
	p.clientsMu.Unlock()

	// Close all database managers
	p.databasesMu.Lock()
	for _, dbManager := range p.staticDatabases {
		p.closeDatabaseManager(dbManager)
	}
	for _, dbManager := range p.dynamicDatabases {
		p.closeDatabaseManager(dbManager)
	}
	p.databasesMu.Unlock()

	// Wait for all goroutines to finish
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	// Wait for shutdown with timeout
	select {
	case <-done:
		p.logger.Info("Graceful shutdown completed")
	case <-time.After(30 * time.Second):
		p.logger.Warn("Shutdown timeout reached, forcing exit")
	}

	return nil
}

// closeDatabaseManager closes a database manager and its connections
func (p *WildcardPooler) closeDatabaseManager(dbManager *DatabaseManager) {
	if dbManager.healthChecker != nil {
		dbManager.healthChecker.Stop()
	}

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

// addStaticDatabase adds a statically configured database
func (p *WildcardPooler) addStaticDatabase(name string, config DatabaseConfig) error {
	config.Name = name
	dbManager := &DatabaseManager{
		config:      config,
		isStatic:    true,
		lastUsed:    time.Now(),
		backendPool: make(chan *BackendConnection, config.MaxConnections),
	}

	if err := dbManager.initializeConnections(); err != nil {
		return fmt.Errorf("failed to initialize connections for %s: %w", name, err)
	}

	if config.HealthCheck.Enabled {
		healthChecker, err := NewHealthChecker(config, p.logger.WithDatabase(name))
		if err != nil {
			p.logger.WithError(err).Warn("Failed to create health checker", "database", name)
		} else {
			dbManager.healthChecker = healthChecker
			go healthChecker.Start(p.ctx)
		}
	}

	p.databasesMu.Lock()
	p.staticDatabases[name] = dbManager
	p.databasesMu.Unlock()

	atomic.AddInt64(&p.stats.StaticDatabases, 1)
	p.logger.Info("Added static database", "database", name, "host", config.Host, "port", config.Port)
	return nil
}

// Stats returns current pooler statistics
func (p *WildcardPooler) Stats() GlobalStats {
	return GlobalStats{
		TotalClients:        atomic.LoadInt64(&p.stats.TotalClients),
		ActiveClients:       atomic.LoadInt64(&p.stats.ActiveClients),
		StaticDatabases:     atomic.LoadInt64(&p.stats.StaticDatabases),
		DynamicDatabases:    atomic.LoadInt64(&p.stats.DynamicDatabases),
		HealthyDatabases:    atomic.LoadInt64(&p.stats.HealthyDatabases),
		TotalQueries:        atomic.LoadInt64(&p.stats.TotalQueries),
		NotificationsSent:   atomic.LoadInt64(&p.stats.NotificationsSent),
		DatabasesDiscovered: atomic.LoadInt64(&p.stats.DatabasesDiscovered),
		DatabasesCreated:    atomic.LoadInt64(&p.stats.DatabasesCreated),
		DatabasesRemoved:    atomic.LoadInt64(&p.stats.DatabasesRemoved),
	}
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

// registerListener registers a client as a listener for a channel
func (p *WildcardPooler) registerListener(channel string, client *ClientConnection) {
	p.listenersMu.Lock()
	defer p.listenersMu.Unlock()

	if p.listeners[channel] == nil {
		p.listeners[channel] = make(map[*ClientConnection]bool)
	}
	p.listeners[channel][client] = true
}

// unregisterListener unregisters a client from a channel
func (p *WildcardPooler) unregisterListener(channel string, client *ClientConnection) {
	p.listenersMu.Lock()
	defer p.listenersMu.Unlock()

	if clients, exists := p.listeners[channel]; exists {
		delete(clients, client)
		if len(clients) == 0 {
			delete(p.listeners, channel)
		}
	}
}

// releaseBackendConnection returns a backend connection to its pool
func (p *WildcardPooler) releaseBackendConnection(conn *BackendConnection) {
	if conn == nil {
		return
	}

	// Find the appropriate database manager
	var dbManager *DatabaseManager

	p.databasesMu.RLock()
	// Check static databases first
	if manager, exists := p.staticDatabases[conn.dbName]; exists {
		dbManager = manager
	} else {
		// Check dynamic databases
		key := fmt.Sprintf("%s:%s", conn.targetName, conn.dbName)
		if manager, exists := p.dynamicDatabases[key]; exists {
			dbManager = manager
		}
	}
	p.databasesMu.RUnlock()

	if dbManager == nil {
		p.logger.Warn("No database manager found for connection, closing",
			"db", conn.dbName, "target", conn.targetName)
		conn.Close()
		return
	}

	conn.mu.Lock()
	isListening := conn.isListening
	conn.inUse = false
	conn.lastUsedAt = time.Now()
	conn.clientRef = nil // Clear client reference
	conn.mu.Unlock()

	if isListening {
		p.logger.Debug("Not returning listening connection to pool, closing",
			"db", conn.dbName)
		conn.Close()
		atomic.AddInt64(&dbManager.stats.TotalConnections, -1)
		return
	}

	// Try to return to pool
	select {
	case dbManager.backendPool <- conn:
		atomic.AddInt64(&dbManager.stats.ActiveConnections, -1)
		atomic.AddInt64(&dbManager.stats.IdleConnections, 1)
		p.logger.Debug("Returned connection to pool", "db", conn.dbName)
	default:
		// Pool is full, close the connection
		p.logger.Debug("Pool full, closing connection", "db", conn.dbName)
		conn.Close()
		atomic.AddInt64(&dbManager.stats.TotalConnections, -1)
	}
}

// startMetricsServer starts the metrics HTTP server
func (p *WildcardPooler) startMetricsServer() {
	defer p.wg.Done()

	addr := fmt.Sprintf(":%d", p.config.Metrics.Port)
	server := NewMetricsServer(p, addr, p.config.Metrics.Path, p.logger)

	p.logger.Info("Starting metrics server", "addr", addr, "path", p.config.Metrics.Path)

	if err := server.Start(p.ctx); err != nil {
		p.logger.WithError(err).Error("Metrics server failed")
	}
}
