package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Server manages dynamic database discovery and pooling.
type Server struct {
	config   Config
	listener net.Listener

	// Unified storage: target_name -> database_name -> username -> pool
	targets   map[string]map[string]map[string]*Pool
	targetsMu sync.RWMutex

	targetConfigs []*Target

	clients     map[net.Conn]*ClientConnection
	clientsMu   sync.RWMutex
	listeners   map[Channel]*Listen
	listenersMu sync.RWMutex
	stats       GlobalStats
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	logger      *Logger

	// targetParams holds ParameterStatus values captured from the privileged
	// connection startup for each target. Populated once during
	// initPrivilegedConnections and read-only after that — no mutex needed.
	targetParams map[string]map[string]string

	// One persistent privileged backend connection per target.
	// Used exclusively for pg_shadow queries during client authentication.
	privilegedConns   map[string]*BackendConnection
	privilegedConnsMu sync.RWMutex

	// privilegedReady[targetName] is closed when the privileged connection for
	// that target is ready. Replaced with a new open channel while reconnecting.
	privilegedReady   map[string]chan struct{}
	privilegedReadyMu sync.RWMutex
}

// Pool manages backend connections for a specific (target, database, user) tuple.
// The pool manager goroutine is the sole creator and owner of backend connections.
// All other code only takes from backendPool and returns via returnCh / closeCh.
type Pool struct {
	config   DatabaseConfig
	target   *Target
	username string

	// backendPool is the queue of idle connections available to borrow.
	// Its capacity is MaxConnections — the hard cap on total open connections.
	backendPool chan *BackendConnection

	// returnCh: consumers return healthy connections here.
	// closeCh:  consumers report dead/failed connections here.
	returnCh chan *BackendConnection
	closeCh  chan *BackendConnection

	// allConns is the authoritative list of every connection owned by this pool,
	// whether idle (in backendPool) or active (checked out by a client or listen
	// monitor). Only the pool manager goroutine reads or writes this slice.
	// len(allConns) is the true total; len(backendPool) is the idle count.
	allConns   []*BackendConnection
	allConnsMu sync.Mutex

	// ready is closed by the pool manager as soon as the first connection
	// is placed in backendPool. Auth goroutines block on this.
	ready chan struct{}

	stats Stats

	maxMessageSize int

	ctx    context.Context
	cancel context.CancelFunc
}

// Stats contains per-pool statistics.
type Stats struct {
	QueriesExecuted int64
	ErrorCount      int64
}

// GlobalStats contains global pooler statistics.
type GlobalStats struct {
	TotalClients          int64
	ActiveClients         int64
	TotalPools            int64
	TotalQueries          int64
	NotificationsSent     int64
	IdleConnectionsClosed int64
}

// DatabaseConfig configures a pool internally.
type DatabaseConfig struct {
	Name           string
	Host           string
	Port           int
	User           string
	MaxConnections int
	ConnectTimeout time.Duration
	Parameters     map[string]string
}

// NewWildcardPooler creates a new wildcard pooler.
func NewWildcardPooler(config Config, logger *Logger) (*Server, error) {
	listener, err := net.Listen("tcp", config.Server.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", config.Server.ListenAddr, err)
	}

	pooler := &Server{
		config:          config,
		listener:        listener,
		targets:         make(map[string]map[string]map[string]*Pool),
		targetConfigs:   make([]*Target, 0, len(config.Targets)),
		clients:         make(map[net.Conn]*ClientConnection),
		listeners:       make(map[Channel]*Listen),
		targetParams:    make(map[string]map[string]string),
		privilegedConns: make(map[string]*BackendConnection),
		privilegedReady: make(map[string]chan struct{}),
		logger:          logger,
	}

	for i := range config.Targets {
		pooler.targetConfigs = append(pooler.targetConfigs, &config.Targets[i])
	}

	return pooler, nil
}

// Start starts the wildcard pooler.
func (p *Server) Start(ctx context.Context) error {
	p.ctx, p.cancel = context.WithCancel(ctx)

	p.logger.Info("PostgreSQL connection pooler starting",
		"listen_addr", p.config.Server.ListenAddr,
		"targets", len(p.targetConfigs))

	if err := p.ensureBootstrapCerts(); err != nil {
		return fmt.Errorf("failed to ensure bootstrap certificates: %w", err)
	}

	if err := p.initPrivilegedConnections(); err != nil {
		return fmt.Errorf("failed to initialize privileged connections: %w", err)
	}

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
			if listener, ok := p.listener.(*net.TCPListener); ok {
				listener.SetDeadline(time.Now().Add(1 * time.Second))
			}

			conn, err := p.listener.Accept()

			if listener, ok := p.listener.(*net.TCPListener); ok {
				listener.SetDeadline(time.Time{})
			}

			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
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

// shutdown gracefully shuts down the pooler.
func (p *Server) shutdown() error {
	p.logger.Info("Starting graceful shutdown")

	if p.listener != nil {
		if err := p.listener.Close(); err != nil {
			p.logger.WithError(err).Warn("Error closing listener")
		}
	}

	p.cancel()

	p.shutdownListeners()

	p.logger.Info("Closing client connections")
	p.clientsMu.Lock()
	for conn := range p.clients {
		conn.Close()
	}
	p.clientsMu.Unlock()

	// Cancel all pool manager goroutines — each drains and closes its own
	// connections on ctx.Done.
	p.logger.Info("Stopping pool managers")
	p.targetsMu.Lock()
	for _, targetMap := range p.targets {
		for _, dbMap := range targetMap {
			for _, pool := range dbMap {
				pool.cancel()
			}
		}
	}
	p.targetsMu.Unlock()

	p.wg.Wait()
	p.logger.Info("Graceful shutdown completed")

	return nil
}

// handleClient handles a client connection.
func (p *Server) handleClient(conn net.Conn) {
	defer p.wg.Done()
	defer func() {
		atomic.AddInt64(&p.stats.ActiveClients, -1)
		conn.Close()
	}()

	clientLogger := p.logger.WithClient(conn.RemoteAddr().String())
	client := NewClientConnection(conn, clientLogger, p.config.Server.MaxMessageSize)

	p.clientsMu.Lock()
	p.clients[conn] = client
	p.clientsMu.Unlock()

	defer func() {
		p.clientsMu.Lock()
		delete(p.clients, conn)
		p.clientsMu.Unlock()

		p.cleanupClientListeners(client)

		backend := client.GetBackendConnection()
		if backend == nil {
			return
		}

		pool := p.getPool(client.GetDatabase(), client.GetUser())

		if client.IsInTransaction() {
			clientLogger.Debug("Closing transaction backend on client disconnect")
			// Best-effort graceful termination.
			_ = backend.WriteMessage('X', []byte{})
			time.Sleep(10 * time.Millisecond)
			if pool != nil {
				pool.closeCh <- backend
			} else {
				backend.Close()
			}
		} else {
			clientLogger.Debug("Releasing pooled backend on client disconnect")
			if pool != nil {
				pool.returnCh <- backend
			} else {
				backend.Close()
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

// Stats returns current pooler statistics.
func (p *Server) Stats() GlobalStats {
	return GlobalStats{
		TotalClients:          atomic.LoadInt64(&p.stats.TotalClients),
		ActiveClients:         atomic.LoadInt64(&p.stats.ActiveClients),
		TotalPools:            atomic.LoadInt64(&p.stats.TotalPools),
		TotalQueries:          atomic.LoadInt64(&p.stats.TotalQueries),
		NotificationsSent:     atomic.LoadInt64(&p.stats.NotificationsSent),
		IdleConnectionsClosed: atomic.LoadInt64(&p.stats.IdleConnectionsClosed),
	}
}

// getPool returns the existing Pool for (dbName, user), or creates a new one
// under the highest-priority target that serves dbName. Returns nil if no
// target serves the database.
func (p *Server) getPool(dbName, user string) *Pool {
	// Fast path: read-only lookup.
	p.targetsMu.RLock()
	for _, t := range p.getSortedTargets() {
		if !p.targetServesDatabase(t, dbName) {
			continue
		}
		if targetMap, ok := p.targets[t.Name]; ok {
			if dbMap, ok := targetMap[dbName]; ok {
				if pool, ok := dbMap[user]; ok {
					p.targetsMu.RUnlock()
					return pool
				}
			}
		}
	}
	p.targetsMu.RUnlock()

	// Pool not found — select the target and create under write lock.
	var target *Target
	for _, t := range p.getSortedTargets() {
		if p.targetServesDatabase(t, dbName) {
			target = t
			break
		}
	}
	if target == nil {
		return nil
	}

	// Slow path: create under write lock.
	p.targetsMu.Lock()
	defer p.targetsMu.Unlock()

	// Re-check under write lock — another goroutine may have created it.
	if targetMap, ok := p.targets[target.Name]; ok {
		if dbMap, ok := targetMap[dbName]; ok {
			if pool, ok := dbMap[user]; ok {
				return pool
			}
		}
	}

	if p.targets[target.Name] == nil {
		p.targets[target.Name] = make(map[string]map[string]*Pool)
	}
	if p.targets[target.Name][dbName] == nil {
		p.targets[target.Name][dbName] = make(map[string]*Pool)
	}

	config := DatabaseConfig{
		Name:           dbName,
		Host:           target.Host,
		Port:           target.Port,
		User:           user,
		MaxConnections: target.MaxConnections,
		ConnectTimeout: target.ConnectTimeout,
		Parameters:     target.Parameters,
	}

	ctx, cancel := context.WithCancel(p.ctx)

	pool := &Pool{
		config:         config,
		target:         target,
		username:       user,
		backendPool:    make(chan *BackendConnection, config.MaxConnections),
		returnCh:       make(chan *BackendConnection, config.MaxConnections),
		closeCh:        make(chan *BackendConnection, config.MaxConnections),
		allConns:       make([]*BackendConnection, 0, config.MaxConnections),
		ready:          make(chan struct{}),
		maxMessageSize: p.config.Server.MaxMessageSize,
		ctx:            ctx,
		cancel:         cancel,
	}

	p.targets[target.Name][dbName][user] = pool
	atomic.AddInt64(&p.stats.TotalPools, 1)

	p.wg.Add(1)
	go pool.run(p)

	return pool
}

// run is the pool manager goroutine — the sole creator of backend connections.
// The pool starts empty. The growth ticker opens connections toward MaxConnections
// and closes pool.ready as soon as the first one is in backendPool.
func (pool *Pool) run(p *Server) {
	defer p.wg.Done()

	logger := p.logger.
		WithField("component", "pool").
		WithField("target", pool.target.Name).
		WithField("database", pool.config.Name).
		WithField("user", pool.username)

	logger.Info("Pool manager started", "max_connections", pool.config.MaxConnections)

	readyClosed := false

	growthTicker := time.NewTicker(250 * time.Millisecond)
	defer growthTicker.Stop()

	healthTicker := time.NewTicker(30 * time.Second)
	defer healthTicker.Stop()

	for {
		select {
		case <-pool.ctx.Done():
			logger.Info("Pool manager stopping, draining connections")
			pool.drain(logger)
			return

		case conn := <-pool.returnCh:
			pool.handleReturn(p, conn, logger)

		case conn := <-pool.closeCh:
			pool.handleClose(p, conn, logger)

		case <-growthTicker.C:
			pool.allConnsMu.Lock()
			total := len(pool.allConns)
			pool.allConnsMu.Unlock()

			if total < pool.config.MaxConnections {
				pool.openOne(p, logger, "growth")

				// Close ready after the first connection is successfully placed.
				if !readyClosed && len(pool.backendPool) > 0 {
					close(pool.ready)
					readyClosed = true
					logger.Info("Pool ready",
						"total", pool.totalConnections(),
						"max", pool.config.MaxConnections)
				}
			}

		case <-healthTicker.C:
			pool.healthCheck(p, logger)
		}
	}
}

// removeFromAllConns removes a connection from allConns.
// Must be called with allConnsMu held.
func (pool *Pool) removeFromAllConns(conn *BackendConnection) {
	for i, c := range pool.allConns {
		if c == conn {
			pool.allConns[i] = pool.allConns[len(pool.allConns)-1]
			pool.allConns = pool.allConns[:len(pool.allConns)-1]
			return
		}
	}
}

// handleReturn validates a returned connection and puts it back in backendPool,
// or closes it and removes it from allConns if dead.
func (pool *Pool) handleReturn(p *Server, conn *BackendConnection, logger *Logger) {
	if !conn.IsAlive() {
		logger.Warn("Returned connection is dead, closing")
		conn.conn.Close()
		pool.allConnsMu.Lock()
		pool.removeFromAllConns(conn)
		pool.allConnsMu.Unlock()
		return
	}

	conn.mu.Lock()
	conn.inUse = false
	conn.lastUsedAt = time.Now()
	conn.clientRef = nil
	conn.mu.Unlock()

	select {
	case pool.backendPool <- conn:
		// Returned to idle queue successfully.
	default:
		// backendPool channel is full — this should not happen since allConns
		// is capped at MaxConnections and backendPool has the same capacity,
		// but guard defensively.
		logger.Warn("backendPool full on return — closing extra connection",
			"total", pool.totalConnections(),
			"max", pool.config.MaxConnections)
		conn.conn.Close()
		pool.allConnsMu.Lock()
		pool.removeFromAllConns(conn)
		pool.allConnsMu.Unlock()
	}
}

// handleClose closes a dead/failed connection, removes it from allConns,
// and opens a replacement if we are still below MaxConnections.
func (pool *Pool) handleClose(p *Server, conn *BackendConnection, logger *Logger) {
	conn.conn.Close()

	pool.allConnsMu.Lock()
	pool.removeFromAllConns(conn)
	total := len(pool.allConns)
	pool.allConnsMu.Unlock()

	logger.Debug("Closed failed connection",
		"remaining", total,
		"max", pool.config.MaxConnections)

	// Open a replacement only if we are still below the cap.
	if total < pool.config.MaxConnections {
		pool.openOne(p, logger, "replacement")
	}
}

// openOne opens a single new backend connection, adds it to allConns, and
// places it in backendPool. reason is used only for logging.
// Must only be called from the pool manager goroutine.
func (pool *Pool) openOne(p *Server, logger *Logger, reason string) {
	conn, err := pool.newConn(p)
	if err != nil {
		logger.WithError(err).Warn("Failed to open backend connection", "reason", reason)
		return
	}

	pool.allConnsMu.Lock()
	// Double-check cap under lock — the ticker may have already filled the slot.
	if len(pool.allConns) >= pool.config.MaxConnections {
		pool.allConnsMu.Unlock()
		conn.conn.Close()
		return
	}
	pool.allConns = append(pool.allConns, conn)
	pool.allConnsMu.Unlock()

	select {
	case pool.backendPool <- conn:
		logger.Debug("Opened backend connection",
			"reason", reason,
			"total", pool.totalConnections(),
			"idle", pool.idleConnections())
	default:
		// backendPool full — shouldn't happen since we checked the cap, but guard.
		conn.conn.Close()
		pool.allConnsMu.Lock()
		pool.removeFromAllConns(conn)
		pool.allConnsMu.Unlock()
	}
}

// healthCheck runs on the ticker. It:
//  1. Scans idle connections in backendPool, closing dead or timed-out ones.
//  2. Grows the pool toward MaxConnections if there is headroom, one connection
//     at a time per tick — natural demand-driven growth without a burst.
func (pool *Pool) healthCheck(p *Server, logger *Logger) {
	idleTimeout := p.config.Server.IdleTimeout
	cutoff := time.Now().Add(-idleTimeout)
	closed := 0

	// Scan idle connections — read exactly as many as were in the channel at
	// the start of the tick so we don't loop forever.
	poolLen := len(pool.backendPool)
	for i := 0; i < poolLen; i++ {
		select {
		case conn := <-pool.backendPool:
			dead := !conn.IsAlive()
			tooOld := idleTimeout > 0 && conn.LastUsedAt().Before(cutoff)

			if dead || tooOld {
				reason := "dead"
				if tooOld {
					reason = "idle timeout"
				}
				logger.Debug("Closing connection during health check", "reason", reason)
				conn.conn.Close()

				pool.allConnsMu.Lock()
				pool.removeFromAllConns(conn)
				pool.allConnsMu.Unlock()

				atomic.AddInt64(&p.stats.IdleConnectionsClosed, 1)
				closed++
			} else {
				// Healthy — put it back.
				select {
				case pool.backendPool <- conn:
				default:
					// Pool channel full — close the extra (shouldn't happen).
					conn.conn.Close()
					pool.allConnsMu.Lock()
					pool.removeFromAllConns(conn)
					pool.allConnsMu.Unlock()
				}
			}
		default:
			break
		}
	}

	if closed > 0 {
		logger.Info("Health check closed connections",
			"closed", closed,
			"remaining", pool.totalConnections())
	}
}

// drain closes all connections owned by this pool. Called on shutdown.
func (pool *Pool) drain(logger *Logger) {
	// Drain returnCh and closeCh first so in-flight connections are accounted for.
	for {
		select {
		case conn := <-pool.returnCh:
			conn.conn.Close()
		case conn := <-pool.closeCh:
			conn.conn.Close()
		default:
			goto drainPool
		}
	}

drainPool:
	for {
		select {
		case conn := <-pool.backendPool:
			conn.conn.Close()
		default:
			pool.allConnsMu.Lock()
			remaining := len(pool.allConns)
			pool.allConns = pool.allConns[:0]
			pool.allConnsMu.Unlock()
			logger.Info("Pool drained", "closed", remaining)
			return
		}
	}
}

// newConn opens a fresh backend connection for this pool using certificate auth.
func (pool *Pool) newConn(p *Server) (*BackendConnection, error) {
	cert, err := p.loadOrGenerateUserCert(pool.username)
	if err != nil {
		return nil, fmt.Errorf("failed to get cert for %s: %w", pool.username, err)
	}
	return p.createCertBackendConnection(pool.target, pool.config.Name, pool.username, cert)
}

// --- Stats helpers ---

// totalConnections returns the count of all connections owned by this pool
// (idle + active). Uses allConns as the authoritative source.
func (pool *Pool) totalConnections() int {
	pool.allConnsMu.Lock()
	defer pool.allConnsMu.Unlock()
	return len(pool.allConns)
}

// idleConnections returns connections currently sitting in backendPool.
func (pool *Pool) idleConnections() int {
	return len(pool.backendPool)
}

// activeConnections returns connections currently checked out from the pool.
func (pool *Pool) activeConnections() int {
	total := pool.totalConnections()
	idle := pool.idleConnections()
	if idle > total {
		return 0
	}
	return total - idle
}

// queriesExecuted returns the query counter.
func (pool *Pool) queriesExecuted() int64 {
	return atomic.LoadInt64(&pool.stats.QueriesExecuted)
}

// errorCount returns the error counter.
func (pool *Pool) errorCount() int64 {
	return atomic.LoadInt64(&pool.stats.ErrorCount)
}

// --- Server helpers ---

// getSortedTargets returns targets sorted by priority (lower = higher priority).
func (p *Server) getSortedTargets() []*Target {
	sorted := make([]*Target, len(p.targetConfigs))
	copy(sorted, p.targetConfigs)

	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Priority == sorted[j].Priority {
			return sorted[i].Name < sorted[j].Name
		}
		return sorted[i].Priority < sorted[j].Priority
	})

	return sorted
}

// targetServesDatabase checks if a target serves a specific database.
func (p *Server) targetServesDatabase(target *Target, dbName string) bool {
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

	for _, excluded := range target.ExcludeDatabases {
		if excluded == dbName {
			return false
		}
	}

	return true
}

// startMetricsServer starts the metrics/web server.
func (p *Server) startMetricsServer(ctx context.Context) {
	defer p.wg.Done()

	addr := fmt.Sprintf(":%d", p.config.Metrics.Port)
	server := NewWebServer(p, addr, p.logger)

	p.logger.Info("Starting web server", "addr", addr)

	if err := server.Start(ctx); err != nil {
		p.logger.WithError(err).Error("Web server failed")
	}
}

// isSSLSupported checks whether the pgfox client-facing TLS cert exists.
func (p *Server) isSSLSupported() bool {
	_, err := os.Stat(p.pgfoxTLSCertPath())
	return err == nil
}

// upgradeToTLS upgrades a client connection to TLS.
func (p *Server) upgradeToTLS(client *ClientConnection) error {
	cert, err := tls.LoadX509KeyPair(p.pgfoxTLSCertPath(), p.pgfoxTLSKeyPath())
	if err != nil {
		return fmt.Errorf("failed to load pgfox TLS cert %s: %w", p.pgfoxTLSCertPath(), err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	tlsConn := tls.Server(client.conn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return fmt.Errorf("TLS handshake failed: %w", err)
	}

	p.logger.Debug("TLS connection established", "client", client.RemoteAddr())

	client.mu.Lock()
	client.conn = tlsConn
	client.reader = bufio.NewReader(tlsConn)
	client.writer = bufio.NewWriter(tlsConn)
	client.mu.Unlock()

	return nil
}

// findBackendByKey finds a backend connection matching processID and secretKey.
// Searches idle pool connections then active client-pinned connections.
func (p *Server) findBackendByKey(processID, secretKey int32) (*Target, *BackendConnection) {
	p.targetsMu.RLock()
	for _, targetMap := range p.targets {
		for _, dbMap := range targetMap {
			for _, pool := range dbMap {
				poolLen := len(pool.backendPool)
				for i := 0; i < poolLen; i++ {
					select {
					case conn := <-pool.backendPool:
						match := conn.GetProcessID() == processID &&
							conn.GetSecretKey() == secretKey
						select {
						case pool.backendPool <- conn:
						default:
							conn.conn.Close()
							pool.allConnsMu.Lock()
							pool.removeFromAllConns(conn)
							pool.allConnsMu.Unlock()
						}
						if match {
							p.targetsMu.RUnlock()
							return pool.target, conn
						}
					default:
					}
				}
			}
		}
	}
	p.targetsMu.RUnlock()

	p.clientsMu.RLock()
	defer p.clientsMu.RUnlock()

	for _, client := range p.clients {
		backend := client.GetBackendConnection()
		if backend == nil {
			continue
		}
		if backend.GetProcessID() == processID && backend.GetSecretKey() == secretKey {
			for _, target := range p.targetConfigs {
				if target.Name == backend.targetName {
					return target, backend
				}
			}
		}
	}

	return nil, nil
}
