package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Target represents a PostgreSQL server. It holds both config (from yaml) and
// all runtime state: the privileged connection, pools, and connection budget.
// The target goroutine is the sole creator and manager of all backend connections
// for this target.
type Target struct {
	Name             string            `yaml:"name"`
	Host             string            `yaml:"host"`
	Port             int               `yaml:"port"`
	MaxConnections   int               `yaml:"max_connections"`
	ConnectTimeout   time.Duration     `yaml:"connect_timeout"`
	Parameters       map[string]string `yaml:"parameters"`
	IncludeDatabases []string          `yaml:"include_databases"`
	ExcludeDatabases []string          `yaml:"exclude_databases"`
	Priority         int               `yaml:"priority"`

	// --- Runtime: privileged connection ---
	// conn is the pgfox_role backend connection used exclusively for
	// pg_shadow queries during client authentication.
	conn   *BackendConnection
	ready  chan struct{}     // closed when conn is ready for the first time
	params map[string]string // ParameterStatus values from conn startup

	// --- Runtime: pools ---
	pools   map[string]map[string]*Pool // database -> user -> pool
	poolsMu sync.RWMutex

	// --- Runtime: connection budget ---
	// totalOpen is the count of all open backend connections on this target
	// across all pools plus the privileged conn. Only the target goroutine
	// writes this — no atomic needed.
	totalOpen int

	// serverMaxConns and serverOpenConns are updated by the stats ticker
	// from pg_stat_activity. They reflect the real PostgreSQL server state
	// regardless of other clients not using pgfox.
	serverMaxConns  int // pg max_connections setting
	serverOpenConns int // total connections currently open on the server

	// listenOpen is the count of active dedicated listen connections on this
	// target. Written atomically from client goroutines.
	listenOpen int32

	// returnCh and closeCh are target-level. conn.pool identifies which pool
	// the connection belongs to. The target goroutine is the sole reader.
	returnCh chan *BackendConnection
	closeCh  chan *BackendConnection

	// connReady is signaled (non-blocking send) whenever a connection is
	// returned to any pool, waking borrowConn waiters.
	connReady chan struct{}

	// --- Runtime: lifecycle ---
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Pool manages the idle connection queue and stats for a (database, user) pair.
// It is a plain data struct — the target goroutine owns all connection lifecycle.
type Pool struct {
	config   DatabaseConfig
	target   *Target
	username string

	// backendPool is the queue of idle connections available to borrow.
	backendPool chan *BackendConnection

	// allConns is the list of every connection owned by this pool (idle or
	// active). Written only by the target goroutine — no mutex needed.
	allConns []*BackendConnection

	// Peak tracking for smart shrink decisions.
	peakSamples []peakSample

	stats Stats
}

// peakSample records active connection count at a point in time.
type peakSample struct {
	active int
	at     time.Time
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

// Server is the top-level pooler. targets is an ordered slice (by priority).
type Server struct {
	config   Config
	listener net.Listener
	targets  []*Target

	clients     map[net.Conn]*ClientConnection
	clientsMu   sync.RWMutex
	listeners   map[Channel]*Listen
	listenersMu sync.RWMutex

	stats  GlobalStats
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *Logger
}

// NewWildcardPooler creates a new wildcard pooler.
func NewWildcardPooler(config Config, logger *Logger) (*Server, error) {
	listener, err := net.Listen("tcp", config.Server.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", config.Server.ListenAddr, err)
	}

	targets := make([]*Target, 0, len(config.Targets))
	for i := range config.Targets {
		src := &config.Targets[i]
		t := &Target{
			Name:             src.Name,
			Host:             src.Host,
			Port:             src.Port,
			MaxConnections:   src.MaxConnections,
			ConnectTimeout:   src.ConnectTimeout,
			Parameters:       src.Parameters,
			IncludeDatabases: src.IncludeDatabases,
			ExcludeDatabases: src.ExcludeDatabases,
			Priority:         src.Priority,
			pools:            make(map[string]map[string]*Pool),
			ready:            make(chan struct{}),
			params:           make(map[string]string),
			returnCh:         make(chan *BackendConnection, src.MaxConnections),
			closeCh:          make(chan *BackendConnection, src.MaxConnections),
			connReady:        make(chan struct{}, 1),
		}
		targets = append(targets, t)
	}

	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Priority == targets[j].Priority {
			return targets[i].Name < targets[j].Name
		}
		return targets[i].Priority < targets[j].Priority
	})

	return &Server{
		config:    config,
		listener:  listener,
		targets:   targets,
		clients:   make(map[net.Conn]*ClientConnection),
		listeners: make(map[Channel]*Listen),
		logger:    logger,
	}, nil
}

// Start starts the wildcard pooler.
func (p *Server) Start(ctx context.Context) error {
	p.ctx, p.cancel = context.WithCancel(ctx)

	p.logger.Info("PostgreSQL connection pooler starting",
		"listen_addr", p.config.Server.ListenAddr,
		"targets", len(p.targets))

	if err := p.ensureBootstrapCerts(); err != nil {
		return fmt.Errorf("failed to ensure bootstrap certificates: %w", err)
	}

	// Start one goroutine per target — it opens the privileged connection
	// and manages all pool connections for that target.
	for _, target := range p.targets {
		tctx, tcancel := context.WithCancel(p.ctx)
		target.ctx = tctx
		target.cancel = tcancel

		p.wg.Add(1)
		go func(t *Target) {
			defer p.wg.Done()
			t.run(p)
		}(target)

		// Wait for privileged connection before accepting clients.
		select {
		case <-target.ready:
			p.logger.Info("Target ready", "target", target.Name)
		case <-time.After(target.ConnectTimeout):
			return fmt.Errorf("timed out waiting for target %s to become ready", target.Name)
		case <-p.ctx.Done():
			return fmt.Errorf("server shutting down during target init")
		}
	}

	if p.config.Metrics.Enabled {
		p.wg.Add(1)
		go p.startMetricsServer(p.ctx)
	}

	for {
		select {
		case <-p.ctx.Done():
			p.logger.Info("Accept loop stopping")
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
		p.listener.Close()
	}

	p.cancel()
	p.shutdownListeners()

	p.clientsMu.Lock()
	for conn := range p.clients {
		conn.Close()
	}
	p.clientsMu.Unlock()

	// Cancel all target goroutines — each drains and closes its own connections.
	for _, target := range p.targets {
		target.cancel()
		target.wg.Wait()
	}

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

		duration := time.Since(client.GetConnectedAt())
		clientLogger.Info("Client disconnected",
			"duration", duration.Round(time.Millisecond),
			"active_clients", atomic.LoadInt64(&p.stats.ActiveClients)-1)

		p.cleanupClientListeners(client)

		backend := client.GetBackendConnection()
		if backend == nil {
			return
		}

		if client.IsInTransaction() {
			clientLogger.Debug("Closing transaction backend on disconnect")
			_ = backend.WriteMessage('X', []byte{})
			time.Sleep(10 * time.Millisecond)
			backend.Release()
		} else {
			clientLogger.Debug("Releasing backend on disconnect")
			backend.pool.target.returnCh <- backend
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
				if isClientGone(err) {
					return
				}
				clientLogger.WithError(err).Error("Client message error")
				return
			}
		}
	}
}

// --- Target goroutine ---

// run is the target manager goroutine. It:
//  1. Opens and maintains the privileged connection (conn).
//  2. Manages all pool connections: growth, shrink, recycling, health checks.
//  3. Periodically queries pg_stat_activity to track real server capacity.
func (t *Target) run(p *Server) {
	logger := p.logger.
		WithField("component", "target").
		WithField("target", t.Name)

	// Open privileged connection first — blocks until ready or ctx cancelled.
	if err := t.openPrivilegedConn(p, logger); err != nil {
		logger.WithError(err).Error("Failed to open privileged connection")
		return
	}

	growthTicker := time.NewTicker(50 * time.Millisecond)
	defer growthTicker.Stop()

	healthTicker := time.NewTicker(30 * time.Second)
	defer healthTicker.Stop()

	statsTicker := time.NewTicker(p.config.Server.StatsInterval)
	defer statsTicker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			logger.Info("Target manager stopping")
			t.drain(p, logger)
			return

		case conn := <-t.returnCh:
			t.handleReturn(p, conn, logger)

		case conn := <-t.closeCh:
			t.handleClose(p, conn, logger)

		case <-growthTicker.C:
			t.growthCycle(p, logger)

		case <-healthTicker.C:
			t.healthCheck(p, logger)

		case <-statsTicker.C:
			t.updateServerStats(p, logger)
		}
	}
}

// openPrivilegedConn opens t.conn and populates t.params, then closes t.ready.
// Retries until successful or ctx is cancelled.
func (t *Target) openPrivilegedConn(p *Server, logger *Logger) error {
	pgfoxCert, err := p.loadOrGenerateUserCert(p.config.Server.PgFoxRole)
	if err != nil {
		return fmt.Errorf("failed to load pgfox cert: %w", err)
	}

	for {
		if t.ctx.Err() != nil {
			return fmt.Errorf("context cancelled")
		}

		conn, err := p.createCertBackendConnection(t, "postgres", p.config.Server.PgFoxRole, pgfoxCert)
		if err != nil {
			logger.WithError(err).Warn("Privileged connection failed, retrying in 5s")
			select {
			case <-t.ctx.Done():
				return fmt.Errorf("context cancelled")
			case <-time.After(5 * time.Second):
			}
			continue
		}

		t.conn = conn
		t.totalOpen++

		for k, v := range conn.parameters {
			t.params[k] = v
		}

		close(t.ready)
		logger.Info("Privileged connection ready", "role", p.config.Server.PgFoxRole)
		return nil
	}
}

// handleReturn validates a returned connection and puts it back in its pool's
// backendPool, or closes it if dead.
func (t *Target) handleReturn(p *Server, conn *BackendConnection, logger *Logger) {
	pool := conn.pool

	if !conn.IsAlive() {
		logger.Warn("Returned connection is dead, closing")
		conn.conn.Close()
		t.totalOpen--
		pool.removeFromAllConns(conn)
		t.signalConnReady()
		return
	}

	conn.mu.Lock()
	conn.inUse = false
	conn.lastUsedAt = time.Now()
	conn.client = nil
	conn.mu.Unlock()

	select {
	case pool.backendPool <- conn:
		t.signalConnReady()
	default:
		// backendPool full — shouldn't happen, close defensively.
		logger.Warn("backendPool full on return, closing extra connection")
		conn.conn.Close()
		t.totalOpen--
		pool.removeFromAllConns(conn)
	}
}

// handleClose closes a dead connection, removes it from its pool, and signals
// connReady so waiting borrowers can react (e.g. trigger growth).
func (t *Target) handleClose(p *Server, conn *BackendConnection, logger *Logger) {
	pool := conn.pool
	conn.conn.Close()
	t.totalOpen--
	pool.removeFromAllConns(conn)
	logger.Debug("Closed failed connection",
		"database", pool.config.Name,
		"user", pool.username,
		"target_total", t.totalOpen)
	t.signalConnReady()
}

// growthCycle runs on every growthTicker. Opens one connection per contended
// pool per tick, recycling slots from idle pools before using fresh server slots.
// Shrinks only when nothing is contended.
func (t *Target) growthCycle(p *Server, logger *Logger) {
	now := time.Now()
	peakWindow := p.config.Server.PeakWindow

	t.poolsMu.RLock()
	pools := t.allPools()
	t.poolsMu.RUnlock()

	serverAvailable := 0
	if t.serverMaxConns > 0 {
		serverAvailable = t.serverMaxConns - t.serverOpenConns
	} else {
		serverAvailable = t.MaxConnections - t.totalOpen
	}

	type poolState struct {
		pool      *Pool
		active    int
		idle      int
		total     int
		hwm       int
		contended bool
		excess    int
	}

	states := make([]poolState, 0, len(pools))

	for _, pool := range pools {
		active := pool.activeConnections()
		pool.peakSamples = append(pool.peakSamples, peakSample{active: active, at: now})

		cutoff := now.Add(-peakWindow)
		i := 0
		for i < len(pool.peakSamples) && pool.peakSamples[i].at.Before(cutoff) {
			i++
		}
		pool.peakSamples = pool.peakSamples[i:]

		hwm := 1
		for _, s := range pool.peakSamples {
			if s.active > hwm {
				hwm = s.active
			}
		}

		idle := pool.idleConnections()
		total := len(pool.allConns)
		contended := total == 0 || active == total
		excess := idle - (hwm + 1)
		if excess < 0 {
			excess = 0
		}

		states = append(states, poolState{pool, active, idle, total, hwm, contended, excess})
	}

	var contendedPools []poolState
	var idlePools []poolState
	for _, s := range states {
		if s.contended {
			contendedPools = append(contendedPools, s)
		}
		if s.excess > 0 {
			idlePools = append(idlePools, s)
		}
	}

	// Open one connection per contended pool — recycling idle slots first.
	idleIdx := 0
	for _, cs := range contendedPools {
		if t.totalOpen >= t.MaxConnections || serverAvailable <= 1 {
			break
		}

		recycled := false
		for idleIdx < len(idlePools) {
			is := idlePools[idleIdx]
			if is.pool == cs.pool {
				idleIdx++
				continue
			}
			if t.closeOneIdle(p, is.pool, logger) {
				logger.Debug("Recycling slot",
					"from", is.pool.config.Name+"/"+is.pool.username,
					"to", cs.pool.config.Name+"/"+cs.pool.username)
				idlePools[idleIdx].excess--
				if idlePools[idleIdx].excess == 0 {
					idleIdx++
				}
				recycled = true
			}
			break
		}

		if recycled || (t.totalOpen < t.MaxConnections && serverAvailable > 1) {
			reason := "growth"
			if recycled {
				reason = "recycled"
			}
			t.openOne(p, cs.pool, logger, reason)
		}
	}

	// Shrink only when nothing is contended.
	if len(contendedPools) == 0 {
		for _, is := range idlePools {
			t.closeOneIdle(p, is.pool, logger)
		}
	}
}

// healthCheck runs on the healthTicker. Validates idle connections and replaces
// dead ones. Also checks and replaces the privileged connection if needed.
func (t *Target) healthCheck(p *Server, logger *Logger) {
	// Check privileged connection.
	if t.conn != nil && !t.conn.IsAlive() {
		logger.Warn("Privileged connection dead, replacing")
		t.conn.conn.Close()
		t.totalOpen--
		t.conn = nil

		pgfoxCert, err := p.loadOrGenerateUserCert(p.config.Server.PgFoxRole)
		if err == nil {
			if conn, err := p.createCertBackendConnection(t, "postgres", p.config.Server.PgFoxRole, pgfoxCert); err == nil {
				t.conn = conn
				t.totalOpen++
				logger.Info("Privileged connection replaced")
			} else {
				logger.WithError(err).Warn("Failed to replace privileged connection")
			}
		}
	}

	idleTimeout := p.config.Server.IdleTimeout
	cutoff := time.Now().Add(-idleTimeout)

	t.poolsMu.RLock()
	pools := t.allPools()
	t.poolsMu.RUnlock()

	for _, pool := range pools {
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
					logger.Debug("Health check closing connection",
						"reason", reason,
						"database", pool.config.Name,
						"user", pool.username)
					conn.conn.Close()
					t.totalOpen--
					pool.removeFromAllConns(conn)
					atomic.AddInt64(&p.stats.IdleConnectionsClosed, 1)
				} else {
					select {
					case pool.backendPool <- conn:
					default:
						conn.conn.Close()
						t.totalOpen--
						pool.removeFromAllConns(conn)
					}
				}
			default:
			}
		}
	}
}

// updateServerStats queries pg_stat_activity on the privileged connection to
// get the real PostgreSQL server capacity regardless of non-pgfox clients.
func (t *Target) updateServerStats(p *Server, logger *Logger) {
	if t.conn == nil {
		return
	}

	query := "SELECT current_setting('max_connections')::int, count(*) FROM pg_stat_activity\x00"
	if err := t.conn.WriteMessage('Q', []byte(query)); err != nil {
		logger.WithError(err).Warn("Failed to send stats query")
		return
	}

	if err := t.conn.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return
	}
	defer t.conn.conn.SetReadDeadline(time.Time{})

	var maxConns, openConns int

	for {
		msgType, body, err := t.conn.ReadMessage()
		if err != nil {
			logger.WithError(err).Warn("Failed to read stats response")
			return
		}
		switch msgType {
		case 'T': // RowDescription — skip
		case 'D': // DataRow
			if len(body) < 2 {
				continue
			}
			colCount := int(body[0])<<8 | int(body[1])
			if colCount < 2 {
				continue
			}
			pos := 2
			// col 0: max_connections
			if pos+4 > len(body) {
				continue
			}
			col0Len := int(int32(body[pos])<<24 | int32(body[pos+1])<<16 | int32(body[pos+2])<<8 | int32(body[pos+3]))
			pos += 4
			if col0Len > 0 && pos+col0Len <= len(body) {
				fmt.Sscanf(string(body[pos:pos+col0Len]), "%d", &maxConns)
				pos += col0Len
			}
			// col 1: count(*)
			if pos+4 > len(body) {
				continue
			}
			col1Len := int(int32(body[pos])<<24 | int32(body[pos+1])<<16 | int32(body[pos+2])<<8 | int32(body[pos+3]))
			pos += 4
			if col1Len > 0 && pos+col1Len <= len(body) {
				fmt.Sscanf(string(body[pos:pos+col1Len]), "%d", &openConns)
			}
		case 'C': // CommandComplete
		case 'Z': // ReadyForQuery
			if maxConns > 0 {
				t.serverMaxConns = maxConns
				t.serverOpenConns = openConns
				logger.Debug("Server stats updated",
					"max_connections", maxConns,
					"open_connections", openConns,
					"available", maxConns-openConns)
			}
			return
		case 'E':
			logger.Warn("Stats query error", "error", parseErrorMessage(body))
			return
		}
	}
}

// drain closes all connections on shutdown.
func (t *Target) drain(p *Server, logger *Logger) {
	// Drain in-flight returns and closes first.
	for {
		select {
		case conn := <-t.returnCh:
			conn.conn.Close()
			t.totalOpen--
		case conn := <-t.closeCh:
			conn.conn.Close()
			t.totalOpen--
		default:
			goto drainPools
		}
	}

drainPools:
	t.poolsMu.RLock()
	pools := t.allPools()
	t.poolsMu.RUnlock()

	for _, pool := range pools {
		for {
			select {
			case conn := <-pool.backendPool:
				conn.conn.Close()
				t.totalOpen--
			default:
				goto nextPool
			}
		}
	nextPool:
	}

	if t.conn != nil {
		t.conn.conn.Close()
		t.totalOpen--
		t.conn = nil
	}

	logger.Info("Target drained", "remaining_open", t.totalOpen)
}

// openOne opens a single new backend connection for pool and adds it to the
// pool. Must only be called from the target goroutine.
func (t *Target) openOne(p *Server, pool *Pool, logger *Logger, reason string) {
	conn, err := pool.newConn(p)
	if err != nil {
		logger.WithError(err).Warn("Failed to open backend connection",
			"reason", reason,
			"database", pool.config.Name,
			"user", pool.username)
		return
	}

	conn.pool = pool
	pool.allConns = append(pool.allConns, conn)
	t.totalOpen++

	select {
	case pool.backendPool <- conn:
		t.signalConnReady()
		logger.Debug("Opened connection",
			"reason", reason,
			"database", pool.config.Name,
			"user", pool.username,
			"pool_total", len(pool.allConns),
			"target_total", t.totalOpen)
	default:
		// backendPool full — shouldn't happen but guard.
		conn.conn.Close()
		t.totalOpen--
		pool.allConns = pool.allConns[:len(pool.allConns)-1]
	}
}

// closeOneIdle closes one idle connection from pool and removes it.
// Returns true if a connection was closed.
func (t *Target) closeOneIdle(p *Server, pool *Pool, logger *Logger) bool {
	select {
	case conn := <-pool.backendPool:
		conn.conn.Close()
		t.totalOpen--
		pool.removeFromAllConns(conn)
		atomic.AddInt64(&p.stats.IdleConnectionsClosed, 1)
		logger.Debug("Shrunk pool",
			"database", pool.config.Name,
			"user", pool.username,
			"pool_total", len(pool.allConns),
			"target_total", t.totalOpen)
		return true
	default:
		return false
	}
}

// signalConnReady does a non-blocking send on connReady to wake borrowConn waiters.
func (t *Target) signalConnReady() {
	select {
	case t.connReady <- struct{}{}:
	default:
	}
}

// allPools returns a flat slice of all pools on this target.
// Caller must hold poolsMu at least RLock.
func (t *Target) allPools() []*Pool {
	var pools []*Pool
	for _, dbMap := range t.pools {
		for _, pool := range dbMap {
			pools = append(pools, pool)
		}
	}
	return pools
}

// --- Pool helpers ---

// removeFromAllConns removes conn from pool.allConns.
// Must be called from the target goroutine.
func (pool *Pool) removeFromAllConns(conn *BackendConnection) {
	for i, c := range pool.allConns {
		if c == conn {
			pool.allConns[i] = pool.allConns[len(pool.allConns)-1]
			pool.allConns = pool.allConns[:len(pool.allConns)-1]
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

// borrowConn takes a connection from the pool, blocking until one is available,
// the timeout fires, or ctx is cancelled. Never creates connections — that is
// the target goroutine's exclusive responsibility.
func (pool *Pool) borrowConn(ctx context.Context) (*BackendConnection, error) {
	timeout := pool.target.ConnectTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	deadline := time.Now().Add(timeout)

	for {
		select {
		case conn := <-pool.backendPool:
			conn.SetInUse(true)
			return conn, nil
		default:
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, fmt.Errorf("timed out waiting for connection for %s/%s",
				pool.config.Name, pool.username)
		}

		select {
		case conn := <-pool.backendPool:
			conn.SetInUse(true)
			return conn, nil
		case <-pool.target.connReady:
			// A connection was returned somewhere on this target — retry.
			continue
		case <-time.After(remaining):
			return nil, fmt.Errorf("timed out waiting for connection for %s/%s",
				pool.config.Name, pool.username)
		case <-ctx.Done():
			return nil, fmt.Errorf("server shutting down")
		}
	}
}

func (pool *Pool) idleConnections() int {
	return len(pool.backendPool)
}

// activeConnections returns connections currently checked out.
func (pool *Pool) activeConnections() int {
	total := len(pool.allConns)
	idle := len(pool.backendPool)
	if idle > total {
		return 0
	}
	return total - idle
}

// totalConnections returns all connections owned by this pool.
func (pool *Pool) totalConnections() int {
	return len(pool.allConns)
}

func (pool *Pool) queriesExecuted() int64 {
	return atomic.LoadInt64(&pool.stats.QueriesExecuted)
}

func (pool *Pool) errorCount() int64 {
	return atomic.LoadInt64(&pool.stats.ErrorCount)
}

// --- Server pool management ---

// getPool returns the existing Pool for (dbName, user), or creates a new one
// under the highest-priority target that serves dbName. Returns nil if no
// target serves the database.
func (p *Server) getPool(dbName, user string) *Pool {
	// Fast path: read-only lookup across all targets.
	for _, target := range p.targets {
		if !p.targetServesDatabase(target, dbName) {
			continue
		}
		target.poolsMu.RLock()
		if dbMap, ok := target.pools[dbName]; ok {
			if pool, ok := dbMap[user]; ok {
				target.poolsMu.RUnlock()
				return pool
			}
		}
		target.poolsMu.RUnlock()
	}

	// Not found — create under the first matching target.
	var selectedTarget *Target
	for _, t := range p.targets {
		if p.targetServesDatabase(t, dbName) {
			selectedTarget = t
			break
		}
	}
	if selectedTarget == nil {
		return nil
	}

	selectedTarget.poolsMu.Lock()
	defer selectedTarget.poolsMu.Unlock()

	// Re-check under write lock.
	if dbMap, ok := selectedTarget.pools[dbName]; ok {
		if pool, ok := dbMap[user]; ok {
			return pool
		}
	}

	if selectedTarget.pools[dbName] == nil {
		selectedTarget.pools[dbName] = make(map[string]*Pool)
	}

	pool := &Pool{
		config: DatabaseConfig{
			Name:           dbName,
			Host:           selectedTarget.Host,
			Port:           selectedTarget.Port,
			User:           user,
			MaxConnections: selectedTarget.MaxConnections,
			ConnectTimeout: selectedTarget.ConnectTimeout,
			Parameters:     selectedTarget.Parameters,
		},
		target:      selectedTarget,
		username:    user,
		backendPool: make(chan *BackendConnection, selectedTarget.MaxConnections),
		allConns:    make([]*BackendConnection, 0, selectedTarget.MaxConnections),
	}

	selectedTarget.pools[dbName][user] = pool
	atomic.AddInt64(&p.stats.TotalPools, 1)

	return pool
}

// --- Server helpers ---

// targetServesDatabase checks if a target serves a specific database.
func (p *Server) targetServesDatabase(target *Target, dbName string) bool {
	if len(target.IncludeDatabases) > 0 {
		for _, included := range target.IncludeDatabases {
			if included == dbName {
				goto checkExclude
			}
		}
		return false
	}
checkExclude:
	for _, excluded := range target.ExcludeDatabases {
		if excluded == dbName {
			return false
		}
	}
	return true
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
		return fmt.Errorf("failed to load pgfox TLS cert: %w", err)
	}

	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
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

// isClientGone returns true for errors that mean the client disconnected
// cleanly — EOF, broken pipe, connection reset. These are not real errors.
func isClientGone(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return msg == "EOF" ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "use of closed network connection")
}
func (p *Server) findBackendByKey(processID, secretKey int32) (*Target, *BackendConnection) {
	for _, target := range p.targets {
		target.poolsMu.RLock()
		pools := target.allPools()
		target.poolsMu.RUnlock()

		for _, pool := range pools {
			poolLen := len(pool.backendPool)
			for i := 0; i < poolLen; i++ {
				select {
				case conn := <-pool.backendPool:
					match := conn.GetProcessID() == processID && conn.GetSecretKey() == secretKey
					select {
					case pool.backendPool <- conn:
					default:
						conn.conn.Close()
						pool.removeFromAllConns(conn)
					}
					if match {
						return target, conn
					}
				default:
				}
			}
		}
	}

	p.clientsMu.RLock()
	defer p.clientsMu.RUnlock()

	for _, client := range p.clients {
		backend := client.GetBackendConnection()
		if backend != nil &&
			backend.GetProcessID() == processID &&
			backend.GetSecretKey() == secretKey {
			return backend.pool.target, backend
		}
	}

	return nil, nil
}
