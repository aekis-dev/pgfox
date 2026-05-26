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

// GlobalStats contains global pooler statistics.
type GlobalStats struct {
	TotalClients          int64
	ActiveClients         int64
	TotalPools            int64
	TotalQueries          int64
	NotificationsSent     int64
	IdleConnectionsClosed int64
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
		target:      selectedTarget,
		dbName:      dbName,
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

// findBackendByKey finds a backend connection matching processID and secretKey.
// Used to route cancel requests. Searches idle pool connections and active
// client-pinned connections.
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
