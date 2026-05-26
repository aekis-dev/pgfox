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
	config     Config
	listener   net.Listener
	listenerMu sync.RWMutex // protects listener during live address changes
	targets    []*Target

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

// NewServer creates a new PgFox server.
func NewServer(config Config, logger *Logger) (*Server, error) {
	listener, err := net.Listen("tcp", config.Server.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", config.Server.ListenAddr, err)
	}

	return &Server{
		config:    config,
		listener:  listener,
		targets:   config.Targets,
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
			p.listenerMu.RLock()
			ln := p.listener
			p.listenerMu.RUnlock()

			if tcpLn, ok := ln.(*net.TCPListener); ok {
				tcpLn.SetDeadline(time.Now().Add(1 * time.Second))
			}

			conn, err := ln.Accept()

			if tcpLn, ok := ln.(*net.TCPListener); ok {
				tcpLn.SetDeadline(time.Time{})
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

	p.listenerMu.Lock()
	if p.listener != nil {
		p.listener.Close()
	}
	p.listenerMu.Unlock()

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
		if target.draining.Load() {
			continue
		}
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

	// Not found — create under the first matching non-draining target.
	var selectedTarget *Target
	for _, t := range p.targets {
		if t.draining.Load() {
			continue
		}
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

// targetServesDatabase checks if a target can serve a specific database.
// Evaluates only database-matching rules; the first match wins.
// Default is permit — consistent with checkAccess.
func (p *Server) targetServesDatabase(target *Target, dbName string) bool {
	for _, r := range target.Rules {
		if !r.matchesDatabase(dbName) {
			continue
		}
		return r.Action == RuleAllow
	}
	return true // no matching rule — default permit
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

// reload applies a new configuration to the running server without restarting.
// It diffs the new config against the current one and:
//  1. Marks removed targets as draining — new connections refused, existing
//     transactions allowed to complete up to query_timeout, then force-closed.
//  2. Updates existing targets in-place (rules, timeouts, pool size, params).
//  3. Starts new target goroutines for added targets.
//
// listen_addr changes are ignored — a restart is required for that.
func (p *Server) reload(newConfig Config) {
	logger := p.logger.WithField("component", "reload")
	logger.Info("Applying config reload")

	oldTargets := make(map[string]*Target, len(p.targets))
	for _, t := range p.targets {
		oldTargets[t.Name] = t
	}

	newTargets := make(map[string]*Target, len(newConfig.Targets))
	for _, t := range newConfig.Targets {
		newTargets[t.Name] = t
	}

	// --- Step 1: mark removed targets as draining ---
	var draining []*Target
	for name, old := range oldTargets {
		if _, exists := newTargets[name]; !exists {
			old.draining.Store(true)
			draining = append(draining, old)
			logger.Info("Target marked for removal", "target", name)
		}
	}

	// Wait for active connections to finish (up to query_timeout), then cancel.
	for _, t := range draining {
		timeout := p.config.Server.QueryTimeout
		if timeout > 0 {
			t.waitDrained(timeout, logger)
		}
		t.cancel()
		t.wg.Wait()
		logger.Info("Target removed", "target", t.Name)
	}

	// Remove drained targets from the slice.
	if len(draining) > 0 {
		kept := p.targets[:0]
		drainSet := make(map[string]bool, len(draining))
		for _, t := range draining {
			drainSet[t.Name] = true
		}
		for _, t := range p.targets {
			if !drainSet[t.Name] {
				kept = append(kept, t)
			}
		}
		p.targets = kept
	}

	// --- Step 2: update existing targets in-place ---
	for name, old := range oldTargets {
		fresh, exists := newTargets[name]
		if !exists {
			continue // already removed above
		}
		old.Rules = fresh.Rules
		old.MaxConnections = fresh.MaxConnections
		old.ConnectTimeout = fresh.ConnectTimeout
		old.Parameters = fresh.Parameters
		logger.Info("Target updated", "target", name)
	}

	// --- Step 3: start new targets ---
	for name, fresh := range newTargets {
		if _, exists := oldTargets[name]; exists {
			continue // already updated above
		}

		tctx, tcancel := context.WithCancel(p.ctx)
		fresh.ctx = tctx
		fresh.cancel = tcancel

		p.wg.Add(1)
		go func(t *Target) {
			defer p.wg.Done()
			t.run(p)
		}(fresh)

		select {
		case <-fresh.ready:
			logger.Info("New target ready", "target", name)
		case <-time.After(fresh.ConnectTimeout):
			logger.Warn("Timed out waiting for new target", "target", name)
		case <-p.ctx.Done():
			return
		}

		p.targets = append(p.targets, fresh)
	}

	// --- Step 4: update server-level config (non-structural fields) ---
	if newConfig.Server.ListenAddr != p.config.Server.ListenAddr {
		newLn, err := net.Listen("tcp", newConfig.Server.ListenAddr)
		if err != nil {
			logger.WithError(err).Warn("Failed to bind new listen_addr, keeping current",
				"current", p.config.Server.ListenAddr,
				"new", newConfig.Server.ListenAddr)
		} else {
			p.listenerMu.Lock()
			oldLn := p.listener
			p.listener = newLn
			p.listenerMu.Unlock()
			oldLn.Close() // accept loop gets a timeout/error and re-reads p.listener
			logger.Info("Listener moved",
				"from", p.config.Server.ListenAddr,
				"to", newConfig.Server.ListenAddr)
		}
	}
	p.config.Server.ConnectTimeout = newConfig.Server.ConnectTimeout
	p.config.Server.IdleTimeout = newConfig.Server.IdleTimeout
	p.config.Server.QueryTimeout = newConfig.Server.QueryTimeout
	p.config.Server.MaxMessageSize = newConfig.Server.MaxMessageSize
	p.config.Server.StatsInterval = newConfig.Server.StatsInterval
	p.config.Server.PeakWindow = newConfig.Server.PeakWindow
	p.config.Logging = newConfig.Logging
	p.config.Targets = newConfig.Targets

	// Re-sort targets by priority after any additions.
	sort.Slice(p.targets, func(i, j int) bool {
		if p.targets[i].Priority == p.targets[j].Priority {
			return p.targets[i].Name < p.targets[j].Name
		}
		return p.targets[i].Priority < p.targets[j].Priority
	})

	logger.Info("Config reload complete",
		"targets", len(p.targets))
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
