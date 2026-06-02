package pgfox

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

	"github.com/aekis-dev/pgfox/pkg/logger"
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
	Config     Config
	Listener   net.Listener
	listenerMu sync.RWMutex // protects Listener during live address changes
	Targets    []*Target

	// clients maps net.Conn → *Client. sync.Map is used because
	// writes (connect/disconnect) are rare compared to reads (cancel lookup,
	// shutdown), and high connection churn no longer serialises through a
	// single RWMutex.
	Clients sync.Map // net.Conn → *Client

	Listeners   map[Channel]*Listen
	ListenersMu sync.RWMutex

	GlobalStats GlobalStats
	Context     context.Context
	Cancel      context.CancelFunc
	Wg          sync.WaitGroup
	Logger      *logger.Logger
}

// NewServer creates a new PgFox server.
func NewServer(config Config, logger *logger.Logger) (*Server, error) {
	listener, err := net.Listen("tcp", config.Server.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", config.Server.ListenAddr, err)
	}

	return &Server{
		Config:    config,
		Listener:  listener,
		Targets:   config.Targets,
		Listeners: make(map[Channel]*Listen),
		Logger:    logger,
	}, nil
}

// Start starts the wildcard pooler.
func (p *Server) Start(ctx context.Context) error {
	p.Context, p.Cancel = context.WithCancel(ctx)

	p.Logger.Info("PostgreSQL connection pooler starting",
		"listen_addr", p.Config.Server.ListenAddr,
		"targets", len(p.Targets))

	if err := p.ensureBootstrapCerts(); err != nil {
		return fmt.Errorf("failed to ensure bootstrap certificates: %w", err)
	}

	// Start one goroutine per target — it opens the privileged connection
	// and manages all Pool connections for that target.
	for _, target := range p.Targets {
		tctx, tcancel := context.WithCancel(p.Context)
		target.Context = tctx
		target.Cancel = tcancel

		p.Wg.Add(1)
		go func(t *Target) {
			defer p.Wg.Done()
			t.run(p)
		}(target)

		// Wait for privileged connection before accepting clients.
		select {
		case <-target.Ready:
			p.Logger.Info("Target ready", "target", target.Name)
		case <-time.After(target.ConnectTimeout):
			return fmt.Errorf("timed out waiting for target %s to become ready", target.Name)
		case <-p.Context.Done():
			return fmt.Errorf("server shutting down during target init")
		}
	}

	for {
		select {
		case <-p.Context.Done():
			p.Logger.Info("Accept loop stopping")
			return p.shutdown()
		default:
			p.listenerMu.RLock()
			ln := p.Listener
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
				if p.Context.Err() != nil {
					return p.shutdown()
				}
				p.Logger.WithError(err).Error("Accept error")
				continue
			}

			atomic.AddInt64(&p.GlobalStats.TotalClients, 1)
			atomic.AddInt64(&p.GlobalStats.ActiveClients, 1)

			p.Wg.Add(1)
			go p.handleClient(conn)
		}
	}
}

// shutdown gracefully shuts down the pooler.
func (p *Server) shutdown() error {
	p.Logger.Info("Starting graceful shutdown")

	p.listenerMu.Lock()
	if p.Listener != nil {
		p.Listener.Close()
	}
	p.listenerMu.Unlock()

	p.Cancel()
	p.shutdownListeners()

	p.Clients.Range(func(conn, _ any) bool {
		conn.(net.Conn).Close()
		return true
	})

	// Cancel all target goroutines — each drains and closes its own connections.
	for _, target := range p.Targets {
		target.Cancel()
		target.Wg.Wait()
	}

	p.Wg.Wait()
	p.Logger.Info("Graceful shutdown completed")
	return nil
}

// handleClient handles a client connection.
func (p *Server) handleClient(conn net.Conn) {
	defer p.Wg.Done()
	defer func() {
		atomic.AddInt64(&p.GlobalStats.ActiveClients, -1)
		conn.Close()
	}()

	clientLogger := p.Logger.WithClient(conn.RemoteAddr().String())
	client := NewClient(conn, clientLogger, p.Config.Server.MaxMessageSize)

	p.Clients.Store(conn, client)

	defer func() {
		p.Clients.Delete(conn)

		duration := time.Since(client.GetConnectedAt())
		clientLogger.Info("Client disconnected",
			"duration", duration.Round(time.Millisecond),
			"active_clients", atomic.LoadInt64(&p.GlobalStats.ActiveClients)-1)

		p.cleanupClientListeners(client)

		backend := client.GetBackend()
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
			backend.Return()
		}
	}()

	if err := p.handleStartupMessage(client); err != nil {
		clientLogger.WithError(err).Error("Startup error")
		return
	}

	for {
		select {
		case <-p.Context.Done():
			return
		default:
			if err := p.HandleClientMessage(client); err != nil {
				if isClientGone(err) {
					return
				}
				clientLogger.WithError(err).Error("Client message error")
				return
			}
		}
	}
}

// --- Server Pool management ---

// getPool returns the existing Pool for (dbName, user), or creates a new one
// under the highest-priority target that serves dbName. Returns nil if no
// target serves the database.
//
// The hot path (Pool already exists) is a single sync.Map.Load per target —
// no mutex, no allocation.
func (p *Server) getPool(dbName, user string) *Pool {
	// Fast path: Pool already exists in the first matching target.
	for _, target := range p.Targets {
		if target.Draining.Load() {
			continue
		}
		if !p.targetServesDatabase(target, dbName) {
			continue
		}
		if pool := target.LookupPool(dbName, user); pool != nil {
			return pool
		}
	}

	// Slow path: select a target and create the Pool.
	var selectedTarget *Target
	for _, t := range p.Targets {
		if t.Draining.Load() {
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

	// LoadOrStore ensures exactly one Pool is created even under concurrent
	// first-access for the same (dbName, user) pair.
	pool := &Pool{
		Target:   selectedTarget,
		DbName:   dbName,
		Username: user,
		Queue:    make(chan *Backend, selectedTarget.MaxConnections),
		All:      make([]*Backend, 0, selectedTarget.MaxConnections),
	}
	actual, loaded := selectedTarget.Pools.LoadOrStore(PoolKey(dbName, user), pool)
	if loaded {
		// Another goroutine created it first — discard ours.
		return actual.(*Pool)
	}
	// We stored the new Pool. Register it in cachedPools via a non-blocking
	// send to the target goroutine so it can append to cachedPools safely.
	select {
	case selectedTarget.PoolRegistered <- pool:
	default:
		// Channel full is benign — the target goroutine will pick it up on
		// the next drain. cachedPools is only used for the growth/health ticks
		// which run every 50ms/30s, so a brief delay is harmless.
	}

	atomic.AddInt64(&p.GlobalStats.TotalPools, 1)
	return pool
}

// --- Server helpers ---

// targetServesDatabase checks if a target can serve a specific database.
// Evaluates only database-matching rules; the first match wins.
// Default is permit — consistent with checkAccess.
func (p *Server) targetServesDatabase(target *Target, dbName string) bool {
	for _, r := range target.Rules {
		if !r.MatchesDatabase(dbName) {
			continue
		}
		return r.Action == RuleAllow
	}
	return true // no matching rule — default permit
}

// reload applies a new configuration to the running server without restarting.
// It diffs the new config against the current one and:
//  1. Marks removed targets as draining — new connections refused, existing
//     transactions allowed to complete up to query_timeout, then force-closed.
//  2. Updates existing targets in-place (rules, timeouts, Pool size, params).
//  3. Starts new target goroutines for added targets.
//
// listen_addr changes are ignored — a restart is required for that.
func (p *Server) Reload(newConfig Config) {
	logger := p.Logger.WithField("component", "reload")
	logger.Info("Applying config reload")

	oldTargets := make(map[string]*Target, len(p.Targets))
	for _, t := range p.Targets {
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
			old.Draining.Store(true)
			draining = append(draining, old)
			logger.Info("Target marked for removal", "target", name)
		}
	}

	// Wait for active connections to finish (up to query_timeout), then cancel.
	for _, t := range draining {
		timeout := p.Config.Server.QueryTimeout
		if timeout > 0 {
			t.waitDrained(timeout, logger)
		}
		t.Cancel()
		t.Wg.Wait()
		logger.Info("Target removed", "target", t.Name)
	}

	// Remove drained targets from the slice.
	if len(draining) > 0 {
		kept := p.Targets[:0]
		drainSet := make(map[string]bool, len(draining))
		for _, t := range draining {
			drainSet[t.Name] = true
		}
		for _, t := range p.Targets {
			if !drainSet[t.Name] {
				kept = append(kept, t)
			}
		}
		p.Targets = kept
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

		tctx, tcancel := context.WithCancel(p.Context)
		fresh.Context = tctx
		fresh.Cancel = tcancel

		p.Wg.Add(1)
		go func(t *Target) {
			defer p.Wg.Done()
			t.run(p)
		}(fresh)

		select {
		case <-fresh.Ready:
			logger.Info("New target ready", "target", name)
		case <-time.After(fresh.ConnectTimeout):
			logger.Warn("Timed out waiting for new target", "target", name)
		case <-p.Context.Done():
			return
		}

		p.Targets = append(p.Targets, fresh)
	}

	// --- Step 4: update server-level config (non-structural fields) ---
	if newConfig.Server.ListenAddr != p.Config.Server.ListenAddr {
		newLn, err := net.Listen("tcp", newConfig.Server.ListenAddr)
		if err != nil {
			logger.WithError(err).Warn("Failed to bind new listen_addr, keeping current",
				"current", p.Config.Server.ListenAddr,
				"new", newConfig.Server.ListenAddr)
		} else {
			p.listenerMu.Lock()
			oldLn := p.Listener
			p.Listener = newLn
			p.listenerMu.Unlock()
			oldLn.Close() // accept loop gets a timeout/error and re-reads p.Listener
			logger.Info("Listener moved",
				"from", p.Config.Server.ListenAddr,
				"to", newConfig.Server.ListenAddr)
		}
	}
	p.Config.Server.ConnectTimeout = newConfig.Server.ConnectTimeout
	p.Config.Server.IdleTimeout = newConfig.Server.IdleTimeout
	p.Config.Server.QueryTimeout = newConfig.Server.QueryTimeout
	p.Config.Server.MaxMessageSize = newConfig.Server.MaxMessageSize
	p.Config.Server.StatsInterval = newConfig.Server.StatsInterval
	p.Config.Server.PeakWindow = newConfig.Server.PeakWindow
	p.Config.Logging = newConfig.Logging
	p.Config.Targets = newConfig.Targets

	// Re-sort targets by priority after any additions.
	sort.Slice(p.Targets, func(i, j int) bool {
		if p.Targets[i].Priority == p.Targets[j].Priority {
			return p.Targets[i].Name < p.Targets[j].Name
		}
		return p.Targets[i].Priority < p.Targets[j].Priority
	})

	logger.Info("Config reload complete",
		"targets", len(p.Targets))
}

// isSSLSupported checks whether the pgfox client-facing TLS cert exists.
func (p *Server) isSSLSupported() bool {
	_, err := os.Stat(p.pgfoxTLSCertPath())
	return err == nil
}

// upgradeToTLS upgrades a client connection to TLS.
func (p *Server) upgradeToTLS(client *Client) error {
	cert, err := tls.LoadX509KeyPair(p.pgfoxTLSCertPath(), p.pgfoxTLSKeyPath())
	if err != nil {
		return fmt.Errorf("failed to load pgfox TLS cert: %w", err)
	}

	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	tlsConn := tls.Server(client.conn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return fmt.Errorf("TLS handshake failed: %w", err)
	}

	p.Logger.Debug("TLS connection established", "client", client.RemoteAddr())

	// writeMu guards conn/reader/writer, which WriteMessage also holds.
	client.writeMu.Lock()
	client.conn = tlsConn
	client.reader = bufio.NewReader(tlsConn)
	client.writer = bufio.NewWriter(tlsConn)
	client.writeMu.Unlock()

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
// Used to route cancel requests.
//
// Active (pinned) connections are found via the clients sync.Map.
// Idle connections are found via the per-target backendIndex sync.Map, which
// is updated atomically when connections enter/leave the idle Pool.
func (p *Server) findBackendByKey(processID, secretKey int32) (*Target, *Backend) {
	// Search active client-pinned connections first (most common for cancel).
	var foundTarget *Target
	var foundConn *Backend

	p.Clients.Range(func(_, v any) bool {
		client := v.(*Client)
		backend := client.GetBackend()
		if backend != nil &&
			backend.GetProcessID() == processID &&
			backend.GetSecretKey() == secretKey {
			foundTarget = backend.Pool.Target
			foundConn = backend
			return false // stop iteration
		}
		return true
	})
	if foundConn != nil {
		return foundTarget, foundConn
	}

	// Search idle connections via per-target index.
	for _, target := range p.Targets {
		if v, ok := target.BackendIndex.Load(processID); ok {
			conn := v.(*Backend)
			if conn.GetSecretKey() == secretKey {
				return target, conn
			}
		}
	}

	return nil, nil
}
