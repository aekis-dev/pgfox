package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Target represents a PostgreSQL server. It holds both config (from yaml) and
// all runtime state: the privileged connection, pools, and connection budget.
// The target goroutine is the sole creator and manager of all backend connections
// for this target.
type Target struct {
	Name           string            `yaml:"name"`
	Host           string            `yaml:"host"`
	Port           int               `yaml:"port"`
	MaxConnections int               `yaml:"max_connections"`
	ConnectTimeout time.Duration     `yaml:"connect_timeout"`
	Parameters     map[string]string `yaml:"parameters"`
	Priority       int               `yaml:"priority"`

	// Rules is the ordered access control list for this target.
	// The first matching rule wins. If no rule matches, access is denied.
	// Populated with defaults by LoadConfig if empty.
	Rules []Rule `yaml:"rules"`

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

// checkAccess evaluates the target's ordered rule list against the client
// address, user, and database. The first matching rule wins — all specified
// fields in a rule must match (AND logic), empty fields match anything.
// Returns an error if access is denied or no rule matches (default deny).
func (t *Target) checkAccess(clientAddr net.Addr, user, database string) error {
	ip := extractIP(clientAddr)

	for _, r := range t.Rules {
		if !r.matchesIP(ip) {
			continue
		}
		if !r.matchesUser(user) {
			continue
		}
		if !r.matchesDatabase(database) {
			continue
		}
		// Rule matched.
		if r.Action == RuleDeny {
			return fmt.Errorf("access denied for user %q database %q from %s", user, database, ip)
		}
		return nil // RuleAllow
	}

	// No rule matched — default permit.
	return nil
}

// matchesIP returns true if the rule's CIDR list contains ip, or the list is empty.
func (r *Rule) matchesIP(ip net.IP) bool {
	if len(r.nets) == 0 {
		return true
	}
	for _, network := range r.nets {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

// matchesUser returns true if the rule's Users list contains user, or is empty.
func (r *Rule) matchesUser(user string) bool {
	if len(r.Users) == 0 {
		return true
	}
	for _, u := range r.Users {
		if u == user {
			return true
		}
	}
	return false
}

// matchesDatabase returns true if the rule's Databases list contains database, or is empty.
func (r *Rule) matchesDatabase(database string) bool {
	if len(r.Databases) == 0 {
		return true
	}
	for _, d := range r.Databases {
		if d == database {
			return true
		}
	}
	return false
}

// extractIP extracts the IP address from a net.Addr, stripping the port.
func extractIP(addr net.Addr) net.IP {
	switch a := addr.(type) {
	case *net.TCPAddr:
		return a.IP
	default:
		host, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			return nil
		}
		return net.ParseIP(host)
	}
}

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
		"database", pool.dbName,
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
					"from", is.pool.dbName+"/"+is.pool.username,
					"to", cs.pool.dbName+"/"+cs.pool.username)
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
						"database", pool.dbName,
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
			"database", pool.dbName,
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
			"database", pool.dbName,
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
			"database", pool.dbName,
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
