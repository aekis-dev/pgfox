package pgfox

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aekis-dev/pgfox/pkg/auth"
	"github.com/aekis-dev/pgfox/pkg/logger"
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
	// backend is the pgfox_role backend connection used exclusively for
	// pg_shadow queries during client authentication.
	Backend *Backend
	Ready   chan struct{}     // closed when conn is ready for the first time
	Params  map[string]string // ParameterStatus values from conn startup

	// --- Runtime: pools ---
	// Pools maps "database\x00user" → *Pool. sync.Map is used because the
	// map is read on every query but written only once per (database, user)
	// pair — exactly the load profile sync.Map is optimised for.
	Pools sync.Map // key: dbUser string → *Pool

	// CachedPools is a flat snapshot maintained exclusively by the target
	// goroutine (growthCycle, healthCheck, drain). It avoids Range() overhead
	// and the allocation of a new slice on every 50ms tick.
	CachedPools []*Pool

	// --- Runtime: connection budget ---
	// TotalOpen is the count of all open backend connections on this target
	// across all pools plus the privileged conn. Only the target goroutine
	// writes this. atomicTotalOpen mirrors it for safe cross-goroutine reads.
	TotalOpen       int
	AtomicTotalOpen atomic.Int32

	// ServerMaxConns and serverOpenConns are updated by the stats ticker
	// from pg_stat_activity. They reflect the real PostgreSQL server state
	// regardless of other clients not using pgfox.
	ServerMaxConns  int // pg max_connections setting
	ServerOpenConns int // total connections currently open on the server

	// ListenOpen is the count of active dedicated listen connections on this
	// target. Written atomically from client goroutines.
	ListenOpen int32

	// Draining is set to true when the target is being removed during a config
	// reload. New pools and new borrows are refused; existing transactions are
	// allowed to complete until query_timeout, then force-closed.
	Draining atomic.Bool

	// StmtCache is the target-level prepared statement registry. It maps a
	// canonical query hash to the parsed/parameterized form and usage stats.
	// All pools on this target share one cache — the same logical query sent
	// against any (database, user) combination maps to a single entry.
	StmtCache *StmtCache

	// ScramCh serialises pg_shadow queries through the target goroutine so that
	// target.conn is never accessed concurrently from client goroutines.
	ScramCh chan ScramRequest

	// CloseCh receives connections that need to be cleaned up (dead or
	// backendPool-full). The target goroutine is the sole reader.
	CloseCh chan *Backend

	// Demand is a capacity-1 signal channel. borrowConn sends a non-blocking
	// token when it enters the slow path (Pool empty), triggering an immediate
	// growthCycle instead of waiting up to 50ms for the ticker. Subsequent
	// waiters within the same tick find the channel full and skip silently.
	Demand chan struct{}

	// PoolRegistered receives newly created Pool pointers from getPool so the
	// target goroutine can append them to cachedPools without a lock.
	PoolRegistered chan *Pool

	// BackendIndex maps processID (int32) → *Backend for idle
	// connections. Updated atomically when connections enter/leave backendPool.
	// Allows O(1) cancel-request lookup without draining the channel.
	BackendIndex sync.Map // int32 → *Backend

	// connReady is signaled (non-blocking send) whenever a connection is
	// returned to any Pool, waking borrowConn waiters.
	ConnReady chan struct{}

	// --- Runtime: lifecycle ---
	Context context.Context
	Cancel  context.CancelFunc
	Wg      sync.WaitGroup
}

// ScramRequest is sent by a client goroutine to the target goroutine to fetch
// a SCRAM verifier from pg_shadow via the privileged connection.
type ScramRequest struct {
	username string
	reply    chan ScramReply
}

type ScramReply struct {
	verifier *auth.SCRAMVerifier
	err      error
}

// ActiveConnections returns the total number of connections currently checked
// out across all pools on this target. Safe to call from any goroutine.
func (t *Target) ActiveConnections() int {
	total := 0
	t.Pools.Range(func(_, v any) bool {
		total += v.(*Pool).ActiveConnections()
		return true
	})
	return total
}

// PoolKey builds the sync.Map key for a (dbName, user) pair.
func PoolKey(dbName, user string) string { return dbName + "\x00" + user }

// LookupPool returns the Pool for (dbName, user), or nil if absent.
func (t *Target) LookupPool(dbName, user string) *Pool {
	v, ok := t.Pools.Load(PoolKey(dbName, user))
	if !ok {
		return nil
	}
	return v.(*Pool)
}

// StorePool registers a new Pool in the sync.Map and appends it to cachedPools.
// cachedPools is used by the target goroutine for lock-free iteration; it must
// only be written from that goroutine (or during Pool creation while the
// caller holds no Pool lock, which is always the case in getPool).
func (t *Target) StorePool(pool *Pool) {
	t.Pools.Store(PoolKey(pool.DbName, pool.Username), pool)
	t.CachedPools = append(t.CachedPools, pool)
}

// waitDrained blocks until all active connections on this target complete or
// the timeout expires, then returns. Used during config reload removal.
func (t *Target) waitDrained(timeout time.Duration, logger *logger.Logger) {
	if timeout <= 0 {
		return
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if t.ActiveConnections() == 0 {
			logger.Info("Target drained of active connections", "target", t.Name)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	logger.Warn("Target drain timed out, force-closing remaining connections",
		"target", t.Name,
		"active", t.ActiveConnections())
}

// checkAccess evaluates the target's ordered rule list against the client
// address, user, and database. The first matching rule wins — all specified
// fields in a rule must match (AND logic), empty fields match anything.
// Returns an error if access is denied or no rule matches (default deny).
func (t *Target) checkAccess(clientAddr net.Addr, user, database string) error {
	ip := extractIP(clientAddr)

	for _, r := range t.Rules {
		if !r.MatchesIP(ip) {
			continue
		}
		if !r.MatchesUser(user) {
			continue
		}
		if !r.MatchesDatabase(database) {
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

// Prepared statement hashes for the two privileged-connection queries.
// These are deployed once on t.conn at startup and on every reconnect.
// Binary format is used for all result columns — both queries return
// text/varchar values which arrive as raw UTF-8 bytes in binary mode,
// identical to text mode, so no additional decoding is needed.
const (
	scramStmtSQL  = "SELECT rolpassword FROM pg_authid WHERE rolname = $1 AND rolcanlogin = true"
	scramStmtHash = "pgfox_scram" // fixed name — no hash needed, single-param

	statsStmtSQL  = "SELECT current_setting('max_connections')::int, count(*)::int FROM pg_stat_activity"
	statsStmtHash = "pgfox_stats" // fixed name — zero params
)

// deployPrivilegedStmts sends Parse for both privileged-connection prepared
// statements and reads their ParseComplete responses. Called after every
// successful (re)connection of t.conn so the statements are always ready.
// Must only be called from the target goroutine.
func (t *Target) deployPrivilegedStmts(logger *logger.Logger) error {
	stmts := []struct {
		hash string
		sql  string
	}{
		{scramStmtHash, scramStmtSQL},
		{statsStmtHash, statsStmtSQL},
	}

	if err := t.Backend.Conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return fmt.Errorf("failed to set deadline: %w", err)
	}
	defer t.Backend.Conn.SetReadDeadline(time.Time{})

	// Send all Parse messages then a single Sync — pipeline them in one shot.
	for _, s := range stmts {
		parseBody := BuildParseBody(s.hash, s.sql, nil)
		if err := t.Backend.WriteMessage('P', parseBody); err != nil {
			return fmt.Errorf("failed to send Parse for %s: %w", s.hash, err)
		}
	}
	if err := t.Backend.WriteMessage('S', SyncBody); err != nil {
		return fmt.Errorf("failed to send Sync: %w", err)
	}

	// Read responses: ParseComplete × 2, then ReadyForQuery.
	deployed := 0
	for {
		msgType, body, err := t.Backend.ReadMessage()
		if err != nil {
			return fmt.Errorf("failed to read Parse response: %w", err)
		}
		switch msgType {
		case '1': // ParseComplete
			deployed++
			t.Backend.MarkStmt(stmts[deployed-1].hash)
			logger.Debug("Privileged stmt deployed", "stmt", stmts[deployed-1].hash)
			PutMsgBody(body)
		case 'Z': // ReadyForQuery
			PutMsgBody(body)
			if deployed != len(stmts) {
				return fmt.Errorf("expected %d ParseComplete, got %d", len(stmts), deployed)
			}
			return nil
		case 'E':
			errStr := ParseErrorMessage(body)
			PutMsgBody(body)
			return fmt.Errorf("Parse error for privileged stmt: %s", errStr)
		default:
			PutMsgBody(body)
		}
	}
}

// run is the target manager goroutine. It:
//  1. Opens and maintains the privileged connection (conn).
//  2. Manages all Pool connections: growth, shrink, recycling, health checks.
//  3. Periodically queries pg_stat_activity to track real server capacity.
//
// fetchSCRAMVerifier queries pg_authid for the SCRAM verifier of username
// using the extended protocol with binary result format.
// Must only be called from the target goroutine — target.conn is not
// safe to access from other goroutines concurrently.
func (t *Target) fetchSCRAMVerifier(username string) (*auth.SCRAMVerifier, error) {
	if t.Backend == nil {
		return nil, fmt.Errorf("privileged connection not ready for target %s", t.Name)
	}

	if err := t.Backend.Conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return nil, fmt.Errorf("failed to set read deadline: %w", err)
	}
	defer t.Backend.Conn.SetReadDeadline(time.Time{})

	// Re-deploy if the statement was lost (e.g. after a reconnect that
	// called deployPrivilegedStmts, this should never trigger, but guard
	// defensively in case t.conn was replaced without going through
	// openPrivilegedConn).
	if !t.Backend.HasStmt(scramStmtHash) {
		parseBody := BuildParseBody(scramStmtHash, scramStmtSQL, nil)
		if err := t.Backend.WriteMessage('P', parseBody); err != nil {
			return nil, fmt.Errorf("failed to send Parse for scram stmt: %w", err)
		}
	}

	// Bind: $1 = username (text format), result column in binary.
	bindBody := BuildBindBody(
		"", // unnamed portal
		scramStmtHash,
		nil, // default text format for params
		[]string{username},
		[]int16{1}, // binary result
	)
	if err := t.Backend.WriteMessage('B', bindBody); err != nil {
		return nil, fmt.Errorf("failed to send Bind for scram stmt: %w", err)
	}

	execBody := BuildExecuteBody("", 0)
	if err := t.Backend.WriteMessage('E', execBody); err != nil {
		return nil, fmt.Errorf("failed to send Execute for scram stmt: %w", err)
	}

	if err := t.Backend.WriteMessage('S', SyncBody); err != nil {
		return nil, fmt.Errorf("failed to send Sync: %w", err)
	}

	var rolpassword string

	for {
		msgType, body, err := t.Backend.ReadMessage()
		if err != nil {
			return nil, fmt.Errorf("failed to read pg_authid response: %w", err)
		}

		switch msgType {
		case '1': // ParseComplete (only present on first call after redeploy)
			t.Backend.MarkStmt(scramStmtHash)
			PutMsgBody(body)
		case '2': // BindComplete
			PutMsgBody(body)
		case 'D': // DataRow — binary format
			// Binary DataRow layout:
			//   Int16  num_columns
			//   for each column:
			//     Int32  col_length  (-1 = NULL)
			//     bytes  col_data
			if len(body) < 2 {
				continue
			}
			colCount := int(int16(body[0])<<8 | int16(body[1]))
			if colCount < 1 {
				continue
			}
			pos := 2
			if pos+4 > len(body) {
				continue
			}
			colLen := int(int32(body[pos])<<24 | int32(body[pos+1])<<16 |
				int32(body[pos+2])<<8 | int32(body[pos+3]))
			pos += 4
			if colLen < 0 {
				// NULL — user exists but has no password set
				return nil, fmt.Errorf("user %q has no password set in pg_authid", username)
			}
			if pos+colLen <= len(body) {
				// Binary text/varchar is raw UTF-8 — identical to text mode.
				rolpassword = string(body[pos : pos+colLen])
			}
			PutMsgBody(body)
		case 'C': // CommandComplete
			PutMsgBody(body)
		case 'Z': // ReadyForQuery
			PutMsgBody(body)
			if rolpassword == "" {
				return nil, fmt.Errorf("user %q not found in pg_authid", username)
			}
			return auth.ParseSCRAMVerifier(rolpassword)
		case 'E':
			errStr := ParseErrorMessage(body)
			PutMsgBody(body)
			return nil, fmt.Errorf("pg_authid query error: %s", errStr)
		default:
			PutMsgBody(body)
		}
	}
}

func (t *Target) run(p *Server) {
	logger := p.Logger.
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

	statsTicker := time.NewTicker(p.Config.Server.StatsInterval)
	defer statsTicker.Stop()

	for {
		select {
		case <-t.Context.Done():
			logger.Info("Target manager stopping")
			t.drain(p, logger)
			return

		case req := <-t.ScramCh:
			verifier, err := t.fetchSCRAMVerifier(req.username)
			req.reply <- ScramReply{verifier: verifier, err: err}

		case conn := <-t.CloseCh:
			t.handleClose(p, conn, logger)

		case <-t.Demand:
			// A borrowConn caller hit an empty Pool. Grow immediately rather
			// than waiting for the next growthTicker tick.
			t.growthCycle(p, logger)

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
// Retries until successful or Context is cancelled.
func (t *Target) openPrivilegedConn(p *Server, logger *logger.Logger) error {
	pgfoxCert, err := p.loadOrGenerateUserCert(p.Config.Server.PgFoxRole)
	if err != nil {
		return fmt.Errorf("failed to load pgfox cert: %w", err)
	}

	for {
		if t.Context.Err() != nil {
			return fmt.Errorf("context cancelled")
		}

		conn, err := p.createCertBackend(t, "postgres", p.Config.Server.PgFoxRole, pgfoxCert)
		if err != nil {
			logger.WithError(err).Warn("Privileged connection failed, retrying in 5s")
			select {
			case <-t.Context.Done():
				return fmt.Errorf("context cancelled")
			case <-time.After(5 * time.Second):
			}
			continue
		}

		t.Backend = conn
		t.TotalOpen++
		t.AtomicTotalOpen.Store(int32(t.TotalOpen))

		for k, v := range conn.parameters {
			t.Params[k] = v
		}

		// Deploy both privileged prepared statements immediately so
		// fetchSCRAMVerifier and updateServerStats can use extended protocol
		// from the very first call.
		if err := t.deployPrivilegedStmts(logger); err != nil {
			logger.WithError(err).Warn("Failed to deploy privileged stmts, will retry on next use")
		}

		close(t.Ready)
		logger.Info("Privileged connection ready", "role", p.Config.Server.PgFoxRole)
		return nil
	}
}

// handleClose closes a dead connection, removes it from its Pool, and signals
// connReady so waiting borrowers can react (e.g. trigger growth).
func (t *Target) handleClose(p *Server, backend *Backend, logger *logger.Logger) {
	pool := backend.Pool
	t.BackendIndex.Delete(backend.GetProcessID())
	backend.Conn.Close()
	t.TotalOpen--
	pool.removeFromAll(backend)
	logger.Debug("Closed failed connection",
		"database", pool.DbName,
		"user", pool.Username,
		"target_total", t.TotalOpen)
	t.signalConnReady()
}

// growthCycle runs on every growthTicker. Opens one connection per contended
// Pool per tick, recycling slots from idle pools before using fresh server slots.
// Shrinks only when nothing is contended.
func (t *Target) growthCycle(p *Server, logger *logger.Logger) {
	now := time.Now()
	peakWindow := p.Config.Server.PeakWindow

	// cachedPools is maintained exclusively by the target goroutine — no lock.
	pools := t.allPools()

	serverAvailable := 0
	if t.ServerMaxConns > 0 {
		serverAvailable = t.ServerMaxConns - t.ServerOpenConns
	} else {
		serverAvailable = t.MaxConnections - t.TotalOpen
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
		active := pool.ActiveConnections()
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

		idle := pool.IdleConnections()
		total := len(pool.All)
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

	// Open one connection per contended Pool — recycling idle slots first.
	idleIdx := 0
	for _, cs := range contendedPools {
		if t.TotalOpen >= t.MaxConnections || serverAvailable <= 1 {
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
					"from", is.pool.DbName+"/"+is.pool.Username,
					"to", cs.pool.DbName+"/"+cs.pool.Username)
				idlePools[idleIdx].excess--
				if idlePools[idleIdx].excess == 0 {
					idleIdx++
				}
				recycled = true
			}
			break
		}

		if recycled || (t.TotalOpen < t.MaxConnections && serverAvailable > 1) {
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

// privilegedResponsive does a lightweight round-trip on the privileged
// connection to confirm it is still usable.
func (t *Target) privilegedResponsive(p *Server) bool {
	if t.Backend == nil {
		return false
	}
	if err := t.Backend.WriteMessage('Q', []byte("SELECT 1\x00")); err != nil {
		return false
	}
	return p.drainUntilReady(t.Backend) == nil
}

// healthCheck runs on the healthTicker. Validates idle connections and replaces
// dead ones. Also checks and replaces the privileged connection if needed.
func (t *Target) healthCheck(p *Server, logger *logger.Logger) {
	// Check privileged connection with a real round-trip: a dead or wedged TLS
	// session can only be detected reliably by using it (a socket-level probe
	// cannot see through the TLS layer, and TCP keepalive only catches a dead
	// peer, not a wedged one).
	if t.Backend != nil && !t.privilegedResponsive(p) {
		logger.Warn("Privileged connection dead, replacing")
		t.Backend.Conn.Close()
		t.TotalOpen--
		t.AtomicTotalOpen.Store(int32(t.TotalOpen))
		t.Backend = nil

		pgfoxCert, err := p.loadOrGenerateUserCert(p.Config.Server.PgFoxRole)
		if err == nil {
			if conn, err := p.createCertBackend(t, "postgres", p.Config.Server.PgFoxRole, pgfoxCert); err == nil {
				t.Backend = conn
				t.TotalOpen++
				t.AtomicTotalOpen.Store(int32(t.TotalOpen))
				if err := t.deployPrivilegedStmts(logger); err != nil {
					logger.WithError(err).Warn("Failed to deploy privileged stmts after reconnect")
				}
				logger.Info("Privileged connection replaced")
			} else {
				logger.WithError(err).Warn("Failed to replace privileged connection")
			}
		}
	}

	idleTimeout := p.Config.Server.IdleTimeout
	cutoff := time.Now().Add(-idleTimeout)

	pools := t.allPools()

	for _, pool := range pools {
		poolLen := len(pool.Queue)
		for i := 0; i < poolLen; i++ {
			select {
			case backend := <-pool.Queue:
				// No liveness probe: a connection that died while idle is caught
				// lazily on next borrow (ReadMessage/WriteMessage error). The
				// reaper only evicts on idle timeout.
				tooOld := idleTimeout > 0 && backend.LastUsedAt().Before(cutoff)

				if tooOld {
					logger.Debug("Health check closing connection",
						"reason", "idle timeout",
						"database", pool.DbName,
						"user", pool.Username)
					backend.Conn.Close()
					t.TotalOpen--
					t.AtomicTotalOpen.Store(int32(t.TotalOpen))
					pool.removeFromAll(backend)
					atomic.AddInt64(&p.GlobalStats.IdleConnectionsClosed, 1)
				} else {
					select {
					case pool.Queue <- backend:
					default:
						backend.Conn.Close()
						t.TotalOpen--
						t.AtomicTotalOpen.Store(int32(t.TotalOpen))
						pool.removeFromAll(backend)
					}
				}
			default:
			}
		}
	}
}

// updateServerStats queries pg_stat_activity on the privileged connection using
// the extended protocol with binary result format to get real PostgreSQL server
// capacity regardless of non-pgfox clients.
func (t *Target) updateServerStats(p *Server, logger *logger.Logger) {
	if t.Backend == nil {
		return
	}

	if err := t.Backend.Conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return
	}
	defer t.Backend.Conn.SetReadDeadline(time.Time{})

	// Re-deploy statement if needed (defensive guard, same as fetchSCRAMVerifier).
	if !t.Backend.HasStmt(statsStmtHash) {
		parseBody := BuildParseBody(statsStmtHash, statsStmtSQL, nil)
		if err := t.Backend.WriteMessage('P', parseBody); err != nil {
			logger.WithError(err).Warn("Failed to send Parse for stats stmt")
			return
		}
	}

	// Zero params, binary results for both integer columns.
	bindBody := BuildBindBody(
		"", // unnamed portal
		statsStmtHash,
		nil,        // no params
		nil,        // no param values
		[]int16{1}, // binary result for all columns
	)
	if err := t.Backend.WriteMessage('B', bindBody); err != nil {
		logger.WithError(err).Warn("Failed to send Bind for stats stmt")
		return
	}

	execBody := BuildExecuteBody("", 0)
	if err := t.Backend.WriteMessage('E', execBody); err != nil {
		logger.WithError(err).Warn("Failed to send Execute for stats stmt")
		return
	}

	if err := t.Backend.WriteMessage('S', SyncBody); err != nil {
		logger.WithError(err).Warn("Failed to send Sync for stats stmt")
		return
	}

	var maxConns, openConns int

	for {
		msgType, body, err := t.Backend.ReadMessage()
		if err != nil {
			logger.WithError(err).Warn("Failed to read stats response")
			return
		}
		switch msgType {
		case '1': // ParseComplete (only on redeploy)
			t.Backend.MarkStmt(statsStmtHash)
			PutMsgBody(body)
		case '2': // BindComplete
			PutMsgBody(body)
		case 'D': // DataRow — binary format
			// Two int4 columns in binary:
			//   Int16  num_columns  (= 2)
			//   Int32  col0_len     (= 4)
			//   Int32  col0_val     (max_connections)
			//   Int32  col1_len     (= 4)
			//   Int32  col1_val     (count(*))
			if len(body) < 2 {
				continue
			}
			colCount := int(int16(body[0])<<8 | int16(body[1]))
			if colCount < 2 {
				continue
			}
			pos := 2
			// col 0: max_connections (int4 → 4 bytes big-endian)
			if pos+4 > len(body) {
				continue
			}
			col0Len := int(int32(body[pos])<<24 | int32(body[pos+1])<<16 |
				int32(body[pos+2])<<8 | int32(body[pos+3]))
			pos += 4
			if col0Len == 4 && pos+4 <= len(body) {
				maxConns = int(int32(body[pos])<<24 | int32(body[pos+1])<<16 |
					int32(body[pos+2])<<8 | int32(body[pos+3]))
				pos += 4
			} else {
				pos += col0Len
			}
			// col 1: count(*) (int8 → 8 bytes, or int4 → 4 bytes depending on PG version)
			if pos+4 > len(body) {
				continue
			}
			col1Len := int(int32(body[pos])<<24 | int32(body[pos+1])<<16 |
				int32(body[pos+2])<<8 | int32(body[pos+3]))
			pos += 4
			if col1Len == 4 && pos+4 <= len(body) {
				openConns = int(int32(body[pos])<<24 | int32(body[pos+1])<<16 |
					int32(body[pos+2])<<8 | int32(body[pos+3]))
			} else if col1Len == 8 && pos+8 <= len(body) {
				openConns = int(int64(body[pos])<<56 | int64(body[pos+1])<<48 |
					int64(body[pos+2])<<40 | int64(body[pos+3])<<32 |
					int64(body[pos+4])<<24 | int64(body[pos+5])<<16 |
					int64(body[pos+6])<<8 | int64(body[pos+7]))
			}
			PutMsgBody(body)
		case 'C': // CommandComplete
			PutMsgBody(body)
		case 'Z': // ReadyForQuery
			PutMsgBody(body)
			if maxConns > 0 {
				t.ServerMaxConns = maxConns
				t.ServerOpenConns = openConns
				logger.Debug("Server stats updated",
					"max_connections", maxConns,
					"open_connections", openConns,
					"available", maxConns-openConns)
			}
			return
		case 'E':
			errStr := ParseErrorMessage(body)
			PutMsgBody(body)
			logger.Warn("Stats query error", "error", errStr)
			return
		default:
			PutMsgBody(body)
		}
	}
}

// drain closes all connections on shutdown.
func (t *Target) drain(p *Server, logger *logger.Logger) {
	// Drain in-flight returns and closes first.
	for {
		select {
		case backend := <-t.CloseCh:
			backend.Conn.Close()
			t.TotalOpen--
			t.AtomicTotalOpen.Store(int32(t.TotalOpen))
		default:
			goto drainPools
		}
	}

drainPools:
	pools := t.allPools()

	for _, pool := range pools {
		for {
			select {
			case backend := <-pool.Queue:
				backend.Conn.Close()
				t.TotalOpen--
				t.AtomicTotalOpen.Store(int32(t.TotalOpen))
			default:
				goto nextPool
			}
		}
	nextPool:
	}

	if t.Backend != nil {
		t.Backend.Conn.Close()
		t.TotalOpen--
		t.AtomicTotalOpen.Store(int32(t.TotalOpen))
		t.Backend = nil
	}

	logger.Info("Target drained", "remaining_open", t.TotalOpen)
}

// openOne opens a single new backend connection for Pool and adds it to the
// Pool. Must only be called from the target goroutine.
func (t *Target) openOne(p *Server, pool *Pool, logger *logger.Logger, reason string) {
	backend, err := pool.newConn(p)
	if err != nil {
		logger.WithError(err).Warn("Failed to open backend connection",
			"reason", reason,
			"database", pool.DbName,
			"user", pool.Username)
		return
	}

	backend.Pool = pool
	pool.All = append(pool.All, backend)
	t.TotalOpen++

	select {
	case pool.Queue <- backend:
		t.signalConnReady()
		logger.Debug("Opened connection",
			"reason", reason,
			"database", pool.DbName,
			"user", pool.Username,
			"pool_total", len(pool.All),
			"target_total", t.TotalOpen)
	default:
		// backendPool full — shouldn't happen but guard.
		backend.Conn.Close()
		t.TotalOpen--
		t.AtomicTotalOpen.Store(int32(t.TotalOpen))
		pool.All = pool.All[:len(pool.All)-1]
	}
}

// closeOneIdle closes one idle connection from Pool and removes it.
// Returns true if a connection was closed.
func (t *Target) closeOneIdle(p *Server, pool *Pool, logger *logger.Logger) bool {
	select {
	case backend := <-pool.Queue:
		backend.Conn.Close()
		t.TotalOpen--
		t.AtomicTotalOpen.Store(int32(t.TotalOpen))
		pool.removeFromAll(backend)
		atomic.AddInt64(&p.GlobalStats.IdleConnectionsClosed, 1)
		logger.Debug("Shrunk Pool",
			"database", pool.DbName,
			"user", pool.Username,
			"pool_total", len(pool.All),
			"target_total", t.TotalOpen)
		return true
	default:
		return false
	}
}

// signalConnReady does a non-blocking send on connReady to wake borrowConn waiters.
func (t *Target) signalConnReady() {
	select {
	case t.ConnReady <- struct{}{}:
	default:
	}
}

// allPools flushes any pending Pool registrations into cachedPools and returns
// the slice. Must only be called from the target goroutine — no lock needed.
func (t *Target) allPools() []*Pool {
	// Drain all pending registrations before returning the slice so callers
	// always see a fully up-to-date view. This prevents the race where getPool
	// registers a Pool and immediately triggers demand, causing growthCycle to
	// run before the poolRegistered case fires in the select.
	for {
		select {
		case pool := <-t.PoolRegistered:
			t.CachedPools = append(t.CachedPools, pool)
		default:
			return t.CachedPools
		}
	}
}
