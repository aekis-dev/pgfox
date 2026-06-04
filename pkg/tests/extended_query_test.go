package tests

// extended_query_test.go — tests for the extended query protocol.
//
// pgfox's core value is multiplexing many client connections through a small
// pool by remapping client-visible statement names to a shared internal cache
// (pfx_<hash>). These tests drive the real extended-protocol message exchanges
// from the client side and let the declarative fake PostgreSQL engine
// (pgServer) react with protocol-faithful responses. The tests assert on
// pgfox's real output to the client and on what the backend actually received
// (statement names, parse counts, describe targets) via the engine's recorded
// observations — never by scripting backend responses.
//
// Playbook rows covered: T07, T08, T09, T10, T11.

import (
	"testing"
)

// selectIntRule is the canonical rule used by most extended-protocol tests:
// the prepared query "SELECT $1::int" returns a single int4 column.
func selectIntRule(rows [][]string) queryRule {
	return queryRule{
		SQL:     "SELECT $1::int",
		Columns: []pgCol{{Name: "val", OID: 23}},
		Rows:    rows,
		Tag:     "SELECT 1",
	}
}

// TestPlaybook_T07_NamedStatementRemap verifies the full two-pipeline sequence
// asyncpg uses on the first call to a cached prepared statement. pgfox must
// remap the client's statement name to an internal pfx_<hash> on the Parse,
// Describe, and Bind, without pinning the backend.
//
// Playbook §3.1 — Named statement, remappable.
func TestPlaybook_T07_NamedStatementRemap(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	fake := h.addBackend(backendSpec{
		Rules: []queryRule{selectIntRule([][]string{{"42"}})},
	})

	c := h.connect()
	defer c.conn.Close()

	// Pipeline 1: prepare + describe, Flush-terminated. No ReadyForQuery.
	c.sendParseDescribeFlush("_asyncpg_abc", "SELECT $1::int")
	c.expect('1') // ParseComplete
	c.expect('t') // ParameterDescription
	c.expect('T') // RowDescription

	// Pipeline 2: execute, Sync-terminated.
	c.sendBindExecuteSync("", "_asyncpg_abc", [][]byte{[]byte("42")})
	c.expect('2') // BindComplete
	c.expect('D') // DataRow
	c.expect('C') // CommandComplete
	c.expectRFQ('I')

	// Backend must have seen the remapped pfx_* name on Parse, Describe, Bind.
	if names := fake.ParsedNames(); len(names) != 1 || !hasPfxPrefix(names[0]) {
		t.Errorf("T07: backend Parse name should be pfx_*, got %v", names)
	}
	if ds := fake.Describes(); len(ds) != 1 || ds[0].typ != 'S' || !hasPfxPrefix(ds[0].name) {
		t.Errorf("T07: backend Describe should be ('S', pfx_*), got %v", ds)
	}
	if names := fake.BoundNames(); len(names) != 1 || !hasPfxPrefix(names[0]) {
		t.Errorf("T07: backend Bind stmt should be pfx_*, got %v", names)
	}
}

// TestPlaybook_T08_NamedStatementReuseNoReParse verifies that when the same
// backend is reused for a second call, pgfox skips Parse (HasStmt is true after
// the first deploy).
//
// Playbook §3.1 (second call reuses same backend); §6.2 (deployment tracking).
func TestPlaybook_T08_NamedStatementReuseNoReParse(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	fake := h.addBackend(backendSpec{
		Rules: []queryRule{selectIntRule([][]string{{"1"}})},
	})

	c := h.connect()
	defer c.conn.Close()

	// First call — Parse is sent; backend returned to the pool after RFQ('I').
	c.sendParseDescribeFlush("_asyncpg_xyz", "SELECT $1::int")
	c.expect('1')
	c.expect('t')
	c.expect('T')
	c.sendBindExecuteSync("", "_asyncpg_xyz", [][]byte{[]byte("1")})
	c.drainUntilRFQ()

	// Second call: Bind+Execute+Sync only (asyncpg caches the statement). pgfox
	// must recognise HasStmt=true on the reused backend and not re-Parse.
	c.sendBindExecuteSync("", "_asyncpg_xyz", [][]byte{[]byte("2")})
	c.drainUntilRFQ()

	if got := fake.ParseCount(); got != 1 {
		t.Errorf("T08: Parse should be sent exactly once, got %d", got)
	}
}

// TestPlaybook_T09_DifferentBackendInjectsParsePhase35 verifies that when the
// second call borrows a backend that has never seen the statement, pgfox
// synthesises a Parse out-of-band (phase 3.5) before forwarding the client's
// Bind+Execute+Sync.
//
// The faithful engine makes this provable: a Bind to a statement the backend
// never Parsed would draw an ErrorResponse (26000). The test passing means
// pgfox really did inject the Parse.
//
// Playbook §3.1 — second call on a different backend.
func TestPlaybook_T09_DifferentBackendInjectsParsePhase35(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	// backend1 serves the first call.
	fake1 := h.addBackend(backendSpec{
		Rules: []queryRule{selectIntRule([][]string{{"1"}})},
	})
	_ = fake1

	c := h.connect()
	defer c.conn.Close()

	c.sendParseDescribeFlush("_asyncpg_q9", "SELECT $1::int")
	c.expect('1')
	c.expect('t')
	c.expect('T')
	c.sendBindExecuteSync("", "_asyncpg_q9", [][]byte{[]byte("1")})
	c.drainUntilRFQ()

	// Drain backend1 out of the idle queue so the next borrow cannot reuse it.
	<-h.pool.Queue

	// backend2 is now the only connection available. Phase 3.5 must trigger
	// because it has never seen the statement.
	fake2 := h.addBackend(backendSpec{
		Rules: []queryRule{selectIntRule([][]string{{"2"}})},
	})

	c.sendBindExecuteSync("", "_asyncpg_q9", [][]byte{[]byte("2")})
	c.drainUntilRFQ()

	if got := fake2.ParseCount(); got != 1 {
		t.Errorf("T09: expected exactly 1 injected Parse on backend2, got %d", got)
	}
}

// TestPlaybook_T10_CloseRemappedStatementRewrittenToUnnamed verifies that a
// client Close('S', clientName) for a remapped statement is rewritten by pgfox
// to Close('S', "") — closing the unnamed slot — so pfx_<hash> is never evicted
// from the backend.
//
// Playbook §3.4 — Close for remapped statement.
func TestPlaybook_T10_CloseRemappedStatementRewrittenToUnnamed(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	// No result columns → the Describe yields NoData (exercises that path too).
	fake := h.addBackend(backendSpec{
		Rules: []queryRule{{SQL: "SELECT $1::int", Tag: "SELECT 0"}},
	})

	c := h.connect()
	defer c.conn.Close()

	c.sendParseDescribeFlush("_asyncpg_t10", "SELECT $1::int")
	c.expect('1') // ParseComplete
	c.expect('t') // ParameterDescription
	c.expect('n') // NoData — no result columns

	c.sendBindExecuteSync("", "_asyncpg_t10", [][]byte{[]byte("1")})
	c.drainUntilRFQ()

	// Client closes the statement — pgfox must NOT forward pfx_<hash>.
	c.sendCloseSync("_asyncpg_t10")
	c.expect('3') // CloseComplete
	c.expectRFQ('I')

	// The backend must have received Close with an empty name (unnamed slot),
	// never the internal pfx_<hash> name.
	cl := fake.Closes()
	if len(cl) != 1 {
		t.Fatalf("T10: expected exactly 1 Close at backend, got %v", cl)
	}
	if cl[0].typ != 'S' || cl[0].name != "" {
		t.Errorf("T10: backend Close should be ('S', \"\"), got (%q, %q) — pfx_ would evict the stmt",
			cl[0].typ, cl[0].name)
	}
}

// TestPlaybook_T11_UnnamedStatementPassthrough verifies that the unnamed
// prepared statement ("") is passed through unchanged and never renamed to
// pfx_<hash> (which would leave the unnamed slot empty and break Describe).
//
// Playbook §3.3 — Unnamed statement (asyncpg with statement_cache_size=0).
func TestPlaybook_T11_UnnamedStatementPassthrough(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	fake := h.addBackend(backendSpec{
		Rules: []queryRule{selectIntRule([][]string{{"5"}})},
	})

	c := h.connect()
	defer c.conn.Close()

	// Pipeline 1: unnamed Parse + Describe + Flush.
	c.sendParseDescribeFlush("", "SELECT $1::int")
	c.expect('1') // ParseComplete
	c.expect('t') // ParameterDescription
	c.expect('T') // RowDescription

	// Pipeline 2: Bind + Execute + Sync using the unnamed slot.
	c.sendBindExecuteSync("", "", [][]byte{[]byte("5")})
	c.expect('2') // BindComplete
	c.expect('D') // DataRow
	c.expect('C') // CommandComplete
	c.expectRFQ('I')

	// The backend must have received Parse("") — pgfox must not have renamed it.
	if names := fake.ParsedNames(); len(names) == 0 || names[0] != "" {
		t.Errorf("T11: backend Parse name should be empty (unnamed), got %v", names)
	}
}
