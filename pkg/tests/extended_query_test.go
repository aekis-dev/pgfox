package tests

// extended_query_test.go — tests for the extended query protocol.
//
// The extended protocol allows clients to separate query parsing, parameter
// binding, and execution into distinct message exchanges. pgfox's core value
// is multiplexing many client connections through a small pool by remapping
// client-visible statement names to a shared internal cache (pfx_<hash>).
//
// These tests verify the exact wire message sequences for:
//   - Named statement remapping (asyncpg with statement cache, the common case)
//   - Statement reuse across calls on the same backend (HasStmt optimization)
//   - Statement injection when a different backend is borrowed (phase 3.5)
//   - Close handling for remapped statements (pgfox owns the lifecycle)
//   - Unnamed statement passthrough (asyncpg with statement_cache_size=0)
//   - Non-remappable statement passthrough with connection pinning
//
// Playbook rows covered: T07, T08, T09, T10, T11, T12.

import (
	"testing"
)

// TestPlaybook_T07_NamedStatementRemap verifies the full two-pipeline sequence
// that asyncpg uses for its first call to a cached prepared statement. pgfox
// must remap the client's statement name to an internal pfx_<hash> name on
// both the Parse and the subsequent Bind/Describe messages, without pinning
// the backend connection (since the statement lives in the shared cache and
// any backend can serve it).
//
// Playbook §3.1 — Named statement, remappable (asyncpg with statement cache).
//
// Wire sequence:
//
//	Pipeline 1 (prepare + describe, Flush-terminated):
//	  C→P  P { name="_asyncpg_abc", query="SELECT $1::int" }
//	  C→P  D { type='S', name="_asyncpg_abc" }
//	  C→P  H
//	  P→B  P { name="pfx_<hash>", query="SELECT $1::int" }
//	  P→B  D { type='S', name="pfx_<hash>" }
//	  P→B  H
//	  B→P  ParseComplete
//	  B→P  ParameterDescription
//	  B→P  RowDescription
//	  P→C  ParseComplete
//	  P→C  ParameterDescription
//	  P→C  RowDescription
//	  (no ReadyForQuery — Flush does not produce one)
//
//	Pipeline 2 (execute, Sync-terminated):
//	  C→P  B { portal="", stmt="_asyncpg_abc", params=[42] }
//	  C→P  E { portal="" }
//	  C→P  S
//	  P→B  B { portal="", stmt="pfx_<hash>", params=[42] }
//	  P→B  E
//	  P→B  S
//	  B→P  BindComplete
//	  B→P  DataRow
//	  B→P  CommandComplete
//	  B→P  ReadyForQuery { 'I' }
//	  P→C  BindComplete
//	  P→C  DataRow
//	  P→C  CommandComplete
//	  P→C  ReadyForQuery { 'I' }
//	  (backend returned to pool — no pinning)
func TestPlaybook_T07_NamedStatementRemap(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	_, fake := h.addBackend()

	go func() {
		for {
			mt, body := fake.recvMsg()
			if mt == 0 {
				return
			}
			switch mt {
			case 'P':
				// pgfox must rewrite the client's _asyncpg_abc name to pfx_*.
				name := parseStmtName(body)
				if len(name) < 4 || name[:4] != "pfx_" {
					t.Errorf("T07: backend Parse name should be pfx_*, got %q", name)
				}
				fake.sendParseComplete()

			case 'D':
				// Describe must also reference the remapped pfx_* name.
				descType, descName := closeStmtTarget(body)
				if descType == 'S' && (len(descName) < 4 || descName[:4] != "pfx_") {
					t.Errorf("T07: Describe name should be pfx_*, got %q", descName)
				}
				fake.sendParameterDescription([]uint32{23}) // int4
				fake.sendRowDescription("val")

			case 'H':
				// Flush — backend does not send ReadyForQuery.

			case 'B':
				// Bind must reference the remapped pfx_* name.
				name := bindStmtName(body)
				if len(name) < 4 || name[:4] != "pfx_" {
					t.Errorf("T07: Bind stmt should be pfx_*, got %q", name)
				}
				fake.sendBindComplete()

			case 'E':
				fake.sendDataRowText("42")
				fake.sendCC("SELECT 1")

			case 'S':
				fake.sendRFQ('I')
			}
		}
	}()

	c := h.connect()
	defer c.conn.Close()

	// Pipeline 1: asyncpg prepare step.
	c.sendParseDescribeFlush("_asyncpg_abc", "SELECT $1::int")

	// Flush response: ParseComplete + ParameterDescription + RowDescription.
	// No ReadyForQuery after Flush.
	c.expect('1') // ParseComplete
	c.expect('t') // ParameterDescription
	c.expect('T') // RowDescription

	// Pipeline 2: asyncpg execute step.
	c.sendBindExecuteSync("", "_asyncpg_abc", [][]byte{[]byte("42")})

	c.expect('2') // BindComplete
	c.expect('D') // DataRow
	c.expect('C') // CommandComplete
	c.expectRFQ('I')
}

// TestPlaybook_T08_NamedStatementReuseNoReParse verifies that when the same
// backend is reused for a second query, pgfox skips sending Parse (HasStmt is
// true after the first successful deploy). This is the key optimization that
// makes prepared statement pooling efficient.
//
// Playbook §3.1 — Named statement, second call reuses same backend.
// Playbook §6.2 — Deployment tracking per backend (MarkStmt / HasStmt).
//
// Wire sequence (second call, same backend):
//
//	C→P  B { stmt="_asyncpg_xyz", params=[2] }
//	C→P  E
//	C→P  S
//	P→B  B { stmt="pfx_<hash>", params=[2] }   ← no Parse prefix
//	P→B  E
//	P→B  S
func TestPlaybook_T08_NamedStatementReuseNoReParse(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	_, fake := h.addBackend()
	parseCount := 0

	go func() {
		for {
			mt, _ := fake.recvMsg()
			if mt == 0 {
				return
			}
			switch mt {
			case 'P':
				parseCount++
				fake.sendParseComplete()
			case 'D':
				fake.sendParameterDescription([]uint32{23})
				fake.sendRowDescription("val")
			case 'H':
				// Flush — no response needed.
			case 'B':
				fake.sendBindComplete()
			case 'E':
				fake.sendDataRowText("1")
				fake.sendCC("SELECT 1")
			case 'S':
				fake.sendRFQ('I')
			}
		}
	}()

	c := h.connect()
	defer c.conn.Close()

	// First call — Parse is sent; backend MarkStmt is called on ParseComplete.
	// returnConn automatically returns the backend to the pool after RFQ('I').
	c.sendParseDescribeFlush("_asyncpg_xyz", "SELECT $1::int")
	c.expect('1')
	c.expect('t')
	c.expect('T')
	c.sendBindExecuteSync("", "_asyncpg_xyz", [][]byte{[]byte("1")})
	c.drainUntilRFQ()

	// Second call: asyncpg sends only Bind+Execute+Sync (no Parse — it caches
	// the statement). pgfox must recognize HasStmt=true and skip Parse.
	// The same backend is reused from the pool automatically.
	c.sendBindExecuteSync("", "_asyncpg_xyz", [][]byte{[]byte("2")})
	c.drainUntilRFQ()

	if parseCount != 1 {
		t.Errorf("T08: Parse should be sent exactly once, got %d", parseCount)
	}
}

// TestPlaybook_T09_DifferentBackendInjectsParsePhase35 verifies that when the
// second call borrows a backend that has never seen the statement, pgfox
// synthesizes a Parse + Sync out-of-band (phase 3.5) to deploy the statement
// before forwarding the client's Bind+Execute+Sync.
//
// Playbook §3.1 — Named statement, second call on different backend.
// Playbook §3.1 step "Subsequent calls".
//
// Wire sequence (second call, different backend B2):
//
//	C→P  B { stmt="_asyncpg_q9", params=[2] }
//	C→P  E
//	C→P  S
//	P→B2 P { name="pfx_<hash>", query="SELECT $1::int" }   ← synthesized
//	P→B2 S
//	B2→P ParseComplete
//	B2→P ReadyForQuery
//	P→B2 B { stmt="pfx_<hash>", params=[2] }
//	P→B2 E
//	P→B2 S
func TestPlaybook_T09_DifferentBackendInjectsParsePhase35(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	// Only backend1 is in the queue initially. backend2 is added only after
	// backend1 is drained out — this guarantees the second borrow gets backend2.
	_, fake1 := h.addBackend()

	// fake1 serves the first call (P+D+H then B+E+S).
	go func() {
		for {
			mt, _ := fake1.recvMsg()
			if mt == 0 {
				return
			}
			switch mt {
			case 'P':
				fake1.sendParseComplete()
			case 'D':
				fake1.sendParameterDescription(nil)
				fake1.sendRowDescription("v")
			case 'H':
			case 'B':
				fake1.sendBindComplete()
			case 'E':
				fake1.sendDataRowText("1")
				fake1.sendCC("SELECT 1")
			case 'S':
				fake1.sendRFQ('I')
			}
		}
	}()

	c := h.connect()
	defer c.conn.Close()

	// First call → backend1 gets Parse and MarkStmt.
	c.sendParseDescribeFlush("_asyncpg_q9", "SELECT $1::int")
	c.expect('1')
	c.expect('t')
	c.expect('T')
	c.sendBindExecuteSync("", "_asyncpg_q9", [][]byte{[]byte("1")})
	c.drainUntilRFQ()

	// returnConn returned backend1 to the queue after RFQ('I').
	// Drain it out permanently — backend1 must not be borrowed again.
	<-h.pool.Queue

	// Now add backend2 — it is the only connection available for the second borrow.
	// Phase 3.5 must trigger because backend2 has never seen the statement.
	parseCount2 := 0
	_, fake2 := h.addBackend()
	go func() {
		for {
			mt, body := fake2.recvMsg()
			if mt == 0 {
				return
			}
			switch mt {
			case 'P':
				// Must be a pfx_* synthesized Parse (phase 3.5 injection).
				name := parseStmtName(body)
				if len(name) >= 4 && name[:4] == "pfx_" {
					parseCount2++
				}
				fake2.sendParseComplete()
			case 'B':
				fake2.sendBindComplete()
			case 'E':
				fake2.sendDataRowText("2")
				fake2.sendCC("SELECT 1")
			case 'S':
				fake2.sendRFQ('I')
			}
		}
	}()

	// Second call: only B+E+S. pgfox must inject Parse on backend2 (phase 3.5).
	c.sendBindExecuteSync("", "_asyncpg_q9", [][]byte{[]byte("2")})
	c.drainUntilRFQ()

	if parseCount2 != 1 {
		t.Errorf("T09: expected 1 injected Parse on backend2, got %d", parseCount2)
	}
}

// TestPlaybook_T10_CloseRemappedStatementRewrittenToUnnamed verifies that when
// a client sends Close('S', clientName) for a remapped statement, pgfox
// intercepts it and rewrites it to Close('S', ") — closing the unnamed slot
// (a no-op if empty). This prevents pgfox from accidentally evicting pfx_<hash>
// from the backend while its deployedStmts map still says it is deployed.
//
// Playbook §3.4 — Close for remapped statement.
//
// Invariant: pfx_<hash> is never closed by client request. pgfox owns the
// backend lifetime of remapped statements.
//
// Wire sequence:
//
//	C→P  C { type='S', name="_asyncpg_t10" }
//	C→P  S
//	P→B  C { type='S', name="" }    ← rewritten to close unnamed slot
//	P→B  S
//	B→P  CloseComplete
//	B→P  ReadyForQuery { 'I' }
//	P→C  CloseComplete
//	P→C  ReadyForQuery { 'I' }
func TestPlaybook_T10_CloseRemappedStatementRewrittenToUnnamed(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	_, fake := h.addBackend()

	var backendCloseName string
	go func() {
		for {
			mt, body := fake.recvMsg()
			if mt == 0 {
				return
			}
			switch mt {
			case 'P':
				fake.sendParseComplete()
			case 'D':
				fake.sendParameterDescription(nil)
				fake.sendNoData()
			case 'H':
			case 'B':
				fake.sendBindComplete()
			case 'E':
				fake.sendCC("SELECT 0")
			case 'C':
				// Record what the backend actually sees for the Close message.
				_, backendCloseName = closeStmtTarget(body)
				fake.sendCloseComplete()
			case 'S':
				fake.sendRFQ('I')
			}
		}
	}()

	c := h.connect()
	defer c.conn.Close()

	// Prepare and execute the statement to register the name mapping.
	c.sendParseDescribeFlush("_asyncpg_t10", "SELECT $1::int")
	c.expect('1')
	c.expect('t')
	c.expect('n') // NoData — no result columns for this Describe variant

	c.sendBindExecuteSync("", "_asyncpg_t10", [][]byte{[]byte("1")})
	c.drainUntilRFQ()

	// Client closes the statement — pgfox must NOT forward pfx_<hash>.
	c.sendCloseSync("_asyncpg_t10")
	c.expect('3') // CloseComplete
	c.expectRFQ('I')

	// The backend must have received Close with an empty name (unnamed slot),
	// not the internal pfx_<hash> name.
	if backendCloseName != "" {
		t.Errorf("T10: backend Close target should be empty, got %q (pfx_ would evict the stmt)", backendCloseName)
	}
}

// TestPlaybook_T11_UnnamedStatementPassthrough verifies that the unnamed
// prepared statement ("") is passed through to the backend unchanged and is
// never registered in the shared statement cache. The backend is pinned for
// the duration of the pipeline and released after the Sync response.
//
// Playbook §3.3 — Unnamed statement (asyncpg with statement_cache_size=0).
//
// Invariant: the unnamed slot is per-connection. pgfox must never rename
// Parse("") to pfx_<hash>, which would leave the unnamed slot empty and
// cause Describe("S","") to fail.
//
// Wire sequence:
//
//	Pipeline 1 (Flush):
//	  C→P  P { name="", query="SELECT $1::int" }
//	  C→P  D { type='S', name="" }
//	  C→P  H
//	  P→B  P { name="", query="SELECT $1::int" }   ← unchanged
//	  P→B  D { type='S', name="" }                 ← unchanged
//	  P→B  H
//	  B→P  ParseComplete
//	  B→P  ParameterDescription
//	  B→P  RowDescription
//	  P→C  ParseComplete
//	  P→C  ParameterDescription
//	  P→C  RowDescription
//
//	Pipeline 2 (Sync):
//	  C→P  B { stmt="", params=[5] }
//	  C→P  E
//	  C→P  S
//	  (same pinned backend — unnamed slot is available)
//	  P→C  BindComplete
//	  P→C  DataRow
//	  P→C  CommandComplete
//	  P→C  ReadyForQuery { 'I' }
//	  (backend unpinned after Sync with status 'I')
func TestPlaybook_T11_UnnamedStatementPassthrough(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	_, fake := h.addBackend()

	var backendParseNames []string
	go func() {
		for {
			mt, body := fake.recvMsg()
			if mt == 0 {
				return
			}
			switch mt {
			case 'P':
				// Record the name — must stay "" (unnamed).
				name := parseStmtName(body)
				backendParseNames = append(backendParseNames, name)
				fake.sendParseComplete()
			case 'D':
				fake.sendParameterDescription([]uint32{23})
				fake.sendRowDescription("val")
			case 'H':
				// Flush — no ReadyForQuery.
			case 'B':
				fake.sendBindComplete()
			case 'E':
				fake.sendDataRowText("5")
				fake.sendCC("SELECT 1")
			case 'S':
				fake.sendRFQ('I')
			}
		}
	}()

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
	if len(backendParseNames) == 0 || backendParseNames[0] != "" {
		t.Errorf("T11: backend Parse name should be empty (unnamed), got %v", backendParseNames)
	}
}
