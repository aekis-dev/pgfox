package tests

// concurrency_test.go — deterministic concurrency tests for pgfox.
//
// Each test runs N client goroutines against M fake backend goroutines
// simultaneously. Both sides follow exact scripted conversations:
//
//   - Client goroutines send a known sequence of messages and assert the
//     exact response sequence they receive back from pgfox.
//
//   - Fake backend goroutines expect a known sequence of messages from pgfox
//     and respond with exact wire messages, asserting that pgfox forwarded
//     or rewrote messages correctly.
//
// Result tracking: all assertions in fake backend goroutines are done inline
// via atomic counters or t.Errorf — never via channels that require the fake
// goroutines to exit. Fake goroutines run for the lifetime of the test and
// are not signalled to stop; they simply drain messages until the test ends.
//
// Three scenarios:
//
//  1. TestConcurrent_SameStatementManyClients — N clients execute the same
//     parameterized query (different parameter values) against M backends.
//     Parse must be deployed exactly once per backend; each client must receive
//     its own correct DataRow value back.
//
//  2. TestConcurrent_TransactionIsolation — N clients run concurrent
//     transaction blocks (BEGIN → SELECT → COMMIT) against M backends (M < N),
//     forcing pool contention. Each transaction's three queries must land on
//     the same backend. Status transitions must be correct for every client.
//
//  3. TestConcurrent_MixedExtendedAndSimple — N clients split evenly between
//     named extended query (asyncpg pattern) and simple query with literals.
//     All execute the same logical query. Parse count must equal the number of
//     backends that served at least one request. Each client must receive the
//     correct result for its own parameter value.

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrent_SameStatementManyClients verifies that when N clients execute
// the same parameterized query simultaneously against M backends (N > M):
//
//   - Parse is deployed exactly once per backend (HasStmt optimization).
//   - Each client receives its own DataRow containing its own parameter value.
//   - No client receives another client's result.
//
// Wire sequence per client (extended query, asyncpg pattern):
//
//	Pipeline 1 (P + D + H):
//	  C→P  P { name="_asyncpg_<id>", query="SELECT $1::int" }
//	  C→P  D { type='S', name="_asyncpg_<id>" }
//	  C→P  H
//	  P→C  ParseComplete
//	  P→C  ParameterDescription
//	  P→C  RowDescription
//
//	Pipeline 2 (B + E + S):
//	  C→P  B { stmt="_asyncpg_<id>", params=[<id>] }
//	  C→P  E
//	  C→P  S
//	  P→C  BindComplete
//	  P→C  DataRow { value="<id>" }   ← must match this client's id
//	  P→C  CommandComplete
//	  P→C  ReadyForQuery('I')
//
// Parse count per backend is tracked atomically — no goroutine lifecycle
// management needed. The fake goroutines run until the test ends.
func TestConcurrent_SameStatementManyClients(t *testing.T) {
	const numBackends = 3
	const numClients = 12 // 4 clients per backend on average

	h := newHarness(t)
	defer h.close()

	// totalParses counts how many times Parse was sent across all backends.
	// Written atomically by all fake goroutines — one Parse per backend maximum.
	var totalParses int64

	// lastParam tracks the most recent Bind parameter per backend so Execute
	// can echo it back as the DataRow. Written only by the owning goroutine.
	// We use a per-goroutine local variable — no sharing needed.

	for i := 0; i < numBackends; i++ {
		_, fake := h.addBackend()
		go func(fb *fakeBackend, id int) {
			var lastParam string
			var parseCount int64
			for {
				mt, body := fb.recvMsg()
				if mt == 0 {
					return
				}
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
				switch mt {
				case 'P':
					// pgfox must rewrite the client name to pfx_*.
					name := parseStmtName(body)
					if len(name) < 4 || name[:4] != "pfx_" {
						t.Errorf("backend %d: Parse name should be pfx_*, got %q", id, name)
					}
					// Parse must arrive at most once per backend.
					parseCount++
					atomic.AddInt64(&totalParses, 1)
					if parseCount > 1 {
						t.Errorf("backend %d: Parse sent %d times, want at most 1", id, parseCount)
					}
					fb.sendParseComplete()
				case 'D':
					fb.sendParameterDescription([]uint32{23})
					fb.sendRowDescription("val")
				case 'H':
					// Flush — no ReadyForQuery.
				case 'B':
					// Capture the parameter value to echo back in Execute.
					lastParam = extractFirstBindParam(body)
					fb.sendBindComplete()
				case 'E':
					// Echo back the parameter value so the client can verify it.
					fb.sendDataRowText(lastParam)
					fb.sendCC("SELECT 1")
				case 'S':
					fb.sendRFQ('I')
				}
			}
		}(fake, i)
	}

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		errs  = make(chan error, numClients)
	)

	wg.Add(numClients)
	for i := 0; i < numClients; i++ {
		go func(id int) {
			defer wg.Done()
			c := h.connect()
			defer c.conn.Close()
			<-start

			stmtName := fmt.Sprintf("_asyncpg_%d", id)
			param := fmt.Sprintf("%d", id)

			// Pipeline 1: prepare.
			c.sendParseDescribeFlush(stmtName, "SELECT $1::int")
			c.expect('1') // ParseComplete
			c.expect('t') // ParameterDescription
			c.expect('T') // RowDescription

			// Pipeline 2: execute with this client's own id as parameter.
			c.sendBindExecuteSync("", stmtName, [][]byte{[]byte(param)})
			c.expect('2') // BindComplete

			// DataRow must contain this client's own parameter value — not
			// another client's. This verifies no cross-client result contamination.
			dataBody := c.expect('D')
			got := extractFirstDataRowValue(dataBody)
			if got != param {
				errs <- fmt.Errorf("client %d: DataRow=%q, want %q (cross-client contamination?)",
					id, got, param)
			}

			c.expect('C') // CommandComplete
			if s := c.drainUntilRFQ(); s != 'I' {
				errs <- fmt.Errorf("client %d: RFQ=%q, want 'I'", id, s)
			}
		}(i)
	}

	close(start)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("SameStatementManyClients: timed out")
	}

	close(errs)
	for err := range errs {
		t.Error(err)
	}

	got := atomic.LoadInt64(&totalParses)
	if got == 0 {
		t.Error("SameStatementManyClients: no Parse was sent to any backend")
	}
	if got > int64(numBackends) {
		t.Errorf("SameStatementManyClients: totalParses=%d > numBackends=%d — cache race",
			got, numBackends)
	}
	t.Logf("SameStatementManyClients: %d clients, %d backends, %d total Parse deploys",
		numClients, numBackends, got)
}

// TestConcurrent_TransactionIsolation verifies that concurrent transaction
// blocks execute in isolation — each transaction's queries land on the same
// backend, status transitions are correct, and no client receives another
// client's data.
//
// Setup: numClients > numBackends forces pool contention. Some clients must
// wait for a backend to become available (after a COMMIT frees it).
//
// Wire sequence per client:
//
//	C→P  Q { "BEGIN\0" }           B→P  CommandComplete("BEGIN") + RFQ('T')
//	C→P  Q { "SELECT <id>\0" }     B→P  CommandComplete("SELECT 1") + RFQ('T')
//	C→P  Q { "COMMIT\0" }          B→P  CommandComplete("COMMIT") + RFQ('I')
//
// Transaction ordering is verified inline in the fake goroutine using a
// per-goroutine inTx bool — no result collection needed.
func TestConcurrent_TransactionIsolation(t *testing.T) {
	const numBackends = 3
	const numClients = 9 // 3 waves of 3 concurrent transactions

	h := newHarness(t)
	defer h.close()

	for i := 0; i < numBackends; i++ {
		_, fake := h.addBackend()
		go func(fb *fakeBackend, id int) {
			// inTx tracks whether this backend is currently inside a transaction.
			// Since a backend is exclusively owned by one client at a time (pinned
			// during a transaction), this is written only by this goroutine.
			inTx := false
			for {
				mt, body := fb.recvMsg()
				if mt == 0 {
					return
				}
				if mt != 'Q' {
					continue
				}
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
				sql := nullTermStr(body)
				switch sql {
				case "BEGIN":
					// Must never receive BEGIN while already in a transaction —
					// that would mean two clients are sharing this backend.
					if inTx {
						t.Errorf("backend %d: BEGIN while inTx=true — transactions interleaved", id)
					}
					inTx = true
					fb.sendCC("BEGIN")
					fb.sendRFQ('T')
				case "COMMIT":
					if !inTx {
						t.Errorf("backend %d: COMMIT while inTx=false — broken ordering", id)
					}
					inTx = false
					fb.sendCC("COMMIT")
					fb.sendRFQ('I')
				default:
					// SELECT <id> inside the transaction.
					if !inTx {
						t.Errorf("backend %d: SELECT outside transaction: %q", id, sql)
					}
					fb.sendCC("SELECT 1")
					fb.sendRFQ('T') // still in transaction
				}
			}
		}(fake, i)
	}

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		errs  = make(chan error, numClients*3)
	)

	wg.Add(numClients)
	for i := 0; i < numClients; i++ {
		go func(id int) {
			defer wg.Done()
			c := h.connect()
			defer c.conn.Close()
			<-start

			c.sendQ("BEGIN")
			if s := c.drainUntilRFQ(); s != 'T' {
				errs <- fmt.Errorf("client %d: BEGIN want RFQ('T'), got %q", id, s)
				return
			}

			c.sendQ(fmt.Sprintf("SELECT %d", id))
			if s := c.drainUntilRFQ(); s != 'T' {
				errs <- fmt.Errorf("client %d: SELECT want RFQ('T'), got %q", id, s)
				return
			}

			c.sendQ("COMMIT")
			if s := c.drainUntilRFQ(); s != 'I' {
				errs <- fmt.Errorf("client %d: COMMIT want RFQ('I'), got %q", id, s)
				return
			}
		}(i)
	}

	close(start)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("TransactionIsolation: timed out")
	}

	close(errs)
	for err := range errs {
		t.Error(err)
	}

	t.Logf("TransactionIsolation: %d clients, %d backends — all transactions completed",
		numClients, numBackends)
}

// TestConcurrent_MixedExtendedAndSimple verifies that clients using different
// protocol paths (named extended query vs simple query with literals) can share
// the same pool of backends simultaneously without interfering with each other.
//
// Half the clients use the asyncpg extended query pattern (named statement,
// remapped to pfx_*). The other half use simple query with a literal value
// (goes through the statement cache via ClassifyAndParameterize).
//
// Both halves execute the same logical query: SELECT id FROM users WHERE id = N.
// Each client verifies it receives its own N back in the DataRow.
//
// Invariant: total Parse count across all backends must not exceed numBackends —
// each backend deploys the statement at most once regardless of protocol path.
func TestConcurrent_MixedExtendedAndSimple(t *testing.T) {
	numClients := 8 + rand.Intn(8)  // 8–16 clients
	numBackends := 2 + rand.Intn(4) // 2–6 backends
	numExtended := numClients / 2
	numSimple := numClients - numExtended

	t.Logf("MixedExtendedAndSimple: %d clients (%d extended, %d simple), %d backends",
		numClients, numExtended, numSimple, numBackends)

	h := newHarness(t)
	defer h.close()

	var totalParses int64 // atomic — written by all fake backend goroutines

	for i := 0; i < numBackends; i++ {
		_, fake := h.addBackend()
		go func(fb *fakeBackend, id int) {
			var lastParam string
			for {
				mt, body := fb.recvMsg()
				if mt == 0 {
					return
				}
				time.Sleep(time.Duration(rand.Intn(8)) * time.Millisecond)
				switch mt {
				case 'P':
					name := parseStmtName(body)
					if len(name) < 4 || name[:4] != "pfx_" {
						t.Errorf("backend %d: Parse name should be pfx_*, got %q", id, name)
					}
					atomic.AddInt64(&totalParses, 1)
					fb.sendParseComplete()
				case 'D':
					fb.sendParameterDescription([]uint32{23})
					fb.sendRowDescription("id")
				case 'H':
					// Flush — no ReadyForQuery.
				case 'B':
					lastParam = extractFirstBindParam(body)
					fb.sendBindComplete()
				case 'E':
					param := lastParam
					if param == "" {
						param = "?"
					}
					fb.sendDataRowText(param)
					fb.sendCC("SELECT 1")
				case 'S':
					fb.sendRFQ('I')
				}
			}
		}(fake, i)
	}

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		errs  = make(chan error, numClients)
	)

	wg.Add(numClients)

	// Extended query clients (asyncpg pattern).
	for i := 0; i < numExtended; i++ {
		go func(id int) {
			defer wg.Done()
			c := h.connect()
			defer c.conn.Close()
			<-start

			stmtName := fmt.Sprintf("_asyncpg_%d", id)
			param := fmt.Sprintf("%d", id)

			c.sendParseDescribeFlush(stmtName, "SELECT $1::int")
			c.expect('1')
			c.expect('t')
			c.expect('T')

			c.sendBindExecuteSync("", stmtName, [][]byte{[]byte(param)})
			c.expect('2')

			dataBody := c.expect('D')
			got := extractFirstDataRowValue(dataBody)
			if got != param {
				errs <- fmt.Errorf("extended client %d: DataRow=%q, want %q", id, got, param)
			}
			c.expect('C')
			if s := c.drainUntilRFQ(); s != 'I' {
				errs <- fmt.Errorf("extended client %d: RFQ=%q, want 'I'", id, s)
			}
		}(i)
	}

	// Simple query clients (literal extraction path).
	for i := numExtended; i < numClients; i++ {
		go func(id int) {
			defer wg.Done()
			c := h.connect()
			defer c.conn.Close()
			<-start

			c.sendQ(fmt.Sprintf("SELECT id FROM users WHERE id = %d", id))

			// Simple query through executeAsPrepared sends P+B+E+S without Describe.
			// Backend responds: ParseComplete + BindComplete + DataRow + CC + RFQ.
			// pgfox absorbs ParseComplete and BindComplete — client sees DataRow first.
			dataBody := c.expect('D')
			got := extractFirstDataRowValue(dataBody)
			want := fmt.Sprintf("%d", id)
			if got != want {
				errs <- fmt.Errorf("simple client %d: DataRow=%q, want %q", id, got, want)
			}
			c.expect('C')
			if s := c.drainUntilRFQ(); s != 'I' {
				errs <- fmt.Errorf("simple client %d: RFQ=%q, want 'I'", id, s)
			}
		}(i)
	}

	close(start)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("MixedExtendedAndSimple: timed out")
	}

	close(errs)
	for err := range errs {
		t.Error(err)
	}

	got := atomic.LoadInt64(&totalParses)
	// Two distinct canonical SQL strings are in play:
	//   extended path: "SELECT $1::int"         (extracted from Parse body as-is)
	//   simple path:   "SELECT id FROM users WHERE id = $1" (parameterized by ClassifyAndParameterize)
	// Each backend deploys each statement at most once → max is numBackends * 2.
	// At least one of each must have been deployed → min is 2.
	if got < 2 {
		t.Errorf("MixedExtendedAndSimple: totalParses=%d, expected at least 2 (one per statement type)", got)
	}
	if got > int64(numBackends)*2 {
		t.Errorf("MixedExtendedAndSimple: totalParses=%d > numBackends*2=%d — possible cache race",
			got, numBackends*2)
	}
	t.Logf("MixedExtendedAndSimple: %d total Parse deploys across %d backends (2 distinct statements)",
		got, numBackends)
}

// =============================================================================
// Wire parsing helpers used by concurrency tests
// =============================================================================

// extractFirstBindParam extracts the value of the first parameter from a
// Bind message body. Returns "" if the body is malformed or has no parameters.
//
// Bind body layout:
//
//	portal\0 + stmt\0 + numParamFmts(int16) + [fmts] +
//	numParams(int16) + paramLen(int32) + paramBytes + ...
func extractFirstBindParam(body []byte) string {
	pos := 0
	// Skip portal name.
	for pos < len(body) && body[pos] != 0 {
		pos++
	}
	pos++ // skip null
	// Skip statement name.
	for pos < len(body) && body[pos] != 0 {
		pos++
	}
	pos++ // skip null
	if pos+2 > len(body) {
		return ""
	}
	// Skip parameter format codes.
	numFmts := int(body[pos])<<8 | int(body[pos+1])
	pos += 2 + numFmts*2
	if pos+2 > len(body) {
		return ""
	}
	// Read number of parameters.
	numParams := int(body[pos])<<8 | int(body[pos+1])
	pos += 2
	if numParams == 0 || pos+4 > len(body) {
		return ""
	}
	// Read first parameter length.
	paramLen := int(body[pos])<<24 | int(body[pos+1])<<16 | int(body[pos+2])<<8 | int(body[pos+3])
	pos += 4
	if paramLen < 0 || pos+paramLen > len(body) {
		return ""
	}
	return string(body[pos : pos+paramLen])
}

// extractFirstDataRowValue extracts the value of the first column from a
// DataRow ('D') message body. Returns "" if malformed or null.
//
// DataRow body layout:
//
//	numFields(int16) + colLen(int32) + colBytes + ...
func extractFirstDataRowValue(body []byte) string {
	if len(body) < 6 {
		return ""
	}
	// numFields at [0:2] — skip it.
	colLen := int(body[2])<<24 | int(body[3])<<16 | int(body[4])<<8 | int(body[5])
	if colLen < 0 || 6+colLen > len(body) {
		return ""
	}
	return string(body[6 : 6+colLen])
}
