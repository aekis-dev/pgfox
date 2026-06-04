package tests

// concurrency_test.go — deterministic concurrency tests for pgfox.
//
// Each test runs N client goroutines against M declarative fake backends
// simultaneously. The client goroutines send known message sequences and assert
// the exact responses pgfox produces; the fake backends are driven by the
// pgServer engine from a backendSpec and react with protocol-faithful
// responses. No test scripts backend bytes — the engine derives them from data.
//
// Echo rules let each backend return the client's own bound parameter, so
// cross-client contamination is detectable. Parse counts are read from each
// engine's recorded observations after the run.
//
// Three scenarios:
//   1. SameStatementManyClients — N clients, same parameterized query, M
//      backends. Parse deployed at most once per backend; each client gets its
//      own DataRow value.
//   2. TransactionIsolation — N concurrent transaction blocks, M<N backends.
//      Each transaction's queries land on one backend; the engine's
//      checkTxInvariant catches any interleaving automatically.
//   3. MixedExtendedAndSimple — half extended (asyncpg), half simple-with-
//      literals, all the same logical query. Parse count bounded by backends.

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestConcurrent_SameStatementManyClients verifies that when N clients execute
// the same parameterized query simultaneously against M backends (N > M):
//   - Parse is deployed at most once per backend (HasStmt optimization).
//   - Each client receives its own DataRow value (no cross-client contamination).
func TestConcurrent_SameStatementManyClients(t *testing.T) {
	const numBackends = 3
	const numClients = 12

	h := newHarness(t)
	defer h.close()

	fakes := make([]*pgServer, numBackends)
	for i := 0; i < numBackends; i++ {
		fakes[i] = h.addBackend(backendSpec{
			Delay: time.Duration(rand.Intn(5)) * time.Millisecond, // induce interleaving
			Rules: []queryRule{
				{SQL: "SELECT $1::int",
					Columns: []pgCol{{Name: "val", OID: 23}},
					Echo:    true, // DataRow = this client's bound parameter
					Tag:     "SELECT 1"},
			},
		})
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

			c.sendParseDescribeFlush(stmtName, "SELECT $1::int")
			c.expect('1')
			c.expect('t')
			c.expect('T')

			c.sendBindExecuteSync("", stmtName, [][]byte{[]byte(param)})
			c.expect('2')
			dataBody := c.expect('D')
			if got := extractFirstDataRowValue(dataBody); got != param {
				errs <- fmt.Errorf("client %d: DataRow=%q, want %q (cross-client contamination?)", id, got, param)
			}
			c.expect('C')
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

	// Parse must be deployed at most once per backend, at least once overall,
	// and always under a pfx_* name.
	var total int
	for i, f := range fakes {
		if pc := f.ParseCount(); pc > 1 {
			t.Errorf("backend %d: Parse sent %d times, want at most 1", i, pc)
		}
		for _, n := range f.ParsedNames() {
			if !hasPfxPrefix(n) {
				t.Errorf("backend %d: Parse name should be pfx_*, got %q", i, n)
			}
		}
		total += f.ParseCount()
	}
	if total == 0 {
		t.Error("SameStatementManyClients: no Parse was sent to any backend")
	}
	if total > numBackends {
		t.Errorf("SameStatementManyClients: total Parse=%d > backends=%d — cache race", total, numBackends)
	}
	t.Logf("SameStatementManyClients: %d clients, %d backends, %d total Parse deploys", numClients, numBackends, total)
}

// TestConcurrent_TransactionIsolation verifies that concurrent transaction
// blocks execute in isolation: each transaction's queries land on one backend
// and status transitions are correct. numClients > numBackends forces pool
// contention. The engine's checkTxInvariant fires if two clients ever share a
// pinned backend (BEGIN while already in transaction).
func TestConcurrent_TransactionIsolation(t *testing.T) {
	const numBackends = 3
	const numClients = 9

	h := newHarness(t)
	defer h.close()

	for i := 0; i < numBackends; i++ {
		h.addBackend(backendSpec{
			Delay: time.Duration(rand.Intn(8)) * time.Millisecond,
			Rules: []queryRule{
				{SQL: "BEGIN", Tag: "BEGIN", Tx: txBegin},
				{SQL: "COMMIT", Tag: "COMMIT", Tx: txEnd},
			},
			Default: &queryRule{
				Columns: []pgCol{{Name: "n", OID: 23}},
				Rows:    [][]string{{"1"}},
				Tag:     "SELECT 1",
			},
		})
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

			c.sendQ("BEGIN")
			if s := c.drainUntilRFQ(); s != 'T' {
				errs <- fmt.Errorf("client %d: BEGIN want RFQ('T'), got %q", id, s)
				return
			}
			c.sendQ(fmt.Sprintf("SELECT %d", id))
			if s := c.drainUntilRFQ(); s != 'T' {
				errs <- fmt.Errorf("client %d: mid-tx want RFQ('T'), got %q", id, s)
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
	t.Logf("TransactionIsolation: %d clients, %d backends — all transactions completed", numClients, numBackends)
}

// TestConcurrent_MixedExtendedAndSimple verifies that clients using different
// protocol paths (named extended query vs simple query with literals) share the
// pool simultaneously without interfering. Each client gets its own value back.
//
// Note: after the simple-query-cache fix, the simple path sends a portal
// Describe, so simple-query clients now receive a RowDescription ('T') before
// the DataRow — the same shape a real simple query yields.
func TestConcurrent_MixedExtendedAndSimple(t *testing.T) {
	numClients := 8 + rand.Intn(8)
	numBackends := 2 + rand.Intn(4)
	numExtended := numClients / 2

	t.Logf("MixedExtendedAndSimple: %d clients (%d extended, %d simple), %d backends",
		numClients, numExtended, numClients-numExtended, numBackends)

	h := newHarness(t)
	defer h.close()

	fakes := make([]*pgServer, numBackends)
	for i := 0; i < numBackends; i++ {
		fakes[i] = h.addBackend(backendSpec{
			Delay: time.Duration(rand.Intn(8)) * time.Millisecond,
			Rules: []queryRule{
				// Extended path canonical SQL.
				{SQL: "SELECT $1::int",
					Columns: []pgCol{{Name: "id", OID: 23}}, Echo: true, Tag: "SELECT 1"},
				// Simple path canonical SQL (ClassifyAndParameterize output).
				{SQL: "SELECT id FROM users WHERE id = $1",
					Columns: []pgCol{{Name: "id", OID: 23}}, Echo: true, Tag: "SELECT 1"},
			},
		})
	}

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		errs  = make(chan error, numClients)
	)
	wg.Add(numClients)

	// Extended clients (asyncpg pattern).
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
			if got := extractFirstDataRowValue(dataBody); got != param {
				errs <- fmt.Errorf("extended client %d: DataRow=%q, want %q", id, got, param)
			}
			c.expect('C')
			if s := c.drainUntilRFQ(); s != 'I' {
				errs <- fmt.Errorf("extended client %d: RFQ=%q, want 'I'", id, s)
			}
		}(i)
	}

	// Simple clients (literal extraction path).
	for i := numExtended; i < numClients; i++ {
		go func(id int) {
			defer wg.Done()
			c := h.connect()
			defer c.conn.Close()
			<-start

			c.sendQ(fmt.Sprintf("SELECT id FROM users WHERE id = %d", id))

			// Protocol-correct simple-query response: RowDescription, then DataRow,
			// CommandComplete, ReadyForQuery. (No ParseComplete/BindComplete/NoData.)
			c.expect('T') // RowDescription — from the portal Describe pgfox now sends
			dataBody := c.expect('D')
			want := fmt.Sprintf("%d", id)
			if got := extractFirstDataRowValue(dataBody); got != want {
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

	// Two distinct canonical statements; each backend deploys each at most once.
	var total int
	for _, f := range fakes {
		total += f.ParseCount()
	}
	if total < 2 {
		t.Errorf("MixedExtendedAndSimple: total Parse=%d, expected ≥2 (one per statement type)", total)
	}
	if total > numBackends*2 {
		t.Errorf("MixedExtendedAndSimple: total Parse=%d > backends*2=%d — possible cache race", total, numBackends*2)
	}
	t.Logf("MixedExtendedAndSimple: %d total Parse deploys across %d backends", total, numBackends)
}

// =============================================================================
// Wire parsing helpers used by concurrency tests
// =============================================================================

// extractFirstDataRowValue extracts the first column value from a DataRow ('D')
// message body. Returns "" if malformed or null.
func extractFirstDataRowValue(body []byte) string {
	if len(body) < 6 {
		return ""
	}
	colLen := int(body[2])<<24 | int(body[3])<<16 | int(body[4])<<8 | int(body[5])
	if colLen < 0 || 6+colLen > len(body) {
		return ""
	}
	return string(body[6 : 6+colLen])
}
