package tests

// transaction_test.go — transaction pinning and concurrent execution.
//
// pgfox must pin a backend to a client for the duration of a transaction block
// (ReadyForQuery status 'T' or 'E'), return it after COMMIT/ROLLBACK (status
// 'I'), keep it pinned across a failed statement ('E') until ROLLBACK, and let
// concurrent transactions each use their own backend.
//
// The transaction status the client observes is produced by the engine's real
// state machine (BEGIN→'T', error→'E', COMMIT/ROLLBACK→'I'), driven by the
// actual command stream — not by scripted status bytes. The engine also flags
// routing bugs automatically (a BEGIN at a backend already in a transaction, or
// a COMMIT at an idle one) via its checkTxInvariant guard.
//
// Playbook rows covered: T23, T24, T25 (T06 lives in simple_query_test.go).

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestPlaybook_T23_FailedTransactionStaysPinned verifies that when a statement
// inside a transaction fails (RFQ 'E'), the backend stays pinned to the client
// until ROLLBACK (RFQ 'I') returns it to the pool.
//
// Playbook §2.4 note on txStatus='E'.
func TestPlaybook_T23_FailedTransactionStaysPinned(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	fake := h.addBackend(backendSpec{
		Rules: []queryRule{
			{SQL: "BEGIN", Tag: "BEGIN", Tx: txBegin},
			{SQL: "SELECT oops", Error: &pgError{
				Code: "42703", Message: `column "oops" does not exist`, Tx: txFail}},
			{SQL: "ROLLBACK", Tag: "ROLLBACK", Tx: txEnd},
		},
	})

	c := h.connect()
	defer c.conn.Close()

	c.sendQ("BEGIN")
	if s := c.drainUntilRFQ(); s != 'T' {
		t.Fatalf("T23: after BEGIN expected 'T', got %q", s)
	}

	c.sendQ("SELECT oops")
	if s := c.drainUntilRFQ(); s != 'E' {
		t.Fatalf("T23: after error expected 'E' (failed tx), got %q", s)
	}

	// Still pinned — the next query must reach the same backend.
	c.sendQ("ROLLBACK")
	if s := c.drainUntilRFQ(); s != 'I' {
		t.Fatalf("T23: after ROLLBACK expected 'I', got %q", s)
	}

	want := []string{"BEGIN", "SELECT oops", "ROLLBACK"}
	got := fake.SimpleQueries()
	if len(got) != len(want) {
		t.Fatalf("T23: expected %v on one backend, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("T23: query[%d] want %q, got %q", i, want[i], got[i])
		}
	}
}

// TestPlaybook_T24_ConcurrentTransactionsIsolated verifies that multiple
// simultaneous transactions each get a dedicated backend and complete with
// correct status transitions without interference.
//
// Each goroutine: BEGIN ('T') → SELECT ('T') → COMMIT ('I'). The engine's
// checkTxInvariant would fire if two clients ever shared a pinned backend.
//
// Playbook §2.4 — concurrent variant.
func TestPlaybook_T24_ConcurrentTransactionsIsolated(t *testing.T) {
	const n = 5
	h := newHarness(t)
	defer h.close()

	// One backend per concurrent transaction. Each knows BEGIN/COMMIT and
	// answers any SELECT from the Default rule (status stays 'T' mid-tx).
	for i := 0; i < n; i++ {
		h.addBackend(backendSpec{
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

	var wg sync.WaitGroup
	wg.Add(n)
	errs := make(chan error, n)
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		go func(id int) {
			defer wg.Done()
			c := h.connect()
			defer c.conn.Close()
			<-start

			c.sendQ("BEGIN")
			if s := c.drainUntilRFQ(); s != 'T' {
				errs <- fmt.Errorf("client %d: after BEGIN want 'T', got %q", id, s)
				return
			}
			c.sendQ(fmt.Sprintf("SELECT %d", id))
			if s := c.drainUntilRFQ(); s != 'T' {
				errs <- fmt.Errorf("client %d: mid-tx want 'T', got %q", id, s)
				return
			}
			c.sendQ("COMMIT")
			if s := c.drainUntilRFQ(); s != 'I' {
				errs <- fmt.Errorf("client %d: after COMMIT want 'I', got %q", id, s)
				return
			}
		}(i)
	}

	close(start)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("T24: concurrent transactions timed out")
	}

	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestPlaybook_T25_SlowAndFastQueriesConcurrent verifies that a fast query is
// not blocked by a slow one — each borrows its own backend and runs
// independently.
//
// The slowness is per-query (the engine delays the pg_sleep query wherever it
// lands), so the assertion is deterministic regardless of which backend each
// client borrows. Two backends are provided so neither client waits for the
// other's connection.
//
// Playbook §5 — Pool growth, concurrent execution.
func TestPlaybook_T25_SlowAndFastQueriesConcurrent(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	// Identical specs on both backends: the pg_sleep query is slow wherever it
	// runs; everything else is fast.
	spec := backendSpec{
		Rules: []queryRule{
			{SQLPrefix: "SELECT pg_sleep",
				Columns: []pgCol{{Name: "v", OID: 23}},
				Rows:    [][]string{{"0"}},
				Tag:     "SELECT 1",
				Delay:   200 * time.Millisecond},
		},
		Default: &queryRule{
			Columns: []pgCol{{Name: "v", OID: 23}},
			Rows:    [][]string{{"1"}},
			Tag:     "SELECT 1",
		},
	}
	h.addBackend(spec)
	h.addBackend(spec)

	slowDone := make(chan time.Duration, 1)
	fastDone := make(chan time.Duration, 1)
	start := make(chan struct{})

	go func() {
		c := h.connect()
		defer c.conn.Close()
		<-start
		t0 := time.Now()
		c.sendQ("SELECT pg_sleep(0.2)")
		c.drainUntilRFQ()
		slowDone <- time.Since(t0)
	}()

	go func() {
		c := h.connect()
		defer c.conn.Close()
		<-start
		t0 := time.Now()
		c.sendQ("SELECT 1")
		c.drainUntilRFQ()
		fastDone <- time.Since(t0)
	}()

	close(start)

	var slowElapsed, fastElapsed time.Duration
	deadline := time.After(3 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case d := <-slowDone:
			slowElapsed = d
		case d := <-fastDone:
			fastElapsed = d
		case <-deadline:
			t.Fatal("T25: queries timed out")
		}
	}

	if fastElapsed >= 150*time.Millisecond {
		t.Errorf("T25: fast query took %v — should not be blocked by the slow query", fastElapsed)
	}
	if slowElapsed < 150*time.Millisecond {
		t.Errorf("T25: slow query took %v — expected ≥150ms", slowElapsed)
	}
}

// TestPlaybook_T26_NotifyInTransactionSimpleUsesPinnedBackend verifies that a
// NOTIFY issued via the simple query protocol *inside a transaction* runs on the
// client's pinned backend — not on a freshly borrowed one. Routing it elsewhere
// would break transactional semantics (NOTIFY must fire at COMMIT) and report a
// bogus ReadyForQuery status.
//
// Regression test for the transactional-special-command misrouting bug: before
// the fix, executeQuery dispatched NOTIFY to handleNotify (which borrows a
// one-shot backend) before checking transaction state, so the client saw 'I'
// after NOTIFY and the command ran outside the transaction.
func TestPlaybook_T26_NotifyInTransactionSimpleUsesPinnedBackend(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	rules := []queryRule{
		{SQL: "BEGIN", Tag: "BEGIN", Tx: txBegin},
		{SQLPrefix: "NOTIFY", Tag: "NOTIFY"}, // Tx none → status unchanged
		{SQL: "COMMIT", Tag: "COMMIT", Tx: txEnd},
	}
	// Two backends: the buggy fresh-borrow path would land NOTIFY on the second.
	a := h.addBackend(backendSpec{Rules: rules})
	b := h.addBackend(backendSpec{Rules: rules})

	c := h.connect()
	defer c.conn.Close()

	c.sendQ("BEGIN")
	if s := c.drainUntilRFQ(); s != 'T' {
		t.Fatalf("T26: after BEGIN want 'T', got %q", s)
	}

	c.sendQ("NOTIFY chan, 'hi'")
	if s := c.drainUntilRFQ(); s != 'T' {
		t.Fatalf("T26: NOTIFY in a transaction must keep status 'T' (it must run on the pinned backend), got %q", s)
	}

	c.sendQ("COMMIT")
	if s := c.drainUntilRFQ(); s != 'I' {
		t.Fatalf("T26: after COMMIT want 'I', got %q", s)
	}

	// Only the pinned backend may be touched. A second touched backend means
	// NOTIFY was misrouted to a freshly borrowed connection.
	if n := touchedCount(a, b); n != 1 {
		t.Errorf("T26: expected exactly 1 backend touched (the pinned one), got %d — NOTIFY leaked to another backend", n)
	}
}

// TestPlaybook_T27_NotifyInTransactionExtendedUsesPinnedBackend is the
// asyncpg-shaped counterpart of T26: NOTIFY issued via the extended protocol
// (Parse+Bind+Execute+Sync) inside a transaction must also run on the pinned
// backend. Before the fix, the extended-protocol dispatch intercepted NOTIFY
// and routed it through handleNotify regardless of transaction state.
func TestPlaybook_T27_NotifyInTransactionExtendedUsesPinnedBackend(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	rules := []queryRule{
		{SQL: "BEGIN", Tag: "BEGIN", Tx: txBegin},
		{SQLPrefix: "NOTIFY", Tag: "NOTIFY"},
		{SQL: "COMMIT", Tag: "COMMIT", Tx: txEnd},
	}
	a := h.addBackend(backendSpec{Rules: rules})
	b := h.addBackend(backendSpec{Rules: rules})

	c := h.connect()
	defer c.conn.Close()

	c.sendQ("BEGIN")
	if s := c.drainUntilRFQ(); s != 'T' {
		t.Fatalf("T27: after BEGIN want 'T', got %q", s)
	}

	// Extended NOTIFY: Parse(unnamed) + Bind + Execute + Sync (no Describe).
	c.send('P', buildParseMsg("", "NOTIFY chan, 'hi'"))
	c.send('B', buildBindMsg("", "", nil))
	c.send('E', []byte{0, 0, 0, 0, 0}) // unnamed portal + maxRows 0
	c.send('S', nil)
	if s := c.drainUntilRFQ(); s != 'T' {
		t.Fatalf("T27: extended NOTIFY in a transaction must keep status 'T', got %q", s)
	}

	c.sendQ("COMMIT")
	if s := c.drainUntilRFQ(); s != 'I' {
		t.Fatalf("T27: after COMMIT want 'I', got %q", s)
	}

	if n := touchedCount(a, b); n != 1 {
		t.Errorf("T27: expected exactly 1 backend touched (the pinned one), got %d — NOTIFY leaked to another backend", n)
	}
}

// touchedCount reports how many of the given fake backends received any message
// from pgfox (a Parse, a Bind, or a simple Query).
func touchedCount(fakes ...*pgServer) int {
	n := 0
	for _, f := range fakes {
		if f.ParseCount() > 0 || len(f.SimpleQueries()) > 0 || len(f.BoundNames()) > 0 {
			n++
		}
	}
	return n
}
