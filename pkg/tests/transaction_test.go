package tests

// transaction_test.go — tests for transaction pinning and concurrent execution.
//
// pgfox must ensure that:
//   - A backend connection is pinned to a client for the duration of a
//     transaction block (ReadyForQuery status 'T' or 'E').
//   - The connection is returned to the pool after COMMIT/ROLLBACK
//     (ReadyForQuery status 'I').
//   - A failed transaction ('E') keeps the connection pinned until ROLLBACK.
//   - Multiple concurrent transactions each get their own backend and execute
//     without interference.
//
// Playbook rows covered: T06 (in simple_query_test.go), T23, T24, T25.

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestPlaybook_T23_FailedTransactionStaysPinned verifies that when a query
// inside a transaction block fails (ReadyForQuery returns 'E' — failed
// transaction), the backend stays pinned to the client. Only after ROLLBACK
// (which returns ReadyForQuery 'I') is the backend returned to the pool.
//
// Playbook §2.4 note on txStatus='E'.
//
// Wire sequence:
//
//	C→P  Q { "BEGIN\0" }         → B→P RFQ('T') → pin
//	C→P  Q { "SELECT oops\0" }   → B→P ErrorResponse + RFQ('E') → stay pinned
//	C→P  Q { "ROLLBACK\0" }      → B→P RFQ('I') → unpin
//
// All three queries must arrive at the same backend.
func TestPlaybook_T23_FailedTransactionStaysPinned(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	_, fake := h.addBackend()

	var received []string
	go func() {
		for {
			mt, body := fake.recvMsg()
			if mt == 0 {
				return
			}
			if mt != 'Q' {
				continue
			}
			sql := string(body[:len(body)-1])
			received = append(received, sql)
			switch sql {
			case "BEGIN":
				fake.sendCC("BEGIN")
				fake.sendRFQ('T')
			case "SELECT oops":
				// Simulate a column-not-found error.
				fake.sendErrorResponse("column \"oops\" does not exist")
				fake.sendRFQ('E') // failed transaction — backend must stay pinned
			case "ROLLBACK":
				fake.sendCC("ROLLBACK")
				fake.sendRFQ('I') // idle — backend can now return to pool
			}
		}
	}()

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

	// Queue must still be pinned — next query must go to the same backend.
	c.sendQ("ROLLBACK")
	if s := c.drainUntilRFQ(); s != 'I' {
		t.Fatalf("T23: after ROLLBACK expected 'I', got %q", s)
	}

	// All three queries must have arrived at the one backend.
	if len(received) != 3 {
		t.Errorf("T23: expected 3 queries on one backend, got %d: %v", len(received), received)
	}
	for i, want := range []string{"BEGIN", "SELECT oops", "ROLLBACK"} {
		if i < len(received) && received[i] != want {
			t.Errorf("T23: query[%d] want %q, got %q", i, want, received[i])
		}
	}
}

// TestPlaybook_T24_ConcurrentTransactionsIsolated verifies that multiple
// simultaneous transactions each receive a dedicated backend connection and
// complete with correct status transitions without interference between
// concurrent clients.
//
// Playbook §2.4 — Transaction block, concurrent variant.
//
// Each goroutine follows: BEGIN ('T') → SELECT ('T') → COMMIT ('I').
// All n goroutines run concurrently. The test fails if any status is wrong
// or if a timeout occurs (which would indicate pool serialisation).
func TestPlaybook_T24_ConcurrentTransactionsIsolated(t *testing.T) {
	const n = 5
	h := newHarness(t)
	defer h.close()

	// Provide one backend per concurrent transaction.
	for i := 0; i < n; i++ {
		_, fake := h.addBackend()
		go func(fb *fakeBackend) {
			for {
				mt, body := fb.recvMsg()
				if mt == 0 {
					return
				}
				if mt != 'Q' {
					continue
				}
				sql := string(body[:len(body)-1])
				switch sql {
				case "BEGIN":
					fb.sendCC("BEGIN")
					fb.sendRFQ('T')
				case "COMMIT":
					fb.sendCC("COMMIT")
					fb.sendRFQ('I')
				default:
					// Any SELECT inside the transaction.
					fb.sendCC("SELECT 1")
					fb.sendRFQ('T') // still in transaction
				}
			}
		}(fake)
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
				errs <- fmt.Errorf("client %d: during tx want 'T', got %q", id, s)
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

// TestPlaybook_T25_SlowAndFastQueriesConcurrent verifies that fast queries
// are not blocked by slow ones — each borrows its own backend from the pool
// and executes independently.
//
// Playbook §5 — Pool growth, concurrent execution.
//
// Two backends are provided: one slow (simulates a long-running query) and
// one fast. Both the slow and fast client goroutines start at the same time.
// The fast client must finish well before the slow one.
func TestPlaybook_T25_SlowAndFastQueriesConcurrent(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	// Slow backend: holds the connection for 200ms before responding.
	_, slow := h.addBackend()
	go func() {
		for {
			mt, _ := slow.recvMsg()
			if mt == 0 {
				return
			}
			if mt == 'Q' {
				time.Sleep(200 * time.Millisecond)
				slow.sendCC("SELECT 1")
				slow.sendRFQ('I')
			} else if mt == 'P' {
				time.Sleep(200 * time.Millisecond)
				slow.sendParseComplete()
			} else if mt == 'B' {
				slow.sendBindComplete()
			} else if mt == 'E' {
				slow.sendDataRowText("slow")
				slow.sendCC("SELECT 1")
			} else if mt == 'S' {
				slow.sendRFQ('I')
			}
		}
	}()

	// Fast backend: responds immediately.
	_, fast := h.addBackend()
	go func() {
		for {
			mt, _ := fast.recvMsg()
			if mt == 0 {
				return
			}
			switch mt {
			case 'Q':
				fast.sendCC("SELECT 1")
				fast.sendRFQ('I')
			case 'P':
				fast.sendParseComplete()
			case 'B':
				fast.sendBindComplete()
			case 'E':
				fast.sendDataRowText("fast")
				fast.sendCC("SELECT 1")
			case 'S':
				fast.sendRFQ('I')
			}
		}
	}()

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

	// Fast query must complete significantly before the slow one.
	if fastElapsed >= 150*time.Millisecond {
		t.Errorf("T25: fast query took %v — should not be blocked by slow query", fastElapsed)
	}
	if slowElapsed < 150*time.Millisecond {
		t.Errorf("T25: slow query took %v — expected ≥150ms", slowElapsed)
	}
}
