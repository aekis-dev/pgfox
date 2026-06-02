package tests

// simple_query_test.go — tests for the simple query protocol ('Q' message).
//
// Covers playbook sections §2.1 (passthrough), §2.2 (stmt cache via literal
// extraction), §2.4 (transaction pinning), and the pool concurrency scenario
// from §5. These tests send raw 'Q' messages and verify the exact response
// sequence pgfox produces.
//
// Playbook rows covered: T03, T04, T04b, T05, T06.

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestPlaybook_T03_SimpleSelectPassthrough verifies that a non-parameterizable
// simple query (DDL, SET, etc.) is forwarded verbatim to the backend and its
// response is forwarded verbatim to the client.
//
// Playbook §2.1 — Plain passthrough query.
//
// Wire sequence:
//
//	C→P  Q { "CREATE TABLE foo (id int)\0" }
//	P→B  Q { "CREATE TABLE foo (id int)\0" }
//	B→P  CommandComplete { "CREATE TABLE" }
//	B→P  ReadyForQuery { 'I' }
//	P→C  CommandComplete { "CREATE TABLE" }
//	P→C  ReadyForQuery { 'I' }
func TestPlaybook_T03_SimpleSelectPassthrough(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	_, fake := h.addBackend()

	go func() {
		mt, body := fake.recvMsg()
		if mt != 'Q' {
			t.Errorf("T03: expected Q from pgfox, got %q", mt)
			return
		}
		// Verify pgfox forwarded the SQL unchanged (strip trailing null).
		got := string(body[:len(body)-1])
		if got != "CREATE TABLE foo (id int)" {
			t.Errorf("T03: SQL mismatch: got %q", got)
		}
		fake.sendCC("CREATE TABLE")
		fake.sendRFQ('I')
	}()

	c := h.connect()
	defer c.conn.Close()

	c.sendQ("CREATE TABLE foo (id int)")
	c.expect('C') // CommandComplete — pgfox must NOT strip this
	c.expectRFQ('I')
}

// TestPlaybook_T04_SimpleLiteralsThroughCache verifies that a simple query
// whose literals can be extracted is served through the statement cache:
// pgfox rewrites it to a parameterized form, registers it as a prepared
// statement (pfx_<hash>), and sends Parse + Bind + Execute + Sync to the
// backend. The client receives only what a simple query response looks like
// (RowDescription, DataRow, CommandComplete, ReadyForQuery) — never
// ParseComplete or BindComplete.
//
// Playbook §2.2 — Parameterizable DML via stmt cache (happy path).
//
// Wire sequence (first call, backend does not have the statement yet):
//
//	C→P  Q { "SELECT id FROM users WHERE id = 42\0" }
//	P→B  P { name="pfx_<hash>", query="SELECT id FROM users WHERE id = $1" }
//	P→B  B { stmt="pfx_<hash>", params=["42"], resultFmts=[1] }
//	P→B  E { portal="" }
//	P→B  S
//	B→P  ParseComplete
//	B→P  BindComplete
//	B→P  RowDescription
//	B→P  DataRow
//	B→P  CommandComplete
//	B→P  ReadyForQuery { 'I' }
//	P→C  RowDescription        ← ParseComplete and BindComplete are NOT forwarded
//	P→C  DataRow
//	P→C  CommandComplete
//	P→C  ReadyForQuery { 'I' }
func TestPlaybook_T04_SimpleLiteralsThroughCache(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	_, fake := h.addBackend()

	go func() {
		// pgfox must send Parse with a pfx_ prefixed statement name.
		mt, body := fake.recvMsg()
		if mt != 'P' {
			t.Errorf("T04: expected Parse, got %q", mt)
			return
		}
		name := parseStmtName(body)
		if len(name) < 4 || name[:4] != "pfx_" {
			t.Errorf("T04: Parse name should be pfx_*, got %q", name)
		}
		fake.sendParseComplete()

		mt, _ = fake.recvMsg()
		if mt != 'B' {
			t.Errorf("T04: expected Bind, got %q", mt)
		}
		fake.sendBindComplete()

		mt, _ = fake.recvMsg()
		if mt != 'E' {
			t.Errorf("T04: expected Execute, got %q", mt)
		}

		mt, _ = fake.recvMsg()
		if mt != 'S' {
			t.Errorf("T04: expected Sync, got %q", mt)
		}

		fake.sendRowDescription("id")
		fake.sendDataRowText("42")
		fake.sendCC("SELECT 1")
		fake.sendRFQ('I')
	}()

	c := h.connect()
	defer c.conn.Close()

	c.sendQ("SELECT id FROM users WHERE id = 42")

	// Client must NOT receive ParseComplete ('1') or BindComplete ('2') —
	// those are extended protocol messages invisible in simple query mode.
	c.expect('T') // RowDescription
	c.expect('D') // DataRow
	c.expect('C') // CommandComplete
	c.expectRFQ('I')
}

// TestPlaybook_T04b_CacheHitSkipsParse verifies that on the second call with
// the same parameterized query, pgfox does not re-send Parse to the backend
// (HasStmt is true after the first successful deploy).
//
// Playbook §2.2 — Parameterizable DML via stmt cache (second call).
// Playbook §6.2 — Deployment tracking per backend.
//
// Wire sequence (second call):
//
//	C→P  Q { "SELECT id FROM users WHERE id = 99\0" }
//	P→B  (no Parse — backend already has pfx_<hash>)
//	P→B  B { stmt="pfx_<hash>", params=["99"] }
//	P→B  E
//	P→B  S
//	...
func TestPlaybook_T04b_CacheHitSkipsParse(t *testing.T) {
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

	// First call — Parse must be sent (backend does not have the statement).
	// returnConn automatically returns the backend to the pool after RFQ('I').
	c.sendQ("SELECT id FROM users WHERE id = 42")
	c.drainUntilRFQ()

	// Second call — same canonical SQL, same hash. Parse must NOT be sent.
	// pgfox borrows the same backend (HasStmt=true) and skips Parse.
	c.sendQ("SELECT id FROM users WHERE id = 99")
	c.drainUntilRFQ()

	if parseCount != 1 {
		t.Errorf("T04b: Parse should be sent exactly once, got %d times", parseCount)
	}
}

// TestPlaybook_T05_ConcurrentSimpleQueries verifies that pgfox handles many
// simultaneous simple queries without serialising them. Each client goroutine
// gets its own backend connection from the pool and completes independently.
//
// Playbook §5 — Pool growth (demand-driven), §5.2 — Connection return.
func TestPlaybook_T05_ConcurrentSimpleQueries(t *testing.T) {
	const n = 10
	h := newHarness(t)
	defer h.close()

	// Provide n backends — one per concurrent client.
	for i := 0; i < n; i++ {
		_, fake := h.addBackend()
		go func(fb *fakeBackend) {
			for {
				mt, _ := fb.recvMsg()
				if mt == 0 {
					return
				}
				switch mt {
				case 'P':
					fb.sendParseComplete()
				case 'B':
					fb.sendBindComplete()
				case 'E':
					fb.sendDataRowText("1")
					fb.sendCC("SELECT 1")
				case 'S':
					fb.sendRFQ('I')
				case 'Q':
					fb.sendCC("SELECT 1")
					fb.sendRFQ('I')
				}
			}
		}(fake)
	}

	var wg sync.WaitGroup
	wg.Add(n)
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		go func(id int) {
			defer wg.Done()
			c := h.connect()
			defer c.conn.Close()
			<-start
			c.sendQ(fmt.Sprintf("SELECT %d", id))
			c.drainUntilRFQ()
		}(i)
	}

	close(start)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		// All queries completed concurrently — pool is not serialising.
	case <-time.After(5 * time.Second):
		t.Fatal("T05: concurrent queries timed out — pool may be serialising")
	}
}

// TestPlaybook_T06_TransactionPinning verifies the core transaction-pinning
// invariant: once a backend returns ReadyForQuery with status 'T', all
// subsequent queries from the same client go to the same backend until a
// response with status 'I' (or 'E') is received.
//
// Playbook §2.4 — Transaction block.
//
// Wire sequence:
//
//	C→P  Q { "BEGIN\0" }        → B→P RFQ('T') → pin backend
//	C→P  Q { "SELECT 1\0" }     → same backend
//	C→P  Q { "COMMIT\0" }       → same backend → B→P RFQ('I') → unpin
func TestPlaybook_T06_TransactionPinning(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	_, fake := h.addBackend()

	// Track every SQL that arrives at this one backend.
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
				fake.sendRFQ('T') // signals pgfox to pin this backend
			case "SELECT 1":
				fake.sendCC("SELECT 1")
				fake.sendRFQ('T') // still in transaction
			case "COMMIT":
				fake.sendCC("COMMIT")
				fake.sendRFQ('I') // signals pgfox to unpin and return to pool
			}
		}
	}()

	c := h.connect()
	defer c.conn.Close()

	c.sendQ("BEGIN")
	c.drainUntilRFQ()

	c.sendQ("SELECT 1")
	c.drainUntilRFQ()

	c.sendQ("COMMIT")
	c.drainUntilRFQ()

	// All three queries must have arrived at the same (only) backend.
	want := []string{"BEGIN", "SELECT 1", "COMMIT"}
	if len(received) != len(want) {
		t.Fatalf("T06: expected %d queries on backend, got %d: %v", len(want), len(received), received)
	}
	for i, sql := range want {
		if received[i] != sql {
			t.Errorf("T06: query[%d] want %q, got %q", i, sql, received[i])
		}
	}
}
