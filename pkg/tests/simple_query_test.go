package tests

// simple_query_test.go — tests for the simple query protocol ('Q' message).
//
// Covers playbook §2.1 (passthrough), §2.2 (stmt cache via literal extraction),
// §2.4 (transaction pinning), and the pool concurrency scenario from §5.
//
// Style: the fake backend is declarative. Each test hands addBackend a
// backendSpec describing the queries the database knows; the pgServer engine
// reacts to whatever pgfox sends with protocol-faithful responses. No test
// hand-feeds a client-facing message — every byte the client sees is produced
// by pgfox's real code.
//
// Playbook rows covered: T03, T04, T04b, T05, T06.

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestPlaybook_T03_SimpleSelectPassthrough verifies that a non-parameterizable
// simple query (DDL) is forwarded verbatim to the backend and its response is
// forwarded verbatim to the client.
//
// Playbook §2.1 — Plain passthrough query.
func TestPlaybook_T03_SimpleSelectPassthrough(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	fake := h.addBackend(backendSpec{
		Rules: []queryRule{
			{SQL: "CREATE TABLE foo (id int)", Tag: "CREATE TABLE"},
		},
	})

	c := h.connect()
	defer c.conn.Close()

	c.sendQ("CREATE TABLE foo (id int)")
	c.expect('C') // CommandComplete — pgfox must NOT strip this
	c.expectRFQ('I')

	// pgfox must have forwarded the DDL verbatim (no parameterization).
	if sqls := fake.SimpleQueries(); len(sqls) != 1 || sqls[0] != "CREATE TABLE foo (id int)" {
		t.Errorf("T03: backend should have received the SQL verbatim, got %v", sqls)
	}
}

// TestPlaybook_T04_SimpleLiteralsThroughCache verifies that a simple query whose
// literals can be extracted is served through the statement cache: pgfox
// rewrites it to a parameterized form, deploys it as a prepared statement,
// Describes the portal, executes it, and translates the extended-protocol
// responses back into a protocol-correct simple-query response.
//
// Playbook §2.2 — Parameterizable DML via stmt cache (happy path).
//
// The key correctness points this guards:
//   - pgfox sends a portal Describe (Execute alone never yields RowDescription).
//   - Result format is text (the simple-query client cannot read binary).
//   - The client never sees ParseComplete/BindComplete/NoData.
func TestPlaybook_T04_SimpleLiteralsThroughCache(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	fake := h.addBackend(backendSpec{
		Rules: []queryRule{
			{SQL: "SELECT id FROM users WHERE id = $1",
				Columns: []pgCol{{Name: "id", OID: 23}},
				Rows:    [][]string{{"42"}},
				Tag:     "SELECT 1"},
		},
	})

	c := h.connect()
	defer c.conn.Close()

	c.sendQ("SELECT id FROM users WHERE id = 42")

	// Client must NOT receive ParseComplete ('1') or BindComplete ('2').
	c.expect('T') // RowDescription (from the portal Describe, text format)
	c.expect('D') // DataRow
	c.expect('C') // CommandComplete
	c.expectRFQ('I')

	// pgfox must have deployed under a pfx_ name and Described the portal.
	if names := fake.ParsedNames(); len(names) != 1 || !hasPfxPrefix(names[0]) {
		t.Errorf("T04: backend Parse name should be pfx_*, got %v", names)
	}
	if names := fake.BoundNames(); len(names) != 1 || !hasPfxPrefix(names[0]) {
		t.Errorf("T04: backend Bind stmt should be pfx_*, got %v", names)
	}
	if !fake.sawDescribePortal() {
		t.Error("T04: pgfox did not Describe the portal — client would get no RowDescription")
	}
	// Result format must be text (empty = all text per the Bind spec).
	for i, f := range fake.LastResultFormats() {
		if f != 0 {
			t.Errorf("T04: result format[%d] = %d, want 0 (text)", i, f)
		}
	}
}

// TestPlaybook_T04b_CacheHitSkipsParse verifies that on the second call with the
// same canonical query, pgfox does not re-send Parse to the same backend
// (HasStmt is true after the first deploy).
//
// Playbook §2.2 (second call); §6.2 (deployment tracking per backend).
func TestPlaybook_T04b_CacheHitSkipsParse(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	fake := h.addBackend(backendSpec{
		Rules: []queryRule{
			{SQLPrefix: "SELECT id FROM users WHERE id = $",
				Columns: []pgCol{{Name: "id", OID: 23}},
				Rows:    [][]string{{"1"}},
				Tag:     "SELECT 1"},
		},
	})

	c := h.connect()
	defer c.conn.Close()

	// First call — Parse is sent; backend returned to the pool after RFQ('I').
	c.sendQ("SELECT id FROM users WHERE id = 42")
	c.drainUntilRFQ()

	// Second call — same canonical SQL, same hash; same backend reused.
	c.sendQ("SELECT id FROM users WHERE id = 99")
	c.drainUntilRFQ()

	if got := fake.ParseCount(); got != 1 {
		t.Errorf("T04b: Parse should be sent exactly once, got %d", got)
	}
}

// TestPlaybook_T05_ConcurrentSimpleQueries verifies that pgfox handles many
// simultaneous simple queries without serialising them. Each client gets its
// own backend from the pool and completes independently.
//
// Playbook §5 — Pool growth (demand-driven), §5.2 — Connection return.
func TestPlaybook_T05_ConcurrentSimpleQueries(t *testing.T) {
	const n = 10
	h := newHarness(t)
	defer h.close()

	// One backend per concurrent client. Each accepts any SELECT (all the
	// "SELECT <id>" queries canonicalize to "SELECT $1").
	for i := 0; i < n; i++ {
		h.addBackend(backendSpec{
			Default: &queryRule{
				Columns: []pgCol{{Name: "n", OID: 23}},
				Rows:    [][]string{{"1"}},
				Tag:     "SELECT 1",
			},
		})
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
	case <-time.After(5 * time.Second):
		t.Fatal("T05: concurrent queries timed out — pool may be serialising")
	}
}

// TestPlaybook_T06_TransactionPinning verifies the core transaction-pinning
// invariant: once a backend returns ReadyForQuery 'T', all subsequent queries
// from the same client go to that backend until a response with status 'I'.
//
// The transaction status the client observes is produced by the engine's real
// state machine (BEGIN→T, query→stays T, COMMIT→I), not by scripted bytes.
//
// Playbook §2.4 — Transaction block.
func TestPlaybook_T06_TransactionPinning(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	fake := h.addBackend(backendSpec{
		Rules: []queryRule{
			{SQL: "BEGIN", Tag: "BEGIN", Tx: txBegin},
			{SQL: "SELECT 1", Columns: []pgCol{{Name: "n", OID: 23}},
				Rows: [][]string{{"1"}}, Tag: "SELECT 1"},
			{SQL: "COMMIT", Tag: "COMMIT", Tx: txEnd},
		},
	})

	c := h.connect()
	defer c.conn.Close()

	c.sendQ("BEGIN")
	if s := c.drainUntilRFQ(); s != 'T' {
		t.Fatalf("T06: after BEGIN expected 'T', got %q", s)
	}
	c.sendQ("SELECT 1")
	if s := c.drainUntilRFQ(); s != 'T' {
		t.Fatalf("T06: in transaction expected 'T', got %q", s)
	}
	c.sendQ("COMMIT")
	if s := c.drainUntilRFQ(); s != 'I' {
		t.Fatalf("T06: after COMMIT expected 'I', got %q", s)
	}

	// All three queries must have arrived at the same (only) backend, in order.
	want := []string{"BEGIN", "SELECT 1", "COMMIT"}
	got := fake.SimpleQueries()
	if len(got) != len(want) {
		t.Fatalf("T06: expected %v on backend, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("T06: query[%d] want %q, got %q", i, want[i], got[i])
		}
	}
}

// hasPfxPrefix reports whether name is an internal pfx_<hash> statement name.
func hasPfxPrefix(name string) bool {
	return len(name) >= 4 && name[:4] == "pfx_"
}
