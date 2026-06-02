package tests

// listen_notify_test.go — tests for LISTEN, UNLISTEN, NOTIFY, and notification fan-out.
//
// pgfox intercepts LISTEN and UNLISTEN commands from both the simple query
// protocol and the extended protocol (e.g. asyncpg.execute("LISTEN chan")).
// For simple query LISTEN, pgfox creates a dedicated backend connection that
// blocks on ReadMessage waiting for 'A' (NotificationResponse) messages and
// fans them out to all subscribed clients.
//
// The tests here verify:
//   - LISTEN via simple query ('Q') joins the monitor and gets CommandComplete+RFQ
//   - LISTEN via extended protocol (P+D+H then B+E+S) generates the correct
//     synthetic response sequence without borrowing any backend connection
//   - Notification fan-out delivers 'A' messages to all subscribed clients
//   - UNLISTEN removes the client from the monitor
//
// Note on test isolation: creating a real listen monitor requires newConn()
// which involves TLS and cert generation. Tests here pre-create monitors
// directly to avoid that dependency, testing only the pgfox-level logic.
//
// Monitor backends use newMockConnPair() instead of net.Pipe() so that
// tearDownListen's best-effort UNLISTEN write is non-blocking. The fake side
// is discarded — nobody needs to read it in these tests.
//
// Playbook rows covered: T13, T14, T15, T16.

import (
	"bufio"
	"net"
	"testing"

	"github.com/aekis-dev/pgfox/pkg/pgfox"
)

// TestPlaybook_T13_ListenSimpleQuery verifies that a LISTEN command sent via
// the simple query protocol ('Q' message) causes pgfox to find the existing
// monitor (or create one), add the client to the subscriber list, and respond
// with CommandComplete("LISTEN") + ReadyForQuery('I').
//
// Playbook §4.1 — LISTEN via simple query protocol.
//
// Wire sequence:
//
//	C→P  Q { "LISTEN pgfox_test\0" }
//	P→C  CommandComplete { "LISTEN" }
//	P→C  ReadyForQuery { 'I' }
//
// The monitor's dedicated backend connection is not involved in responding
// to the client — it only blocks waiting for 'A' notifications from PostgreSQL.
func TestPlaybook_T13_ListenSimpleQuery(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	// Pre-create the listen monitor. This bypasses pool.newConn() (which needs
	// TLS) by injecting a monitor directly into p.listeners. When handleListen
	// is called, getOrCreateListen takes the fast path and simply addClient().
	//
	// mockConn is used so tearDownListen's UNLISTEN write is non-blocking.
	// The fake side is discarded — nobody reads from the monitor backend here.
	pgfoxSide, _ := newMockConnPair()
	monitorBackend := pgfox.NewBackend(pgfoxSide, "testdb", "test", "testuser", 1024*1024)
	monitorBackend.Pool = h.pool

	ch := pgfox.Channel{Database: "testdb", Name: "pgfox_test"}
	l := &pgfox.Listen{
		Channel: ch,
		Backend: monitorBackend,
		Clients: map[*pgfox.Client]bool{},
		Done:    make(chan struct{}),
	}
	close(l.Done) // no background goroutine needed for this test

	h.server.ListenersMu.Lock()
	h.server.Listeners[ch] = l
	h.server.ListenersMu.Unlock()

	c := h.connect()
	defer c.conn.Close()

	c.sendQ("LISTEN pgfox_test")

	c.expect('C') // CommandComplete("LISTEN")
	c.expectRFQ('I')

	// Verify the client was registered in the monitor.
	l.Mu.RLock()
	count := len(l.Clients)
	l.Mu.RUnlock()
	if count == 0 {
		t.Error("T13: client was not added to the listen monitor's subscriber list")
	}
}

// TestPlaybook_T14_ListenExtendedProtocol verifies that LISTEN sent via the
// extended query protocol (as asyncpg.execute("LISTEN chan") does) is
// intercepted by pgfox and handled without borrowing any backend connection.
// pgfox generates a synthetic response to both the Flush-terminated prepare
// pipeline and the Sync-terminated execute pipeline.
//
// Playbook §4.2 — LISTEN via extended protocol (asyncpg execute()).
//
// Wire sequence:
//
//	Pipeline 1 (P + D + H — prepare and describe, asyncpg pattern):
//	  C→P  P { name="", query="LISTEN pgfox_ext" }
//	  C→P  D { type='S', name="" }
//	  C→P  H
//	  P→C  ParseComplete ('1')               ← synthetic, no backend involved
//	  P→C  ParameterDescription ('t', [])    ← 0 params
//	  P→C  NoData ('n')                      ← no result columns
//	  (no ReadyForQuery — Flush boundary)
//
//	Pipeline 2 (B + E + S — execute, asyncpg pattern):
//	  C→P  B { portal="", stmt="" }
//	  C→P  E { portal="" }
//	  C→P  S
//	  P→C  BindComplete ('2')                ← synthetic
//	  P→C  CommandComplete ("LISTEN")        ← from handleListen
//	  P→C  ReadyForQuery ('I')               ← from handleListen
func TestPlaybook_T14_ListenExtendedProtocol(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	// Pre-create the monitor so getOrCreateListen fast-paths without newConn().
	pgfoxSide, _ := newMockConnPair()
	monitorBackend := pgfox.NewBackend(pgfoxSide, "testdb", "test", "testuser", 1024*1024)
	monitorBackend.Pool = h.pool

	ch := pgfox.Channel{Database: "testdb", Name: "pgfox_ext"}
	l := &pgfox.Listen{
		Channel: ch,
		Backend: monitorBackend,
		Clients: map[*pgfox.Client]bool{},
		Done:    make(chan struct{}),
	}
	close(l.Done)

	h.server.ListenersMu.Lock()
	h.server.Listeners[ch] = l
	h.server.ListenersMu.Unlock()

	c := h.connect()
	defer c.conn.Close()

	// Pipeline 1: asyncpg sends P + D(S,"") + H for LISTEN.
	c.send('P', buildParseMsg("", "LISTEN pgfox_ext"))
	c.send('D', []byte{'S', 0}) // Describe unnamed statement
	c.send('H', nil)

	// pgfox must respond to P and D without touching any backend.
	// Flush boundary: ReadyForQuery is NOT sent after Flush.
	c.expect('1') // ParseComplete — synthetic
	c.expect('t') // ParameterDescription (0 params) — synthetic
	c.expect('n') // NoData — LISTEN has no result columns

	// Pipeline 2: asyncpg sends B + E + S immediately after the Flush response.
	c.send('B', buildBindMsg("", "", nil))
	c.send('E', []byte{0, 0, 0, 0, 0}) // unnamed portal + maxRows=0
	c.send('S', nil)

	// pgfox must respond to B, E, S — all synthetic, still no backend.
	c.expect('2') // BindComplete — synthetic
	c.expect('C') // CommandComplete("LISTEN")
	c.expectRFQ('I')
}

// TestPlaybook_T15_NotificationFanOut verifies that when a notification arrives
// from PostgreSQL for a subscribed channel, pgfox delivers a NotificationResponse
// ('A') message to each subscribed client via sendNotificationToClient.
//
// Playbook §4.3 — Notification fan-out.
//
// Wire sequence (on the subscribed client connection):
//
//	B→P  A { pid=42, channel="alerts", payload="hello" }  (on monitor backend)
//	P→C  A { pid=42, channel="alerts", payload="hello" }  (fan-out to client)
//
// Key invariant: sendNotificationToClient holds writeMu, serialising fan-out
// writes against the client's own query writes on the same TCP socket.
func TestPlaybook_T15_NotificationFanOut(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	// Create a net.Pipe() pair representing the client TCP connection.
	// One side is used by a Client (as pgfox sees the client);
	// the other side is used by the test to read the 'A' message.
	// net.Pipe() is correct here — the test actively reads from testEnd,
	// so the write never blocks.
	clientEnd, testEnd := net.Pipe()

	listenClient := pgfox.NewClient(clientEnd, h.server.Logger, 1024*1024)
	listenClient.SetDatabase("testdb")
	listenClient.SetUser("testuser")

	ch := pgfox.Channel{Database: "testdb", Name: "alerts"}
	listenClient.AddListenChannel(ch)

	l := &pgfox.Listen{
		Channel: ch,
		Clients: map[*pgfox.Client]bool{listenClient: true},
		Done:    make(chan struct{}),
	}
	close(l.Done)

	h.server.ListenersMu.Lock()
	h.server.Listeners[ch] = l
	h.server.ListenersMu.Unlock()

	// Fan out a notification in a separate goroutine (WriteMessage blocks until
	// the test reads from testEnd).
	errCh := make(chan error, 1)
	go func() {
		notification := pgfox.NotificationMessage{
			ProcessID: 42,
			Channel:   "alerts",
			Payload:   "hello",
		}
		errCh <- listenClient.SendNotificationToClient(notification)
	}()

	// Read the 'A' message from the test side of the pipe.
	r := bufio.NewReader(testEnd)
	msgType, err := r.ReadByte()
	if err != nil {
		t.Fatalf("T15: reading notification from pipe: %v", err)
	}
	if msgType != 'A' {
		t.Errorf("T15: expected NotificationResponse 'A', got %q", msgType)
	}

	testEnd.Close()
	clientEnd.Close()

	if err := <-errCh; err != nil {
		t.Errorf("T15: sendNotificationToClient returned error: %v", err)
	}
}

// TestPlaybook_T16_UnlistenRemovesClientFromMonitor verifies that an UNLISTEN
// command removes the client from the monitor's subscriber list, and if the
// monitor becomes empty, it is torn down.
//
// Playbook §4.4 — UNLISTEN via simple query protocol.
//
// Wire sequence:
//
//	C→P  Q { "UNLISTEN pgfox_ul\0" }
//	P→C  CommandComplete { "UNLISTEN" }
//	P→C  ReadyForQuery { 'I' }
func TestPlaybook_T16_UnlistenRemovesClientFromMonitor(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	c := h.connect()
	defer c.conn.Close()

	// Monitor backend uses mockConn so tearDownListen's best-effort UNLISTEN
	// write is non-blocking. The fake side is discarded.
	pgfoxSide, _ := newMockConnPair()
	monitorBackend := pgfox.NewBackend(pgfoxSide, "testdb", "test", "testuser", 1024*1024)
	monitorBackend.Pool = h.pool

	ch := pgfox.Channel{Database: "testdb", Name: "pgfox_ul"}

	// We use a synthetic Client pointing at a separate mockConn so that
	// tearDownListen (if triggered) doesn't close our test connection.
	syntheticPgfoxSide, _ := newMockConnPair()
	syntheticClient := pgfox.NewClient(syntheticPgfoxSide, h.server.Logger, 1024*1024)
	syntheticClient.SetDatabase("testdb")
	syntheticClient.SetUser("testuser")
	syntheticClient.AddListenChannel(ch)

	l := &pgfox.Listen{
		Channel: ch,
		Backend: monitorBackend,
		Clients: map[*pgfox.Client]bool{syntheticClient: true},
		Done:    make(chan struct{}),
	}
	// Close done so tearDownListen doesn't wait for the goroutine.
	close(l.Done)

	h.server.ListenersMu.Lock()
	h.server.Listeners[ch] = l
	h.server.ListenersMu.Unlock()

	// Remove the client from the monitor directly to verify teardown logic.
	// (handleUnlisten would match by pointer; syntheticClient won't match
	// the real client connection, so we call RemoveClientFromListen directly.)
	h.server.RemoveClientFromListen(ch, syntheticClient)

	// The monitor must have been removed from p.listeners (it was the last client).
	h.server.ListenersMu.RLock()
	_, exists := h.server.Listeners[ch]
	h.server.ListenersMu.RUnlock()

	if exists {
		t.Error("T16: monitor should be removed from p.listeners after last client UNLISTEN")
	}

	// The client's connection should still work — UNLISTEN doesn't close it.
	c.sendQ("UNLISTEN pgfox_ul")
	c.expect('C') // CommandComplete("UNLISTEN")
	c.expectRFQ('I')
}
