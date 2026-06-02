package tests

// harness_test.go — shared test infrastructure for pgfox protocol tests.
//
// Provides three layers used by all test files in this package:
//
//  1. pgConn      — client-side wire protocol helper. Wraps a net.Conn with
//                   typed send/recv helpers for building exact PostgreSQL
//                   message sequences without a real driver.
//
//  2. fakeBackend — server-side wire protocol helper. Sits on the backend
//                   side of a mockConn and lets each test control exactly
//                   what PostgreSQL would respond with.
//
//  3. testHarness — wires a real pgfox Server to fake backends via mockConn,
//                   bypassing TLS and SCRAM entirely. Tests interact with
//                   pgfox exactly as a real client would, at the wire level,
//                   without needing a running PostgreSQL instance.
//
// Architecture:
//
//	[test client]  ←→  [pgfox Server]  ←→  [fakeBackend goroutine]
//	  pgConn            net.Listener         mockConn
//	  (TCP)             (real Server)        (Backend)
//
// The harness pre-populates the pool with mockConn-backed Backends so
// pgfox's borrowConn succeeds immediately. No TLS, no SCRAM, no cert files.
//
// mockConn design:
//
// net.Pipe() is synchronous and unbuffered — a write on one end blocks until
// the other end reads. This causes deadlocks when pgfox pipelines multiple
// messages (e.g. P+B+E+S) and the fake backend tries to respond to the first
// before consuming the rest: both sides block waiting for the other to read.
//
// mockConn solves this with two independent buffered pipes — one for each
// direction. Writes never block (they go into a bytes.Buffer immediately);
// reads block until data is available, matching real TCP behaviour. This lets
// pgfox pipeline messages freely and lets the fake backend respond per-message
// without any artificial ordering constraint.
//
// mockConn also implements SetReadDeadline correctly so that Backend.IsAlive()
// works as expected: the 1ms peek times out and returns true (connection alive,
// no data pending), exactly as it would on a real idle TCP socket.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/aekis-dev/pgfox/pkg/logger"
	"github.com/aekis-dev/pgfox/pkg/pgfox"
)

// =============================================================================
// mockConn — buffered net.Conn for deterministic protocol testing
// =============================================================================

// timeoutError is a net.Error that signals a deadline exceeded.
type timeoutError struct{}

func (e *timeoutError) Error() string   { return "i/o timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }

// bufferedPipe is a one-directional in-memory pipe with independent read/write
// ends. Writes never block. Reads block until data is available or the pipe is
// closed. An optional read deadline wakes blocked reads with a timeout error.
type bufferedPipe struct {
	mu           sync.Mutex
	cond         *sync.Cond
	buf          bytes.Buffer
	closed       bool
	readDeadline time.Time // zero = no deadline
}

func newBufferedPipe() *bufferedPipe {
	p := &bufferedPipe{}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// write appends data to the buffer and wakes any blocked reader. Never blocks.
func (p *bufferedPipe) write(data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return io.ErrClosedPipe
	}
	p.buf.Write(data)
	p.cond.Broadcast()
	return nil
}

// read blocks until data is available, the pipe is closed, or the read
// deadline expires. Returns (0, timeoutError) on deadline, (0, io.EOF) when
// the pipe is closed with no data remaining.
func (p *bufferedPipe) read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.buf.Len() == 0 && !p.closed {
		if !p.readDeadline.IsZero() && !time.Now().Before(p.readDeadline) {
			return 0, &timeoutError{}
		}
		p.cond.Wait()
	}
	if p.buf.Len() == 0 {
		return 0, io.EOF
	}
	return p.buf.Read(b)
}

// setReadDeadline updates the deadline and wakes any blocked reader so it can
// re-evaluate the deadline condition. A goroutine fires a broadcast at the
// deadline moment to guarantee the reader wakes even without a new write.
func (p *bufferedPipe) setReadDeadline(t time.Time) {
	p.mu.Lock()
	p.readDeadline = t
	p.cond.Broadcast() // wake reader to re-check immediately
	p.mu.Unlock()

	if !t.IsZero() {
		go func() {
			d := time.Until(t)
			if d > 0 {
				time.Sleep(d)
			}
			p.mu.Lock()
			p.cond.Broadcast()
			p.mu.Unlock()
		}()
	}
}

// close marks the pipe as closed and wakes any blocked reader.
func (p *bufferedPipe) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.cond.Broadcast()
}

// mockAddr is a minimal net.Addr for mockConn.
type mockAddr struct{ s string }

func (a mockAddr) Network() string { return "mock" }
func (a mockAddr) String() string  { return a.s }

// mockConn is a net.Conn backed by two bufferedPipes — one for each direction.
// It behaves like a real TCP connection: writes are non-blocking, reads block
// until data arrives, and SetReadDeadline is fully supported.
//
// Two mockConn instances are created in pairs via newMockConnPair(). Each side
// writes into the other side's read buffer.
type mockConn struct {
	readBuf  *bufferedPipe // this side reads from here
	writeBuf *bufferedPipe // this side writes into here (other side reads from here)
	local    net.Addr
	remote   net.Addr
	closed   sync.Once
}

// newMockConnPair returns two connected mockConn instances: pgfoxSide is given
// to pgfox's Backend, fakeSide is used by the fakeBackend in tests.
func newMockConnPair() (pgfoxSide, fakeSide *mockConn) {
	aToB := newBufferedPipe() // pgfox writes → fake reads
	bToA := newBufferedPipe() // fake writes → pgfox reads

	pgfoxSide = &mockConn{
		readBuf:  bToA,
		writeBuf: aToB,
		local:    mockAddr{"pgfox:0"},
		remote:   mockAddr{"fake:0"},
	}
	fakeSide = &mockConn{
		readBuf:  aToB,
		writeBuf: bToA,
		local:    mockAddr{"fake:0"},
		remote:   mockAddr{"pgfox:0"},
	}
	return
}

func (c *mockConn) Read(b []byte) (int, error) {
	return c.readBuf.read(b)
}

func (c *mockConn) Write(b []byte) (int, error) {
	if err := c.writeBuf.write(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (c *mockConn) Close() error {
	c.closed.Do(func() {
		c.readBuf.close()
		c.writeBuf.close()
	})
	return nil
}

func (c *mockConn) LocalAddr() net.Addr  { return c.local }
func (c *mockConn) RemoteAddr() net.Addr { return c.remote }

// SetDeadline sets both read and write deadlines. Write deadlines are ignored
// (writes never block in mockConn). Only the read deadline has effect.
func (c *mockConn) SetDeadline(t time.Time) error {
	return c.SetReadDeadline(t)
}

// SetReadDeadline updates the read deadline on the read buffer. This enables
// Backend.IsAlive() to work correctly: it sets a 1ms deadline, attempts a
// peek read on an empty buffer, the read times out, and IsAlive returns true
// (no data pending = connection is alive and idle).
func (c *mockConn) SetReadDeadline(t time.Time) error {
	c.readBuf.setReadDeadline(t)
	return nil
}

// SetWriteDeadline is a no-op — writes in mockConn are always non-blocking.
func (c *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// =============================================================================
// pgConn — client-side wire protocol helper
// =============================================================================

// pgConn wraps a plain net.Conn and provides typed send/recv helpers for
// speaking the PostgreSQL frontend (client) protocol against pgfox.
type pgConn struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func newPGConn(t *testing.T, conn net.Conn) *pgConn {
	t.Helper()
	return &pgConn{t: t, conn: conn, r: bufio.NewReader(conn)}
}

// send writes a single typed message (msgType + 4-byte length + body).
func (c *pgConn) send(msgType byte, body []byte) {
	c.t.Helper()
	buf := make([]byte, 5+len(body))
	buf[0] = msgType
	binary.BigEndian.PutUint32(buf[1:5], uint32(4+len(body)))
	copy(buf[5:], body)
	if _, err := c.conn.Write(buf); err != nil {
		c.t.Fatalf("send %q: %v", string([]byte{msgType}), err)
	}
}

// sendQ sends a simple Query ('Q') message with a null-terminated SQL string.
// Ref: PostgreSQL protocol §55.2.2 Simple Query.
func (c *pgConn) sendQ(sql string) {
	c.t.Helper()
	c.send('Q', append([]byte(sql), 0))
}

// sendParseDescribeFlush sends the three-message prepare pipeline that asyncpg
// uses on the first call to a statement:
//
//	P (Parse, named or unnamed)
//	D (Describe statement)
//	H (Flush — triggers immediate response without a ReadyForQuery)
//
// Ref: playbook §3.1 (named remappable), §3.3 (unnamed).
func (c *pgConn) sendParseDescribeFlush(stmtName, sql string) {
	c.t.Helper()
	c.send('P', buildParseMsg(stmtName, sql))
	c.send('D', append([]byte{'S'}, append([]byte(stmtName), 0)...))
	c.send('H', nil)
}

// sendBindExecuteSync sends the three-message execute pipeline:
//
//	B (Bind — associates parameters with a prepared statement into a portal)
//	E (Execute portal)
//	S (Sync — triggers ReadyForQuery)
//
// Ref: playbook §3.1.
func (c *pgConn) sendBindExecuteSync(portal, stmtName string, params [][]byte) {
	c.t.Helper()
	c.send('B', buildBindMsg(portal, stmtName, params))
	// Execute: portal name (null-terminated) + maxRows (int32 = 0 = unlimited).
	execBody := append([]byte(portal), 0)
	execBody = append(execBody, 0, 0, 0, 0)
	c.send('E', execBody)
	c.send('S', nil)
}

// sendCloseSync sends a statement-close pipeline:
//
//	C (Close statement)
//	S (Sync)
//
// Ref: playbook §3.4 (remapped close), §3.5 (passthrough close).
func (c *pgConn) sendCloseSync(stmtName string) {
	c.t.Helper()
	body := append([]byte{'S'}, append([]byte(stmtName), 0)...)
	c.send('C', body)
	c.send('S', nil)
}

// recv reads and returns the next typed message from pgfox.
func (c *pgConn) recv() (byte, []byte) {
	c.t.Helper()
	msgType, err := c.r.ReadByte()
	if err != nil {
		c.t.Fatalf("recv ReadByte: %v", err)
	}
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(c.r, lenBuf); err != nil {
		c.t.Fatalf("recv ReadFull length: %v", err)
	}
	length := int(binary.BigEndian.Uint32(lenBuf)) - 4
	body := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(c.r, body); err != nil {
			c.t.Fatalf("recv ReadFull body: %v", err)
		}
	}
	return msgType, body
}

// expect reads the next message and fails if it is not the expected type.
// Returns the message body for further inspection.
func (c *pgConn) expect(want byte) []byte {
	c.t.Helper()
	got, body := c.recv()
	if got != want {
		c.t.Fatalf("expect %q (%d), got %q (%d) body=%q",
			want, want, got, got, body)
	}
	return body
}

// expectRFQ reads the next message and fails if it is not ReadyForQuery ('Z')
// with the expected transaction status byte ('I', 'T', or 'E').
func (c *pgConn) expectRFQ(wantStatus byte) {
	c.t.Helper()
	body := c.expect('Z')
	var got byte = '?'
	if len(body) > 0 {
		got = body[0]
	}
	if got != wantStatus {
		c.t.Fatalf("expectRFQ: want status %q, got %q", wantStatus, got)
	}
}

// drainUntilRFQ reads and discards all messages until ReadyForQuery.
// Returns the transaction status byte from the ReadyForQuery message.
func (c *pgConn) drainUntilRFQ() byte {
	c.t.Helper()
	for {
		mt, body := c.recv()
		if mt == 'Z' {
			if len(body) > 0 {
				return body[0]
			}
			return 'I'
		}
	}
}

// =============================================================================
// Wire message builders
// =============================================================================

// buildParseMsg builds the body of a Parse ('P') message.
//
//	stmtName  — prepared statement name; "" = unnamed
//	sql       — the query text
//
// Layout: stmtName\0 + sql\0 + numParams(int16=0)
func buildParseMsg(stmtName, sql string) []byte {
	var buf []byte
	buf = append(buf, []byte(stmtName)...)
	buf = append(buf, 0)
	buf = append(buf, []byte(sql)...)
	buf = append(buf, 0)
	buf = append(buf, 0, 0) // numParams = 0 (let server infer OIDs)
	return buf
}

// buildBindMsg builds the body of a Bind ('B') message with text-format params.
//
//	portal    — destination portal name; "" = unnamed
//	stmtName  — source prepared statement name; "" = unnamed
//	params    — parameter values as raw byte slices (text format)
//
// Layout: portal\0 + stmtName\0 + numParamFmts(0) + numParams + params + numResultFmts(0)
func buildBindMsg(portal, stmtName string, params [][]byte) []byte {
	var buf []byte
	buf = append(buf, []byte(portal)...)
	buf = append(buf, 0)
	buf = append(buf, []byte(stmtName)...)
	buf = append(buf, 0)
	buf = append(buf, 0, 0) // numParamFormats = 0 → all text
	buf = append(buf, byte(len(params)>>8), byte(len(params)))
	for _, p := range params {
		buf = append(buf,
			byte(len(p)>>24), byte(len(p)>>16), byte(len(p)>>8), byte(len(p)))
		buf = append(buf, p...)
	}
	buf = append(buf, 0, 0) // numResultFormats = 0 → all text
	return buf
}

// =============================================================================
// fakeBackend — PostgreSQL server-side wire protocol helper
// =============================================================================

// fakeBackend controls the backend (PostgreSQL server) side of a mockConn.
// Each test goroutine drives a fakeBackend to simulate specific PostgreSQL
// response sequences. The send* methods correspond directly to PostgreSQL
// backend message types.
type fakeBackend struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
	mu   sync.Mutex // serialises w.Flush() calls
}

func newFakeBackend(t *testing.T, conn net.Conn) *fakeBackend {
	return &fakeBackend{
		t:    t,
		conn: conn,
		r:    bufio.NewReader(conn),
		w:    bufio.NewWriter(conn),
	}
}

// send writes a typed message to pgfox (the backend-to-frontend direction).
func (fb *fakeBackend) send(msgType byte, body []byte) {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	buf := make([]byte, 5+len(body))
	buf[0] = msgType
	binary.BigEndian.PutUint32(buf[1:5], uint32(4+len(body)))
	copy(buf[5:], body)
	fb.w.Write(buf)
	fb.w.Flush()
}

// recvMsg reads the next message that pgfox sent to the backend.
// Returns (0, nil) on EOF/close — callers use this as a loop-exit signal.
func (fb *fakeBackend) recvMsg() (byte, []byte) {
	msgType, err := fb.r.ReadByte()
	if err != nil {
		if err == io.EOF {
			return 0, nil
		}
		fb.t.Errorf("fakeBackend recvMsg: %v", err)
		return 0, nil
	}
	lenBuf := make([]byte, 4)
	io.ReadFull(fb.r, lenBuf)
	length := int(binary.BigEndian.Uint32(lenBuf)) - 4
	body := make([]byte, length)
	if length > 0 {
		io.ReadFull(fb.r, body)
	}
	return msgType, body
}

// --- Backend message senders (server → client direction) ---

// sendRFQ sends ReadyForQuery ('Z') with the given transaction status byte.
// status: 'I' = idle, 'T' = in transaction, 'E' = failed transaction.
func (fb *fakeBackend) sendRFQ(status byte) { fb.send('Z', []byte{status}) }

// sendCC sends CommandComplete ('C') with the given command tag (e.g. "SELECT 1").
func (fb *fakeBackend) sendCC(tag string) { fb.send('C', append([]byte(tag), 0)) }

// sendParseComplete sends ParseComplete ('1'). Confirms a Parse succeeded.
func (fb *fakeBackend) sendParseComplete() { fb.send('1', nil) }

// sendBindComplete sends BindComplete ('2'). Confirms a Bind succeeded.
func (fb *fakeBackend) sendBindComplete() { fb.send('2', nil) }

// sendCloseComplete sends CloseComplete ('3'). Confirms a Close succeeded.
func (fb *fakeBackend) sendCloseComplete() { fb.send('3', nil) }

// sendNoData sends NoData ('n'). Sent in response to Describe when the
// statement produces no result columns (e.g. LISTEN, INSERT without RETURNING).
func (fb *fakeBackend) sendNoData() { fb.send('n', nil) }

// sendParameterDescription sends ParameterDescription ('t') listing the OIDs
// of the statement's parameters. Pass nil for a zero-parameter statement.
func (fb *fakeBackend) sendParameterDescription(oids []uint32) {
	body := make([]byte, 2+4*len(oids))
	binary.BigEndian.PutUint16(body[0:2], uint16(len(oids)))
	for i, oid := range oids {
		binary.BigEndian.PutUint32(body[2+i*4:], oid)
	}
	fb.send('t', body)
}

// sendRowDescription sends RowDescription ('T') describing a single int4 column.
// This is a convenience builder; tests only need one column.
func (fb *fakeBackend) sendRowDescription(colName string) {
	body := []byte{0, 1} // numFields = 1
	body = append(body, []byte(colName)...)
	body = append(body, 0)
	body = append(body,
		0, 0, 0, 0, // tableOID = 0
		0, 0, // colAttrNum = 0
		0, 0, 0, 23, // typeOID = 23 (int4)
		0, 4, // typeSize = 4
		0xff, 0xff, 0xff, 0xff, // typeMod = -1
		0, 0, // format = 0 (text)
	)
	fb.send('T', body)
}

// sendDataRowText sends DataRow ('D') with a single text-format column value.
func (fb *fakeBackend) sendDataRowText(val string) {
	valBytes := []byte(val)
	body := make([]byte, 2+4+len(valBytes))
	binary.BigEndian.PutUint16(body[0:2], 1) // numFields = 1
	binary.BigEndian.PutUint32(body[2:6], uint32(len(valBytes)))
	copy(body[6:], valBytes)
	fb.send('D', body)
}

// sendErrorResponse sends ErrorResponse ('E') with severity=ERROR and the
// given human-readable message. pgfox forwards this to the client unchanged.
func (fb *fakeBackend) sendErrorResponse(msg string) {
	body := append([]byte("SERROR\x00MERROR: "+msg+"\x00"), 0)
	fb.send('E', body)
}

// =============================================================================
// testHarness — integrates pgfox Server with fake backends
// =============================================================================

// testHarness wires a real pgfox Server to fake PostgreSQL backends using
// mockConn. TLS and SCRAM are bypassed: the accept goroutine sets the client
// fields directly and immediately sends ReadyForQuery('I').
//
// Usage:
//
//	h := newHarness(t)
//	defer h.close()
//	backend, fake := h.addBackend()
//	go func() { /* drive fake */ }()
//	c := h.connect()
//	c.sendQ("SELECT 1")
//	c.expectRFQ('I')
type testHarness struct {
	t         *testing.T
	server    *pgfox.Server
	target    *pgfox.Target
	pool      *pgfox.Pool
	pgfoxAddr string
	cancel    context.CancelFunc
	done      chan struct{}
}

// newHarness creates and starts a pgfox Server with one target and an empty
// pool. Call addBackend() to give it fake PostgreSQL connections.
func newHarness(t *testing.T) *testHarness {
	t.Helper()

	log := logger.NewLogger(logger.LoggingConfig{Level: "error", Format: "text"})

	ctx, cancel := context.WithCancel(context.Background())

	target := &pgfox.Target{
		Name:           "test",
		Host:           "127.0.0.1",
		Port:           0,
		MaxConnections: 10,
		ConnectTimeout: 5 * time.Second,
		StmtCache:      pgfox.NewStmtCache(),
		Ready:          make(chan struct{}),
		ScramCh:        make(chan pgfox.ScramRequest),
		CloseCh:        make(chan *pgfox.Backend, 10),
		ConnReady:      make(chan struct{}, 10),
		Demand:         make(chan struct{}, 1),
		PoolRegistered: make(chan *pgfox.Pool, 64),
		Params:         make(map[string]string),
		Context:        ctx,
		Cancel:         cancel,
	}

	pool := &pgfox.Pool{
		Target:   target,
		DbName:   "testdb",
		Username: "testuser",
		Queue:    make(chan *pgfox.Backend, 10),
		All:      make([]*pgfox.Backend, 0, 10),
	}
	target.Pools.Store(pgfox.PoolKey("testdb", "testuser"), pool)
	target.CachedPools = []*pgfox.Pool{pool}

	// Pick an ephemeral port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cfg := pgfox.Config{
		Server: pgfox.ServerConfig{
			ListenAddr:     addr,
			MaxMessageSize: 1024 * 1024,
			ConnectTimeout: 5 * time.Second,
		},
		Targets: []*pgfox.Target{target},
	}

	pgfoxLn, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("pgfox listen: %v", err)
	}

	srv := &pgfox.Server{
		Config:    cfg,
		Listener:  pgfoxLn,
		Targets:   []*pgfox.Target{target},
		Listeners: make(map[pgfox.Channel]*pgfox.Listen),
		Logger:    log,
	}
	srv.Context, srv.Cancel = ctx, cancel

	// Background closer for dead backend connections — mirrors what target.run()
	// does for the CloseCh. Keeps TotalOpen accurate during tests.
	go func() {
		for {
			select {
			case backend := <-target.CloseCh:
				backend.Conn.Close()
				target.TotalOpen--
			case <-ctx.Done():
				return
			}
		}
	}()

	// Accept loop: skip TLS/SCRAM and go straight to the message loop.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := pgfoxLn.Accept()
			if err != nil {
				return
			}
			srv.Wg.Add(1)
			go func(c net.Conn) {
				defer srv.Wg.Done()
				defer c.Close()
				cl := pgfox.NewClient(c, srv.Logger.WithClient(c.RemoteAddr().String()),
					cfg.Server.MaxMessageSize)
				cl.SetDatabase("testdb")
				cl.SetUser("testuser")
				cl.SetAuthenticated(true)
				// Send ReadyForQuery immediately — no startup negotiation in tests.
				cl.SendReadyForQuery('I')
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					if err := srv.HandleClientMessage(cl); err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	return &testHarness{
		t:         t,
		server:    srv,
		target:    target,
		pool:      pool,
		pgfoxAddr: addr,
		cancel:    cancel,
		done:      done,
	}
}

// addBackend creates a mockConn-backed Backend and deposits it into the pool's
// idle queue. Returns both ends: pgfox uses backend, and the test controls fake.
//
// mockConn replaces net.Pipe() to avoid pipeline deadlocks: since writes are
// non-blocking, pgfox can pipeline P+B+E+S freely and the fake backend can
// respond per-message without ordering constraints.
func (h *testHarness) addBackend() (*pgfox.Backend, *fakeBackend) {
	h.t.Helper()
	pgfoxSide, fakeSide := newMockConnPair()
	backend := pgfox.NewBackend(pgfoxSide, "testdb", "test", "testuser", 1024*1024)
	backend.Pool = h.pool
	h.pool.All = append(h.pool.All, backend)
	h.pool.Queue <- backend
	h.target.TotalOpen++
	return backend, newFakeBackend(h.t, fakeSide)
}

// connect dials pgfox and reads the synthetic ReadyForQuery('I') that the
// harness sends immediately (bypassing SCRAM). Returns a pgConn ready for use.
func (h *testHarness) connect() *pgConn {
	h.t.Helper()
	conn, err := net.Dial("tcp", h.pgfoxAddr)
	if err != nil {
		h.t.Fatalf("connect: %v", err)
	}
	c := newPGConn(h.t, conn)
	c.expectRFQ('I') // harness sends this on accept
	return c
}

// close cancels the server context and waits for the accept loop to exit.
func (h *testHarness) close() {
	h.cancel()
	h.server.Listener.Close()
	<-h.done
}

// =============================================================================
// Shared body-parsing helpers used across test files
// =============================================================================

// parseStmtName extracts the null-terminated statement name from the body of
// a Parse ('P') or Close ('C') or Describe ('D') message.
func parseStmtName(body []byte) string {
	for i, b := range body {
		if b == 0 {
			return string(body[:i])
		}
	}
	return string(body)
}

// bindStmtName extracts the statement name from a Bind ('B') message body.
// The body begins with the null-terminated portal name, followed by the
// null-terminated statement name.
func bindStmtName(body []byte) string {
	// Skip portal name (first null-terminated string).
	for i, b := range body {
		if b == 0 {
			rest := body[i+1:]
			for j, c := range rest {
				if c == 0 {
					return string(rest[:j])
				}
			}
		}
	}
	return ""
}

// closeStmtTarget extracts (type byte, name) from a Close ('C') message body.
// type is 'S' for statement, 'P' for portal.
func closeStmtTarget(body []byte) (byte, string) {
	if len(body) < 2 {
		return 0, ""
	}
	closeType := body[0]
	rest := body[1:]
	for i, b := range rest {
		if b == 0 {
			return closeType, string(rest[:i])
		}
	}
	return closeType, string(rest)
}

// nullTermStr extracts a null-terminated string from a byte slice, returning
// everything up to the first null byte (or the whole slice if no null found).
func nullTermStr(b []byte) string {
	for i, c := range b {
		if c == 0 {
			return string(b[:i])
		}
	}
	return string(b)
}
