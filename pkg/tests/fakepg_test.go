package tests

// fakepg_test.go — a declarative, protocol-faithful fake PostgreSQL backend.
//
// Philosophy:
//
//	The fake is allowed to impersonate PostgreSQL. It is NEVER allowed to
//	impersonate pgfox. Every byte the client receives must be produced by
//	pgfox's real code; the fake only ever produces bytes a real PostgreSQL
//	server would, given the exact message stream pgfox sent it.
//
// Tests therefore declare *data and rules*, not message sequences. A test says
// "this database knows query X, which returns these columns/rows with this
// command tag, and moves the transaction like so." The engine below is the
// PostgreSQL protocol state machine: it reads whatever pgfox sends (Parse,
// Bind, Describe, Execute, Sync, Flush, Close, Query, Terminate) and emits the
// response sequence the protocol contract requires for that input — no more,
// no less.
//
// This makes whole classes of bug impossible to hide:
//   - Execute never produces a RowDescription (only Describe does), so a path
//     that forgets to Describe is caught, not masked.
//   - A portal Describe never produces a ParameterDescription.
//   - A Bind to a statement this connection never Parsed is rejected (26000),
//     which is what makes the "inject Parse on a fresh backend" logic provable.
//   - ReadyForQuery always reports the engine's real transaction status, driven
//     by the actual command stream, so pinning tests test something real.
//
// What the engine deliberately does NOT do: plan queries, own a catalog, or
// invent data. Anything it returns comes from the per-backend backendSpec the
// test supplied.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aekis-dev/pgfox/pkg/pgfox"
)

// =============================================================================
// Declarative spec — what a test hands the fake backend
// =============================================================================

// txEffect is the transaction-status transition a command causes.
type txEffect int

const (
	txNone  txEffect = iota // status unchanged
	txBegin                 // → 'T' (in transaction)
	txEnd                   // → 'I' (idle)  — COMMIT / ROLLBACK
	txFail                  // → 'E' (failed transaction)
)

// pgCol describes one result column.
type pgCol struct {
	Name string
	OID  uint32 // type OID: 23=int4, 25=text, 1043=varchar, 16=bool, ...
}

// pgError makes the fake respond with an ErrorResponse instead of data.
type pgError struct {
	Severity string   // default "ERROR"
	Code     string   // SQLSTATE, e.g. "42703"
	Message  string   // human-readable message
	Tx       txEffect // status transition the error causes (usually txFail inside a tx)
}

// queryRule declares how the fake answers one query. A rule is matched against
// the SQL pgfox actually sends — for the cache/extended paths that is the
// canonical "... $1" text carried in the Parse; for simple-query passthrough it
// is the raw 'Q' text. Match precedence: exact SQL, then SQLPrefix, then the
// spec Default, then a loud test failure.
type queryRule struct {
	SQL       string        // exact match
	SQLPrefix string        // prefix match (fallback)
	ParamOIDs []uint32      // ParameterDescription OIDs; nil = infer count from $N (typed int4)
	Columns   []pgCol       // result columns; nil/empty = no rows (NoData on Describe)
	Rows      [][]string    // row values as text; engine encodes per the requested format
	Tag       string        // CommandComplete tag; "" = derived ("SELECT n" / "OK")
	Tx        txEffect      // transaction-status transition this command causes
	Error     *pgError      // non-nil = ErrorResponse instead of normal flow
	Delay     time.Duration // artificial delay before responding (per-query slow)
	Echo      bool          // if true, emit one DataRow whose column is the first bound param
}

// backendSpec is the full declarative description of one fake backend.
type backendSpec struct {
	Rules   []queryRule
	Default *queryRule    // used when no rule matches; nil = fail the test loudly
	Delay   time.Duration // artificial delay before Execute/Query responses (slow backend)
}

// =============================================================================
// pgServer — the protocol state machine
// =============================================================================

type boundPortal struct {
	stmt       string
	params     [][]byte // bound parameter values (text), for Echo rules
	resultFmts []int16
}

type msgTarget struct {
	typ  byte
	name string
}

// pgServer drives the PostgreSQL side of one mockConn for the lifetime of a
// connection. It is single-goroutine (the run loop) for its protocol state, and
// records observed events under mu for tests to assert against afterwards.
type pgServer struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
	spec backendSpec

	// per-connection protocol state — owned by the run goroutine.
	prepared map[string]string      // prepared statement name → SQL
	portals  map[string]boundPortal // portal name → bound info
	txStatus byte                   // 'I' / 'T' / 'E'

	// observed events — guarded by mu, read by test assertions after the
	// round-trip has completed.
	mu             sync.Mutex
	parseCount     int
	parsedNames    []string
	boundNames     []string
	describes      []msgTarget
	closes         []msgTarget
	simpleSQL      []string
	lastResultFmts []int16
}

func newPGServer(t *testing.T, conn net.Conn, spec backendSpec) *pgServer {
	return &pgServer{
		t:        t,
		conn:     conn,
		r:        bufio.NewReader(conn),
		w:        bufio.NewWriter(conn),
		spec:     spec,
		prepared: make(map[string]string),
		portals:  make(map[string]boundPortal),
		txStatus: 'I',
	}
}

// run is the message loop. It exits on EOF/close or Terminate.
func (s *pgServer) run() {
	for {
		msgType, body, ok := s.readMsg()
		if !ok {
			return
		}
		switch msgType {
		case 'P': // Parse
			s.onParse(body)
		case 'B': // Bind
			s.onBind(body)
		case 'D': // Describe
			s.onDescribe(body)
		case 'E': // Execute
			s.onExecute(body)
		case 'S': // Sync
			s.send('Z', []byte{s.txStatus})
		case 'H': // Flush — produces no ReadyForQuery
		case 'C': // Close
			s.onClose(body)
		case 'Q': // simple Query
			s.onSimpleQuery(body)
		case 'X': // Terminate
			return
		default:
			s.t.Errorf("fakepg: unexpected message type %q from pgfox", msgType)
		}
	}
}

// --- message handlers ---

func (s *pgServer) onParse(body []byte) {
	name := pgfoxParseName(body)
	sql := pgfoxParseQuery(body)
	s.prepared[name] = sql

	s.mu.Lock()
	s.parseCount++
	s.parsedNames = append(s.parsedNames, name)
	s.mu.Unlock()

	s.send('1', nil) // ParseComplete
}

func (s *pgServer) onBind(body []byte) {
	portal, stmt, params, resultFmts := parseBindBody(body)

	s.mu.Lock()
	s.boundNames = append(s.boundNames, stmt)
	s.lastResultFmts = append([]int16(nil), resultFmts...)
	s.mu.Unlock()

	if _, ok := s.prepared[stmt]; !ok {
		// Faithful PostgreSQL: cannot bind a statement this connection never
		// prepared. This is what makes the inject-Parse-on-fresh-backend logic
		// provable rather than assumed.
		s.sendError(&pgError{Code: "26000",
			Message: fmt.Sprintf("prepared statement %q does not exist", stmt)})
		return
	}
	s.portals[portal] = boundPortal{stmt: stmt, params: params, resultFmts: resultFmts}
	s.send('2', nil) // BindComplete
}

func (s *pgServer) onDescribe(body []byte) {
	typ, name := pgfoxDescribeTarget(body)

	s.mu.Lock()
	s.describes = append(s.describes, msgTarget{typ, name})
	s.mu.Unlock()

	switch typ {
	case 'S': // statement: ParameterDescription + (RowDescription | NoData)
		sql := s.prepared[name]
		rule := s.match(sql)
		s.sendParameterDescription(s.paramOIDs(rule, sql))
		if len(rule.Columns) > 0 {
			s.sendRowDescription(rule.Columns, nil) // all text (no portal bound)
		} else {
			s.send('n', nil) // NoData
		}
	case 'P': // portal: (RowDescription | NoData), never ParameterDescription
		p := s.portals[name]
		rule := s.match(s.prepared[p.stmt])
		if len(rule.Columns) > 0 {
			s.sendRowDescription(rule.Columns, p.resultFmts)
		} else {
			s.send('n', nil) // NoData
		}
	}
}

func (s *pgServer) onExecute(body []byte) {
	portal := cString(body)
	p := s.portals[portal]
	rule := s.match(s.prepared[p.stmt])

	s.delay(rule)

	if rule.Error != nil {
		s.sendError(rule.Error)
		s.applyTx(rule.Error.Tx)
		return
	}
	rows := rule.Rows
	tag := commandTag(rule)
	if rule.Echo && len(p.params) > 0 {
		rows = [][]string{{string(p.params[0])}}
		if rule.Tag == "" {
			tag = "SELECT 1"
		}
	}
	for _, row := range rows {
		s.sendDataRow(row, rule.Columns, p.resultFmts)
	}
	s.send('C', append([]byte(tag), 0))
	s.applyTx(rule.Tx)
}

func (s *pgServer) onClose(body []byte) {
	typ, name := pgfoxDescribeTarget(body) // Close shares Describe's layout

	s.mu.Lock()
	s.closes = append(s.closes, msgTarget{typ, name})
	s.mu.Unlock()

	switch typ {
	case 'S':
		delete(s.prepared, name)
	case 'P':
		delete(s.portals, name)
	}
	s.send('3', nil) // CloseComplete
}

func (s *pgServer) onSimpleQuery(body []byte) {
	sql := cString(body)

	s.mu.Lock()
	s.simpleSQL = append(s.simpleSQL, sql)
	s.mu.Unlock()

	rule := s.match(sql)

	s.delay(rule)

	if rule.Error != nil {
		s.sendError(rule.Error)
		s.applyTx(rule.Error.Tx)
		s.send('Z', []byte{s.txStatus})
		return
	}
	// Simple query always returns text-format columns (the client never asked
	// for binary; it issued 'Q').
	if len(rule.Columns) > 0 {
		s.sendRowDescription(rule.Columns, nil)
		for _, row := range rule.Rows {
			s.sendDataRow(row, rule.Columns, nil)
		}
	}
	s.checkTxInvariant(rule)
	s.send('C', append([]byte(commandTag(rule)), 0))
	s.applyTx(rule.Tx)
	s.send('Z', []byte{s.txStatus})
}

// checkTxInvariant flags transaction-routing bugs a correct pool never creates:
// a BEGIN arriving at a backend already in a transaction (two clients sharing a
// pinned backend), or a COMMIT/ROLLBACK arriving at an idle backend.
func (s *pgServer) checkTxInvariant(r queryRule) {
	if r.Tx == txBegin && s.txStatus == 'T' {
		s.t.Errorf("fakepg: BEGIN while already in transaction — backend shared across clients?")
	}
	if r.Tx == txEnd && s.txStatus == 'I' {
		s.t.Errorf("fakepg: COMMIT/ROLLBACK outside a transaction — broken pinning?")
	}
}

// --- rule resolution & helpers ---

// match resolves a SQL string to a rule: exact, then prefix, then Default, then
// a loud failure (so a test cannot silently pass against an undeclared query).
func (s *pgServer) match(sql string) queryRule {
	for _, r := range s.spec.Rules {
		if r.SQL != "" && r.SQL == sql {
			return r
		}
	}
	for _, r := range s.spec.Rules {
		if r.SQLPrefix != "" && strings.HasPrefix(sql, r.SQLPrefix) {
			return r
		}
	}
	if s.spec.Default != nil {
		return *s.spec.Default
	}
	s.t.Errorf("fakepg: no rule matches SQL %q (declare it in the backendSpec or set Default)", sql)
	return queryRule{Tag: "OK"}
}

func (s *pgServer) paramOIDs(r queryRule, sql string) []uint32 {
	if r.ParamOIDs != nil {
		return r.ParamOIDs
	}
	n := countParams(sql)
	out := make([]uint32, n)
	for i := range out {
		out[i] = 23 // int4 by default
	}
	return out
}

// delay sleeps for the rule's delay if set, otherwise the spec-wide delay.
func (s *pgServer) delay(r queryRule) {
	d := s.spec.Delay
	if r.Delay > 0 {
		d = r.Delay
	}
	if d > 0 {
		time.Sleep(d)
	}
}

func (s *pgServer) applyTx(e txEffect) {
	switch e {
	case txBegin:
		s.txStatus = 'T'
	case txEnd:
		s.txStatus = 'I'
	case txFail:
		s.txStatus = 'E'
	}
}

// --- wire writers ---

func (s *pgServer) send(msgType byte, body []byte) {
	buf := make([]byte, 5+len(body))
	buf[0] = msgType
	binary.BigEndian.PutUint32(buf[1:5], uint32(4+len(body)))
	copy(buf[5:], body)
	if _, err := s.w.Write(buf); err != nil {
		return
	}
	s.w.Flush()
}

func (s *pgServer) sendParameterDescription(oids []uint32) {
	body := make([]byte, 2+4*len(oids))
	binary.BigEndian.PutUint16(body[0:2], uint16(len(oids)))
	for i, oid := range oids {
		binary.BigEndian.PutUint32(body[2+i*4:], oid)
	}
	s.send('t', body)
}

func (s *pgServer) sendRowDescription(cols []pgCol, fmts []int16) {
	var body []byte
	body = appendU16(body, uint16(len(cols)))
	for i, c := range cols {
		body = append(body, []byte(c.Name)...)
		body = append(body, 0)
		body = appendU32(body, 0) // table OID
		body = appendU16(body, 0) // column attr number
		body = appendU32(body, c.OID)
		body = appendU16(body, uint16(typeSize(c.OID))) // type size
		body = appendU32(body, 0xFFFFFFFF)              // type modifier -1
		body = appendU16(body, uint16(fmtFor(fmts, i))) // format code
	}
	s.send('T', body)
}

func (s *pgServer) sendDataRow(values []string, cols []pgCol, fmts []int16) {
	var body []byte
	body = appendU16(body, uint16(len(values)))
	for i, v := range values {
		var oid uint32
		if i < len(cols) {
			oid = cols[i].OID
		}
		enc := encodeValue(v, oid, fmtFor(fmts, i))
		body = appendU32(body, uint32(len(enc)))
		body = append(body, enc...)
	}
	s.send('D', body)
}

func (s *pgServer) sendError(e *pgError) {
	sev := e.Severity
	if sev == "" {
		sev = "ERROR"
	}
	var body []byte
	body = append(body, 'S')
	body = append(body, []byte(sev)...)
	body = append(body, 0)
	if e.Code != "" {
		body = append(body, 'C')
		body = append(body, []byte(e.Code)...)
		body = append(body, 0)
	}
	body = append(body, 'M')
	body = append(body, []byte(e.Message)...)
	body = append(body, 0)
	body = append(body, 0) // terminator
	s.send('E', body)
}

// readMsg reads one framed message. ok=false on EOF/close.
func (s *pgServer) readMsg() (byte, []byte, bool) {
	msgType, err := s.r.ReadByte()
	if err != nil {
		return 0, nil, false
	}
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(s.r, lenBuf); err != nil {
		return 0, nil, false
	}
	length := int(binary.BigEndian.Uint32(lenBuf)) - 4
	if length < 0 {
		return 0, nil, false
	}
	body := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(s.r, body); err != nil {
			return 0, nil, false
		}
	}
	return msgType, body, true
}

// =============================================================================
// Observation accessors (read by tests after the round-trip)
// =============================================================================

func (s *pgServer) ParseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.parseCount
}

func (s *pgServer) ParsedNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.parsedNames...)
}

func (s *pgServer) BoundNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.boundNames...)
}

func (s *pgServer) Describes() []msgTarget {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]msgTarget(nil), s.describes...)
}

func (s *pgServer) Closes() []msgTarget {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]msgTarget(nil), s.closes...)
}

func (s *pgServer) SimpleQueries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.simpleSQL...)
}

func (s *pgServer) LastResultFormats() []int16 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int16(nil), s.lastResultFmts...)
}

// sawDescribePortal reports whether a portal Describe ('D','P') was received.
func (s *pgServer) sawDescribePortal() bool {
	for _, d := range s.Describes() {
		if d.typ == 'P' {
			return true
		}
	}
	return false
}

// =============================================================================
// pure helpers
// =============================================================================

func commandTag(r queryRule) string {
	if r.Tag != "" {
		return r.Tag
	}
	if len(r.Columns) > 0 {
		return fmt.Sprintf("SELECT %d", len(r.Rows))
	}
	return "OK"
}

func fmtFor(fmts []int16, i int) int16 {
	switch len(fmts) {
	case 0:
		return 0 // no format codes = all text
	case 1:
		return fmts[0] // single code applies to all columns
	default:
		if i < len(fmts) {
			return fmts[i]
		}
		return 0
	}
}

func typeSize(oid uint32) int {
	switch oid {
	case 23: // int4
		return 4
	case 16: // bool
		return 1
	default:
		return 0xFFFF // variable / -1
	}
}

// encodeValue encodes a text value into the requested wire format. Text format
// (or unknown binary types) passes the bytes through; binary int4 packs 4 BE
// bytes. Extend here as more typed binary cases are genuinely needed.
func encodeValue(v string, oid uint32, format int16) []byte {
	if format == 1 && oid == 23 {
		n, err := strconv.ParseInt(v, 10, 32)
		if err == nil {
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, uint32(int32(n)))
			return b
		}
	}
	return []byte(v)
}

// countParams returns the highest $N placeholder index in sql.
func countParams(sql string) int {
	max := 0
	for i := 0; i < len(sql); i++ {
		if sql[i] != '$' {
			continue
		}
		j := i + 1
		for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
			j++
		}
		if j > i+1 {
			if n, err := strconv.Atoi(sql[i+1 : j]); err == nil && n > max {
				max = n
			}
		}
		i = j - 1
	}
	return max
}

// cString returns the first null-terminated string in b (without the null).
func cString(b []byte) string {
	for i, c := range b {
		if c == 0 {
			return string(b[:i])
		}
	}
	return string(b)
}

// parseBindBody extracts the portal name, statement name, and result-format
// codes from a Bind ('B') message body.
//
// Layout: portal\0 stmt\0 int16 nParamFmts [int16]* int16 nParams (int32 len +
// bytes)* int16 nResultFmts [int16]*.
func parseBindBody(body []byte) (portal, stmt string, params [][]byte, resultFmts []int16) {
	pos := 0
	readCStr := func() string {
		start := pos
		for pos < len(body) && body[pos] != 0 {
			pos++
		}
		s := string(body[start:pos])
		pos++ // consume null
		return s
	}
	readU16 := func() int {
		if pos+2 > len(body) {
			return -1
		}
		v := int(body[pos])<<8 | int(body[pos+1])
		pos += 2
		return v
	}

	portal = readCStr()
	stmt = readCStr()

	nFmt := readU16()
	if nFmt < 0 {
		return
	}
	pos += nFmt * 2 // skip parameter format codes

	nParams := readU16()
	if nParams < 0 {
		return
	}
	for i := 0; i < nParams; i++ {
		if pos+4 > len(body) {
			return
		}
		l := int32(binary.BigEndian.Uint32(body[pos:]))
		pos += 4
		if l < 0 {
			params = append(params, nil) // SQL NULL
			continue
		}
		if pos+int(l) > len(body) {
			return
		}
		params = append(params, append([]byte(nil), body[pos:pos+int(l)]...))
		pos += int(l)
	}

	nRes := readU16()
	if nRes <= 0 {
		return
	}
	for i := 0; i < nRes; i++ {
		v := readU16()
		if v < 0 {
			return
		}
		resultFmts = append(resultFmts, int16(v))
	}
	return
}

func appendU16(b []byte, v uint16) []byte { return append(b, byte(v>>8), byte(v)) }
func appendU32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// Thin wrappers over the pgfox exported body parsers, so the engine and the
// production code parse Parse/Describe bodies the same way.
func pgfoxParseName(body []byte) string           { return pgfox.ParseBodyStatementName(body) }
func pgfoxParseQuery(body []byte) string          { return pgfox.ParseBodyQuery(body) }
func pgfoxDescribeTarget(b []byte) (byte, string) { return pgfox.DescribeBodyTarget(b) }
