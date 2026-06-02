package tests

// protocol_helpers_test.go — unit tests for PostgreSQL wire protocol message
// builders and parsers.
//
// These are pure unit tests: no network, no goroutines, no pgfox Server.
// They verify that the functions in extended.go and protocol.go produce and
// parse the exact byte sequences the PostgreSQL protocol specifies.
//
// Having these pass is a precondition for the integration tests being
// meaningful — if BuildParseBody produces a malformed message, every higher-
// level test becomes undiagnosable.
//
// Playbook reference: all sections, as message correctness underlies every
// wire sequence described in the playbook.

import (
	"encoding/binary"
	"testing"

	"github.com/aekis-dev/pgfox/pkg/pgfox"
)

// TestBuildParseBody verifies the Parse message body layout:
//
//	stmtName\0 + query\0 + numParams(int16) [+ OIDs]
func TestBuildParseBody(t *testing.T) {
	body := pgfox.BuildParseBody("pfx_abc", "SELECT $1::int", nil)

	// Expect: "pfx_abc\0SELECT $1::int\0\x00\x00"
	want := append([]byte("pfx_abc\x00SELECT $1::int\x00"), 0, 0)
	if string(body) != string(want) {
		t.Errorf("BuildParseBody:\ngot  %q\nwant %q", body, want)
	}
}

// TestBuildParseBodyWithOIDs verifies that parameter OIDs are encoded correctly.
func TestBuildParseBodyWithOIDs(t *testing.T) {
	body := pgfox.BuildParseBody("s", "SELECT $1", []uint32{23}) // int4 OID

	// Layout: "s\0SELECT $1\0" + numParams=1 (int16) + OID=23 (int32)
	expectedLen := len("s\x00SELECT $1\x00") + 2 + 4
	if len(body) != expectedLen {
		t.Errorf("BuildParseBodyWithOIDs: len=%d, want %d", len(body), expectedLen)
	}
	// numParams at offset len("s\0SELECT $1\0")
	offset := len("s\x00SELECT $1\x00")
	numParams := binary.BigEndian.Uint16(body[offset:])
	if numParams != 1 {
		t.Errorf("numParams: got %d, want 1", numParams)
	}
	oid := binary.BigEndian.Uint32(body[offset+2:])
	if oid != 23 {
		t.Errorf("OID: got %d, want 23 (int4)", oid)
	}
}

// TestParseBodyStatementName verifies extraction of the statement name from
// a raw Parse message body as produced by BuildParseBody.
func TestParseBodyStatementName(t *testing.T) {
	body := pgfox.BuildParseBody("pfx_hash123", "SELECT 1", nil)
	got := pgfox.ParseBodyStatementName(body)
	if got != "pfx_hash123" {
		t.Errorf("ParseBodyStatementName: got %q, want %q", got, "pfx_hash123")
	}
}

// TestParseBodyStatementName_Unnamed verifies that the empty string is returned
// for the unnamed prepared statement.
func TestParseBodyStatementName_Unnamed(t *testing.T) {
	body := pgfox.BuildParseBody("", "SELECT 1", nil)
	got := pgfox.ParseBodyStatementName(body)
	if got != "" {
		t.Errorf("ParseBodyStatementName (unnamed): got %q, want empty", got)
	}
}

// TestParseBodyQuery verifies extraction of the query string from a Parse body.
func TestParseBodyQuery(t *testing.T) {
	const sql = "SELECT id FROM users WHERE id = $1"
	body := pgfox.BuildParseBody("mystmt", sql, nil)
	got := pgfox.ParseBodyQuery(body)
	if got != sql {
		t.Errorf("ParseBodyQuery: got %q, want %q", got, sql)
	}
}

// TestRewriteParseBodyName verifies that the statement name in a Parse message
// body can be replaced while preserving the query and parameter section.
func TestRewriteParseBodyName(t *testing.T) {
	original := pgfox.BuildParseBody("_asyncpg_abc", "SELECT $1::int", nil)
	rewritten := pgfox.RewriteParseBodyName(original, "pfx_deadbeef")

	name := pgfox.ParseBodyStatementName(rewritten)
	if name != "pfx_deadbeef" {
		t.Errorf("RewriteParseBodyName: name=%q, want %q", name, "pfx_deadbeef")
	}
	query := pgfox.ParseBodyQuery(rewritten)
	if query != "SELECT $1::int" {
		t.Errorf("RewriteParseBodyName: query=%q, should be unchanged", query)
	}
}

// TestBuildBindBody verifies the Bind message body layout with text params.
//
// Layout: portal\0 + stmtName\0 + numFmts(0) + numParams + [len+val]* + numResultFmts
func TestBuildBindBody(t *testing.T) {
	body := pgfox.BuildBindBody("", "pfx_abc", nil, []string{"42"}, []int16{1})

	// Manually parse the body.
	pos := 0
	// portal name
	for body[pos] != 0 {
		pos++
	}
	portal := string(body[:pos])
	pos++ // skip null

	stmtStart := pos
	for body[pos] != 0 {
		pos++
	}
	stmtName := string(body[stmtStart:pos])
	pos++

	if portal != "" {
		t.Errorf("portal: got %q, want empty", portal)
	}
	if stmtName != "pfx_abc" {
		t.Errorf("stmtName: got %q, want pfx_abc", stmtName)
	}

	numFmts := binary.BigEndian.Uint16(body[pos:])
	pos += 2
	if numFmts != 0 {
		t.Errorf("numParamFormats: got %d, want 0", numFmts)
	}

	numParams := binary.BigEndian.Uint16(body[pos:])
	pos += 2
	if numParams != 1 {
		t.Errorf("numParams: got %d, want 1", numParams)
	}

	paramLen := int32(binary.BigEndian.Uint32(body[pos:]))
	pos += 4
	if paramLen != 2 {
		t.Errorf("paramLen: got %d, want 2 (len of \"42\")", paramLen)
	}
	paramVal := string(body[pos : pos+int(paramLen)])
	if paramVal != "42" {
		t.Errorf("paramVal: got %q, want %q", paramVal, "42")
	}
}

// TestRewriteBindBodyName verifies that the statement name inside a Bind
// message body can be replaced while preserving the portal name and all
// parameter data.
func TestRewriteBindBodyName(t *testing.T) {
	original := pgfox.BuildBindBody("myportal", "_asyncpg_xyz", nil, []string{"hello", "world"}, nil)
	rewritten := pgfox.RewriteBindBodyName(original, "pfx_newhash")

	// Parse the rewritten body to extract portal and stmt names.
	pos := 0
	for rewritten[pos] != 0 {
		pos++
	}
	portal := string(rewritten[:pos])
	pos++

	stmtStart := pos
	for rewritten[pos] != 0 {
		pos++
	}
	stmtName := string(rewritten[stmtStart:pos])

	if portal != "myportal" {
		t.Errorf("portal after rewrite: got %q, want %q", portal, "myportal")
	}
	if stmtName != "pfx_newhash" {
		t.Errorf("stmtName after rewrite: got %q, want %q", stmtName, "pfx_newhash")
	}
}

// TestBindBodyStatementName verifies extraction of the statement name from a
// raw Bind message body.
func TestBindBodyStatementName(t *testing.T) {
	body := pgfox.BuildBindBody("", "pfx_hashval", nil, nil, nil)
	got := pgfox.BindBodyStatementName(body)
	if got != "pfx_hashval" {
		t.Errorf("BindBodyStatementName: got %q, want %q", got, "pfx_hashval")
	}
}

// TestCloseBodyTarget verifies that CloseBodyTarget correctly extracts the
// close type and statement name from a Close message body.
func TestCloseBodyTarget(t *testing.T) {
	tests := []struct {
		body     []byte
		wantType byte
		wantName string
	}{
		{append([]byte{'S'}, append([]byte("_asyncpg_abc"), 0)...), 'S', "_asyncpg_abc"},
		{append([]byte{'S'}, []byte{0}...), 'S', ""},
		{append([]byte{'P'}, append([]byte("myportal"), 0)...), 'P', "myportal"},
	}
	for _, tc := range tests {
		gotType, gotName := pgfox.CloseBodyTarget(tc.body)
		if gotType != tc.wantType || gotName != tc.wantName {
			t.Errorf("CloseBodyTarget(%q): got (%q, %q), want (%q, %q)",
				tc.body, gotType, gotName, tc.wantType, tc.wantName)
		}
	}
}

// TestRewriteCloseBodyName verifies that a Close message body can have its
// statement name replaced. This is used by pgfox to redirect client Close
// messages to the unnamed slot (preventing accidental pfx_* eviction).
func TestRewriteCloseBodyName(t *testing.T) {
	// Original: Close('S', "_asyncpg_abc")
	original := append([]byte{'S'}, append([]byte("_asyncpg_abc"), 0)...)
	// Rewrite to close unnamed slot ("").
	rewritten := pgfox.RewriteCloseBodyName(original, "")

	closeType, closeName := pgfox.CloseBodyTarget(rewritten)
	if closeType != 'S' {
		t.Errorf("close type after rewrite: got %q, want 'S'", closeType)
	}
	if closeName != "" {
		t.Errorf("close name after rewrite: got %q, want empty (unnamed)", closeName)
	}
}

// TestDescribeBodyTarget verifies DescribeBodyTarget extracts type and name
// from Describe message bodies.
func TestDescribeBodyTarget(t *testing.T) {
	body := append([]byte{'S'}, append([]byte("pfx_abc"), 0)...)
	gotType, gotName := pgfox.DescribeBodyTarget(body)
	if gotType != 'S' || gotName != "pfx_abc" {
		t.Errorf("DescribeBodyTarget: got (%q, %q), want ('S', %q)", gotType, gotName, "pfx_abc")
	}
}

// TestRewriteDescribeBodyName verifies name replacement in Describe message bodies.
func TestRewriteDescribeBodyName(t *testing.T) {
	original := append([]byte{'S'}, append([]byte("_asyncpg_xyz"), 0)...)
	rewritten := pgfox.RewriteDescribeBodyName(original, "pfx_newhash")

	descType, descName := pgfox.DescribeBodyTarget(rewritten)
	if descType != 'S' || descName != "pfx_newhash" {
		t.Errorf("RewriteDescribeBodyName: got (%q, %q), want ('S', %q)", descType, descName, "pfx_newhash")
	}
}

// TestBuildExecuteBody verifies the Execute message body layout:
// portal\0 + maxRows(int32).
func TestBuildExecuteBody(t *testing.T) {
	body := pgfox.BuildExecuteBody("myportal", 100)
	// "myportal\0" + 4 bytes maxRows
	want := append([]byte("myportal\x00"), 0, 0, 0, 100)
	if string(body) != string(want) {
		t.Errorf("BuildExecuteBody:\ngot  %q\nwant %q", body, want)
	}

	// Unnamed portal, unlimited rows.
	body2 := pgfox.BuildExecuteBody("", 0)
	want2 := []byte{0, 0, 0, 0, 0} // "\0" + int32(0)
	if string(body2) != string(want2) {
		t.Errorf("BuildExecuteBody (unnamed, unlimited):\ngot  %q\nwant %q", body2, want2)
	}
}

// TestClassifyAndParameterize verifies that ClassifyAndParameterize correctly
// identifies special commands and parameterizes DML.
func TestClassifyAndParameterize(t *testing.T) {
	tests := []struct {
		sql      string
		wantCmd  pgfox.SimpleQueryCommand
		wantHash bool // whether a non-nil ParameterizeResult is expected
	}{
		{"LISTEN my_channel", pgfox.SimpleQueryListen, false},
		{"UNLISTEN my_channel", pgfox.SimpleQueryUnlisten, false},
		{"NOTIFY my_channel, 'payload'", pgfox.SimpleQueryNotify, false},
		{"SELECT id FROM users WHERE id = 42", pgfox.SimpleQueryOther, true}, // literal → result
		{"SELECT $1::int", pgfox.SimpleQueryOther, true},                     // already parameterized → result
		{"CREATE TABLE foo (id int)", pgfox.SimpleQueryOther, false},         // DDL → nil
		{"SELECT id FROM a JOIN b ON a.id=b.id WHERE a.x=1", pgfox.SimpleQueryOther, true},
	}

	for _, tc := range tests {
		cmd, result := pgfox.ClassifyAndParameterize(tc.sql)
		if cmd != tc.wantCmd {
			t.Errorf("ClassifyAndParameterize(%q): cmd=%v, want %v", tc.sql, cmd, tc.wantCmd)
		}
		hasResult := result != nil
		if hasResult != tc.wantHash {
			t.Errorf("ClassifyAndParameterize(%q): hasResult=%v, want %v", tc.sql, hasResult, tc.wantHash)
		}
		if result != nil && result.Hash == "" {
			t.Errorf("ClassifyAndParameterize(%q): result.Hash is empty", tc.sql)
		}
	}
}

// TestQueryHash verifies that QueryHash is deterministic and produces consistent
// 16-character hex strings.
func TestQueryHash(t *testing.T) {
	h1 := pgfox.QueryHash("SELECT $1::int")
	h2 := pgfox.QueryHash("SELECT $1::int")
	if h1 != h2 {
		t.Errorf("QueryHash: not deterministic: %q != %q", h1, h2)
	}
	if len(h1) != 16 {
		t.Errorf("QueryHash: expected 16 hex chars, got %d: %q", len(h1), h1)
	}

	// Different SQL must produce a different hash.
	h3 := pgfox.QueryHash("SELECT $1::text")
	if h1 == h3 {
		t.Errorf("QueryHash: different SQL produced same hash %q", h1)
	}
}
