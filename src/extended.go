package main

import (
	"encoding/binary"
)

// --- Parse message ---

// BuildParseBody builds the body of a PostgreSQL Parse ('P') message.
//
//	stmtName  — prepared statement name (empty = unnamed)
//	query     — the SQL query text (must be null-terminated by this function)
//	paramOIDs — parameter type OIDs (nil or empty = let the server infer)
func BuildParseBody(stmtName, query string, paramOIDs []uint32) []byte {
	nameBytes := append([]byte(stmtName), 0)
	queryBytes := append([]byte(query), 0)

	size := len(nameBytes) + len(queryBytes) + 2 + len(paramOIDs)*4
	buf := make([]byte, 0, size)

	buf = append(buf, nameBytes...)
	buf = append(buf, queryBytes...)

	// Number of parameter type OIDs (Int16)
	buf = append(buf, byte(len(paramOIDs)>>8), byte(len(paramOIDs)))

	for _, oid := range paramOIDs {
		buf = append(buf,
			byte(oid>>24), byte(oid>>16), byte(oid>>8), byte(oid))
	}

	return buf
}

// RewriteParseBodyName replaces the statement name in a Parse message body
// while keeping the query and parameter OID section intact.
// Returns the rewritten body, or the original body if it cannot be parsed.
func RewriteParseBodyName(body []byte, newName string) []byte {
	// Parse message body layout:
	//   null-terminated statement name
	//   null-terminated query string
	//   Int16 num_params
	//   Int32[num_params] param_type_oids

	// Find end of statement name.
	nameEnd := -1
	for i, b := range body {
		if b == 0 {
			nameEnd = i
			break
		}
	}
	if nameEnd == -1 {
		return body // malformed
	}

	// Build new body: new name + null + rest of original body after old name.
	rest := body[nameEnd+1:] // query\0 + param section
	newBody := make([]byte, 0, len(newName)+1+len(rest))
	newBody = append(newBody, []byte(newName)...)
	newBody = append(newBody, 0)
	newBody = append(newBody, rest...)
	return newBody
}

// ParseBodyStatementName extracts the statement name from a Parse message body.
// Returns "" for the unnamed statement.
func ParseBodyStatementName(body []byte) string {
	for i, b := range body {
		if b == 0 {
			return string(body[:i])
		}
	}
	return ""
}

// ParseBodyQuery extracts the query string from a Parse message body.
func ParseBodyQuery(body []byte) string {
	// Skip past the null-terminated statement name.
	nameEnd := -1
	for i, b := range body {
		if b == 0 {
			nameEnd = i
			break
		}
	}
	if nameEnd == -1 {
		return ""
	}
	rest := body[nameEnd+1:]

	// The query is the next null-terminated string.
	for i, b := range rest {
		if b == 0 {
			return string(rest[:i])
		}
	}
	return string(rest)
}

// --- Bind message ---

// BuildBindBody builds the body of a PostgreSQL Bind ('B') message for use
// when pgfox dispatches a cached prepared statement.
//
//	portal    — destination portal name (empty = unnamed)
//	stmtName  — source prepared statement name
//	paramFmts — per-parameter format codes (0=text, 1=binary); nil = all text
//	paramVals — parameter values as text strings (nil element = SQL NULL)
//	resultFmts — result column format codes; [1] = all binary, nil = all text
func BuildBindBody(portal, stmtName string, paramFmts []int16, paramVals []string, resultFmts []int16) []byte {
	buf := make([]byte, 0, 256)

	// Portal name (null-terminated)
	buf = append(buf, []byte(portal)...)
	buf = append(buf, 0)

	// Statement name (null-terminated)
	buf = append(buf, []byte(stmtName)...)
	buf = append(buf, 0)

	// Number of parameter format codes (Int16)
	buf = appendInt16(buf, int16(len(paramFmts)))
	for _, fc := range paramFmts {
		buf = appendInt16(buf, fc)
	}

	// Number of parameter values (Int16)
	buf = appendInt16(buf, int16(len(paramVals)))
	for _, val := range paramVals {
		// Each value is: Int32 length (or -1 for NULL) + bytes
		buf = appendInt32(buf, int32(len(val)))
		buf = append(buf, []byte(val)...)
	}

	// Number of result format codes (Int16)
	buf = appendInt16(buf, int16(len(resultFmts)))
	for _, fc := range resultFmts {
		buf = appendInt16(buf, fc)
	}

	return buf
}

// RewriteBindBodyName replaces the statement name in a Bind message body while
// keeping portal, parameter values, and result format codes intact.
// Returns the rewritten body, or the original body if it cannot be parsed.
func RewriteBindBodyName(body []byte, newStmtName string) []byte {
	// Bind message body layout:
	//   null-terminated portal name
	//   null-terminated statement name
	//   Int16 num_param_formats
	//   Int16[num_param_formats] param_format_codes
	//   Int16 num_params
	//   for each param: Int32 length (-1=null), then length bytes
	//   Int16 num_result_formats
	//   Int16[num_result_formats] result_format_codes

	// Skip portal name.
	portalEnd := -1
	for i, b := range body {
		if b == 0 {
			portalEnd = i
			break
		}
	}
	if portalEnd == -1 {
		return body
	}

	// Find end of statement name.
	stmtStart := portalEnd + 1
	stmtEnd := -1
	for i := stmtStart; i < len(body); i++ {
		if body[i] == 0 {
			stmtEnd = i
			break
		}
	}
	if stmtEnd == -1 {
		return body
	}

	// Reconstruct: portal section + new stmt name + rest.
	portalSection := body[:stmtStart] // includes portal name + null
	rest := body[stmtEnd+1:]          // everything after old stmt name null

	newBody := make([]byte, 0, len(portalSection)+len(newStmtName)+1+len(rest))
	newBody = append(newBody, portalSection...)
	newBody = append(newBody, []byte(newStmtName)...)
	newBody = append(newBody, 0)
	newBody = append(newBody, rest...)
	return newBody
}

// BindBodyStatementName extracts the statement name from a Bind message body.
func BindBodyStatementName(body []byte) string {
	// Skip portal name.
	portalEnd := -1
	for i, b := range body {
		if b == 0 {
			portalEnd = i
			break
		}
	}
	if portalEnd == -1 {
		return ""
	}

	stmtStart := portalEnd + 1
	for i := stmtStart; i < len(body); i++ {
		if body[i] == 0 {
			return string(body[stmtStart:i])
		}
	}
	return ""
}

// --- Close message ---

// CloseBodyTarget extracts the close type ('S'=statement, 'P'=portal) and name
// from a Close message body. Already defined in query.go as parseCloseTarget;
// this version is the canonical one used by the extended protocol layer.
func CloseBodyTarget(body []byte) (byte, string) {
	if len(body) < 2 {
		return 0, ""
	}
	closeType := body[0]
	for i, b := range body[1:] {
		if b == 0 {
			return closeType, string(body[1 : 1+i])
		}
	}
	return closeType, string(body[1:])
}

// --- Describe message ---

// DescribeBodyTarget extracts the describe type ('S'=statement, 'P'=portal)
// and name from a Describe message body.
func DescribeBodyTarget(body []byte) (byte, string) {
	// Same layout as Close.
	return CloseBodyTarget(body)
}

// RewriteDescribeBodyName rewrites the name in a Describe message body.
func RewriteDescribeBodyName(body []byte, newName string) []byte {
	if len(body) < 1 {
		return body
	}
	descType := body[0]
	newBody := make([]byte, 0, 1+len(newName)+1)
	newBody = append(newBody, descType)
	newBody = append(newBody, []byte(newName)...)
	newBody = append(newBody, 0)
	return newBody
}

// RewriteCloseBodyName rewrites the name in a Close message body.
func RewriteCloseBodyName(body []byte, newName string) []byte {
	// Same layout as Describe.
	return RewriteDescribeBodyName(body, newName)
}

// --- Execute message ---

// BuildExecuteBody builds the body of an Execute ('E') message.
//
//	portal   — portal name (empty = unnamed)
//	maxRows  — maximum rows to return (0 = no limit)
func BuildExecuteBody(portal string, maxRows int32) []byte {
	buf := make([]byte, 0, len(portal)+1+4)
	buf = append(buf, []byte(portal)...)
	buf = append(buf, 0)
	buf = appendInt32(buf, maxRows)
	return buf
}

// --- Sync / Flush ---

// SyncBody is the body of a Sync message (empty).
var SyncBody = []byte{}

// FlushBody is the body of a Flush message (empty).
var FlushBody = []byte{}

// --- binary helpers ---

func appendInt16(buf []byte, v int16) []byte {
	return append(buf, byte(v>>8), byte(v))
}

func appendInt32(buf []byte, v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return append(buf, b...)
}
