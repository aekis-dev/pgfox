package pgfox

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/pgplex/pgparser/nodes"
	"github.com/pgplex/pgparser/parser"
)

// ParameterizeResult is returned by ParameterizeQuery on success.
type ParameterizeResult struct {
	// CanonicalSQL is the rewritten query with $1..$N placeholders replacing
	// all extracted literal values.
	CanonicalSQL string

	// Values holds the extracted literal values as text strings in parameter
	// order ($1 = Values[0], $2 = Values[1], …).
	Values []string

	// Hash is the 16-character hex prefix of SHA-256(CanonicalSQL).
	// Used as the statement name key in StmtCache and on Backend.
	Hash string
}

// ParameterizeQuery rewrites a SQL query by extracting literal constant values
// and replacing them with PostgreSQL positional parameters ($1, $2, …).
//
// It uses pgplex/pgparser (pure Go, no CGo) to parse the SQL into a real
// PostgreSQL AST. The AST walk extracts literal values in left-to-right source
// order; the SQL text is then scanned left-to-right to find and replace each
// literal token with $N.
//
// Returns (nil, nil) for queries that should be passed through unchanged:
//   - More than one statement (multi-statement strings)
//   - DDL, COPY, EXPLAIN, DO, CALL, LISTEN, UNLISTEN, NOTIFY, transaction
//     control, SET/SHOW/RESET, VACUUM, CLUSTER, REINDEX, GRANT/REVOKE
//   - Queries already containing positional parameters ($1…)
//   - Queries the parser cannot parse
//
// The caller must treat a nil result as "use simple query protocol as-is".
// ClassifyAndParameterize performs a single parse pass and returns both the
// SimpleQueryCommand classification and the ParameterizeResult. This replaces
// the previous two-pass pattern (DetectSimpleQueryCommand + ParameterizeQuery)
// which parsed the SQL twice on every simple query.
//
// If the query is LISTEN/UNLISTEN/NOTIFY, result is always nil.
// If the query is parameterizable DML, cmd is SimpleQueryOther and result is set.
// If neither, both are SimpleQueryOther and nil respectively.
func ClassifyAndParameterize(sql string) (SimpleQueryCommand, *ParameterizeResult) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return SimpleQueryOther, nil
	}

	trimmed := strings.TrimRight(sql, " \t\n\r;")

	list, err := parser.Parse(trimmed)
	if err != nil || list == nil || len(list.Items) == 0 {
		return SimpleQueryOther, nil
	}

	// Multi-statement: passthrough as-is.
	if len(list.Items) > 1 {
		return SimpleQueryOther, nil
	}

	stmt := list.Items[0]

	// Classify special commands first.
	switch stmt.(type) {
	case *nodes.ListenStmt:
		return SimpleQueryListen, nil
	case *nodes.UnlistenStmt:
		return SimpleQueryUnlisten, nil
	case *nodes.NotifyStmt:
		return SimpleQueryNotify, nil
	case *nodes.SelectStmt:
		// Catch SELECT pg_notify(...) — check before attempting parameterization.
		upper := strings.ToUpper(strings.ReplaceAll(trimmed, " ", ""))
		if strings.Contains(upper, "PG_NOTIFY(") {
			return SimpleQueryNotify, nil
		}
	}

	// Attempt parameterization for DML.
	if !isDMLStatement(stmt) {
		return SimpleQueryOther, nil
	}

	// If the query already contains $N parameters it cannot be rewritten further,
	// but it CAN still be registered in the stmt cache so pgfox manages it as a
	// shared prepared statement. Return a ParameterizeResult with the SQL as-is
	// and no extracted values — the caller supplies parameters via Bind as usual.
	if containsParam(stmt) {
		h := sha256.Sum256([]byte(trimmed))
		return SimpleQueryOther, &ParameterizeResult{
			CanonicalSQL: trimmed,
			Values:       nil,
			Hash:         fmt.Sprintf("%x", h[:8]),
		}
	}

	// Borrow scratch space from the Pool to avoid per-query allocations for
	// the literal slice, strings.Builder, and values slice. All three are reset
	// and returned to the Pool before this function returns; the ParameterizeResult
	// that escapes to the heap contains only heap-allocated strings.
	ws := getParamWorkspace()
	defer putParamWorkspace(ws)

	collectLiterals(stmt, &ws.litValues, false)

	if len(ws.litValues) == 0 {
		h := sha256.Sum256([]byte(trimmed))
		return SimpleQueryOther, &ParameterizeResult{
			CanonicalSQL: trimmed,
			Values:       nil,
			Hash:         fmt.Sprintf("%x", h[:8]),
		}
	}

	canonical, values, err := rewriteLiteralsWS(trimmed, ws.litValues, ws)
	if err != nil {
		return SimpleQueryOther, nil //nolint:nilerr
	}

	// values slice elements are Go string literals (immutable, heap-safe) so we
	// copy them into a fresh slice before returning the workspace to the Pool.
	heapValues := make([]string, len(values))
	copy(heapValues, values)

	h := sha256.Sum256([]byte(canonical))
	return SimpleQueryOther, &ParameterizeResult{
		CanonicalSQL: canonical,
		Values:       heapValues,
		Hash:         fmt.Sprintf("%x", h[:8]),
	}
}

func ParameterizeQuery(sql string) (*ParameterizeResult, error) {
	_, result := ClassifyAndParameterize(sql)
	return result, nil
}

// StmtName returns the backend prepared statement name for a hash.
// Format: "pfx_<hash>" — short, unique, avoids clashes with client names.
func StmtName(hash string) string {
	return "pfx_" + hash
}

// QueryHash computes the 16-char hex hash of an already-canonical SQL string.
// Used when a client sends an already-parameterized extended-protocol Parse.
func QueryHash(canonicalSQL string) string {
	h := sha256.Sum256([]byte(canonicalSQL))
	return fmt.Sprintf("%x", h[:8])
}

// SimpleQueryCommand is the classification result from ClassifyAndParameterize.
type SimpleQueryCommand int

const (
	SimpleQueryOther SimpleQueryCommand = iota
	SimpleQueryListen
	SimpleQueryUnlisten
	SimpleQueryNotify
)

// --- AST classification ---

// isDMLStatement returns true for SELECT, INSERT, UPDATE, DELETE (incl. VALUES).
func isDMLStatement(stmt nodes.Node) bool {
	switch stmt.(type) {
	case *nodes.SelectStmt, *nodes.InsertStmt, *nodes.UpdateStmt, *nodes.DeleteStmt:
		return true
	default:
		return false
	}
}

// containsParam returns true if the statement tree already has $N parameters.
func containsParam(node nodes.Node) bool {
	if node == nil {
		return false
	}
	// NOTE: same typed-nil issue as collectLiterals — every case must nil-check
	// its concrete pointer before accessing any fields.
	switch n := node.(type) {
	case *nodes.ParamRef:
		if n == nil {
			return false
		}
		return true
	case *nodes.SelectStmt:
		if n == nil {
			return false
		}
		return listHasParam(n.TargetList) || containsParam(n.WhereClause) ||
			listHasParam(n.GroupClause) || containsParam(n.HavingClause) ||
			listHasParam(n.ValuesLists) || listHasParam(n.FromClause)
	case *nodes.InsertStmt:
		if n == nil {
			return false
		}
		return containsParam(n.SelectStmt)
	case *nodes.UpdateStmt:
		if n == nil {
			return false
		}
		return listHasParam(n.TargetList) || containsParam(n.WhereClause)
	case *nodes.DeleteStmt:
		if n == nil {
			return false
		}
		return containsParam(n.WhereClause)
	case *nodes.A_Expr:
		if n == nil {
			return false
		}
		return containsParam(n.Lexpr) || containsParam(n.Rexpr)
	case *nodes.FuncCall:
		if n == nil {
			return false
		}
		return listHasParam(n.Args)
	case *nodes.ResTarget:
		if n == nil {
			return false
		}
		return containsParam(n.Val)
	case *nodes.List:
		if n == nil {
			return false
		}
		return listHasParam(n)
	case *nodes.SubLink:
		if n == nil {
			return false
		}
		return containsParam(n.Subselect)
	case *nodes.TypeCast:
		if n == nil {
			return false
		}
		return containsParam(n.Arg)
	case *nodes.BoolExpr:
		if n == nil {
			return false
		}
		return listHasParam(n.Args)
	case *nodes.CaseExpr:
		if n == nil {
			return false
		}
		return containsParam(n.Arg) || listHasParam(n.Args) || containsParam(n.Defresult)
	case *nodes.CaseWhen:
		if n == nil {
			return false
		}
		return containsParam(n.Expr) || containsParam(n.Result)
	case *nodes.CoalesceExpr:
		if n == nil {
			return false
		}
		return listHasParam(n.Args)
	case *nodes.NullTest:
		if n == nil {
			return false
		}
		return containsParam(n.Arg)
	case *nodes.RowExpr:
		if n == nil {
			return false
		}
		return listHasParam(n.Args)
	case *nodes.ArrayExpr:
		if n == nil {
			return false
		}
		return listHasParam(n.Elements)
	}
	return false
}

func listHasParam(list *nodes.List) bool {
	if list == nil {
		return false
	}
	for _, item := range list.Items {
		if containsParam(item) {
			return true
		}
	}
	return false
}

// --- Literal collection ---

// literalValue holds one extracted literal from the AST.
type literalValue struct {
	// kind distinguishes how the value is quoted in the source.
	kind  litKind
	value string // the raw unquoted value
}

type litKind int

const (
	litString litKind = iota // single-quoted string: 'hello'
	litInt                   // integer: 42
	litFloat                 // float: 3.14
	litBitStr                // bit string: B'101' or X'ff'
)

// collectLiterals walks the AST in left-to-right source order and appends
// parameterizable A_Const nodes to *out. inLimit marks LIMIT/OFFSET context
// where integer/float constants are structural and must not be replaced.
func collectLiterals(node nodes.Node, out *[]literalValue, inLimit bool) {
	if node == nil {
		return
	}

	// NOTE: pgplex/pgparser stores typed nil pointers in interface fields for
	// optional nodes (e.g. SelectStmt.Larg is *SelectStmt typed-nil when unset).
	// The interface itself is non-nil, so `if node == nil` above won't catch it.
	// Every case branch must nil-check its concrete pointer before use.
	switch n := node.(type) {
	case *nodes.A_Const:
		if n == nil {
			return
		}
		if n.Isnull {
			return // NULL keyword
		}
		switch v := n.Val.(type) {
		case *nodes.Boolean:
			return // TRUE/FALSE are keywords
		case *nodes.Integer:
			if inLimit {
				return
			}
			*out = append(*out, literalValue{litInt, fmt.Sprintf("%d", v.Ival)})
		case *nodes.Float:
			if inLimit {
				return
			}
			*out = append(*out, literalValue{litFloat, v.Fval})
		case *nodes.String:
			*out = append(*out, literalValue{litString, v.Str})
		case *nodes.BitString:
			*out = append(*out, literalValue{litBitStr, v.Bsval})
		}

	case *nodes.SelectStmt:
		if n == nil {
			return
		}
		// Collect in SQL text order so the literal sequence matches a left-to-
		// right rescan in findNextLiteral. FROM appears textually before WHERE;
		// collecting it after WHERE (as before) made any query with a literal in
		// the FROM clause (e.g. a table function like generate_series(1,5)) fail
		// the rescan and silently bypass the prepared-statement cache.
		collectInList(n.TargetList, out, false)
		collectInList(n.FromClause, out, false)
		collectLiterals(n.WhereClause, out, false)
		collectInList(n.GroupClause, out, false)
		collectLiterals(n.HavingClause, out, false)
		collectInList(n.ValuesLists, out, false)
		collectLiterals(n.Larg, out, false)
		collectLiterals(n.Rarg, out, false)
		collectLiterals(n.LimitOffset, out, true) // structural (not emitted)
		collectLiterals(n.LimitCount, out, true)  // structural (not emitted)

	case *nodes.InsertStmt:
		if n == nil {
			return
		}
		collectLiterals(n.SelectStmt, out, false)

	case *nodes.UpdateStmt:
		if n == nil {
			return
		}
		collectInList(n.TargetList, out, false)
		collectLiterals(n.WhereClause, out, false)

	case *nodes.DeleteStmt:
		if n == nil {
			return
		}
		collectLiterals(n.WhereClause, out, false)

	case *nodes.A_Expr:
		if n == nil {
			return
		}
		collectLiterals(n.Lexpr, out, inLimit)
		collectLiterals(n.Rexpr, out, inLimit)

	case *nodes.FuncCall:
		if n == nil {
			return
		}
		collectInList(n.Args, out, inLimit)

	case *nodes.ResTarget:
		if n == nil {
			return
		}
		collectLiterals(n.Val, out, inLimit)

	case *nodes.List:
		if n == nil {
			return
		}
		collectInList(n, out, inLimit)

	case *nodes.SubLink:
		if n == nil {
			return
		}
		collectLiterals(n.Subselect, out, inLimit)

	case *nodes.TypeCast:
		if n == nil {
			return
		}
		collectLiterals(n.Arg, out, inLimit)
		// Do NOT descend into TypeName — type names are not values.

	case *nodes.BoolExpr:
		if n == nil {
			return
		}
		collectInList(n.Args, out, inLimit)

	case *nodes.CaseExpr:
		if n == nil {
			return
		}
		collectLiterals(n.Arg, out, inLimit)
		collectInList(n.Args, out, inLimit)
		collectLiterals(n.Defresult, out, inLimit)

	case *nodes.CaseWhen:
		if n == nil {
			return
		}
		collectLiterals(n.Expr, out, inLimit)
		collectLiterals(n.Result, out, inLimit)

	case *nodes.CoalesceExpr:
		if n == nil {
			return
		}
		collectInList(n.Args, out, inLimit)

	case *nodes.NullTest:
		if n == nil {
			return
		}
		collectLiterals(n.Arg, out, inLimit)

	case *nodes.RowExpr:
		if n == nil {
			return
		}
		collectInList(n.Args, out, inLimit)

	case *nodes.ArrayExpr:
		if n == nil {
			return
		}
		collectInList(n.Elements, out, inLimit)

	case *nodes.RangeSubselect:
		if n == nil {
			return
		}
		collectLiterals(n.Subquery, out, inLimit)

	case *nodes.JoinExpr:
		if n == nil {
			return
		}
		collectLiterals(n.Larg, out, inLimit)
		collectLiterals(n.Rarg, out, inLimit)
		collectLiterals(n.Quals, out, inLimit)

	case *nodes.SortBy:
		if n == nil {
			return
		}
		collectLiterals(n.Node, out, inLimit)

	case *nodes.WindowDef:
		if n == nil {
			return
		}
		collectInList(n.PartitionClause, out, false)
		collectInList(n.OrderClause, out, false)
	}
}

func collectInList(list *nodes.List, out *[]literalValue, inLimit bool) {
	if list == nil {
		return
	}
	for _, item := range list.Items {
		collectLiterals(item, out, inLimit)
	}
}

// --- SQL text rewriter ---

// rewriteLiterals scans sql left-to-right and replaces each literal token
// described by litValues[i] with $i+1. The scan works because the AST walk
// order matches left-to-right source order for all DML statement types.
// rewriteLiteralsWS is the pooled version of rewriteLiterals; it writes into
// a caller-supplied workspace so the Builder and values slice are not allocated
// per call. The caller must copy ws.values out before returning ws to the Pool.
func rewriteLiteralsWS(sql string, litValues []literalValue, ws *paramWorkspace) (string, []string, error) {
	if len(litValues) == 0 {
		return sql, nil, nil
	}

	sb := &ws.sb
	ws.values = ws.values[:0]
	sb.Grow(len(sql))

	src := []byte(sql)
	pos := 0
	paramIdx := 0

	for _, lit := range litValues {
		// Find the next occurrence of this literal starting at pos.
		start, end, err := findNextLiteral(src, pos, lit)
		if err != nil {
			return "", nil, fmt.Errorf("could not locate literal %q: %w", lit.value, err)
		}

		// Append everything before this literal.
		sb.Write(src[pos:start])

		// Emit $N.
		paramIdx++
		sb.WriteString(fmt.Sprintf("$%d", paramIdx))
		ws.values = append(ws.values, lit.value)

		pos = end
	}

	// Append remaining SQL after the last literal.
	sb.Write(src[pos:])

	return sb.String(), ws.values, nil
}

// rewriteLiterals is the original API kept for callers outside ClassifyAndParameterize.
func rewriteLiterals(sql string, litValues []literalValue) (string, []string, error) {
	ws := getParamWorkspace()
	defer putParamWorkspace(ws)
	result, vals, err := rewriteLiteralsWS(sql, litValues, ws)
	if err != nil || vals == nil {
		return result, vals, err
	}
	heap := make([]string, len(vals))
	copy(heap, vals)
	return result, heap, nil
}

// findNextLiteral scans pkg starting at startPos for the next occurrence of
// the literal token described by lit. It returns the start and end byte
// offsets of the token in pkg.
//
// The scan skips over SQL comments, double-quoted identifiers, and other
// string literals to avoid false matches inside them.
func findNextLiteral(src []byte, startPos int, lit literalValue) (start, end int, err error) {
	i := startPos
	n := len(src)

	for i < n {
		// Skip whitespace (fast path).
		if isWS(src[i]) {
			i++
			continue
		}

		// Skip line comment.
		if src[i] == '-' && i+1 < n && src[i+1] == '-' {
			for i < n && src[i] != '\n' {
				i++
			}
			continue
		}

		// Skip block comment.
		if src[i] == '/' && i+1 < n && src[i+1] == '*' {
			i += 2
			for i+1 < n && !(src[i] == '*' && src[i+1] == '/') {
				i++
			}
			i += 2
			continue
		}

		// Skip double-quoted identifier.
		if src[i] == '"' {
			i++
			for i < n && src[i] != '"' {
				if src[i] == '"' && i+1 < n && src[i+1] == '"' {
					i += 2
				} else {
					i++
				}
			}
			if i < n {
				i++ // closing "
			}
			continue
		}

		// Try to match the literal at current position.
		switch lit.kind {
		case litString:
			if src[i] == '\'' {
				// Read the full quoted string.
				tokenStart := i
				rawStr, tokenEnd := readSingleQuotedString(src, i)
				if rawStr == lit.value {
					return tokenStart, tokenEnd, nil
				}
				i = tokenEnd
				continue
			}
			// E-strings: E'...' or e'...'
			if (src[i] == 'E' || src[i] == 'e') && i+1 < n && src[i+1] == '\'' {
				tokenStart := i
				rawStr, tokenEnd := readEString(src, i)
				if rawStr == lit.value {
					return tokenStart, tokenEnd, nil
				}
				i = tokenEnd
				continue
			}
			// Dollar-quoted: skip over them (we never parameterize dollar-quoted).
			if src[i] == '$' {
				_, tokenEnd := readDollarQuoted(src, i)
				i = tokenEnd
				continue
			}

		case litInt:
			if isDigit(src[i]) {
				tokenStart := i
				numStr, tokenEnd := readNumber(src, i)
				// Only match plain integers (no decimal point).
				if !strings.Contains(numStr, ".") && !strings.ContainsAny(numStr, "eE") &&
					numStr == lit.value {
					return tokenStart, tokenEnd, nil
				}
				i = tokenEnd
				continue
			}

		case litFloat:
			if isDigit(src[i]) || (src[i] == '.' && i+1 < n && isDigit(src[i+1])) {
				tokenStart := i
				numStr, tokenEnd := readNumber(src, i)
				if numStr == lit.value {
					return tokenStart, tokenEnd, nil
				}
				i = tokenEnd
				continue
			}

		case litBitStr:
			if (src[i] == 'B' || src[i] == 'b' || src[i] == 'X' || src[i] == 'x') &&
				i+1 < n && src[i+1] == '\'' {
				tokenStart := i
				rawStr, tokenEnd := readBitString(src, i)
				if rawStr == lit.value {
					return tokenStart, tokenEnd, nil
				}
				i = tokenEnd
				continue
			}
		}

		// Skip any string literal we're not trying to match (to avoid
		// finding our target inside another string).
		if src[i] == '\'' {
			_, i = readSingleQuotedString(src, i)
			continue
		}
		if (src[i] == 'E' || src[i] == 'e') && i+1 < n && src[i+1] == '\'' {
			_, i = readEString(src, i)
			continue
		}
		if src[i] == '$' {
			_, i = readDollarQuoted(src, i)
			if i == startPos {
				i++ // avoid infinite loop if not dollar-quoted
			}
			continue
		}
		if (src[i] == 'B' || src[i] == 'b' || src[i] == 'X' || src[i] == 'x') &&
			i+1 < n && src[i+1] == '\'' {
			_, i = readBitString(src, i)
			continue
		}

		i++
	}

	return 0, 0, fmt.Errorf("literal not found in SQL after offset %d", startPos)
}

// --- token readers ---

// readSingleQuotedString reads a single-quoted SQL string starting at pos.
// Returns the unescaped string value and the position after the closing quote.
func readSingleQuotedString(src []byte, pos int) (value string, end int) {
	i := pos + 1 // skip opening '
	var sb strings.Builder
	for i < len(src) {
		if src[i] == '\'' {
			if i+1 < len(src) && src[i+1] == '\'' {
				sb.WriteByte('\'')
				i += 2
				continue
			}
			i++ // closing '
			break
		}
		sb.WriteByte(src[i])
		i++
	}
	return sb.String(), i
}

// readEString reads an E'...' escape string starting at pos (at the 'E').
// Returns the unescaped value and the end position.
func readEString(src []byte, pos int) (value string, end int) {
	i := pos + 2 // skip E'
	var sb strings.Builder
	for i < len(src) {
		if src[i] == '\\' && i+1 < len(src) {
			switch src[i+1] {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case 'r':
				sb.WriteByte('\r')
			default:
				sb.WriteByte(src[i+1])
			}
			i += 2
			continue
		}
		if src[i] == '\'' {
			if i+1 < len(src) && src[i+1] == '\'' {
				sb.WriteByte('\'')
				i += 2
				continue
			}
			i++
			break
		}
		sb.WriteByte(src[i])
		i++
	}
	return sb.String(), i
}

// readDollarQuoted reads a dollar-quoted string starting at pos (at '$').
// Returns the body and the end position. If pos doesn't start a dollar-quote,
// returns ("", pos+1).
func readDollarQuoted(src []byte, pos int) (value string, end int) {
	i := pos + 1
	for i < len(src) && (isAlpha(src[i]) || isDigit(src[i]) || src[i] == '_') {
		i++
	}
	if i >= len(src) || src[i] != '$' {
		return "", pos + 1
	}
	tag := string(src[pos : i+1]) // e.g. "$$" or "$body$"
	body := i + 1
	idx := strings.Index(string(src[body:]), tag)
	if idx < 0 {
		return string(src[body:]), len(src)
	}
	return string(src[body : body+idx]), body + idx + len(tag)
}

// readBitString reads a B'...' or X'...' bit string starting at pos (at 'B'/'X').
func readBitString(src []byte, pos int) (value string, end int) {
	prefix := string(src[pos : pos+1])
	i := pos + 2 // skip prefix and '
	start := i
	for i < len(src) && src[i] != '\'' {
		i++
	}
	if i < len(src) {
		i++ // closing '
	}
	return prefix + string(src[start:i-1]), i
}

// readNumber reads an integer or float literal starting at pos.
func readNumber(src []byte, pos int) (value string, end int) {
	i := pos
	for i < len(src) && isDigit(src[i]) {
		i++
	}
	if i < len(src) && src[i] == '.' {
		i++
		for i < len(src) && isDigit(src[i]) {
			i++
		}
	}
	if i < len(src) && (src[i] == 'e' || src[i] == 'E') {
		i++
		if i < len(src) && (src[i] == '+' || src[i] == '-') {
			i++
		}
		for i < len(src) && isDigit(src[i]) {
			i++
		}
	}
	return string(src[pos:i]), i
}

func isWS(ch byte) bool    { return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' }
func isDigit(ch byte) bool { return ch >= '0' && ch <= '9' }
func isAlpha(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}
