package tests

// parser_test.go — unit tests for SQL literal extraction / parameterization
// used to key the prepared-statement cache.

import (
	"testing"

	"github.com/aekis-dev/pgfox/pkg/pgfox"
)

// TestParameterize_FromClauseLiteralOrdering is a regression test for the
// FROM-clause ordering bug. A literal inside the FROM clause (here, a JOIN ON
// qualifier) appears textually before the WHERE literal. Literals must be
// collected in SQL text order; collecting the FROM clause after WHERE (the old
// behavior) made findNextLiteral's left-to-right rescan fail, so the query
// silently bypassed the cache (ParameterizeQuery returns nil, nil).
func TestParameterize_FromClauseLiteralOrdering(t *testing.T) {
	// JOIN ON literal (1) is textually before the WHERE literal (2).
	const sql = "SELECT o.id FROM orders o JOIN items i ON i.code = 1 WHERE o.total > 2"

	res, err := pgfox.ParameterizeQuery(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res == nil {
		t.Fatal("query silently bypassed the cache (nil result) — FROM-clause literal ordering regression")
	}

	want := []string{"1", "2"} // textual left-to-right order
	if len(res.Values) != len(want) {
		t.Fatalf("expected %d extracted values %v, got %d: %v", len(want), want, len(res.Values), res.Values)
	}
	for i := range want {
		if res.Values[i] != want[i] {
			t.Errorf("value[%d] = %q, want %q (literals must be in text order) — full: %v",
				i, res.Values[i], want[i], res.Values)
		}
	}
}

// TestParameterize_SimpleWhere sanity-checks the ordinary case still works.
func TestParameterize_SimpleWhere(t *testing.T) {
	res, err := pgfox.ParameterizeQuery("SELECT id FROM users WHERE id = 42 AND age > 18")
	if err != nil || res == nil {
		t.Fatalf("expected parameterization, got res=%v err=%v", res, err)
	}
	want := []string{"42", "18"}
	if len(res.Values) != 2 || res.Values[0] != want[0] || res.Values[1] != want[1] {
		t.Errorf("values = %v, want %v", res.Values, want)
	}
}
