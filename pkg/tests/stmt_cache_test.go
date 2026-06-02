package tests

// stmt_cache_test.go — unit tests for the statement cache and name mapping.
//
// These tests verify the internal bookkeeping that makes connection multiplexing
// correct: the per-target statement cache (StmtCache), the per-backend deployment
// tracker (Backend.deployedStmts), and the per-client name mapping
// (Client.stmtNameMap / stmtRevMap).
//
// Unlike the other test files, these tests operate directly on the data
// structures without going through the pgfox wire protocol layer. They are
// intentionally unit-level, fast, and deterministic.
//
// Playbook sections referenced: §6.1 (Registration), §6.2 (Deployment tracking),
// §6.3 (Statement name mapping per client).

import (
	"net"
	"testing"

	"github.com/aekis-dev/pgfox/pkg/logger"
	"github.com/aekis-dev/pgfox/pkg/pgfox"
)

// TestStmtCache_RegisterAndGet verifies that GetOrRegister creates a new entry
// on first call and returns the same entry (with isNew=false) on subsequent
// calls with the same hash.
//
// Playbook §6.1 — Registration.
func TestStmtCache_RegisterAndGet(t *testing.T) {
	cache := pgfox.NewStmtCache()

	entry1, isNew := cache.GetOrRegister("abc123", "SELECT $1", "SELECT 42", 1)
	if !isNew {
		t.Error("GetOrRegister: first call should return isNew=true")
	}
	if entry1 == nil {
		t.Fatal("GetOrRegister: returned nil entry")
	}
	if entry1.Hash != "abc123" {
		t.Errorf("Hash: want %q, got %q", "abc123", entry1.Hash)
	}
	if entry1.CanonicalSQL != "SELECT $1" {
		t.Errorf("CanonicalSQL: want %q, got %q", "SELECT $1", entry1.CanonicalSQL)
	}
	if entry1.ParamCount != 1 {
		t.Errorf("ParamCount: want 1, got %d", entry1.ParamCount)
	}

	// Second call with the same hash must return the same entry.
	entry2, isNew2 := cache.GetOrRegister("abc123", "SELECT $1", "SELECT 99", 1)
	if isNew2 {
		t.Error("GetOrRegister: second call should return isNew=false")
	}
	if entry2 != entry1 {
		t.Error("GetOrRegister: second call should return the same *CachedStmt pointer")
	}
}

// TestStmtCache_GetNonExistent verifies that Get returns nil for an unknown hash.
//
// Playbook §6.1 — Registration (negative case).
func TestStmtCache_GetNonExistent(t *testing.T) {
	cache := pgfox.NewStmtCache()
	entry := cache.Get("doesnotexist")
	if entry != nil {
		t.Errorf("Get: expected nil for unknown hash, got %+v", entry)
	}
}

// TestStmtCache_RecordExecution verifies that RecordExecution increments the
// execution counter and updates the last-used timestamp.
//
// Playbook §6.1 — CachedStmt.Executions counter.
func TestStmtCache_RecordExecution(t *testing.T) {
	cache := pgfox.NewStmtCache()
	entry, _ := cache.GetOrRegister("h1", "SELECT $1", "SELECT 1", 1)

	if entry.Executions != 0 {
		t.Errorf("initial Executions: want 0, got %d", entry.Executions)
	}

	entry.RecordExecution()
	entry.RecordExecution()

	if entry.Executions != 2 {
		t.Errorf("after 2 RecordExecution calls: want 2, got %d", entry.Executions)
	}
	if entry.LastUsed().IsZero() {
		t.Error("LastUsed should not be zero after RecordExecution")
	}
}

// TestBackend_HasStmtMarkStmt verifies the per-backend deployment
// tracker: HasStmt returns false before MarkStmt and true after.
//
// Playbook §6.2 — Deployment tracking per backend.
func TestBackend_HasStmtMarkStmt(t *testing.T) {
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	backend := pgfox.NewBackend(conn1, "db", "target", "user", 1024*1024)

	const hash = "deadbeef"

	if backend.HasStmt(hash) {
		t.Error("HasStmt: should be false before MarkStmt")
	}

	backend.MarkStmt(hash)

	if !backend.HasStmt(hash) {
		t.Error("HasStmt: should be true after MarkStmt")
	}

	// A different hash must still return false.
	if backend.HasStmt("otherhash") {
		t.Error("HasStmt: unrelated hash should still be false")
	}
}

// TestBackend_DeployedStmtsArePerConnection verifies that two
// separate Backends track their deployed statements independently.
// This is the invariant that makes phase 3.5 (inject Parse on a new backend)
// necessary and correct.
//
// Playbook §6.2 — Deployment tracking per backend.
func TestBackend_DeployedStmtsArePerConnection(t *testing.T) {
	c1a, c1b := net.Pipe()
	c2a, c2b := net.Pipe()
	defer c1a.Close()
	defer c1b.Close()
	defer c2a.Close()
	defer c2b.Close()

	b1 := pgfox.NewBackend(c1a, "db", "t", "u", 1024*1024)
	b2 := pgfox.NewBackend(c2a, "db", "t", "u", 1024*1024)

	const hash = "shared_hash"

	// Mark the statement on b1 only.
	b1.MarkStmt(hash)

	if !b1.HasStmt(hash) {
		t.Error("b1.HasStmt: should be true after MarkStmt on b1")
	}
	if b2.HasStmt(hash) {
		t.Error("b2.HasStmt: should be false — b2 has not had Parse sent to it")
	}
}

// TestClient_StmtNameMapping verifies the per-client name-mapping
// functions: MapStmtName (register), LookupInternalName (read), UnmapStmtName
// (delete).
//
// Playbook §6.3 — Statement name mapping per client.
func TestClient_StmtNameMapping(t *testing.T) {
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	log := logger.NewLogger(logger.LoggingConfig{Level: "error", Format: "text"})
	client := pgfox.NewClient(conn1, log, 1024*1024)

	// Map a client name to an internal hash.
	client.MapStmtName("_asyncpg_abc", "deadbeef")

	// Lookup must return the hash.
	hash, ok := client.LookupInternalName("_asyncpg_abc")
	if !ok || hash != "deadbeef" {
		t.Errorf("LookupInternalName: want (deadbeef, true), got (%q, %v)", hash, ok)
	}

	// Looking up an unmapped name must return false.
	_, ok2 := client.LookupInternalName("_asyncpg_xyz")
	if ok2 {
		t.Error("LookupInternalName: unknown name should return false")
	}

	// Unmap the name.
	client.UnmapStmtName("_asyncpg_abc")

	_, ok3 := client.LookupInternalName("_asyncpg_abc")
	if ok3 {
		t.Error("LookupInternalName: name should be gone after UnmapStmtName")
	}
}

// TestClient_NamedStmtCount verifies AddNamedStatement /
// RemoveNamedStatement / HasNamedStatements — the counter used by reconcileConn
// to decide whether to keep the backend pinned after a Sync.
//
// Playbook §3.2 — Non-remappable named statement pinning.
// Playbook §3.5 — Passthrough Close decrements the counter.
func TestClient_NamedStmtCount(t *testing.T) {
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()
	log := logger.NewLogger(logger.LoggingConfig{Level: "error", Format: "text"})
	client := pgfox.NewClient(conn1, log, 1024*1024)

	if client.HasNamedStatements() {
		t.Error("HasNamedStatements: should be false initially")
	}

	client.AddNamedStatement()
	if !client.HasNamedStatements() {
		t.Error("HasNamedStatements: should be true after AddNamedStatement")
	}

	client.AddNamedStatement()
	client.RemoveNamedStatement()
	if !client.HasNamedStatements() {
		t.Error("HasNamedStatements: should still be true (count=1)")
	}

	client.RemoveNamedStatement()
	if client.HasNamedStatements() {
		t.Error("HasNamedStatements: should be false after all removes")
	}

	// RemoveNamedStatement when count is already 0 must not panic or underflow.
	client.RemoveNamedStatement()
	if client.HasNamedStatements() {
		t.Error("HasNamedStatements: underflow guard failed — should stay false")
	}
}

// TestStmtName verifies that the StmtName helper produces the expected
// "pfx_<hash>" format used as the internal statement name on backends.
//
// Playbook §6.1 — Statement name format.
func TestStmtName(t *testing.T) {
	tests := []struct {
		hash string
		want string
	}{
		{"a1b2c3d4", "pfx_a1b2c3d4"},
		{"deadbeef12345678", "pfx_deadbeef12345678"},
		{"", "pfx_"},
	}
	for _, tc := range tests {
		got := pgfox.StmtName(tc.hash)
		if got != tc.want {
			t.Errorf("StmtName(%q) = %q, want %q", tc.hash, got, tc.want)
		}
	}
}
