package pgfox

import (
	"sync"
	"sync/atomic"
	"time"
)

// CachedStmt holds the canonical parameterized form of a query registered in
// the target-level statement cache. It is immutable after registration — only
// the atomic stats fields are written after the entry is visible.
type CachedStmt struct {
	// Hash is the 16-hex-char prefix of SHA-256(canonicalSQL). Used as the
	// prepared statement name on backend connections ("pfx_<hash>").
	Hash string

	// CanonicalSQL is the rewritten query with $1..$N placeholders.
	CanonicalSQL string

	// ParamCount is the number of extracted parameters.
	ParamCount int

	// OriginalSQL is the first query text that produced this canonical form.
	// Stored for logging and debugging only.
	OriginalSQL string

	// RegisteredAt is when this entry was first created.
	RegisteredAt time.Time

	// --- Atomic stats (written from many goroutines) ---

	// Executions is the total number of times this statement has been executed
	// via the cache (both simple-query rewrites and extended protocol hits).
	Executions int64

	// LastUsedNano is the unix-nanosecond timestamp of the last execution.
	LastUsedNano int64

	// DeployCount is the number of backend connections that currently have this
	// statement deployed (Parse sent and acknowledged).
	DeployCount int64
}

// RecordExecution atomically increments the execution counter and updates the
// last-used timestamp.
func (s *CachedStmt) RecordExecution() {
	atomic.AddInt64(&s.Executions, 1)
	atomic.StoreInt64(&s.LastUsedNano, time.Now().UnixNano())
}

// RecordDeploy atomically increments the deploy counter.
func (s *CachedStmt) RecordDeploy() {
	atomic.AddInt64(&s.DeployCount, 1)
}

// RecordUndeploy atomically decrements the deploy counter.
func (s *CachedStmt) RecordUndeploy() {
	atomic.AddInt64(&s.DeployCount, -1)
}

// LastUsed returns the last-used time from the atomic nanosecond timestamp.
func (s *CachedStmt) LastUsed() time.Time {
	ns := atomic.LoadInt64(&s.LastUsedNano)
	if ns == 0 {
		return s.RegisteredAt
	}
	return time.Unix(0, ns)
}

// StmtCache is the target-level registry of canonical prepared statements.
// It is safe for concurrent use from all client goroutines.
//
// Scope: one StmtCache per Target. Statements are keyed by the hash of their
// canonical SQL, so the same logical query from any (database, user) pair
// shares a single entry and a single set of stats. Deployment state (whether
// a specific backend connection has had Parse sent) is tracked separately on
// Backend.deployedStmts.
type StmtCache struct {
	mu      sync.RWMutex
	entries map[string]*CachedStmt // hash → entry
}

// NewStmtCache creates an empty statement cache.
func NewStmtCache() *StmtCache {
	return &StmtCache{
		entries: make(map[string]*CachedStmt),
	}
}

// Get returns the cached entry for hash, or nil if not present.
func (c *StmtCache) Get(hash string) *CachedStmt {
	c.mu.RLock()
	entry := c.entries[hash]
	c.mu.RUnlock()
	return entry
}

// GetOrRegister returns the existing entry for hash or registers a new one.
// The returned bool is true when a new entry was created.
func (c *StmtCache) GetOrRegister(hash, canonicalSQL, originalSQL string, paramCount int) (*CachedStmt, bool) {
	// Fast path: already registered.
	c.mu.RLock()
	if entry, ok := c.entries[hash]; ok {
		c.mu.RUnlock()
		return entry, false
	}
	c.mu.RUnlock()

	// Slow path: register under write lock (double-check for races).
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[hash]; ok {
		return entry, false
	}

	entry := &CachedStmt{
		Hash:         hash,
		CanonicalSQL: canonicalSQL,
		ParamCount:   paramCount,
		OriginalSQL:  originalSQL,
		RegisteredAt: time.Now(),
	}
	c.entries[hash] = entry
	return entry, true
}

// All returns a snapshot of all cached statements for stats/metrics reporting.
func (c *StmtCache) All() []*CachedStmt {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*CachedStmt, 0, len(c.entries))
	for _, e := range c.entries {
		out = append(out, e)
	}
	return out
}

// Len returns the number of registered statements.
func (c *StmtCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}
