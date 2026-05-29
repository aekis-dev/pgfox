package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// peakSample records active connection count at a point in time.
type peakSample struct {
	active int
	at     time.Time
}

// Stats contains per-pool statistics.
type Stats struct {
	QueriesExecuted int64
	ErrorCount      int64
}

// Pool manages the idle connection queue and stats for a (database, user) pair.
// It is a plain data struct — the target goroutine owns all connection lifecycle.
type Pool struct {
	target   *Target
	username string
	dbName   string

	// backendPool is the queue of idle connections available to borrow.
	backendPool chan *BackendConnection

	// allConns is the list of every connection owned by this pool (idle or
	// active). Written only by the target goroutine — no mutex needed.
	allConns []*BackendConnection

	// Peak tracking for smart shrink decisions.
	peakSamples []peakSample

	stats Stats
}

// removeFromAllConns removes conn from pool.allConns.
// Must be called from the target goroutine.
func (pool *Pool) removeFromAllConns(conn *BackendConnection) {
	for i, c := range pool.allConns {
		if c == conn {
			pool.allConns[i] = pool.allConns[len(pool.allConns)-1]
			pool.allConns = pool.allConns[:len(pool.allConns)-1]
			return
		}
	}
}

// newConn opens a fresh backend connection for this pool using certificate auth.
func (pool *Pool) newConn(p *Server) (*BackendConnection, error) {
	cert, err := p.loadOrGenerateUserCert(pool.username)
	if err != nil {
		return nil, fmt.Errorf("failed to get cert for %s: %w", pool.username, err)
	}
	return p.createCertBackendConnection(pool.target, pool.dbName, pool.username, cert)
}

// borrowConn takes a connection from the pool, blocking until one is available,
// the timeout fires, or ctx is cancelled.
//
// A single timer is allocated for the entire wait to avoid the allocation-per-
// iteration that time.After would cause under contention.
func (pool *Pool) borrowConn(ctx context.Context) (*BackendConnection, error) {
	if pool.target.draining.Load() {
		return nil, fmt.Errorf("target %s is being removed, try again after reconnect",
			pool.target.Name)
	}
	timeout := pool.target.ConnectTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	// Fast path: connection available immediately.
	select {
	case conn := <-pool.backendPool:
		conn.SetInUse(true)
		return conn, nil
	default:
	}

	// Slow path: wait with a single timer allocation.
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	// Signal the target goroutine that this pool is contended so it triggers
	// an immediate growthCycle instead of waiting up to 50ms for the ticker.
	select {
	case pool.target.demand <- struct{}{}:
	default: // already signalled by another waiter this tick — fine
	}

	for {
		select {
		case conn := <-pool.backendPool:
			conn.SetInUse(true)
			return conn, nil
		case <-pool.target.connReady:
			// A connection is available — retry.
			select {
			case conn := <-pool.backendPool:
				conn.SetInUse(true)
				return conn, nil
			default:
				// Grabbed by a concurrent waiter; re-signal demand and keep waiting.
				select {
				case pool.target.demand <- struct{}{}:
				default:
				}
			}
		case <-timer.C:
			return nil, fmt.Errorf("timed out waiting for connection for %s/%s",
				pool.dbName, pool.username)
		case <-ctx.Done():
			return nil, fmt.Errorf("server shutting down")
		}
	}
}

// idleConnections returns the number of connections currently idle in backendPool.
func (pool *Pool) idleConnections() int {
	return len(pool.backendPool)
}

// activeConnections returns connections currently checked out.
func (pool *Pool) activeConnections() int {
	total := len(pool.allConns)
	idle := len(pool.backendPool)
	if idle > total {
		return 0
	}
	return total - idle
}

// totalConnections returns all connections owned by this pool.
func (pool *Pool) totalConnections() int {
	return len(pool.allConns)
}

func (pool *Pool) queriesExecuted() int64 {
	return atomic.LoadInt64(&pool.stats.QueriesExecuted)
}

func (pool *Pool) errorCount() int64 {
	return atomic.LoadInt64(&pool.stats.ErrorCount)
}

// returnConn returns a backend connection to its pool directly from the calling
// goroutine, bypassing the target goroutine's event loop for the common healthy
// case. This eliminates the serialisation bottleneck where all concurrent
// returns queue through the single target goroutine.
//
// If the connection is dead it is handed to the target via closeCh for proper
// bookkeeping (totalOpen decrement, allConns removal). If the backendPool
// channel is unexpectedly full (which should not happen given the channel is
// sized to MaxConnections), it is also sent to closeCh rather than leaked.
func returnConn(conn *BackendConnection) {
	conn.SetInUse(false)
	conn.SetClient(nil)

	if !conn.IsAlive() {
		conn.pool.target.closeCh <- conn
		return
	}

	// Direct deposit into the pool channel — no target goroutine involvement.
	select {
	case conn.pool.backendPool <- conn:
		// Update cancel-lookup index and wake any waiting borrowers.
		conn.pool.target.backendIndex.Store(conn.GetProcessID(), conn)
		conn.pool.target.signalConnReady()
	default:
		// backendPool full — let target goroutine close it cleanly.
		conn.pool.target.closeCh <- conn
	}
}
