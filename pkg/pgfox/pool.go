package pgfox

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

// Stats contains per-Pool statistics.
type Stats struct {
	QueriesExecuted int64
	ErrorCount      int64
}

// Pool manages the idle connection queue and stats for a (database, user) pair.
// It is a plain data struct — the target goroutine owns all connection lifecycle.
type Pool struct {
	Target   *Target
	Username string
	DbName   string

	// Queue is the queue of idle connections available to borrow.
	Queue chan *Backend

	// All is the list of every connection owned by this Pool (idle or
	// active). Written only by the target goroutine — no mutex needed.
	All []*Backend

	// Peak tracking for smart shrink decisions.
	peakSamples []peakSample

	stats Stats
}

// removeFromAll removes conn from Pool.All.
// Must be called from the target goroutine.
func (pool *Pool) removeFromAll(conn *Backend) {
	for i, c := range pool.All {
		if c == conn {
			pool.All[i] = pool.All[len(pool.All)-1]
			pool.All = pool.All[:len(pool.All)-1]
			return
		}
	}
}

// newConn opens a fresh backend connection for this Pool using certificate auth.
func (pool *Pool) newConn(p *Server) (*Backend, error) {
	cert, err := p.loadOrGenerateUserCert(pool.Username)
	if err != nil {
		return nil, fmt.Errorf("failed to get cert for %s: %w", pool.Username, err)
	}
	return p.createCertBackend(pool.Target, pool.DbName, pool.Username, cert)
}

// borrowConn takes a connection from the Pool, blocking until one is available,
// the timeout fires, or ctx is cancelled.
//
// A single timer is allocated for the entire wait to avoid the allocation-per-
// iteration that time.After would cause under contention.
func (pool *Pool) borrowConn(ctx context.Context) (*Backend, error) {
	if pool.Target.Draining.Load() {
		return nil, fmt.Errorf("target %s is being removed, try again after reconnect",
			pool.Target.Name)
	}
	timeout := pool.Target.ConnectTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	// Fast path: connection available immediately.
	select {
	case conn := <-pool.Queue:
		conn.SetInUse(true)
		return conn, nil
	default:
	}

	// Slow path: wait with a single timer allocation.
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	// Signal the target goroutine that this Pool is contended so it triggers
	// an immediate growthCycle instead of waiting up to 50ms for the ticker.
	select {
	case pool.Target.Demand <- struct{}{}:
	default: // already signalled by another waiter this tick — fine
	}

	for {
		select {
		case conn := <-pool.Queue:
			conn.SetInUse(true)
			return conn, nil
		case <-pool.Target.ConnReady:
			// A connection is available — retry.
			select {
			case conn := <-pool.Queue:
				conn.SetInUse(true)
				return conn, nil
			default:
				// Grabbed by a concurrent waiter; re-signal demand and keep waiting.
				select {
				case pool.Target.Demand <- struct{}{}:
				default:
				}
			}
		case <-timer.C:
			return nil, fmt.Errorf("timed out waiting for connection for %s/%s",
				pool.DbName, pool.Username)
		case <-ctx.Done():
			return nil, fmt.Errorf("server shutting down")
		}
	}
}

// IdleConnections returns the number of connections currently idle in backendPool.
func (pool *Pool) IdleConnections() int {
	return len(pool.Queue)
}

// ActiveConnections returns connections currently checked out.
func (pool *Pool) ActiveConnections() int {
	total := len(pool.All)
	idle := len(pool.Queue)
	if idle > total {
		return 0
	}
	return total - idle
}

// TotalConnections returns all connections owned by this Pool.
func (pool *Pool) TotalConnections() int {
	return len(pool.All)
}

func (pool *Pool) QueriesExecuted() int64 {
	return atomic.LoadInt64(&pool.stats.QueriesExecuted)
}

func (pool *Pool) ErrorCount() int64 {
	return atomic.LoadInt64(&pool.stats.ErrorCount)
}
