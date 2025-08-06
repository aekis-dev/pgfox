package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// QueryTask represents a query execution task
type QueryTask struct {
	client   *ClientConnection
	query    string
	msgType  byte
	body     []byte
	resultCh chan QueryResult
}

// QueryResult represents the result of a query execution
type QueryResult struct {
	err error
}

// WorkerPool manages goroutines for query execution
type WorkerPool struct {
	pooler         *WildcardPooler
	taskQueue      chan QueryTask
	workerCount    int
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	logger         *Logger
	completedTasks int64 // Counter for completed tasks
}

// NewWorkerPool creates a new worker pool for query execution
func NewWorkerPool(pooler *WildcardPooler, workerCount int, queueSize int) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		pooler:      pooler,
		taskQueue:   make(chan QueryTask, queueSize),
		workerCount: workerCount,
		ctx:         ctx,
		cancel:      cancel,
		logger:      pooler.logger.WithField("component", "worker_pool"),
	}
}

// Start starts the worker pool
func (wp *WorkerPool) Start() {
	wp.logger.Info("Starting worker pool", "worker_count", wp.workerCount, "queue_size", cap(wp.taskQueue))

	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

// Stop stops the worker pool
func (wp *WorkerPool) Stop() {
	wp.logger.Info("Stopping worker pool")
	wp.cancel()
	close(wp.taskQueue)
	wp.wg.Wait()
	wp.logger.Info("Worker pool stopped")
}

// worker is the main worker goroutine
func (wp *WorkerPool) worker(workerID int) {
	defer wp.wg.Done()

	logger := wp.logger.WithField("worker_id", workerID)
	logger.Debug("Worker started")

	for {
		select {
		case <-wp.ctx.Done():
			logger.Debug("Worker stopping due to context cancellation")
			return
		case task, ok := <-wp.taskQueue:
			if !ok {
				logger.Debug("Worker stopping due to closed task queue")
				return
			}

			startTime := time.Now()
			logger.Debug("Worker processing task",
				"client", task.client.RemoteAddr(),
				"msg_type", string(task.msgType))

			// Process the task
			var err error
			switch task.msgType {
			case Query:
				err = wp.pooler.executeQuery(task.client, task.query)
			case Parse, Bind, Execute, Sync:
				err = wp.pooler.executeExtendedQuery(task.client, task.msgType, task.body)
			default:
				err = wp.pooler.executeUnknownMessage(task.client, task.msgType, task.body)
			}

			// Update metrics
			atomic.AddInt64(&wp.completedTasks, 1)
			duration := time.Since(startTime)

			if err != nil {
				logger.WithError(err).Warn("Task execution failed", "duration", duration)
			} else {
				logger.Debug("Task completed successfully", "duration", duration)
			}

			// Send result back
			select {
			case task.resultCh <- QueryResult{err: err}:
			case <-wp.ctx.Done():
				return
			case <-time.After(1 * time.Second):
				logger.Warn("Failed to send task result within timeout")
			}
		}
	}
}

// SubmitQuery submits a query for execution
func (wp *WorkerPool) SubmitQuery(client *ClientConnection, query string) error {
	resultCh := make(chan QueryResult, 1)

	task := QueryTask{
		client:   client,
		query:    query,
		msgType:  Query,
		resultCh: resultCh,
	}

	// Submit task
	select {
	case wp.taskQueue <- task:
		// Task submitted successfully
	case <-wp.ctx.Done():
		return fmt.Errorf("worker pool is shutting down")
	default:
		// Queue is full, execute synchronously as fallback
		wp.logger.Warn("Task queue is full, executing synchronously",
			"client", client.RemoteAddr())
		return wp.pooler.executeQuery(client, query)
	}

	// Wait for result with timeout
	timeout := 60 * time.Second // Configurable timeout
	if wp.pooler.config.Server.QueryTimeout > 0 {
		timeout = wp.pooler.config.Server.QueryTimeout
	}

	select {
	case result := <-resultCh:
		return result.err
	case <-wp.ctx.Done():
		return fmt.Errorf("worker pool is shutting down")
	case <-time.After(timeout):
		return fmt.Errorf("query execution timeout after %v", timeout)
	}
}

// SubmitExtendedQuery submits an extended query for execution
func (wp *WorkerPool) SubmitExtendedQuery(client *ClientConnection, msgType byte, body []byte) error {
	resultCh := make(chan QueryResult, 1)

	task := QueryTask{
		client:   client,
		msgType:  msgType,
		body:     body,
		resultCh: resultCh,
	}

	// Submit task
	select {
	case wp.taskQueue <- task:
		// Task submitted successfully
	case <-wp.ctx.Done():
		return fmt.Errorf("worker pool is shutting down")
	default:
		// Queue is full, fallback to synchronous execution
		wp.logger.Warn("Task queue is full, executing extended query synchronously",
			"client", client.RemoteAddr(), "msg_type", string(msgType))
		return wp.pooler.executeExtendedQuery(client, msgType, body)
	}

	// Wait for result with timeout
	timeout := 60 * time.Second
	if wp.pooler.config.Server.QueryTimeout > 0 {
		timeout = wp.pooler.config.Server.QueryTimeout
	}

	select {
	case result := <-resultCh:
		return result.err
	case <-wp.ctx.Done():
		return fmt.Errorf("worker pool is shutting down")
	case <-time.After(timeout):
		return fmt.Errorf("extended query execution timeout after %v", timeout)
	}
}

// GetStats returns worker pool statistics
func (wp *WorkerPool) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"worker_count":    wp.workerCount,
		"queue_capacity":  cap(wp.taskQueue),
		"queued_tasks":    len(wp.taskQueue),
		"completed_tasks": atomic.LoadInt64(&wp.completedTasks),
		"is_running":      wp.ctx.Err() == nil,
	}
}
