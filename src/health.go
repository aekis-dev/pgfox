package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// HealthChecker monitors database health using raw TCP connections
type HealthChecker struct {
	config DatabaseConfig
	stats  HealthStats
	logger *Logger
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex
}

// HealthStats contains health check statistics
type HealthStats struct {
	LastCheck    time.Time
	IsHealthy    bool
	CheckCount   int64
	ErrorCount   int64
	ResponseTime time.Duration
	LastError    string
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(config DatabaseConfig, logger *Logger) (*HealthChecker, error) {
	ctx, cancel := context.WithCancel(context.Background())

	return &HealthChecker{
		config: config,
		logger: logger.WithField("component", "health"),
		ctx:    ctx,
		cancel: cancel,
		stats:  HealthStats{IsHealthy: true, LastCheck: time.Now()},
	}, nil
}

// Start starts the health checker
func (hc *HealthChecker) Start(ctx context.Context) {
	hc.wg.Add(1)
	defer hc.wg.Done()

	hc.logger.Info("Starting health checker",
		"database", hc.config.Name,
		"interval", hc.config.HealthCheck.Interval,
		"timeout", hc.config.HealthCheck.Timeout)

	ticker := time.NewTicker(hc.config.HealthCheck.Interval)
	defer ticker.Stop()

	// Initial health check
	hc.performHealthCheck()

	for {
		select {
		case <-ctx.Done():
			hc.logger.Info("Health checker stopping due to parent context cancellation")
			return
		case <-hc.ctx.Done():
			hc.logger.Info("Health checker stopping")
			return
		case <-ticker.C:
			hc.performHealthCheck()
		}
	}
}

// Stop stops the health checker
func (hc *HealthChecker) Stop() error {
	hc.logger.Info("Stopping health checker")
	hc.cancel()
	hc.wg.Wait()
	return nil
}

// performHealthCheck performs a single health check using raw TCP connection
func (hc *HealthChecker) performHealthCheck() {
	start := time.Now()

	hc.mu.Lock()
	hc.stats.LastCheck = start
	atomic.AddInt64(&hc.stats.CheckCount, 1)
	hc.mu.Unlock()

	err := hc.checkConnection()
	responseTime := time.Since(start)

	hc.mu.Lock()
	hc.stats.ResponseTime = responseTime

	if err != nil {
		hc.stats.IsHealthy = false
		hc.stats.LastError = err.Error()
		atomic.AddInt64(&hc.stats.ErrorCount, 1)
		hc.mu.Unlock()

		hc.logger.WithError(err).Error("Health check failed",
			"database", hc.config.Name,
			"response_time", responseTime)
	} else {
		wasUnhealthy := !hc.stats.IsHealthy
		hc.stats.IsHealthy = true
		hc.stats.LastError = ""
		hc.mu.Unlock()

		if wasUnhealthy {
			hc.logger.Info("Health check recovered",
				"database", hc.config.Name,
				"response_time", responseTime)
		} else {
			hc.logger.Debug("Health check passed",
				"database", hc.config.Name,
				"response_time", responseTime)
		}
	}
}

// checkConnection performs the actual connection check
func (hc *HealthChecker) checkConnection() error {
	// Create a temporary TCP connection
	addr := fmt.Sprintf("%s:%d", hc.config.Host, hc.config.Port)

	conn, err := net.DialTimeout("tcp", addr, hc.config.HealthCheck.Timeout)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	// Set overall timeout for the health check
	conn.SetDeadline(time.Now().Add(hc.config.HealthCheck.Timeout))

	// Create a backend connection wrapper
	backend := NewBackendConnection(conn, hc.config.Name, hc.config.Name)
	defer backend.Close()

	// Try to authenticate (basic health check)
	startupMsg := buildStartupMessage(hc.config.User, hc.config.Name)
	if _, err := backend.writer.Write(startupMsg); err != nil {
		return fmt.Errorf("failed to send startup message: %w", err)
	}
	if err := backend.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush startup message: %w", err)
	}

	// Read at least one response to verify the server is responding
	msgType, body, err := backend.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	// Check if we got an authentication response
	if msgType == 'R' && len(body) >= 4 {
		// We got an authentication response, which means the server is alive
		return nil
	} else if msgType == 'E' {
		// Error response - server is alive but there might be an auth issue
		// For health check purposes, this still means the server is responding
		return nil
	}

	return fmt.Errorf("unexpected response type: %c", msgType)
}

// IsHealthy returns the current health status
func (hc *HealthChecker) IsHealthy() bool {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.stats.IsHealthy
}

// GetStats returns the current health statistics
func (hc *HealthChecker) GetStats() HealthStats {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	return HealthStats{
		LastCheck:    hc.stats.LastCheck,
		IsHealthy:    hc.stats.IsHealthy,
		CheckCount:   atomic.LoadInt64(&hc.stats.CheckCount),
		ErrorCount:   atomic.LoadInt64(&hc.stats.ErrorCount),
		ResponseTime: hc.stats.ResponseTime,
		LastError:    hc.stats.LastError,
	}
}

// GetLastError returns the last error message
func (hc *HealthChecker) GetLastError() string {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.stats.LastError
}

// GetResponseTime returns the last response time
func (hc *HealthChecker) GetResponseTime() time.Duration {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.stats.ResponseTime
}

// GetCheckCount returns the total number of health checks performed
func (hc *HealthChecker) GetCheckCount() int64 {
	return atomic.LoadInt64(&hc.stats.CheckCount)
}

// GetErrorCount returns the total number of health check errors
func (hc *HealthChecker) GetErrorCount() int64 {
	return atomic.LoadInt64(&hc.stats.ErrorCount)
}

// GetErrorRate returns the error rate as a percentage
func (hc *HealthChecker) GetErrorRate() float64 {
	checkCount := atomic.LoadInt64(&hc.stats.CheckCount)
	if checkCount == 0 {
		return 0.0
	}

	errorCount := atomic.LoadInt64(&hc.stats.ErrorCount)
	return float64(errorCount) / float64(checkCount) * 100.0
}

// GetUptime returns how long the database has been healthy
func (hc *HealthChecker) GetUptime() time.Duration {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	if !hc.stats.IsHealthy {
		return 0
	}

	return time.Since(hc.stats.LastCheck)
}

// GlobalHealthChecker manages health checking for all databases
type GlobalHealthChecker struct {
	pooler     *WildcardPooler
	checkers   map[string]*HealthChecker
	checkersMu sync.RWMutex
	logger     *Logger
}

// NewGlobalHealthChecker creates a new global health checker
func NewGlobalHealthChecker(pooler *WildcardPooler) *GlobalHealthChecker {
	return &GlobalHealthChecker{
		pooler:   pooler,
		checkers: make(map[string]*HealthChecker),
		logger:   pooler.logger.WithField("component", "global_health"),
	}
}

// AddChecker adds a health checker for a database
func (ghc *GlobalHealthChecker) AddChecker(name string, checker *HealthChecker) {
	ghc.checkersMu.Lock()
	defer ghc.checkersMu.Unlock()

	ghc.checkers[name] = checker
	ghc.logger.Info("Added health checker", "database", name)
}

// RemoveChecker removes a health checker for a database
func (ghc *GlobalHealthChecker) RemoveChecker(name string) {
	ghc.checkersMu.Lock()
	defer ghc.checkersMu.Unlock()

	if checker, exists := ghc.checkers[name]; exists {
		checker.Stop()
		delete(ghc.checkers, name)
		ghc.logger.Info("Removed health checker", "database", name)
	}
}

// GetChecker returns a health checker for a database
func (ghc *GlobalHealthChecker) GetChecker(name string) (*HealthChecker, bool) {
	ghc.checkersMu.RLock()
	defer ghc.checkersMu.RUnlock()

	checker, exists := ghc.checkers[name]
	return checker, exists
}

// GetAllHealthy returns all healthy databases
func (ghc *GlobalHealthChecker) GetAllHealthy() []string {
	ghc.checkersMu.RLock()
	defer ghc.checkersMu.RUnlock()

	var healthy []string
	for name, checker := range ghc.checkers {
		if checker.IsHealthy() {
			healthy = append(healthy, name)
		}
	}

	return healthy
}

// GetAllUnhealthy returns all unhealthy databases
func (ghc *GlobalHealthChecker) GetAllUnhealthy() []string {
	ghc.checkersMu.RLock()
	defer ghc.checkersMu.RUnlock()

	var unhealthy []string
	for name, checker := range ghc.checkers {
		if !checker.IsHealthy() {
			unhealthy = append(unhealthy, name)
		}
	}

	return unhealthy
}

// GetGlobalStats returns global health statistics
func (ghc *GlobalHealthChecker) GetGlobalStats() map[string]interface{} {
	ghc.checkersMu.RLock()
	defer ghc.checkersMu.RUnlock()

	totalDatabases := len(ghc.checkers)
	healthyDatabases := 0
	totalChecks := int64(0)
	totalErrors := int64(0)

	for _, checker := range ghc.checkers {
		if checker.IsHealthy() {
			healthyDatabases++
		}
		totalChecks += checker.GetCheckCount()
		totalErrors += checker.GetErrorCount()
	}

	errorRate := 0.0
	if totalChecks > 0 {
		errorRate = float64(totalErrors) / float64(totalChecks) * 100.0
	}

	return map[string]interface{}{
		"total_databases":     totalDatabases,
		"healthy_databases":   healthyDatabases,
		"unhealthy_databases": totalDatabases - healthyDatabases,
		"total_checks":        totalChecks,
		"total_errors":        totalErrors,
		"error_rate":          errorRate,
	}
}

// GetDetailedStats returns detailed health statistics for all databases
func (ghc *GlobalHealthChecker) GetDetailedStats() map[string]HealthStats {
	ghc.checkersMu.RLock()
	defer ghc.checkersMu.RUnlock()

	stats := make(map[string]HealthStats)
	for name, checker := range ghc.checkers {
		stats[name] = checker.GetStats()
	}

	return stats
}

// Stop stops all health checkers
func (ghc *GlobalHealthChecker) Stop() {
	ghc.checkersMu.Lock()
	defer ghc.checkersMu.Unlock()

	ghc.logger.Info("Stopping all health checkers")

	for name, checker := range ghc.checkers {
		checker.Stop()
		ghc.logger.Debug("Stopped health checker", "database", name)
	}

	ghc.checkers = make(map[string]*HealthChecker)
}
