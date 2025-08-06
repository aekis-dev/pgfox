package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

// MetricsServer provides HTTP endpoints for metrics and health checks
type MetricsServer struct {
	pooler *WildcardPooler
	server *http.Server
	logger *Logger
}

// NewMetricsServer creates a new metrics server
func NewMetricsServer(pooler *WildcardPooler, addr, path string, logger *Logger) *MetricsServer {
	mux := http.NewServeMux()

	ms := &MetricsServer{
		pooler: pooler,
		logger: logger.WithField("component", "metrics"),
		server: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}

	// Register endpoints
	mux.HandleFunc(path, ms.handleMetrics)
	mux.HandleFunc("/health", ms.handleHealth)
	mux.HandleFunc("/stats", ms.handleStats)
	mux.HandleFunc("/databases", ms.handleDatabases)
	mux.HandleFunc("/listeners", ms.handleListeners)
	mux.HandleFunc("/discovery", ms.handleDiscovery)

	return ms
}

// Start starts the metrics server
func (ms *MetricsServer) Start(ctx context.Context) error {
	ms.logger.Info("Starting metrics server", "addr", ms.server.Addr)

	// Start server in goroutine
	go func() {
		if err := ms.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			ms.logger.WithError(err).Error("Metrics server failed")
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ms.logger.Info("Shutting down metrics server")
	return ms.server.Shutdown(shutdownCtx)
}

// handleMetrics handles the /metrics endpoint (Prometheus format)
func (ms *MetricsServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	stats := ms.pooler.Stats()

	// Write Prometheus metrics
	fmt.Fprintf(w, "# HELP pgjoint_clients_total Total number of client connections\n")
	fmt.Fprintf(w, "# TYPE pgjoint_clients_total counter\n")
	fmt.Fprintf(w, "pgjoint_clients_total %d\n", stats.TotalClients)

	fmt.Fprintf(w, "# HELP pgjoint_clients_active Current active client connections\n")
	fmt.Fprintf(w, "# TYPE pgjoint_clients_active gauge\n")
	fmt.Fprintf(w, "pgjoint_clients_active %d\n", stats.ActiveClients)

	fmt.Fprintf(w, "# HELP pgjoint_databases_static Number of static databases\n")
	fmt.Fprintf(w, "# TYPE pgjoint_databases_static gauge\n")
	fmt.Fprintf(w, "pgjoint_databases_static %d\n", stats.StaticDatabases)

	fmt.Fprintf(w, "# HELP pgjoint_databases_dynamic Number of dynamic databases\n")
	fmt.Fprintf(w, "# TYPE pgjoint_databases_dynamic gauge\n")
	fmt.Fprintf(w, "pgjoint_databases_dynamic %d\n", stats.DynamicDatabases)

	fmt.Fprintf(w, "# HELP pgjoint_databases_healthy Number of healthy databases\n")
	fmt.Fprintf(w, "# TYPE pgjoint_databases_healthy gauge\n")
	fmt.Fprintf(w, "pgjoint_databases_healthy %d\n", stats.HealthyDatabases)

	fmt.Fprintf(w, "# HELP pgjoint_queries_total Total number of queries executed\n")
	fmt.Fprintf(w, "# TYPE pgjoint_queries_total counter\n")
	fmt.Fprintf(w, "pgjoint_queries_total %d\n", stats.TotalQueries)

	fmt.Fprintf(w, "# HELP pgjoint_notifications_sent_total Total number of notifications sent\n")
	fmt.Fprintf(w, "# TYPE pgjoint_notifications_sent_total counter\n")
	fmt.Fprintf(w, "pgjoint_notifications_sent_total %d\n", stats.NotificationsSent)

	fmt.Fprintf(w, "# HELP pgjoint_databases_discovered_total Total number of databases discovered\n")
	fmt.Fprintf(w, "# TYPE pgjoint_databases_discovered_total counter\n")
	fmt.Fprintf(w, "pgjoint_databases_discovered_total %d\n", stats.DatabasesDiscovered)

	fmt.Fprintf(w, "# HELP pgjoint_databases_created_total Total number of database pools created\n")
	fmt.Fprintf(w, "# TYPE pgjoint_databases_created_total counter\n")
	fmt.Fprintf(w, "pgjoint_databases_created_total %d\n", stats.DatabasesCreated)

	fmt.Fprintf(w, "# HELP pgjoint_databases_removed_total Total number of database pools removed\n")
	fmt.Fprintf(w, "# TYPE pgjoint_databases_removed_total counter\n")
	fmt.Fprintf(w, "pgjoint_databases_removed_total %d\n", stats.DatabasesRemoved)

	// Per-database metrics
	ms.pooler.databasesMu.RLock()
	for name, dbManager := range ms.pooler.staticDatabases {
		ms.writeDBMetrics(w, name, "static", dbManager)
	}
	for key, dbManager := range ms.pooler.dynamicDatabases {
		ms.writeDBMetrics(w, key, "dynamic", dbManager)
	}
	ms.pooler.databasesMu.RUnlock()

	// Listener metrics
	listenerStats := ms.pooler.getListenerStats()
	for channel, count := range listenerStats {
		fmt.Fprintf(w, "pgjoint_listeners{channel=\"%s\"} %d\n", channel, count)
	}
}

// writeDBMetrics writes database-specific metrics
func (ms *MetricsServer) writeDBMetrics(w http.ResponseWriter, name, dbType string, dbManager *DatabaseManager) {
	stats := dbManager.stats

	fmt.Fprintf(w, "pgjoint_db_connections_total{database=\"%s\",type=\"%s\"} %d\n",
		name, dbType, atomic.LoadInt64(&stats.TotalConnections))

	fmt.Fprintf(w, "pgjoint_db_connections_active{database=\"%s\",type=\"%s\"} %d\n",
		name, dbType, atomic.LoadInt64(&stats.ActiveConnections))

	fmt.Fprintf(w, "pgjoint_db_connections_idle{database=\"%s\",type=\"%s\"} %d\n",
		name, dbType, atomic.LoadInt64(&stats.IdleConnections))

	fmt.Fprintf(w, "pgjoint_db_queries_total{database=\"%s\",type=\"%s\"} %d\n",
		name, dbType, atomic.LoadInt64(&stats.QueriesExecuted))

	fmt.Fprintf(w, "pgjoint_db_errors_total{database=\"%s\",type=\"%s\"} %d\n",
		name, dbType, atomic.LoadInt64(&stats.ErrorCount))

	fmt.Fprintf(w, "pgjoint_db_bytes_received_total{database=\"%s\",type=\"%s\"} %d\n",
		name, dbType, atomic.LoadInt64(&stats.BytesReceived))

	fmt.Fprintf(w, "pgjoint_db_bytes_sent_total{database=\"%s\",type=\"%s\"} %d\n",
		name, dbType, atomic.LoadInt64(&stats.BytesSent))

	// Health metrics if available
	if dbManager.healthChecker != nil {
		healthStats := dbManager.healthChecker.GetStats()
		healthy := 0
		if healthStats.IsHealthy {
			healthy = 1
		}
		fmt.Fprintf(w, "pgjoint_db_healthy{database=\"%s\",type=\"%s\"} %d\n",
			name, dbType, healthy)

		fmt.Fprintf(w, "pgjoint_db_health_checks_total{database=\"%s\",type=\"%s\"} %d\n",
			name, dbType, healthStats.CheckCount)

		fmt.Fprintf(w, "pgjoint_db_health_errors_total{database=\"%s\",type=\"%s\"} %d\n",
			name, dbType, healthStats.ErrorCount)

		fmt.Fprintf(w, "pgjoint_db_health_response_time_seconds{database=\"%s\",type=\"%s\"} %f\n",
			name, dbType, healthStats.ResponseTime.Seconds())
	}
}

// handleHealth handles the /health endpoint
func (ms *MetricsServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC(),
		"version":   Version,
		"uptime":    time.Since(time.Now()).String(), // This would be actual uptime in real implementation
	}

	// Check if any critical systems are down
	stats := ms.pooler.Stats()
	if stats.ActiveClients > 0 && (stats.StaticDatabases+stats.DynamicDatabases) == 0 {
		health["status"] = "degraded"
		health["message"] = "No databases available"
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(health)
}

// handleStats handles the /stats endpoint
func (ms *MetricsServer) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	stats := map[string]interface{}{
		"global":    ms.pooler.Stats(),
		"discovery": ms.pooler.getDiscoveryStats(),
		"listeners": ms.pooler.getListenerStats(),
		"timestamp": time.Now().UTC(),
	}

	// Add database stats
	databaseStats := make(map[string]interface{})

	ms.pooler.databasesMu.RLock()
	for name, dbManager := range ms.pooler.staticDatabases {
		databaseStats[name] = map[string]interface{}{
			"type":   "static",
			"stats":  dbManager.stats,
			"config": dbManager.config,
			"health": ms.getDBHealthInfo(dbManager),
		}
	}
	for key, dbManager := range ms.pooler.dynamicDatabases {
		databaseStats[key] = map[string]interface{}{
			"type":   "dynamic",
			"stats":  dbManager.stats,
			"config": dbManager.config,
			"health": ms.getDBHealthInfo(dbManager),
		}
	}
	ms.pooler.databasesMu.RUnlock()

	stats["databases"] = databaseStats

	json.NewEncoder(w).Encode(stats)
}

// handleDatabases handles the /databases endpoint
func (ms *MetricsServer) handleDatabases(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	databases := make(map[string]interface{})

	ms.pooler.databasesMu.RLock()
	for name, dbManager := range ms.pooler.staticDatabases {
		databases[name] = map[string]interface{}{
			"type":      "static",
			"host":      dbManager.config.Host,
			"port":      dbManager.config.Port,
			"pool_mode": dbManager.config.PoolMode,
			"max_conn":  dbManager.config.MaxConnections,
			"min_conn":  dbManager.config.MinConnections,
			"healthy":   ms.isDBHealthy(dbManager),
			"last_used": dbManager.lastUsed,
		}
	}
	for key, dbManager := range ms.pooler.dynamicDatabases {
		databases[key] = map[string]interface{}{
			"type":      "dynamic",
			"host":      dbManager.config.Host,
			"port":      dbManager.config.Port,
			"pool_mode": dbManager.config.PoolMode,
			"max_conn":  dbManager.config.MaxConnections,
			"min_conn":  dbManager.config.MinConnections,
			"healthy":   ms.isDBHealthy(dbManager),
			"last_used": dbManager.lastUsed,
			"target":    dbManager.wildcardTarget.Name,
		}
	}
	ms.pooler.databasesMu.RUnlock()

	response := map[string]interface{}{
		"databases": databases,
		"total":     len(databases),
		"timestamp": time.Now().UTC(),
	}

	json.NewEncoder(w).Encode(response)
}

// handleListeners handles the /listeners endpoint
func (ms *MetricsServer) handleListeners(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	channels := ms.pooler.getListeningChannels()
	listenerStats := ms.pooler.getListenerStats()

	channelDetails := make(map[string]interface{})
	for _, channel := range channels {
		clients := ms.pooler.getListeningClients(channel)
		clientAddrs := make([]string, len(clients))
		for i, client := range clients {
			clientAddrs[i] = client.RemoteAddr().String()
		}

		channelDetails[channel] = map[string]interface{}{
			"client_count": len(clients),
			"clients":      clientAddrs,
		}
	}

	response := map[string]interface{}{
		"channels":  channelDetails,
		"stats":     listenerStats,
		"total":     len(channels),
		"timestamp": time.Now().UTC(),
	}

	json.NewEncoder(w).Encode(response)
}

// handleDiscovery handles the /discovery endpoint
func (ms *MetricsServer) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	discoveredDBs := ms.pooler.getDiscoveredDatabases()
	discoveryStats := ms.pooler.getDiscoveryStats()

	targets := make([]map[string]interface{}, len(ms.pooler.wildcardTargets))
	for i, target := range ms.pooler.wildcardTargets {
		targets[i] = map[string]interface{}{
			"name":                   target.Name,
			"host":                   target.Host,
			"port":                   target.Port,
			"priority":               target.Priority,
			"max_connections_per_db": target.MaxConnectionsPerDB,
			"min_connections_per_db": target.MinConnectionsPerDB,
			"pool_mode":              target.PoolMode,
		}
	}

	response := map[string]interface{}{
		"targets":        targets,
		"discovered_dbs": discoveredDBs,
		"stats":          discoveryStats,
		"timestamp":      time.Now().UTC(),
	}

	json.NewEncoder(w).Encode(response)
}

// getDBHealthInfo gets health information for a database
func (ms *MetricsServer) getDBHealthInfo(dbManager *DatabaseManager) interface{} {
	if dbManager.healthChecker == nil {
		return map[string]interface{}{
			"enabled": false,
		}
	}

	stats := dbManager.healthChecker.GetStats()
	return map[string]interface{}{
		"enabled":       true,
		"healthy":       stats.IsHealthy,
		"last_check":    stats.LastCheck,
		"check_count":   stats.CheckCount,
		"error_count":   stats.ErrorCount,
		"response_time": stats.ResponseTime,
		"error_rate":    dbManager.healthChecker.GetErrorRate(),
		"last_error":    stats.LastError,
	}
}

// isDBHealthy checks if a database is healthy
func (ms *MetricsServer) isDBHealthy(dbManager *DatabaseManager) bool {
	if dbManager.healthChecker == nil {
		return true // Assume healthy if no health checker
	}
	return dbManager.healthChecker.IsHealthy()
}

// handleIndex handles the root endpoint with basic info
func (ms *MetricsServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	info := map[string]interface{}{
		"name":        "PgJoint",
		"version":     Version,
		"description": "PostgreSQL Connection Pooler with Wildcard Database Support",
		"endpoints": map[string]string{
			"/metrics":   "Prometheus metrics",
			"/health":    "Health check",
			"/stats":     "Detailed statistics",
			"/databases": "Database information",
			"/listeners": "LISTEN/NOTIFY information",
			"/discovery": "Discovery information",
		},
		"timestamp": time.Now().UTC(),
	}

	json.NewEncoder(w).Encode(info)
}
