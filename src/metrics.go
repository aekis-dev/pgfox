package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

// handleMetrics handles the /metrics endpoint in Prometheus format.
func (ws *WebServer) handleMetrics(c *gin.Context) {
	c.Header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	stats := ws.pooler.Stats()

	metrics := ""

	metrics += "# HELP pgfox_clients_total Total number of client connections\n"
	metrics += "# TYPE pgfox_clients_total counter\n"
	metrics += fmt.Sprintf("pgfox_clients_total %d\n\n", stats.TotalClients)

	metrics += "# HELP pgfox_clients_active Current active client connections\n"
	metrics += "# TYPE pgfox_clients_active gauge\n"
	metrics += fmt.Sprintf("pgfox_clients_active %d\n\n", stats.ActiveClients)

	metrics += "# HELP pgfox_pools Total number of pools (target/database/user combinations)\n"
	metrics += "# TYPE pgfox_pools gauge\n"
	metrics += fmt.Sprintf("pgfox_pools %d\n\n", stats.TotalPools)

	metrics += "# HELP pgfox_queries_total Total number of queries executed\n"
	metrics += "# TYPE pgfox_queries_total counter\n"
	metrics += fmt.Sprintf("pgfox_queries_total %d\n\n", stats.TotalQueries)

	metrics += "# HELP pgfox_notifications_sent_total Total number of notifications sent\n"
	metrics += "# TYPE pgfox_notifications_sent_total counter\n"
	metrics += fmt.Sprintf("pgfox_notifications_sent_total %d\n\n", stats.NotificationsSent)

	metrics += "# HELP pgfox_idle_connections_closed_total Total number of idle connections closed\n"
	metrics += "# TYPE pgfox_idle_connections_closed_total counter\n"
	metrics += fmt.Sprintf("pgfox_idle_connections_closed_total %d\n\n", stats.IdleConnectionsClosed)

	// Per-pool metrics.
	ws.pooler.targetsMu.RLock()

	metrics += "# HELP pgfox_pool_connections_total Total connections open in pool\n"
	metrics += "# TYPE pgfox_pool_connections_total gauge\n"
	for targetName, targetMap := range ws.pooler.targets {
		for dbName, dbMap := range targetMap {
			for userName, pool := range dbMap {
				metrics += fmt.Sprintf(
					"pgfox_pool_connections_total{target=%q,database=%q,user=%q} %d\n",
					targetName, dbName, userName, pool.totalConnections())
			}
		}
	}
	metrics += "\n"

	metrics += "# HELP pgfox_pool_connections_active Connections currently checked out\n"
	metrics += "# TYPE pgfox_pool_connections_active gauge\n"
	for targetName, targetMap := range ws.pooler.targets {
		for dbName, dbMap := range targetMap {
			for userName, pool := range dbMap {
				metrics += fmt.Sprintf(
					"pgfox_pool_connections_active{target=%q,database=%q,user=%q} %d\n",
					targetName, dbName, userName, pool.activeConnections())
			}
		}
	}
	metrics += "\n"

	metrics += "# HELP pgfox_pool_connections_idle Connections currently idle in pool\n"
	metrics += "# TYPE pgfox_pool_connections_idle gauge\n"
	for targetName, targetMap := range ws.pooler.targets {
		for dbName, dbMap := range targetMap {
			for userName, pool := range dbMap {
				metrics += fmt.Sprintf(
					"pgfox_pool_connections_idle{target=%q,database=%q,user=%q} %d\n",
					targetName, dbName, userName, pool.idleConnections())
			}
		}
	}
	metrics += "\n"

	metrics += "# HELP pgfox_pool_queries_total Total queries executed on pool\n"
	metrics += "# TYPE pgfox_pool_queries_total counter\n"
	for targetName, targetMap := range ws.pooler.targets {
		for dbName, dbMap := range targetMap {
			for userName, pool := range dbMap {
				metrics += fmt.Sprintf(
					"pgfox_pool_queries_total{target=%q,database=%q,user=%q} %d\n",
					targetName, dbName, userName, pool.queriesExecuted())
			}
		}
	}
	metrics += "\n"

	metrics += "# HELP pgfox_pool_errors_total Total errors on pool\n"
	metrics += "# TYPE pgfox_pool_errors_total counter\n"
	for targetName, targetMap := range ws.pooler.targets {
		for dbName, dbMap := range targetMap {
			for userName, pool := range dbMap {
				metrics += fmt.Sprintf(
					"pgfox_pool_errors_total{target=%q,database=%q,user=%q} %d\n",
					targetName, dbName, userName, pool.errorCount())
			}
		}
	}
	metrics += "\n"

	ws.pooler.targetsMu.RUnlock()

	// Listener metrics.
	listenerStats := ws.pooler.getListenerStats()
	if len(listenerStats) > 0 {
		metrics += "# HELP pgfox_listeners Number of clients listening on channel\n"
		metrics += "# TYPE pgfox_listeners gauge\n"
		for channel, count := range listenerStats {
			metrics += fmt.Sprintf("pgfox_listeners{channel=%q} %d\n", channel, count)
		}
		metrics += "\n"
	}

	c.String(http.StatusOK, metrics)
}
