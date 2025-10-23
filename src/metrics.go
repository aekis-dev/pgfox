package main

import (
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/gin-gonic/gin"
)

// handleMetrics handles the /metrics endpoint in Prometheus format
func (ws *WebServer) handleMetrics(c *gin.Context) {
	c.Header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	stats := ws.pooler.Stats()

	// Global metrics
	metrics := ""

	// Total clients
	metrics += "# HELP pgfox_clients_total Total number of client connections\n"
	metrics += "# TYPE pgfox_clients_total counter\n"
	metrics += fmt.Sprintf("pgfox_clients_total %d\n", stats.TotalClients)
	metrics += "\n"

	// Active clients
	metrics += "# HELP pgfox_clients_active Current active client connections\n"
	metrics += "# TYPE pgfox_clients_active gauge\n"
	metrics += fmt.Sprintf("pgfox_clients_active %d\n", stats.ActiveClients)
	metrics += "\n"

	// Total database pools
	metrics += "# HELP pgfox_database_pools Total number of database pools (target/database/user combinations)\n"
	metrics += "# TYPE pgfox_database_pools gauge\n"
	metrics += fmt.Sprintf("pgfox_database_pools %d\n", stats.TotalDatabases)
	metrics += "\n"

	// Total queries
	metrics += "# HELP pgfox_queries_total Total number of queries executed\n"
	metrics += "# TYPE pgfox_queries_total counter\n"
	metrics += fmt.Sprintf("pgfox_queries_total %d\n", stats.TotalQueries)
	metrics += "\n"

	// Notifications sent
	metrics += "# HELP pgfox_notifications_sent_total Total number of notifications sent\n"
	metrics += "# TYPE pgfox_notifications_sent_total counter\n"
	metrics += fmt.Sprintf("pgfox_notifications_sent_total %d\n", stats.NotificationsSent)
	metrics += "\n"

	// Idle connections closed
	metrics += "# HELP pgfox_idle_connections_closed_total Total number of idle connections closed\n"
	metrics += "# TYPE pgfox_idle_connections_closed_total counter\n"
	metrics += fmt.Sprintf("pgfox_idle_connections_closed_total %d\n", stats.IdleConnectionsClosed)
	metrics += "\n"

	// Per-database metrics
	ws.pooler.targetsMu.RLock()

	// Connection metrics
	metrics += "# HELP pgfox_db_connections_total Total connections created for database pool\n"
	metrics += "# TYPE pgfox_db_connections_total counter\n"
	for targetName, targetMap := range ws.pooler.targets {
		for dbName, dbMap := range targetMap {
			for userName, dbManager := range dbMap {
				metrics += fmt.Sprintf("pgfox_db_connections_total{target=\"%s\",database=\"%s\",user=\"%s\"} %d\n",
					targetName, dbName, userName, atomic.LoadInt64(&dbManager.stats.TotalConnections))
			}
		}
	}
	metrics += "\n"

	metrics += "# HELP pgfox_db_connections_active Current active connections in use\n"
	metrics += "# TYPE pgfox_db_connections_active gauge\n"
	for targetName, targetMap := range ws.pooler.targets {
		for dbName, dbMap := range targetMap {
			for userName, dbManager := range dbMap {
				metrics += fmt.Sprintf("pgfox_db_connections_active{target=\"%s\",database=\"%s\",user=\"%s\"} %d\n",
					targetName, dbName, userName, atomic.LoadInt64(&dbManager.stats.ActiveConnections))
			}
		}
	}
	metrics += "\n"

	metrics += "# HELP pgfox_db_connections_idle Current idle connections in pool\n"
	metrics += "# TYPE pgfox_db_connections_idle gauge\n"
	for targetName, targetMap := range ws.pooler.targets {
		for dbName, dbMap := range targetMap {
			for userName, dbManager := range dbMap {
				metrics += fmt.Sprintf("pgfox_db_connections_idle{target=\"%s\",database=\"%s\",user=\"%s\"} %d\n",
					targetName, dbName, userName, atomic.LoadInt64(&dbManager.stats.IdleConnections))
			}
		}
	}
	metrics += "\n"

	metrics += "# HELP pgfox_db_queries_total Total queries executed on database pool\n"
	metrics += "# TYPE pgfox_db_queries_total counter\n"
	for targetName, targetMap := range ws.pooler.targets {
		for dbName, dbMap := range targetMap {
			for userName, dbManager := range dbMap {
				metrics += fmt.Sprintf("pgfox_db_queries_total{target=\"%s\",database=\"%s\",user=\"%s\"} %d\n",
					targetName, dbName, userName, atomic.LoadInt64(&dbManager.stats.QueriesExecuted))
			}
		}
	}
	metrics += "\n"

	metrics += "# HELP pgfox_db_errors_total Total errors on database pool\n"
	metrics += "# TYPE pgfox_db_errors_total counter\n"
	for targetName, targetMap := range ws.pooler.targets {
		for dbName, dbMap := range targetMap {
			for userName, dbManager := range dbMap {
				metrics += fmt.Sprintf("pgfox_db_errors_total{target=\"%s\",database=\"%s\",user=\"%s\"} %d\n",
					targetName, dbName, userName, atomic.LoadInt64(&dbManager.stats.ErrorCount))
			}
		}
	}
	metrics += "\n"

	metrics += "# HELP pgfox_db_bytes_received_total Total bytes received from database\n"
	metrics += "# TYPE pgfox_db_bytes_received_total counter\n"
	for targetName, targetMap := range ws.pooler.targets {
		for dbName, dbMap := range targetMap {
			for userName, dbManager := range dbMap {
				metrics += fmt.Sprintf("pgfox_db_bytes_received_total{target=\"%s\",database=\"%s\",user=\"%s\"} %d\n",
					targetName, dbName, userName, atomic.LoadInt64(&dbManager.stats.BytesReceived))
			}
		}
	}
	metrics += "\n"

	metrics += "# HELP pgfox_db_bytes_sent_total Total bytes sent to database\n"
	metrics += "# TYPE pgfox_db_bytes_sent_total counter\n"
	for targetName, targetMap := range ws.pooler.targets {
		for dbName, dbMap := range targetMap {
			for userName, dbManager := range dbMap {
				metrics += fmt.Sprintf("pgfox_db_bytes_sent_total{target=\"%s\",database=\"%s\",user=\"%s\"} %d\n",
					targetName, dbName, userName, atomic.LoadInt64(&dbManager.stats.BytesSent))
			}
		}
	}
	metrics += "\n"

	ws.pooler.targetsMu.RUnlock()

	// Listener metrics
	listenerStats := ws.pooler.getListenerStats()
	if len(listenerStats) > 0 {
		metrics += "# HELP pgfox_listeners Number of clients listening on channel\n"
		metrics += "# TYPE pgfox_listeners gauge\n"
		for channel, count := range listenerStats {
			metrics += fmt.Sprintf("pgfox_listeners{channel=\"%s\"} %d\n", channel, count)
		}
		metrics += "\n"
	}

	c.String(http.StatusOK, metrics)
}
