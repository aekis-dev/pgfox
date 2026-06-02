package web

import (
	"fmt"
	"net/http"

	"github.com/aekis-dev/pgfox/pkg/pgfox"
	//"github.com/aekis-dev/pgfox/pkg/pgfox"
	"github.com/gin-gonic/gin"
)

// handleMetrics handles the /metrics endpoint in Prometheus format.
func (ws *WebServer) handleMetrics(c *gin.Context) {
	c.Header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	stats := ws.pooler.GlobalStats

	metrics := ""

	metrics += "# HELP pgfox_clients_total Total number of client connections\n"
	metrics += "# TYPE pgfox_clients_total counter\n"
	metrics += fmt.Sprintf("pgfox_clients_total %d\n\n", stats.TotalClients)

	metrics += "# HELP pgfox_clients_active Current active client connections\n"
	metrics += "# TYPE pgfox_clients_active gauge\n"
	metrics += fmt.Sprintf("pgfox_clients_active %d\n\n", stats.ActiveClients)

	metrics += "# HELP pgfox_pools Total number of pools\n"
	metrics += "# TYPE pgfox_pools gauge\n"
	metrics += fmt.Sprintf("pgfox_pools %d\n\n", stats.TotalPools)

	metrics += "# HELP pgfox_queries_total Total number of queries executed\n"
	metrics += "# TYPE pgfox_queries_total counter\n"
	metrics += fmt.Sprintf("pgfox_queries_total %d\n\n", stats.TotalQueries)

	metrics += "# HELP pgfox_notifications_sent_total Total number of notifications sent\n"
	metrics += "# TYPE pgfox_notifications_sent_total counter\n"
	metrics += fmt.Sprintf("pgfox_notifications_sent_total %d\n\n", stats.NotificationsSent)

	metrics += "# HELP pgfox_idle_connections_closed_total Total idle connections closed\n"
	metrics += "# TYPE pgfox_idle_connections_closed_total counter\n"
	metrics += fmt.Sprintf("pgfox_idle_connections_closed_total %d\n\n", stats.IdleConnectionsClosed)

	// Per-target metrics.
	for _, target := range ws.pooler.Targets {
		metrics += "# HELP pgfox_target_connections_total Total open connections on target\n"
		metrics += "# TYPE pgfox_target_connections_total gauge\n"
		metrics += fmt.Sprintf("pgfox_target_connections_total{target=%q} %d\n\n",
			target.Name, target.TotalOpen)

		metrics += "# HELP pgfox_target_server_max_connections PostgreSQL max_connections\n"
		metrics += "# TYPE pgfox_target_server_max_connections gauge\n"
		metrics += fmt.Sprintf("pgfox_target_server_max_connections{target=%q} %d\n\n",
			target.Name, target.ServerMaxConns)

		metrics += "# HELP pgfox_target_server_open_connections PostgreSQL open connections\n"
		metrics += "# TYPE pgfox_target_server_open_connections gauge\n"
		metrics += fmt.Sprintf("pgfox_target_server_open_connections{target=%q} %d\n\n",
			target.Name, target.ServerOpenConns)

		// pools is now a sync.Map; Range is the safe iteration API.
		target.Pools.Range(func(_, v any) bool {
			pool := v.(*pgfox.Pool)
			labels := fmt.Sprintf("target=%q,database=%q,user=%q", target.Name, pool.DbName, pool.Username)

			metrics += fmt.Sprintf("pgfox_pool_connections_total{%s} %d\n", labels, pool.TotalConnections())
			metrics += fmt.Sprintf("pgfox_pool_connections_active{%s} %d\n", labels, pool.ActiveConnections())
			metrics += fmt.Sprintf("pgfox_pool_connections_idle{%s} %d\n", labels, pool.IdleConnections())
			metrics += fmt.Sprintf("pgfox_pool_queries_total{%s} %d\n", labels, pool.QueriesExecuted())
			metrics += fmt.Sprintf("pgfox_pool_errors_total{%s} %d\n", labels, pool.ErrorCount())
			return true
		})
	}
	metrics += "\n"

	// Listener metrics.
	listenerStats := ws.pooler.GetListenerStats()
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
