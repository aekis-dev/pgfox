package main

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsCollector implements prometheus.Collector
type MetricsCollector struct {
	pooler *Server

	// Global metrics
	clientsTotal          *prometheus.Desc
	clientsActive         *prometheus.Desc
	pools                 *prometheus.Desc
	queriesTotal          *prometheus.Desc
	notificationsSent     *prometheus.Desc
	idleConnectionsClosed *prometheus.Desc

	// Per-pool metrics
	poolConnectionsTotal  *prometheus.Desc
	poolConnectionsActive *prometheus.Desc
	poolConnectionsIdle   *prometheus.Desc
	poolQueriesTotal      *prometheus.Desc
	poolErrorsTotal       *prometheus.Desc
}

// NewMetricsCollector creates a new Prometheus collector.
func NewMetricsCollector(pooler *Server) *MetricsCollector {
	labels := []string{"target", "database", "user"}

	return &MetricsCollector{
		pooler: pooler,

		clientsTotal: prometheus.NewDesc(
			"pgfox_clients_total",
			"Total number of client connections",
			nil, nil,
		),
		clientsActive: prometheus.NewDesc(
			"pgfox_clients_active",
			"Current active client connections",
			nil, nil,
		),
		pools: prometheus.NewDesc(
			"pgfox_pools",
			"Total number of pools (target/database/user combinations)",
			nil, nil,
		),
		queriesTotal: prometheus.NewDesc(
			"pgfox_queries_total",
			"Total number of queries executed",
			nil, nil,
		),
		notificationsSent: prometheus.NewDesc(
			"pgfox_notifications_sent_total",
			"Total number of notifications sent",
			nil, nil,
		),
		idleConnectionsClosed: prometheus.NewDesc(
			"pgfox_idle_connections_closed_total",
			"Total number of idle connections closed by health checks",
			nil, nil,
		),

		poolConnectionsTotal: prometheus.NewDesc(
			"pgfox_pool_connections_total",
			"Total connections currently open in the pool",
			labels, nil,
		),
		poolConnectionsActive: prometheus.NewDesc(
			"pgfox_pool_connections_active",
			"Connections currently checked out from the pool",
			labels, nil,
		),
		poolConnectionsIdle: prometheus.NewDesc(
			"pgfox_pool_connections_idle",
			"Connections currently idle in the pool",
			labels, nil,
		),
		poolQueriesTotal: prometheus.NewDesc(
			"pgfox_pool_queries_total",
			"Total queries executed on this pool",
			labels, nil,
		),
		poolErrorsTotal: prometheus.NewDesc(
			"pgfox_pool_errors_total",
			"Total errors on this pool",
			labels, nil,
		),
	}
}

// Describe sends the descriptors to the channel.
func (mc *MetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- mc.clientsTotal
	ch <- mc.clientsActive
	ch <- mc.pools
	ch <- mc.queriesTotal
	ch <- mc.notificationsSent
	ch <- mc.idleConnectionsClosed
	ch <- mc.poolConnectionsTotal
	ch <- mc.poolConnectionsActive
	ch <- mc.poolConnectionsIdle
	ch <- mc.poolQueriesTotal
	ch <- mc.poolErrorsTotal
}

// Collect collects the current metrics.
func (mc *MetricsCollector) Collect(ch chan<- prometheus.Metric) {
	stats := mc.pooler.Stats()

	ch <- prometheus.MustNewConstMetric(mc.clientsTotal, prometheus.CounterValue, float64(stats.TotalClients))
	ch <- prometheus.MustNewConstMetric(mc.clientsActive, prometheus.GaugeValue, float64(stats.ActiveClients))
	ch <- prometheus.MustNewConstMetric(mc.pools, prometheus.GaugeValue, float64(stats.TotalPools))
	ch <- prometheus.MustNewConstMetric(mc.queriesTotal, prometheus.CounterValue, float64(stats.TotalQueries))
	ch <- prometheus.MustNewConstMetric(mc.notificationsSent, prometheus.CounterValue, float64(stats.NotificationsSent))
	ch <- prometheus.MustNewConstMetric(mc.idleConnectionsClosed, prometheus.CounterValue, float64(stats.IdleConnectionsClosed))

	mc.pooler.targetsMu.RLock()
	for targetName, targetMap := range mc.pooler.targets {
		for dbName, dbMap := range targetMap {
			for userName, pool := range dbMap {
				labels := []string{targetName, dbName, userName}

				ch <- prometheus.MustNewConstMetric(mc.poolConnectionsTotal, prometheus.GaugeValue,
					float64(pool.totalConnections()), labels...)
				ch <- prometheus.MustNewConstMetric(mc.poolConnectionsActive, prometheus.GaugeValue,
					float64(pool.activeConnections()), labels...)
				ch <- prometheus.MustNewConstMetric(mc.poolConnectionsIdle, prometheus.GaugeValue,
					float64(pool.idleConnections()), labels...)
				ch <- prometheus.MustNewConstMetric(mc.poolQueriesTotal, prometheus.CounterValue,
					float64(pool.queriesExecuted()), labels...)
				ch <- prometheus.MustNewConstMetric(mc.poolErrorsTotal, prometheus.CounterValue,
					float64(pool.errorCount()), labels...)
			}
		}
	}
	mc.pooler.targetsMu.RUnlock()
}

// WebServer provides HTTP endpoints.
type WebServer struct {
	pooler   *Server
	server   *http.Server
	router   *gin.Engine
	logger   *Logger
	registry *prometheus.Registry
}

// NewWebServer creates a new web server.
func NewWebServer(pooler *Server, addr string, logger *Logger) *WebServer {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	registry := prometheus.NewRegistry()
	registry.MustRegister(NewMetricsCollector(pooler))

	ws := &WebServer{
		pooler:   pooler,
		router:   router,
		logger:   logger.WithField("component", "webserver"),
		registry: registry,
		server: &http.Server{
			Addr:    addr,
			Handler: router,
		},
	}

	ws.registerRoutes()
	return ws
}

func (ws *WebServer) registerRoutes() {
	ws.router.GET("/", ws.handleIndex)
	ws.router.GET("/metrics", gin.WrapH(promhttp.HandlerFor(ws.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})))
}

func (ws *WebServer) Start(ctx context.Context) error {
	ws.logger.Info("Starting web server", "addr", ws.server.Addr)

	go func() {
		if err := ws.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			ws.logger.WithError(err).Error("Web server failed")
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ws.logger.Info("Shutting down web server")
	return ws.server.Shutdown(shutdownCtx)
}

func (ws *WebServer) handleIndex(c *gin.Context) {
	c.Header("Content-Type", "text/html; charset=utf-8")
	html := `<!DOCTYPE html>
<html>
<head>
    <title>PgFox - PostgreSQL Connection Pooler</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
        h1 { color: #2c3e50; }
        .info { background: #ecf0f1; padding: 15px; border-radius: 5px; margin: 20px 0; }
        .endpoints { list-style: none; padding: 0; }
        .endpoints li { padding: 10px; margin: 5px 0; background: #3498db; color: white; border-radius: 3px; }
        .endpoints a { color: white; text-decoration: none; }
        .endpoints a:hover { text-decoration: underline; }
    </style>
</head>
<body>
    <h1>🦊 PgFox</h1>
    <div class="info">
        <p><strong>Version:</strong> ` + Version + `</p>
        <p><strong>Description:</strong> PostgreSQL Connection Pooler with Wildcard Database Support</p>
    </div>
    <h2>Available Endpoints</h2>
    <ul class="endpoints">
        <li><a href="/metrics">/metrics</a> - Prometheus metrics</li>
    </ul>
</body>
</html>`
	c.String(http.StatusOK, html)
}
