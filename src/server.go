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
	pooler *WildcardPooler

	// Global metrics
	clientsTotal          *prometheus.Desc
	clientsActive         *prometheus.Desc
	databasePools         *prometheus.Desc
	queriesTotal          *prometheus.Desc
	notificationsSent     *prometheus.Desc
	idleConnectionsClosed *prometheus.Desc

	// Per-database metrics
	dbConnectionsTotal  *prometheus.Desc
	dbConnectionsActive *prometheus.Desc
	dbConnectionsIdle   *prometheus.Desc
	dbQueriesTotal      *prometheus.Desc
	dbErrorsTotal       *prometheus.Desc
	dbBytesReceived     *prometheus.Desc
	dbBytesSent         *prometheus.Desc
}

// NewMetricsCollector creates a new Prometheus collector
func NewMetricsCollector(pooler *WildcardPooler) *MetricsCollector {
	return &MetricsCollector{
		pooler: pooler,

		// Global metrics
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
		databasePools: prometheus.NewDesc(
			"pgfox_database_pools",
			"Total number of database pools",
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
			"Total number of idle connections closed",
			nil, nil,
		),

		// Per-database metrics with labels
		dbConnectionsTotal: prometheus.NewDesc(
			"pgfox_db_connections_total",
			"Total connections created for database pool",
			[]string{"target", "database", "user"}, nil,
		),
		dbConnectionsActive: prometheus.NewDesc(
			"pgfox_db_connections_active",
			"Current active connections in use",
			[]string{"target", "database", "user"}, nil,
		),
		dbConnectionsIdle: prometheus.NewDesc(
			"pgfox_db_connections_idle",
			"Current idle connections in pool",
			[]string{"target", "database", "user"}, nil,
		),
		dbQueriesTotal: prometheus.NewDesc(
			"pgfox_db_queries_total",
			"Total queries executed on database pool",
			[]string{"target", "database", "user"}, nil,
		),
		dbErrorsTotal: prometheus.NewDesc(
			"pgfox_db_errors_total",
			"Total errors on database pool",
			[]string{"target", "database", "user"}, nil,
		),
		dbBytesReceived: prometheus.NewDesc(
			"pgfox_db_bytes_received_total",
			"Total bytes received from database",
			[]string{"target", "database", "user"}, nil,
		),
		dbBytesSent: prometheus.NewDesc(
			"pgfox_db_bytes_sent_total",
			"Total bytes sent to database",
			[]string{"target", "database", "user"}, nil,
		),
	}
}

// Describe sends the descriptors to the channel
func (mc *MetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- mc.clientsTotal
	ch <- mc.clientsActive
	ch <- mc.databasePools
	ch <- mc.queriesTotal
	ch <- mc.notificationsSent
	ch <- mc.idleConnectionsClosed
	ch <- mc.dbConnectionsTotal
	ch <- mc.dbConnectionsActive
	ch <- mc.dbConnectionsIdle
	ch <- mc.dbQueriesTotal
	ch <- mc.dbErrorsTotal
	ch <- mc.dbBytesReceived
	ch <- mc.dbBytesSent
}

// Collect collects the current metrics
func (mc *MetricsCollector) Collect(ch chan<- prometheus.Metric) {
	stats := mc.pooler.Stats()

	// Global metrics
	ch <- prometheus.MustNewConstMetric(
		mc.clientsTotal,
		prometheus.CounterValue,
		float64(stats.TotalClients),
	)
	ch <- prometheus.MustNewConstMetric(
		mc.clientsActive,
		prometheus.GaugeValue,
		float64(stats.ActiveClients),
	)
	ch <- prometheus.MustNewConstMetric(
		mc.databasePools,
		prometheus.GaugeValue,
		float64(stats.TotalDatabases),
	)
	ch <- prometheus.MustNewConstMetric(
		mc.queriesTotal,
		prometheus.CounterValue,
		float64(stats.TotalQueries),
	)
	ch <- prometheus.MustNewConstMetric(
		mc.notificationsSent,
		prometheus.CounterValue,
		float64(stats.NotificationsSent),
	)
	ch <- prometheus.MustNewConstMetric(
		mc.idleConnectionsClosed,
		prometheus.CounterValue,
		float64(stats.IdleConnectionsClosed),
	)

	// Per-database metrics
	mc.pooler.targetsMu.RLock()
	for targetName, targetMap := range mc.pooler.targets {
		for dbName, dbMap := range targetMap {
			for userName, dbManager := range dbMap {
				labels := []string{targetName, dbName, userName}

				ch <- prometheus.MustNewConstMetric(
					mc.dbConnectionsTotal,
					prometheus.CounterValue,
					float64(dbManager.stats.TotalConnections),
					labels...,
				)
				ch <- prometheus.MustNewConstMetric(
					mc.dbConnectionsActive,
					prometheus.GaugeValue,
					float64(dbManager.stats.ActiveConnections),
					labels...,
				)
				ch <- prometheus.MustNewConstMetric(
					mc.dbConnectionsIdle,
					prometheus.GaugeValue,
					float64(dbManager.stats.IdleConnections),
					labels...,
				)
				ch <- prometheus.MustNewConstMetric(
					mc.dbQueriesTotal,
					prometheus.CounterValue,
					float64(dbManager.stats.QueriesExecuted),
					labels...,
				)
				ch <- prometheus.MustNewConstMetric(
					mc.dbErrorsTotal,
					prometheus.CounterValue,
					float64(dbManager.stats.ErrorCount),
					labels...,
				)
				ch <- prometheus.MustNewConstMetric(
					mc.dbBytesReceived,
					prometheus.CounterValue,
					float64(dbManager.stats.BytesReceived),
					labels...,
				)
				ch <- prometheus.MustNewConstMetric(
					mc.dbBytesSent,
					prometheus.CounterValue,
					float64(dbManager.stats.BytesSent),
					labels...,
				)
			}
		}
	}
	mc.pooler.targetsMu.RUnlock()
}

// WebServer provides HTTP endpoints
type WebServer struct {
	pooler   *WildcardPooler
	server   *http.Server
	router   *gin.Engine
	logger   *Logger
	registry *prometheus.Registry
}

// NewWebServer creates a new web server
func NewWebServer(pooler *WildcardPooler, addr string, logger *Logger) *WebServer {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	// Create custom registry
	registry := prometheus.NewRegistry()

	// Register our collector
	collector := NewMetricsCollector(pooler)
	registry.MustRegister(collector)

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

	// Use Prometheus handler with custom registry
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
