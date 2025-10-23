package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const Version = "1.0.0"

func main() {
	var (
		configPath = flag.String("config", "config.yaml", "Configuration file path")
		version    = flag.Bool("version", false, "Show version information")
		debug      = flag.Bool("debug", false, "Enable debug logging")
	)
	flag.Parse()

	if *version {
		fmt.Printf("PgFox %s\n", Version)
		fmt.Printf("PostgreSQL Connection Pooler with Wildcard Database Support\n")
		os.Exit(0)
	}

	// Load configuration
	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Override debug setting if specified
	if *debug {
		config.Logging.Level = "debug"
	}

	// Initialize logger
	logger := NewLogger(config.Logging)
	logger.Info("Starting PgFox", "version", Version, "config", *configPath)

	// Create pooler
	pooler, err := NewWildcardPooler(*config, logger)
	if err != nil {
		logger.Fatal("Failed to create pooler", "error", err)
	}

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	go func() {
		sig := <-sigChan
		logger.Info("Received shutdown signal", "signal", sig)
		cancel()
	}()

	// Start the pooler
	logger.Info("Starting PostgreSQL connection pooler",
		"listen_addr", config.Server.ListenAddr,
		"targets", len(config.Targets),
	)

	if err := pooler.Start(ctx); err != nil {
		logger.Fatal("Pooler failed to start", "error", err)
	}

	logger.Info("PgFox shutdown complete")
}
