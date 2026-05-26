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

	// Create server
	server, err := NewServer(*config, logger)
	if err != nil {
		logger.Fatal("Failed to create server", "error", err)
	}

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Handle reload signal separately.
	reloadChan := make(chan os.Signal, 1)
	signal.Notify(reloadChan, syscall.SIGHUP)

	go func() {
		sig := <-sigChan
		logger.Info("Received shutdown signal", "signal", sig)
		cancel()
	}()

	go func() {
		for range reloadChan {
			logger.Info("Received SIGHUP, reloading config")
			newConfig, err := LoadConfig(*configPath)
			if err != nil {
				logger.WithError(err).Error("Config reload failed, keeping current config")
				continue
			}
			server.reload(*newConfig)
		}
	}()

	if err := server.Start(ctx); err != nil {
		logger.Fatal("Server failed to start", "error", err)
	}

	logger.Info("PgFox shutdown complete")
}
