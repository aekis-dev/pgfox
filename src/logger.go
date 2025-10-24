package main

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// Logger provides structured logging interface
type Logger struct {
	*slog.Logger
}

// NewLogger creates a new structured logger
func NewLogger(config LoggingConfig) *Logger {
	var handler slog.Handler
	var level slog.Level

	// Parse log level
	switch strings.ToLower(config.Level) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}

	// Determine output destination - default to stdout
	output := os.Stdout
	if config.File != "" {
		file, err := os.OpenFile(config.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open log file %s: %v, using stdout\n", config.File, err)
		} else {
			output = file
		}
	}

	// Create handler based on format - default to JSON for containers
	format := strings.ToLower(config.Format)
	if format == "" {
		format = "json" // Default to JSON for better container log parsing
	}

	switch format {
	case "json":
		handler = slog.NewJSONHandler(output, opts)
	case "text":
		handler = slog.NewTextHandler(output, opts)
	default:
		handler = slog.NewTextHandler(output, opts)
	}

	return &Logger{
		Logger: slog.New(handler),
	}
}

// WithFields adds fields to the logger
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	args := make([]interface{}, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}
	return &Logger{
		Logger: l.Logger.With(args...),
	}
}

// WithField adds a single field to the logger
func (l *Logger) WithField(key string, value interface{}) *Logger {
	return &Logger{
		Logger: l.Logger.With(key, value),
	}
}

// WithError adds an error field to the logger
func (l *Logger) WithError(err error) *Logger {
	return l.WithField("error", err)
}

// WithDatabase adds database context to the logger
func (l *Logger) WithDatabase(database string) *Logger {
	return l.WithField("database", database)
}

// WithUser adds user context to the logger
func (l *Logger) WithUser(user string) *Logger {
	return l.WithField("user", user)
}

// WithClient adds client context to the logger
func (l *Logger) WithClient(clientAddr string) *Logger {
	return l.WithField("client", clientAddr)
}

// WithTarget adds target context to the logger
func (l *Logger) WithTarget(target string) *Logger {
	return l.WithField("target", target)
}

// Fatal logs a fatal error and exits
func (l *Logger) Fatal(msg string, args ...interface{}) {
	l.Error(msg, args...)
	os.Exit(1)
}
