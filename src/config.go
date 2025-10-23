package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the main configuration structure
type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Targets []Target      `yaml:"targets"`
	Logging LoggingConfig `yaml:"logging"`
	Metrics MetricsConfig `yaml:"metrics"`
}

// ServerConfig contains server-level configuration
type ServerConfig struct {
	ListenAddr     string        `yaml:"listen_addr"`
	MaxConnections int           `yaml:"max_connections"`
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
	IdleTimeout    time.Duration `yaml:"idle_timeout"`

	// SSL Configuration
	SSLCertFile string `yaml:"ssl_cert_file"`
	SSLKeyFile  string `yaml:"ssl_key_file"`
	SSLCAFile   string `yaml:"ssl_ca_file"`
}

// Target represents a PostgreSQL server that can serve one or more databases
// If IncludeDatabases is specified, only those databases are accessible
// If IncludeDatabases is empty, all databases are accessible (wildcard mode)
type Target struct {
	Name           string            `yaml:"name"`
	Host           string            `yaml:"host"`
	Port           int               `yaml:"port"`
	SSLMode        string            `yaml:"ssl_mode"`
	SSLCAFile      string            `yaml:"ssl_ca_file"`
	MaxConnections int               `yaml:"max_connections"`
	ConnectTimeout time.Duration     `yaml:"connect_timeout"`
	Parameters     map[string]string `yaml:"parameters"`

	// Database filtering (both optional)
	IncludeDatabases []string `yaml:"include_databases"` // If specified, ONLY these databases
	ExcludeDatabases []string `yaml:"exclude_databases"` // Exclude these databases

	// Priority for multi-target scenarios (lower = higher priority)
	Priority int `yaml:"priority"`
}

// LoggingConfig contains logging configuration
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	File   string `yaml:"file"`
}

// MetricsConfig contains metrics configuration
type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Port    int    `yaml:"port"`
	Path    string `yaml:"path"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Expand environment variables
	expandedData := os.ExpandEnv(string(data))

	var config Config
	if err := yaml.Unmarshal([]byte(expandedData), &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Set defaults
	if err := config.setDefaults(); err != nil {
		return nil, fmt.Errorf("failed to set defaults: %w", err)
	}

	// Validate configuration
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

// setDefaults sets default values for configuration
func (c *Config) setDefaults() error {
	// Server defaults
	if c.Server.ListenAddr == "" {
		c.Server.ListenAddr = ":5432"
	}
	if c.Server.MaxConnections == 0 {
		c.Server.MaxConnections = 100
	}
	if c.Server.ConnectTimeout == 0 {
		c.Server.ConnectTimeout = 10 * time.Second
	}
	if c.Server.IdleTimeout == 0 {
		c.Server.IdleTimeout = 10 * time.Minute
	}

	// Target defaults
	for i := range c.Targets {
		target := &c.Targets[i]
		if target.Port == 0 {
			target.Port = 5432
		}
		if target.MaxConnections == 0 {
			target.MaxConnections = 20
		}
		if target.ConnectTimeout == 0 {
			target.ConnectTimeout = c.Server.ConnectTimeout
		}
		if target.SSLMode == "" {
			target.SSLMode = "prefer"
		}
		// Default exclude databases
		if len(target.ExcludeDatabases) == 0 && len(target.IncludeDatabases) == 0 {
			target.ExcludeDatabases = []string{"template0", "template1"}
		}
	}

	// Logging defaults
	if c.Logging.Level == "" {
		c.Logging.Level = "info"
	}
	if c.Logging.Format == "" {
		c.Logging.Format = "text"
	}

	// Metrics defaults
	if c.Metrics.Port == 0 {
		c.Metrics.Port = 4502
	}
	if c.Metrics.Path == "" {
		c.Metrics.Path = "/metrics"
	}

	return nil
}

// validate validates the configuration
func (c *Config) validate() error {
	if len(c.Targets) == 0 {
		return fmt.Errorf("no targets configured")
	}

	// Validate targets
	targetNames := make(map[string]bool)
	for i, target := range c.Targets {
		if target.Name == "" {
			return fmt.Errorf("targets[%d]: name is required", i)
		}
		if target.Host == "" {
			return fmt.Errorf("targets[%d]: host is required", i)
		}

		// Check for duplicate names
		if targetNames[target.Name] {
			return fmt.Errorf("targets[%d]: duplicate target name: %s", i, target.Name)
		}
		targetNames[target.Name] = true
	}

	return nil
}
