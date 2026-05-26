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

	// QueryTimeout is the maximum time allowed for a single query response to
	// complete. Set to 0 to disable — recommended when clients may upload large
	// binary attachments via the extended query protocol.
	QueryTimeout time.Duration `yaml:"query_timeout"`

	// MaxMessageSize is the maximum allowed size of a single PostgreSQL protocol
	// message in bytes. Must be large enough for binary attachments sent via Bind
	// messages. Default: 256 MB.
	MaxMessageSize int `yaml:"max_message_size"`

	// PgFoxDir is the base directory for PgFox runtime files.
	// Bootstrap files (generated automatically if missing):
	//   {pgfox_dir}/ca.crt          CA certificate
	//   {pgfox_dir}/ca.key          CA private key
	//   {pgfox_dir}/server.crt      PostgreSQL server cert (copy to $PGDATA)
	//   {pgfox_dir}/server.key      PostgreSQL server key  (copy to $PGDATA)
	//   {pgfox_dir}/pgfox.crt       PgFox client-facing TLS cert (CN=Hostname)
	//   {pgfox_dir}/pgfox.key       PgFox client-facing TLS key
	// Runtime certs (generated on demand):
	//   {pgfox_dir}/certs/{role}.crt   Per-role backend client certs
	//   {pgfox_dir}/certs/{role}.key
	PgFoxDir string `yaml:"pgfox_dir"`

	// Hostname is the CN used for the PgFox client-facing TLS certificate
	// ({pgfox_dir}/pgfox.crt). Must match the hostname or IP clients use to
	// connect to PgFox, otherwise TLS verification will fail.
	// Default: "localhost"
	Hostname string `yaml:"hostname"`

	// PgFoxRole is the PostgreSQL role used for the privileged connection.
	// Its certificate is generated automatically at {pgfox_dir}/certs/{pgfox_role}.crt.
	// This role must have pg_read_all_auth_data (PG14+) or superuser privilege.
	PgFoxRole string `yaml:"pgfox_role"`

	// StatsInterval is how often the target goroutine queries pg_stat_activity
	// to update the available connection slots on the PostgreSQL server.
	// Default: 10s
	StatsInterval time.Duration `yaml:"stats_interval"`

	// PeakWindow is the rolling window over which peak active connections are
	// tracked per pool for smart shrink decisions.
	// Default: 5m
	PeakWindow time.Duration `yaml:"peak_window"`

	// Certs contains certificate generation settings for user certificates.
	Certs CertsConfig `yaml:"certs"`
}

// CertsConfig contains settings for dynamically generated user certificates.
type CertsConfig struct {
	// TTL is how long generated user certificates are valid.
	// Certificates are renewed automatically when they expire.
	TTL time.Duration `yaml:"ttl"`

	// Subject fields for generated certificates.
	// CN is always set to the PostgreSQL username — not configurable.
	Organization       string `yaml:"organization"`
	OrganizationalUnit string `yaml:"organizational_unit"`
	Country            string `yaml:"country"`
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

// LoadConfig loads configuration from a YAML file.
// Defaults are applied first via defaultConfig(); only fields present in the
// file are overwritten — same pattern as the dfne project.
func LoadConfig(configPath string) (*Config, error) {
	config := Config{
		Server: ServerConfig{
			ListenAddr:     ":5432",
			MaxConnections: 20,
			ConnectTimeout: 10 * time.Second,
			IdleTimeout:    10 * time.Minute,
			QueryTimeout:   0, // disabled — no fixed deadline on query execution
			MaxMessageSize: 256 * 1024 * 1024,
			StatsInterval:  10 * time.Second,
			PeakWindow:     5 * time.Minute,
			PgFoxDir:       "/etc/pgfox",
			Hostname:       "localhost",
			PgFoxRole:      "postgres",
			Certs: CertsConfig{
				TTL:                2160 * time.Hour, // 90 days
				Organization:       "PgFox",
				OrganizationalUnit: "PostgreSQL Users",
				Country:            "US",
			},
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "text",
		},
		Metrics: MetricsConfig{
			Enabled: false,
			Port:    4502,
			Path:    "/metrics",
		},
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal([]byte(os.ExpandEnv(string(data))), &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Per-target defaults that depend on other config values and cannot be
	// expressed as struct literals.
	for i := range config.Targets {
		t := &config.Targets[i]
		if t.Port == 0 {
			t.Port = 5432
		}
		if t.MaxConnections == 0 {
			t.MaxConnections = 20
		}
		if t.ConnectTimeout == 0 {
			t.ConnectTimeout = config.Server.ConnectTimeout
		}
		if len(t.ExcludeDatabases) == 0 && len(t.IncludeDatabases) == 0 {
			t.ExcludeDatabases = []string{"template0", "template1"}
		}
	}

	return &config, config.validate()
}

// validate checks that required fields are present and consistent.
func (c *Config) validate() error {
	if len(c.Targets) == 0 {
		return fmt.Errorf("no targets configured")
	}

	seen := make(map[string]bool)
	for i, t := range c.Targets {
		if t.Name == "" {
			return fmt.Errorf("targets[%d]: name is required", i)
		}
		if t.Host == "" {
			return fmt.Errorf("targets[%d]: host is required", i)
		}
		if seen[t.Name] {
			return fmt.Errorf("targets[%d]: duplicate target name: %s", i, t.Name)
		}
		seen[t.Name] = true
	}

	return nil
}
