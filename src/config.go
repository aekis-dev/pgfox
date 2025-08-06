package main

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the main configuration structure
type Config struct {
	Server          ServerConfig              `yaml:"server"`
	Databases       map[string]DatabaseConfig `yaml:"databases"`
	WildcardTargets []WildcardTarget          `yaml:"wildcard_targets"`
	AutoDiscovery   AutoDiscoveryConfig       `yaml:"auto_discovery"`
	Logging         LoggingConfig             `yaml:"logging"`
	Metrics         MetricsConfig             `yaml:"metrics"`
}

// ServerConfig contains server-level configuration
type ServerConfig struct {
	ListenAddr        string        `yaml:"listen_addr"`
	MaxConnections    int           `yaml:"max_connections"`
	DefaultPoolMode   string        `yaml:"default_pool_mode"`
	ConnectionTimeout time.Duration `yaml:"connection_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
}

// DatabaseConfig contains database-specific configuration
type DatabaseConfig struct {
	Name           string            `yaml:"name"`
	Host           string            `yaml:"host"`
	Port           int               `yaml:"port"`
	User           string            `yaml:"user"`
	Password       string            `yaml:"password"`
	SSLMode        string            `yaml:"ssl_mode"`
	MaxConnections int               `yaml:"max_connections"`
	MinConnections int               `yaml:"min_connections"`
	PoolMode       string            `yaml:"pool_mode"`
	ConnectTimeout time.Duration     `yaml:"connect_timeout"`
	Parameters     map[string]string `yaml:"parameters"`
	HealthCheck    HealthCheckConfig `yaml:"health_check"`
}

// WildcardTarget defines a PostgreSQL server that can serve any database
type WildcardTarget struct {
	Name                string               `yaml:"name"`
	Host                string               `yaml:"host"`
	Port                int                  `yaml:"port"`
	AdminUser           string               `yaml:"admin_user"`
	AdminPassword       string               `yaml:"admin_password"`
	DefaultUser         string               `yaml:"default_user"`
	DefaultPassword     string               `yaml:"default_password"`
	SSLMode             string               `yaml:"ssl_mode"`
	MaxConnectionsPerDB int                  `yaml:"max_connections_per_db"`
	MinConnectionsPerDB int                  `yaml:"min_connections_per_db"`
	PoolMode            string               `yaml:"pool_mode"`
	ConnectTimeout      time.Duration        `yaml:"connect_timeout"`
	Parameters          map[string]string    `yaml:"parameters"`
	HealthCheck         HealthCheckConfig    `yaml:"health_check"`
	UserMappings        []UserMappingRule    `yaml:"user_mappings"`
	DatabaseFilters     DatabaseFilterConfig `yaml:"database_filters"`
	Priority            int                  `yaml:"priority"`
}

// UserMappingRule defines how to map client users to database users
type UserMappingRule struct {
	ClientUser     string `yaml:"client_user"`
	DatabaseUser   string `yaml:"database_user"`
	DatabasePass   string `yaml:"database_password"`
	DatabaseFilter string `yaml:"database_filter"`
}

// DatabaseFilterConfig defines which databases to include/exclude
type DatabaseFilterConfig struct {
	IncludePatterns  []string `yaml:"include_patterns"`
	ExcludePatterns  []string `yaml:"exclude_patterns"`
	ExcludeDatabases []string `yaml:"exclude_databases"`
	IncludeDatabases []string `yaml:"include_databases"`
}

// AutoDiscoveryConfig contains auto-discovery configuration
type AutoDiscoveryConfig struct {
	Enabled               bool          `yaml:"enabled"`
	DatabaseQueryInterval time.Duration `yaml:"database_query_interval"`
	CreatePoolsOnDemand   bool          `yaml:"create_pools_on_demand"`
	RemoveUnusedPools     bool          `yaml:"remove_unused_pools"`
	UnusedPoolTimeout     time.Duration `yaml:"unused_pool_timeout"`
	DiscoveryQuery        string        `yaml:"discovery_query"`
	CacheDiscoveredDBs    bool          `yaml:"cache_discovered_dbs"`
	CacheTTL              time.Duration `yaml:"cache_ttl"`
}

// HealthCheckConfig contains health check settings
type HealthCheckConfig struct {
	Enabled  bool          `yaml:"enabled"`
	Interval time.Duration `yaml:"interval"`
	Timeout  time.Duration `yaml:"timeout"`
	Query    string        `yaml:"query"`
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
	if c.Server.DefaultPoolMode == "" {
		c.Server.DefaultPoolMode = "transaction"
	}
	if c.Server.ConnectionTimeout == 0 {
		c.Server.ConnectionTimeout = 30 * time.Second
	}
	if c.Server.IdleTimeout == 0 {
		c.Server.IdleTimeout = 10 * time.Minute
	}

	// Auto discovery defaults
	if c.AutoDiscovery.DatabaseQueryInterval == 0 {
		c.AutoDiscovery.DatabaseQueryInterval = 60 * time.Second
	}
	if c.AutoDiscovery.UnusedPoolTimeout == 0 {
		c.AutoDiscovery.UnusedPoolTimeout = 30 * time.Minute
	}
	if c.AutoDiscovery.DiscoveryQuery == "" {
		c.AutoDiscovery.DiscoveryQuery = `
			SELECT datname, pg_database_size(datname) as size, 
			       pg_get_userbyid(datdba) as owner
			FROM pg_database 
			WHERE datallowconn = true 
			  AND datname NOT IN ('template0', 'template1')
		`
	}
	if c.AutoDiscovery.CacheTTL == 0 {
		c.AutoDiscovery.CacheTTL = 5 * time.Minute
	}

	// Wildcard target defaults
	for i := range c.WildcardTargets {
		target := &c.WildcardTargets[i]
		if target.Port == 0 {
			target.Port = 5432
		}
		if target.MaxConnectionsPerDB == 0 {
			target.MaxConnectionsPerDB = 20
		}
		if target.MinConnectionsPerDB == 0 {
			target.MinConnectionsPerDB = 2
		}
		if target.PoolMode == "" {
			target.PoolMode = c.Server.DefaultPoolMode
		}
		if target.ConnectTimeout == 0 {
			target.ConnectTimeout = 10 * time.Second
		}
		if target.SSLMode == "" {
			target.SSLMode = "prefer"
		}
		if target.HealthCheck.Interval == 0 {
			target.HealthCheck.Interval = 30 * time.Second
		}
		if target.HealthCheck.Timeout == 0 {
			target.HealthCheck.Timeout = 5 * time.Second
		}
		if target.HealthCheck.Query == "" {
			target.HealthCheck.Query = "SELECT 1"
		}
	}

	// Static database defaults
	for name, db := range c.Databases {
		if db.Port == 0 {
			db.Port = 5432
		}
		if db.MaxConnections == 0 {
			db.MaxConnections = 20
		}
		if db.MinConnections == 0 {
			db.MinConnections = 2
		}
		if db.PoolMode == "" {
			db.PoolMode = c.Server.DefaultPoolMode
		}
		if db.ConnectTimeout == 0 {
			db.ConnectTimeout = 10 * time.Second
		}
		if db.SSLMode == "" {
			db.SSLMode = "prefer"
		}
		if db.HealthCheck.Interval == 0 {
			db.HealthCheck.Interval = 30 * time.Second
		}
		if db.HealthCheck.Timeout == 0 {
			db.HealthCheck.Timeout = 5 * time.Second
		}
		if db.HealthCheck.Query == "" {
			db.HealthCheck.Query = "SELECT 1"
		}
		c.Databases[name] = db
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
		c.Metrics.Port = 9090
	}
	if c.Metrics.Path == "" {
		c.Metrics.Path = "/metrics"
	}

	return nil
}

// validate validates the configuration
func (c *Config) validate() error {
	if len(c.Databases) == 0 && len(c.WildcardTargets) == 0 {
		return fmt.Errorf("no databases or wildcard targets configured")
	}

	validPoolModes := map[string]bool{
		"session":     true,
		"transaction": true,
		"statement":   true,
	}

	if !validPoolModes[c.Server.DefaultPoolMode] {
		return fmt.Errorf("invalid default pool mode: %s", c.Server.DefaultPoolMode)
	}

	// Validate wildcard targets
	for i, target := range c.WildcardTargets {
		if target.Name == "" {
			return fmt.Errorf("wildcard_targets[%d]: name is required", i)
		}
		if target.Host == "" {
			return fmt.Errorf("wildcard_targets[%d]: host is required", i)
		}
		if target.AdminUser == "" {
			return fmt.Errorf("wildcard_targets[%d]: admin_user is required for database discovery", i)
		}
		if !validPoolModes[target.PoolMode] {
			return fmt.Errorf("wildcard_targets[%d]: invalid pool mode: %s", i, target.PoolMode)
		}

		// Validate regex patterns
		for _, pattern := range target.DatabaseFilters.IncludePatterns {
			if _, err := regexp.Compile(pattern); err != nil {
				return fmt.Errorf("wildcard_targets[%d]: invalid include pattern %s: %w", i, pattern, err)
			}
		}
		for _, pattern := range target.DatabaseFilters.ExcludePatterns {
			if _, err := regexp.Compile(pattern); err != nil {
				return fmt.Errorf("wildcard_targets[%d]: invalid exclude pattern %s: %w", i, pattern, err)
			}
		}
	}

	// Validate static databases
	for name, db := range c.Databases {
		if db.Host == "" {
			return fmt.Errorf("database %s: host is required", name)
		}
		if db.User == "" {
			return fmt.Errorf("database %s: user is required", name)
		}
		if !validPoolModes[db.PoolMode] {
			return fmt.Errorf("database %s: invalid pool mode: %s", name, db.PoolMode)
		}
	}

	return nil
}
