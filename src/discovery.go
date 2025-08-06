package main

import (
	"fmt"
	"net"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

// databaseDiscoveryWorker periodically discovers databases on wildcard targets
func (p *WildcardPooler) databaseDiscoveryWorker() {
	defer p.wg.Done()

	logger := p.logger.WithField("component", "discovery")
	logger.Info("Starting database discovery worker")

	ticker := time.NewTicker(p.config.AutoDiscovery.DatabaseQueryInterval)
	defer ticker.Stop()

	// Initial discovery
	p.discoverDatabases()

	for {
		select {
		case <-p.ctx.Done():
			logger.Info("Database discovery worker stopping")
			return
		case <-ticker.C:
			p.discoverDatabases()
		}
	}
}

// discoverDatabases discovers all databases on wildcard targets
func (p *WildcardPooler) discoverDatabases() {
	logger := p.logger.WithField("component", "discovery")
	logger.Debug("Starting database discovery scan")

	totalDiscovered := 0

	for _, target := range p.wildcardTargets {
		databases, err := p.queryDatabasesOnTarget(target)
		if err != nil {
			logger.WithError(err).Warn("Failed to discover databases on target", "target", target.Name)
			continue
		}

		targetLogger := logger.WithTarget(target.Name)
		targetLogger.Debug("Discovered databases", "count", len(databases))

		for _, dbInfo := range databases {
			// Check if database should be included
			if !p.shouldIncludeDatabase(target, dbInfo.DatabaseName) {
				continue
			}

			// Check if we already have this database
			key := fmt.Sprintf("%s:%s", target.Name, dbInfo.DatabaseName)

			p.databasesMu.RLock()
			_, existsStatic := p.staticDatabases[dbInfo.DatabaseName]
			_, existsDynamic := p.dynamicDatabases[key]
			p.databasesMu.RUnlock()

			if existsStatic || existsDynamic {
				// Update cache
				p.updateDiscoveryCache(key, dbInfo)
				continue
			}

			// Create new dynamic database pool
			if err := p.addDynamicDatabase(target, dbInfo.DatabaseName); err != nil {
				targetLogger.WithError(err).Warn("Failed to add dynamic database", "database", dbInfo.DatabaseName)
				continue
			}

			atomic.AddInt64(&p.stats.DatabasesDiscovered, 1)
			atomic.AddInt64(&p.stats.DatabasesCreated, 1)
			totalDiscovered++
			targetLogger.Info("Discovered and added database", "database", dbInfo.DatabaseName)
		}
	}

	if totalDiscovered > 0 {
		logger.Info("Database discovery completed", "new_databases", totalDiscovered)
	} else {
		logger.Debug("Database discovery completed", "new_databases", 0)
	}
}

// queryDatabasesOnTarget queries for databases on a specific target using raw TCP
func (p *WildcardPooler) queryDatabasesOnTarget(target *WildcardTarget) ([]*DatabaseDiscoveryInfo, error) {
	logger := p.logger.WithTarget(target.Name)
	logger.Debug("Querying databases on target")

	// Create TCP connection to target
	addr := fmt.Sprintf("%s:%d", target.Host, target.Port)
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	defer conn.Close()

	// Set timeout for the entire operation
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	// Create backend connection wrapper
	backend := NewBackendConnection(conn, "postgres", target.Name)
	defer backend.Close()

	// Send startup message to connect to postgres database
	startupMsg := buildStartupMessage(target.AdminUser, "postgres")
	if _, err := backend.writer.Write(startupMsg); err != nil {
		return nil, fmt.Errorf("failed to send startup message: %w", err)
	}
	if err := backend.writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush startup message: %w", err)
	}

	// Handle authentication
	if err := p.handleBackendAuth(backend, target.AdminUser, target.AdminPassword); err != nil {
		return nil, fmt.Errorf("authentication failed: %w", err)
	}

	// Send discovery query
	query := strings.TrimSpace(p.config.AutoDiscovery.DiscoveryQuery)
	if query == "" {
		query = `SELECT datname, pg_database_size(datname) as size, pg_get_userbyid(datdba) as owner FROM pg_database WHERE datallowconn = true AND datname NOT IN ('template0', 'template1')`
	}

	queryMsg := []byte(query + "\x00")
	if err := backend.WriteMessage('Q', queryMsg); err != nil {
		return nil, fmt.Errorf("failed to send query: %w", err)
	}

	// Read response
	var databases []*DatabaseDiscoveryInfo
	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}

		switch msgType {
		case 'T': // Row description
			// Skip row description for now
			continue
		case 'D': // Data row
			dbInfo := p.parseDataRow(body, target)
			if dbInfo != nil {
				databases = append(databases, dbInfo)
			}
		case 'C': // Command complete
			continue
		case 'Z': // Ready for query
			return databases, nil
		case 'E': // Error response
			return nil, fmt.Errorf("query error: %s", string(body))
		}
	}
}

// handleBackendAuth handles authentication with the backend
func (p *WildcardPooler) handleBackendAuth(backend *BackendConnection, user, password string) error {
	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			return fmt.Errorf("failed to read auth response: %w", err)
		}

		switch msgType {
		case 'R': // Authentication
			if len(body) < 4 {
				return fmt.Errorf("invalid authentication response")
			}

			authType := uint32(body[0])<<24 | uint32(body[1])<<16 | uint32(body[2])<<8 | uint32(body[3])

			switch authType {
			case AuthenticationOK:
				continue
			case AuthenticationMD5:
				if len(body) < 8 {
					return fmt.Errorf("invalid MD5 auth response")
				}
				salt := body[4:8]
				response := buildMD5Response(user, password, salt)
				passMsg := []byte(response + "\x00")
				if err := backend.WriteMessage('p', passMsg); err != nil {
					return fmt.Errorf("failed to send password: %w", err)
				}
			case AuthenticationSASL:
				// Handle SCRAM-SHA-256 authentication
				if len(body) < 4 {
					return fmt.Errorf("invalid SASL auth response")
				}
				saslData := body[4:] // Skip the auth type (first 4 bytes)
				return handleSCRAMAuth(backend, user, password, saslData)
			default:
				return fmt.Errorf("unsupported authentication type: %d", authType)
			}

		case 'K': // Backend key data
			if len(body) >= 8 {
				processID := int32(body[0])<<24 | int32(body[1])<<16 | int32(body[2])<<8 | int32(body[3])
				secretKey := int32(body[4])<<24 | int32(body[5])<<16 | int32(body[6])<<8 | int32(body[7])
				backend.SetProcessID(processID)
				backend.SetSecretKey(secretKey)
			}

		case 'S': // Parameter status
			continue

		case 'Z': // Ready for query
			return nil // Authentication complete and ready for queries

		case 'E': // Error response
			errorMsg := parseErrorMessage(body)
			return fmt.Errorf("authentication failed: %s", errorMsg)
		}
	}
}

// parseDataRow parses a data row from the discovery query
func (p *WildcardPooler) parseDataRow(body []byte, target *WildcardTarget) *DatabaseDiscoveryInfo {
	if len(body) < 2 {
		return nil
	}

	// Skip field count (2 bytes)
	pos := 2

	// Parse database name (first field)
	if pos+4 > len(body) {
		return nil
	}

	nameLen := int(body[pos])<<24 | int(body[pos+1])<<16 | int(body[pos+2])<<8 | int(body[pos+3])
	pos += 4

	if nameLen < 0 || pos+nameLen > len(body) {
		return nil
	}

	dbName := string(body[pos : pos+nameLen])
	pos += nameLen

	// For simplicity, we'll just parse the database name
	// In a full implementation, you'd parse all fields (size, owner)

	return &DatabaseDiscoveryInfo{
		DatabaseName: dbName,
		Exists:       true,
		Owner:        "unknown", // Would be parsed from query result
		Size:         0,         // Would be parsed from query result
		LastChecked:  time.Now(),
		Target:       target,
	}
}

// shouldIncludeDatabase checks if a database should be included based on filters
func (p *WildcardPooler) shouldIncludeDatabase(target *WildcardTarget, dbName string) bool {
	filters := target.DatabaseFilters

	// Check exclude list first
	for _, excludeDB := range filters.ExcludeDatabases {
		if dbName == excludeDB {
			return false
		}
	}

	// Check include list (if specified, only these are allowed)
	if len(filters.IncludeDatabases) > 0 {
		found := false
		for _, includeDB := range filters.IncludeDatabases {
			if dbName == includeDB {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check exclude patterns
	for _, pattern := range filters.ExcludePatterns {
		if matched, err := regexp.MatchString(pattern, dbName); err == nil && matched {
			return false
		}
	}

	// Check include patterns (if specified, at least one must match)
	if len(filters.IncludePatterns) > 0 {
		found := false
		for _, pattern := range filters.IncludePatterns {
			if matched, err := regexp.MatchString(pattern, dbName); err == nil && matched {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// addDynamicDatabase adds a dynamically discovered database
func (p *WildcardPooler) addDynamicDatabase(target *WildcardTarget, dbName string) error {
	// Create database config from wildcard target
	config := DatabaseConfig{
		Name:           dbName,
		Host:           target.Host,
		Port:           target.Port,
		User:           target.DefaultUser,
		Password:       target.DefaultPassword,
		SSLMode:        target.SSLMode,
		MaxConnections: target.MaxConnectionsPerDB,
		MinConnections: target.MinConnectionsPerDB,
		PoolMode:       target.PoolMode,
		ConnectTimeout: target.ConnectTimeout,
		Parameters:     target.Parameters,
		HealthCheck:    target.HealthCheck,
	}

	dbManager := &DatabaseManager{
		config:         config,
		wildcardTarget: target,
		isStatic:       false,
		lastUsed:       time.Now(),
		backendPool:    make(chan *BackendConnection, config.MaxConnections),
	}

	if err := dbManager.initializeConnections(); err != nil {
		return fmt.Errorf("failed to initialize connections for %s: %w", dbName, err)
	}

	if target.HealthCheck.Enabled {
		healthChecker, err := NewHealthChecker(config, p.logger.WithDatabase(dbName))
		if err != nil {
			p.logger.WithError(err).Warn("Failed to create health checker", "database", dbName)
		} else {
			dbManager.healthChecker = healthChecker
			go healthChecker.Start(p.ctx)
		}
	}

	key := fmt.Sprintf("%s:%s", target.Name, dbName)

	p.databasesMu.Lock()
	p.dynamicDatabases[key] = dbManager
	p.databasesMu.Unlock()

	atomic.AddInt64(&p.stats.DynamicDatabases, 1)
	return nil
}

// cleanupUnusedPoolsWorker removes unused dynamic database pools
func (p *WildcardPooler) cleanupUnusedPoolsWorker() {
	defer p.wg.Done()

	logger := p.logger.WithField("component", "cleanup")
	logger.Info("Starting cleanup worker for unused pools")

	ticker := time.NewTicker(p.config.AutoDiscovery.UnusedPoolTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			logger.Info("Cleanup worker stopping")
			return
		case <-ticker.C:
			p.cleanupUnusedPools()
		}
	}
}

// cleanupUnusedPools removes dynamic pools that haven't been used recently
func (p *WildcardPooler) cleanupUnusedPools() {
	if !p.config.AutoDiscovery.RemoveUnusedPools {
		return
	}

	logger := p.logger.WithField("component", "cleanup")
	cutoff := time.Now().Add(-p.config.AutoDiscovery.UnusedPoolTimeout)

	p.databasesMu.Lock()
	defer p.databasesMu.Unlock()

	removedCount := 0
	for key, dbManager := range p.dynamicDatabases {
		dbManager.mu.RLock()
		lastUsed := dbManager.lastUsed
		dbManager.mu.RUnlock()

		if lastUsed.Before(cutoff) {
			// Close health checker
			if dbManager.healthChecker != nil {
				dbManager.healthChecker.Stop()
			}

			// Close all backend connections
			close(dbManager.backendPool)
			for conn := range dbManager.backendPool {
				conn.Close()
			}

			delete(p.dynamicDatabases, key)
			atomic.AddInt64(&p.stats.DynamicDatabases, -1)
			atomic.AddInt64(&p.stats.DatabasesRemoved, 1)
			removedCount++

			logger.Info("Removed unused dynamic database pool", "key", key, "last_used", lastUsed)
		}
	}

	if removedCount > 0 {
		logger.Info("Cleanup completed", "removed_pools", removedCount)
	} else {
		logger.Debug("Cleanup completed", "removed_pools", 0)
	}
}

// checkDatabaseExists checks if a database exists on a target using raw TCP
func (p *WildcardPooler) checkDatabaseExists(target *WildcardTarget, dbName string) (bool, error) {
	logger := p.logger.WithTarget(target.Name).WithDatabase(dbName)
	logger.Debug("Checking database existence")

	// Check cache first
	key := fmt.Sprintf("%s:%s", target.Name, dbName)

	if p.config.AutoDiscovery.CacheDiscoveredDBs {
		p.discoveryCacheMu.RLock()
		if info, exists := p.discoveryCache[key]; exists {
			if time.Since(info.LastChecked) < p.config.AutoDiscovery.CacheTTL {
				p.discoveryCacheMu.RUnlock()
				logger.Debug("Found in cache", "exists", info.Exists)
				return info.Exists, nil
			}
		}
		p.discoveryCacheMu.RUnlock()
	}

	// Create TCP connection
	addr := fmt.Sprintf("%s:%d", target.Host, target.Port)
	logger.Debug("Connecting to target", "addr", addr)

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return false, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(10 * time.Second))

	// Create backend connection wrapper
	backend := NewBackendConnection(conn, "postgres", target.Name)
	defer backend.Close()

	// Send startup message
	startupMsg := buildStartupMessage(target.AdminUser, "postgres")
	if _, err := backend.writer.Write(startupMsg); err != nil {
		return false, fmt.Errorf("failed to send startup message: %w", err)
	}
	if err := backend.writer.Flush(); err != nil {
		return false, fmt.Errorf("failed to flush startup message: %w", err)
	}

	// Handle authentication
	logger.Debug("Authenticating with backend")
	if err := p.handleBackendAuth(backend, target.AdminUser, target.AdminPassword); err != nil {
		return false, fmt.Errorf("authentication failed: %w", err)
	}

	logger.Debug("Authentication complete, preparing to send query")

	// Query for database existence
	query := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = '%s' AND datallowconn = true)", dbName)
	logger.Debug("Executing existence query", "query", query)

	queryMsg := []byte(query + "\x00")
	if err := backend.WriteMessage('Q', queryMsg); err != nil {
		return false, fmt.Errorf("failed to send query: %w", err)
	}

	logger.Debug("Query sent, waiting for response")

	// Read response and parse the boolean result
	exists := false
	queryResponseStarted := false

	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			return false, fmt.Errorf("failed to read response: %w", err)
		}

		logger.Debug("Received message", "type", string(msgType), "body_len", len(body))

		switch msgType {
		case 'T': // Row description - indicates query response started
			logger.Debug("Received row description - query response started")
			queryResponseStarted = true
			continue
		case 'D': // Data row
			if !queryResponseStarted {
				logger.Debug("Ignoring data row before query response started")
				continue
			}
			logger.Debug("Received data row", "body", fmt.Sprintf("%x", body))
			if len(body) >= 7 {
				// Parse the boolean value from the EXISTS query
				// Format: field_count(2) + field_length(4) + field_data(1)
				// So the actual boolean value is at position 6
				exists = body[6] == 't' // PostgreSQL boolean true is 't' (0x74)
				logger.Debug("Parsed existence result", "exists", exists, "value_byte", fmt.Sprintf("%x", body[6]))
			}
		case 'C': // Command complete
			if !queryResponseStarted {
				logger.Debug("Ignoring command complete before query response started")
				continue
			}
			logger.Debug("Received command complete")
			continue
		case 'Z': // Ready for query
			if queryResponseStarted {
				logger.Debug("Ready for query - query complete")
				goto done
			} else {
				logger.Debug("Ready for query - but query response not started yet, continuing")
				continue
			}
		case 'S': // Parameter status (leftover from auth)
			logger.Debug("Ignoring parameter status message")
			continue
		case 'K': // Backend key data (leftover from auth)
			logger.Debug("Ignoring backend key data message")
			continue
		case 'R': // Authentication (leftover from auth)
			logger.Debug("Ignoring authentication message")
			continue
		case 'E': // Error
			errorMsg := parseErrorMessage(body)
			return false, fmt.Errorf("query error: %s", errorMsg)
		default:
			logger.Debug("Ignoring unexpected message type", "type", string(msgType))
			continue
		}
	}

done:
	// Update cache
	if p.config.AutoDiscovery.CacheDiscoveredDBs {
		info := &DatabaseDiscoveryInfo{
			DatabaseName: dbName,
			Exists:       exists,
			LastChecked:  time.Now(),
			Target:       target,
		}
		p.updateDiscoveryCache(key, info)
	}

	logger.Debug("Database existence check complete", "exists", exists)
	return exists, nil
}

// updateDiscoveryCache updates the discovery cache
func (p *WildcardPooler) updateDiscoveryCache(key string, info *DatabaseDiscoveryInfo) {
	if !p.config.AutoDiscovery.CacheDiscoveredDBs {
		return
	}

	p.discoveryCacheMu.Lock()
	p.discoveryCache[key] = info
	p.discoveryCacheMu.Unlock()
}

// clearDiscoveryCache clears expired entries from the discovery cache
func (p *WildcardPooler) clearDiscoveryCache() {
	if !p.config.AutoDiscovery.CacheDiscoveredDBs {
		return
	}

	cutoff := time.Now().Add(-p.config.AutoDiscovery.CacheTTL)

	p.discoveryCacheMu.Lock()
	defer p.discoveryCacheMu.Unlock()

	for key, info := range p.discoveryCache {
		if info.LastChecked.Before(cutoff) {
			delete(p.discoveryCache, key)
		}
	}
}

// getDiscoveredDatabases returns all discovered databases
func (p *WildcardPooler) getDiscoveredDatabases() map[string]*DatabaseDiscoveryInfo {
	p.discoveryCacheMu.RLock()
	defer p.discoveryCacheMu.RUnlock()

	result := make(map[string]*DatabaseDiscoveryInfo)
	for key, info := range p.discoveryCache {
		if info.Exists {
			result[key] = info
		}
	}

	return result
}

// getDiscoveryStats returns discovery statistics
func (p *WildcardPooler) getDiscoveryStats() map[string]interface{} {
	p.discoveryCacheMu.RLock()
	cacheSize := len(p.discoveryCache)
	p.discoveryCacheMu.RUnlock()

	return map[string]interface{}{
		"cache_enabled":        p.config.AutoDiscovery.CacheDiscoveredDBs,
		"cache_size":           cacheSize,
		"cache_ttl":            p.config.AutoDiscovery.CacheTTL,
		"query_interval":       p.config.AutoDiscovery.DatabaseQueryInterval,
		"on_demand_enabled":    p.config.AutoDiscovery.CreatePoolsOnDemand,
		"unused_timeout":       p.config.AutoDiscovery.UnusedPoolTimeout,
		"databases_discovered": atomic.LoadInt64(&p.stats.DatabasesDiscovered),
		"databases_created":    atomic.LoadInt64(&p.stats.DatabasesCreated),
		"databases_removed":    atomic.LoadInt64(&p.stats.DatabasesRemoved),
	}
}
