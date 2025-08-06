package main

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

// handleClientMessage handles messages from clients
func (p *WildcardPooler) handleClientMessage(client *ClientConnection) error {
	msgType, body, err := client.ReadMessage()
	if err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("failed to read client message: %w", err)
	}

	logger := client.Logger().WithField("msg_type", string(msgType)).WithField("body_len", len(body))
	logger.Debug("Received client message")

	switch msgType {
	case Query:
		query := string(body)
		if len(query) > 0 && query[len(query)-1] == 0 {
			query = query[:len(query)-1] // Remove null terminator
		}

		// Submit to worker pool for async execution
		return p.workerPool.SubmitQuery(client, query)

	case Parse, Bind, Execute, Sync:
		// Submit to worker pool for async execution
		return p.workerPool.SubmitExtendedQuery(client, msgType, body)

	case Terminate:
		logger.Debug("Client terminating connection")
		return io.EOF

	default:
		logger.Debug("Forwarding unhandled message type to worker pool")
		return p.workerPool.SubmitExtendedQuery(client, msgType, body)
	}
}

// handleQuery handles a query from the client
func (p *WildcardPooler) executeQuery(client *ClientConnection, query string) error {
	logger := client.Logger().WithField("query", query).WithField("worker", "async")

	// Check if this is a LISTEN/UNLISTEN/NOTIFY command
	queryUpper := strings.ToUpper(strings.TrimSpace(query))

	if strings.HasPrefix(queryUpper, "LISTEN ") {
		return p.handleListen(client, query)
	} else if strings.HasPrefix(queryUpper, "UNLISTEN") {
		return p.handleUnlisten(client, query)
	} else if strings.HasPrefix(queryUpper, "NOTIFY ") {
		return p.handleNotify(client, query)
	} else if p.containsPgNotify(queryUpper) {
		logger.Info("Detected pg_notify function call", "query", query)
		return p.handleNotify(client, query)
	}

	// Determine transaction state changes BEFORE getting backend
	isTransactionStart := strings.HasPrefix(queryUpper, "BEGIN") || strings.HasPrefix(queryUpper, "START TRANSACTION")
	isTransactionEnd := strings.HasPrefix(queryUpper, "COMMIT") || strings.HasPrefix(queryUpper, "ROLLBACK")

	// Get or create backend connection based on pool mode
	backend, shouldRelease, err := p.getBackendForClient(client)
	if err != nil {
		logger.WithError(err).Error("Failed to get backend connection")
		return sendErrorResponse(client, "FATAL", "53300", "too many connections")
	}

	// Set read timeout for this query operation
	if conn, ok := backend.conn.(net.Conn); ok {
		deadline := time.Now().Add(30 * time.Second)
		conn.SetReadDeadline(deadline)
		defer conn.SetReadDeadline(time.Time{})
	}

	// Forward query to backend
	if err := p.forwardQueryToBackend(backend, query); err != nil {
		logger.WithError(err).Error("Failed to forward query to backend")
		if shouldRelease {
			p.releaseBackendConnection(backend)
		}
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	// Forward complete response from backend to client
	if err := p.forwardCompleteBackendResponse(client, backend); err != nil {
		logger.WithError(err).Error("Failed to forward backend response")
		if netErr, ok := err.(*net.OpError); ok && netErr.Timeout() {
			if shouldRelease {
				p.releaseBackendConnection(backend)
			}
			return sendErrorResponse(client, "ERROR", "57014", "query timeout")
		}
		if shouldRelease {
			p.releaseBackendConnection(backend)
		}
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	// Update transaction state based on query AFTER successful execution
	p.updateTransactionState(client, query)

	// Update statistics
	if dbManager, _ := p.getDatabaseManager(client.GetDatabase(), client.GetUser()); dbManager != nil {
		atomic.AddInt64(&dbManager.stats.QueriesExecuted, 1)
	}
	atomic.AddInt64(&p.stats.TotalQueries, 1)

	// Connection release logic
	logger.Debug("Determining connection release strategy",
		"should_release", shouldRelease,
		"is_transaction_start", isTransactionStart,
		"is_transaction_end", isTransactionEnd,
		"client_in_transaction", client.IsInTransaction(),
		"client_is_listening", client.IsListening())

	if shouldRelease {
		if isTransactionStart {
			logger.Debug("Transaction started - converting pooled connection to dedicated")
			client.SetBackendConnection(backend)
			backend.SetClientRef(client)
		} else {
			logger.Debug("Releasing pooled backend connection after query")
			p.releaseBackendConnection(backend)
		}
	} else {
		if isTransactionEnd {
			if !client.IsListening() {
				logger.Debug("Transaction ended - releasing dedicated connection to pool")
				client.SetBackendConnection(nil)
				backend.SetClientRef(nil)
				p.releaseBackendConnection(backend)
			} else {
				logger.Debug("Transaction ended but client is listening - keeping dedicated connection")
			}
		} else {
			logger.Debug("Keeping dedicated backend connection")
		}
	}

	return nil
}

// getBackendForClient gets the appropriate backend connection for a client
func (p *WildcardPooler) getBackendForClient(client *ClientConnection) (*BackendConnection, bool, error) {
	logger := client.Logger()

	// Check what pool mode we're using for this database
	dbManager, err := p.getDatabaseManager(client.GetDatabase(), client.GetUser())
	if err != nil {
		return nil, false, err
	}

	poolMode := dbManager.config.PoolMode
	if poolMode == "" {
		poolMode = p.config.Server.DefaultPoolMode
	}

	logger.Debug("Determining backend connection strategy",
		"pool_mode", poolMode,
		"in_transaction", client.IsInTransaction(),
		"is_listening", client.IsListening(),
		"has_existing_backend", client.GetBackendConnection() != nil)

	// For session mode, always use dedicated connection
	if poolMode == "session" {
		backend := client.GetBackendConnection()
		if backend == nil {
			// Create new dedicated connection
			logger.Debug("Creating dedicated backend connection for session mode")
			backend, err = p.acquireBackendConnection(client.GetDatabase())
			if err != nil {
				return nil, false, err
			}
			client.SetBackendConnection(backend)
			backend.SetClientRef(client)
		}
		logger.Debug("Using dedicated backend connection", "backend_addr", backend.RemoteAddr())
		return backend, false, nil // Never release session mode connections
	}

	// For listening clients, always use dedicated connection regardless of pool mode
	if client.IsListening() {
		backend := client.GetBackendConnection()
		if backend == nil {
			logger.Debug("Creating dedicated backend connection for listening client")
			backend, err = p.acquireBackendConnection(client.GetDatabase())
			if err != nil {
				return nil, false, err
			}
			client.SetBackendConnection(backend)
			backend.SetClientRef(client)
		}
		logger.Debug("Using dedicated backend connection for listener")
		return backend, false, nil // Never release listener connections
	}

	// For transaction mode
	if poolMode == "transaction" {
		// If client is in a transaction, use dedicated connection
		if client.IsInTransaction() {
			backend := client.GetBackendConnection()
			if backend == nil {
				logger.Debug("Creating dedicated backend connection for active transaction")
				backend, err = p.acquireBackendConnection(client.GetDatabase())
				if err != nil {
					return nil, false, err
				}
				client.SetBackendConnection(backend)
				backend.SetClientRef(client)
			}
			logger.Debug("Using dedicated backend connection for transaction")
			return backend, false, nil // Don't release during transaction
		}

		// Outside of transactions, use pooled connections
		logger.Debug("Acquiring pooled backend connection for transaction mode (not in transaction)")
		backend, err := p.acquireBackendConnection(client.GetDatabase())
		if err != nil {
			return nil, false, err
		}
		return backend, true, nil // Release after use when not in transaction
	}

	// For statement mode, always use pooled connections
	logger.Debug("Acquiring pooled backend connection for statement mode")
	backend, err := p.acquireBackendConnection(client.GetDatabase())
	if err != nil {
		return nil, false, err
	}
	return backend, true, nil // Release after use
}

// executeExtendedQuery handles extended query protocol (Parse/Bind/Execute/Sync)
func (p *WildcardPooler) executeExtendedQuery(client *ClientConnection, msgType byte, body []byte) error {
	logger := client.Logger().WithField("msg_type", string(msgType)).WithField("worker", "async")

	// Get backend connection
	backend := client.GetBackendConnection()
	if backend == nil {
		logger.Error("No backend connection available for extended query")
		return sendErrorResponse(client, "FATAL", "08003", "connection does not exist")
	}

	// Set timeout for extended query operations
	if conn, ok := backend.conn.(net.Conn); ok {
		deadline := time.Now().Add(30 * time.Second)
		conn.SetReadDeadline(deadline)
		defer conn.SetReadDeadline(time.Time{})
	}

	// Forward message to backend
	if err := backend.WriteMessage(msgType, body); err != nil {
		logger.WithError(err).Error("Failed to forward extended query message")
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	// Handle responses based on message type
	switch msgType {
	case Parse:
		return p.forwardSingleResponse(client, backend, "ParseComplete")
	case Bind:
		return p.forwardSingleResponse(client, backend, "BindComplete")
	case Execute:
		return p.forwardExecuteResponse(client, backend)
	case Sync:
		return p.forwardUntilReady(client, backend)
	default:
		logger.Warn("Unknown extended query message type", "type", string(msgType))
		return p.forwardSingleResponse(client, backend, "Unknown")
	}
}

// executeUnknownMessage forwards unknown message types directly
func (p *WildcardPooler) executeUnknownMessage(client *ClientConnection, msgType byte, body []byte) error {
	logger := client.Logger().WithField("msg_type", string(msgType)).WithField("worker", "async")

	backend := client.GetBackendConnection()
	if backend == nil {
		logger.Error("No backend connection for unknown message")
		return sendErrorResponse(client, "FATAL", "08003", "connection does not exist")
	}

	// Set timeout
	if conn, ok := backend.conn.(net.Conn); ok {
		deadline := time.Now().Add(30 * time.Second)
		conn.SetReadDeadline(deadline)
		defer conn.SetReadDeadline(time.Time{})
	}

	// Forward the message as-is
	if err := backend.WriteMessage(msgType, body); err != nil {
		logger.WithError(err).Error("Failed to forward unknown message")
		return err
	}

	// Try to forward any response
	return p.forwardSingleResponse(client, backend, "Unknown")
}

// forwardSingleResponse forwards a single response message
func (p *WildcardPooler) forwardSingleResponse(client *ClientConnection, backend *BackendConnection, expectedType string) error {
	logger := client.Logger().WithField("expected", expectedType)

	msgType, body, err := backend.ReadMessage()
	if err != nil {
		if netErr, ok := err.(*net.OpError); ok && netErr.Timeout() {
			logger.WithError(err).Warn("Timeout reading single response")
			return sendErrorResponse(client, "ERROR", "57014", "query timeout")
		}
		logger.WithError(err).Error("Failed to read backend response")
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	// Handle notifications that arrive during single response
	if msgType == NotificationResponse { // 'A'
		logger.Info("Received notification during single response processing")
		p.handleNotificationResponse(body)
		// After handling notification, try to read the actual response
		return p.forwardSingleResponse(client, backend, expectedType)
	}

	if err := client.WriteMessage(msgType, body); err != nil {
		logger.WithError(err).Error("Failed to forward response to client")
		return err
	}

	return nil
}

// forwardExecuteResponse forwards Execute response which can be multiple messages
func (p *WildcardPooler) forwardExecuteResponse(client *ClientConnection, backend *BackendConnection) error {
	logger := client.Logger()

	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			if netErr, ok := err.(*net.OpError); ok && netErr.Timeout() {
				logger.WithError(err).Warn("Timeout reading execute response")
				return sendErrorResponse(client, "ERROR", "57014", "query timeout")
			}
			logger.WithError(err).Error("Failed to read execute response")
			return sendErrorResponse(client, "ERROR", "08006", "connection failure")
		}

		if err := client.WriteMessage(msgType, body); err != nil {
			logger.WithError(err).Error("Failed to forward execute response")
			return err
		}

		// Execute sequence ends with these message types
		switch msgType {
		case 'C': // CommandComplete
			return nil
		case 'I': // EmptyQueryResponse
			return nil
		case 'E': // ErrorResponse
			return nil
		case 's': // PortalSuspended
			return nil
		}
		// Continue forwarding other messages like RowDescription, DataRow, etc.
	}
}

// forwardUntilReady forwards messages until ReadyForQuery
func (p *WildcardPooler) forwardUntilReady(client *ClientConnection, backend *BackendConnection) error {
	logger := client.Logger()

	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			if netErr, ok := err.(*net.OpError); ok && netErr.Timeout() {
				logger.WithError(err).Warn("Timeout waiting for ready")
				return sendErrorResponse(client, "ERROR", "57014", "query timeout")
			}
			logger.WithError(err).Error("Failed to read response waiting for ready")
			return sendErrorResponse(client, "ERROR", "08006", "connection failure")
		}

		if err := client.WriteMessage(msgType, body); err != nil {
			logger.WithError(err).Error("Failed to forward message waiting for ready")
			return err
		}

		if msgType == 'Z' { // ReadyForQuery
			return nil
		}
	}
}

// forwardCompleteBackendResponse forwards a complete query response
func (p *WildcardPooler) forwardCompleteBackendResponse(client *ClientConnection, backend *BackendConnection) error {
	logger := client.Logger()

	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			if netErr, ok := err.(*net.OpError); ok && netErr.Timeout() {
				logger.WithError(err).Warn("Timeout reading complete response")
				return fmt.Errorf("query timeout")
			}
			logger.WithError(err).Error("Failed to read complete response")
			return err
		}

		if err := client.WriteMessage(msgType, body); err != nil {
			logger.WithError(err).Error("Failed to forward complete response")
			return err
		}

		// Query sequence ends with ReadyForQuery or ErrorResponse
		if msgType == 'Z' || msgType == 'E' {
			return nil
		}
	}
}

// containsPgNotify checks if a query contains a pg_notify function call
func (p *WildcardPooler) containsPgNotify(queryUpper string) bool {
	// Remove extra whitespace and normalize
	normalized := strings.ReplaceAll(queryUpper, " ", "")

	// Check for various forms of pg_notify:
	// - pg_notify(...)
	// - "pg_notify"(...)  (quoted function name)
	// - pg_notify (...)   (with space)
	patterns := []string{
		"PG_NOTIFY(",
		"\"PG_NOTIFY\"(",
		"'PG_NOTIFY'(",
		"`PG_NOTIFY`(",
	}

	for _, pattern := range patterns {
		if strings.Contains(normalized, pattern) {
			return true
		}
		// Also check with spaces
		spaced := strings.ReplaceAll(pattern, "(", " (")
		if strings.Contains(queryUpper, spaced) {
			return true
		}
	}

	return false
}

// updateTransactionState updates the client's transaction state based on the query
func (p *WildcardPooler) updateTransactionState(client *ClientConnection, query string) {
	queryUpper := strings.ToUpper(strings.TrimSpace(query))
	logger := client.Logger()

	// Check for transaction commands
	if strings.HasPrefix(queryUpper, "BEGIN") ||
		strings.HasPrefix(queryUpper, "START TRANSACTION") {

		client.SetInTransaction(true)
		logger.Debug("Transaction started")

	} else if strings.HasPrefix(queryUpper, "COMMIT") ||
		strings.HasPrefix(queryUpper, "ROLLBACK") {

		client.SetInTransaction(false)
		logger.Debug("Transaction ended")
	}
	// Note: We removed the automatic connection clearing here since it's now handled in handleQuery
}

// forwardQueryToBackend sends a query to the backend
func (p *WildcardPooler) forwardQueryToBackend(backend *BackendConnection, query string) error {
	queryMsg := []byte(query + "\x00") // Add null terminator
	return backend.WriteMessage('Q', queryMsg)
}

// forwardBackendResponse forwards the backend response to the client with proper message handling
func (p *WildcardPooler) forwardBackendResponse(client *ClientConnection, backend *BackendConnection) error {
	for {
		// Read message type
		msgType, err := backend.reader.ReadByte()
		if err != nil {
			return fmt.Errorf("failed to read backend message type: %w", err)
		}

		// Read message length
		length, err := readUint32(backend.reader)
		if err != nil {
			return fmt.Errorf("failed to read backend message length: %w", err)
		}

		// Calculate body length (length includes the length field itself)
		bodyLength := int(length - 4)
		if bodyLength < 0 {
			return fmt.Errorf("invalid message length: %d", length)
		}

		// Read message body
		body := make([]byte, bodyLength)
		if bodyLength > 0 {
			if _, err := io.ReadFull(backend.reader, body); err != nil {
				return fmt.Errorf("failed to read backend message body: %w", err)
			}
		}

		// Forward the complete message to client
		if err := client.WriteMessage(msgType, body); err != nil {
			return fmt.Errorf("failed to forward message to client: %w", err)
		}

		// Check if this is the end of the response
		if msgType == ReadyForQuery {
			return nil
		}
		if msgType == ErrorResponse {
			return nil
		}
	}
}

// acquireBackendConnection gets a backend connection for a specific database
func (p *WildcardPooler) acquireBackendConnection(dbName string) (*BackendConnection, error) {
	dbManager, err := p.getDatabaseManager(dbName, "")
	if err != nil {
		return nil, err
	}

	logger := p.logger.WithDatabase(dbName)

	// Log pool status before attempting acquisition
	totalConns := atomic.LoadInt64(&dbManager.stats.TotalConnections)
	activeConns := atomic.LoadInt64(&dbManager.stats.ActiveConnections)
	idleConns := atomic.LoadInt64(&dbManager.stats.IdleConnections)

	logger.Debug("Attempting to acquire backend connection",
		"total_connections", totalConns,
		"active_connections", activeConns,
		"idle_connections", idleConns,
		"max_connections", dbManager.config.MaxConnections,
		"pool_capacity", cap(dbManager.backendPool),
		"pool_size", len(dbManager.backendPool))

	select {
	case conn := <-dbManager.backendPool:
		conn.SetInUse(true)
		atomic.AddInt64(&dbManager.stats.ActiveConnections, 1)
		atomic.AddInt64(&dbManager.stats.IdleConnections, -1)
		logger.Debug("Successfully acquired pooled connection",
			"active_connections", atomic.LoadInt64(&dbManager.stats.ActiveConnections),
			"idle_connections", atomic.LoadInt64(&dbManager.stats.IdleConnections))
		return conn, nil
	default:
		// Try to create a new connection if we haven't reached the limit
		if totalConns < int64(dbManager.config.MaxConnections) {
			logger.Debug("Creating new backend connection",
				"current_total", totalConns,
				"max_allowed", dbManager.config.MaxConnections)

			conn, err := dbManager.createBackendConnection()
			if err != nil {
				logger.WithError(err).Error("Failed to create new backend connection")
				return nil, fmt.Errorf("failed to create new connection: %w", err)
			}

			conn.SetInUse(true)
			atomic.AddInt64(&dbManager.stats.TotalConnections, 1)
			atomic.AddInt64(&dbManager.stats.ActiveConnections, 1)
			logger.Debug("Successfully created new backend connection",
				"total_connections", atomic.LoadInt64(&dbManager.stats.TotalConnections),
				"active_connections", atomic.LoadInt64(&dbManager.stats.ActiveConnections))
			return conn, nil
		}

		logger.Error("Connection pool exhausted",
			"total_connections", totalConns,
			"max_connections", dbManager.config.MaxConnections,
			"active_connections", activeConns,
			"idle_connections", idleConns,
			"pool_size", len(dbManager.backendPool))
		return nil, fmt.Errorf("no available backend connections for database %s (pool exhausted: %d/%d active)",
			dbName, activeConns, dbManager.config.MaxConnections)
	}
}

func (p *WildcardPooler) getDatabaseManager(dbName, user string) (*DatabaseManager, error) {
	logger := p.logger.WithDatabase(dbName).WithUser(user)

	// First check static databases
	p.databasesMu.RLock()
	if dbManager, exists := p.staticDatabases[dbName]; exists {
		logger.Debug("Found static database manager")
		p.databasesMu.RUnlock()
		return dbManager, nil
	}
	p.databasesMu.RUnlock()

	// Check dynamic databases for each wildcard target
	for _, target := range p.wildcardTargets {
		key := fmt.Sprintf("%s:%s", target.Name, dbName)
		logger.Debug("Checking dynamic database key", "key", key)

		p.databasesMu.RLock()
		if dbManager, exists := p.dynamicDatabases[key]; exists {
			logger.Debug("Found dynamic database manager", "key", key)
			p.databasesMu.RUnlock()
			return dbManager, nil
		}
		p.databasesMu.RUnlock()
	}

	logger.Debug("No existing database manager found, checking if on-demand creation is enabled",
		"create_on_demand", p.config.AutoDiscovery.CreatePoolsOnDemand)

	// If on-demand creation is enabled, try to create the database pool
	if p.config.AutoDiscovery.CreatePoolsOnDemand {
		return p.createDatabaseOnDemand(dbName, user)
	}

	// If on-demand creation is disabled, but we have wildcard targets,
	// we should still try to create a pool if the database exists
	if len(p.wildcardTargets) > 0 {
		logger.Info("On-demand creation disabled but wildcard targets available, attempting to create pool anyway")
		return p.createDatabaseOnDemand(dbName, user)
	}

	logger.Error("No database manager available and no wildcard targets configured")
	return nil, fmt.Errorf("database not found: %s", dbName)
}

func (p *WildcardPooler) createDatabaseOnDemand(dbName, user string) (*DatabaseManager, error) {
	logger := p.logger.WithDatabase(dbName).WithUser(user)
	logger.Info("Creating database pool on demand")

	if len(p.wildcardTargets) == 0 {
		logger.Error("No wildcard targets configured for on-demand database creation")
		return nil, fmt.Errorf("no wildcard targets available for database %s", dbName)
	}

	// Try each wildcard target to see if the database exists
	var lastErr error
	for i, target := range p.wildcardTargets {
		targetLogger := logger.WithTarget(target.Name)
		targetLogger.Debug("Trying wildcard target", "attempt", i+1, "total", len(p.wildcardTargets))

		// Check user mapping - resolve credentials first
		dbUser, _ := p.resolveUserCredentials(target, user, dbName)
		if dbUser == "" {
			targetLogger.Debug("No valid user mapping for this target")
			continue
		}

		targetLogger.Debug("Resolved credentials", "db_user", dbUser)

		// Check if database should be included based on filters
		if !p.shouldIncludeDatabase(target, dbName) {
			targetLogger.Debug("Database excluded by filters")
			continue
		}

		// Check if database exists on this target
		exists, err := p.checkDatabaseExists(target, dbName)
		if err != nil {
			targetLogger.WithError(err).Warn("Failed to check database existence")
			lastErr = err
			continue
		}

		if !exists {
			targetLogger.Debug("Database does not exist on target")
			continue
		}

		targetLogger.Info("Database exists on target, creating pool")

		// Database exists and passes filters, create the pool
		if err := p.addDynamicDatabase(target, dbName); err != nil {
			targetLogger.WithError(err).Warn("Failed to create on-demand pool")
			lastErr = err
			continue
		}

		// Return the newly created database manager
		key := fmt.Sprintf("%s:%s", target.Name, dbName)
		p.databasesMu.RLock()
		dbManager := p.dynamicDatabases[key]
		p.databasesMu.RUnlock()

		if dbManager == nil {
			targetLogger.Error("Database manager not found after creation")
			lastErr = fmt.Errorf("database manager creation failed")
			continue
		}

		atomic.AddInt64(&p.stats.DatabasesCreated, 1)
		targetLogger.Info("Successfully created on-demand pool")
		return dbManager, nil
	}

	// If we get here, no wildcard target worked
	if lastErr != nil {
		logger.WithError(lastErr).Error("Failed to create on-demand database pool")
		return nil, fmt.Errorf("failed to create pool for database %s: %w", dbName, lastErr)
	}

	logger.Error("Database not found on any wildcard target")
	return nil, fmt.Errorf("database %s not found on any wildcard target", dbName)
}

func (p *WildcardPooler) resolveUserCredentials(target *WildcardTarget, clientUser, dbName string) (string, string) {
	logger := p.logger.WithTarget(target.Name).WithUser(clientUser).WithDatabase(dbName)

	// Check user mappings
	for i, mapping := range target.UserMappings {
		logger.Debug("Checking user mapping", "mapping_index", i, "mapping_client_user", mapping.ClientUser)

		if mapping.ClientUser == clientUser || mapping.ClientUser == "*" {
			// TODO: Implement database filter regex matching
			if mapping.DatabaseFilter == "" || mapping.DatabaseFilter == "*" {
				logger.Debug("User mapping matched", "db_user", mapping.DatabaseUser)
				return mapping.DatabaseUser, mapping.DatabasePass
			}
		}
	}

	// Fall back to default credentials
	logger.Debug("Using default credentials", "default_user", target.DefaultUser)
	return target.DefaultUser, target.DefaultPassword
}
