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
		return p.executeQuery(client, query)

	case Parse, Bind, Execute, Sync:
		return p.executeExtendedQuery(client, msgType, body)

	case Terminate:
		logger.Debug("Client terminating connection")
		return io.EOF

	default:
		logger.Debug("Forwarding unhandled message type")
		return p.executeExtendedQuery(client, msgType, body)
	}
}

// handleQuery handles a query from the client
func (p *WildcardPooler) executeQuery(client *ClientConnection, query string) error {
	logger := client.Logger()

	// Check if this is a LISTEN/UNLISTEN/NOTIFY command
	queryUpper := strings.ToUpper(strings.TrimSpace(query))

	if strings.HasPrefix(queryUpper, "LISTEN ") {
		return p.handleListen(client, query)
	} else if strings.HasPrefix(queryUpper, "UNLISTEN") {
		return p.handleUnlisten(client, query)
	} else if strings.HasPrefix(queryUpper, "NOTIFY ") {
		return p.handleNotify(client, query)
	} else if p.containsPgNotify(queryUpper) {
		logger.Info("Detected pg_notify function call")
		return p.handleNotify(client, query)
	}

	// Determine transaction state changes
	isTransactionStart := strings.HasPrefix(queryUpper, "BEGIN") || strings.HasPrefix(queryUpper, "START TRANSACTION")
	isTransactionEnd := strings.HasPrefix(queryUpper, "COMMIT") || strings.HasPrefix(queryUpper, "ROLLBACK")

	// Get or create backend connection
	backend, shouldRelease, err := p.getBackendForClient(client)
	if err != nil {
		logger.WithError(err).Error("Failed to get backend connection")
		return sendErrorResponse(client, "FATAL", "53300", "too many connections")
	}

	// Track connection state
	connectionOK := false

	defer func() {
		if shouldRelease {
			if connectionOK {
				p.releaseBackendConnection(backend)
			} else {
				// Connection is in unknown state, close it
				backend.Close()
				if dbManager, _ := p.getDatabaseManager(client.GetDatabase(), client.GetUser()); dbManager != nil {
					atomic.AddInt64(&dbManager.stats.TotalConnections, -1)
					atomic.AddInt64(&dbManager.stats.ActiveConnections, -1)
				}
			}
		}
	}()

	// Set read timeout
	if conn, ok := backend.conn.(net.Conn); ok {
		deadline := time.Now().Add(30 * time.Second)
		conn.SetReadDeadline(deadline)
		defer conn.SetReadDeadline(time.Time{})
	}

	// Forward query to backend
	if err := p.forwardQueryToBackend(backend, query); err != nil {
		logger.WithError(err).Error("Failed to forward query to backend")
		// OK to send error here - query didn't make it to backend
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	// Forward complete response from backend to client
	err = p.forwardCompleteBackendResponse(client, backend)
	if err != nil {
		logger.WithError(err).Error("Failed to forward backend response")
		// Do NOT send error response - just close the connection to client
		// The client will see a connection close, which is better than desynced messages
		return err
	}

	// Successfully completed query
	connectionOK = true

	// Update transaction state
	if isTransactionStart {
		client.SetInTransaction(true)
	} else if isTransactionEnd {
		client.SetInTransaction(false)
	}

	// Update statistics
	if dbManager, _ := p.getDatabaseManager(client.GetDatabase(), client.GetUser()); dbManager != nil {
		atomic.AddInt64(&dbManager.stats.QueriesExecuted, 1)
	}
	atomic.AddInt64(&p.stats.TotalQueries, 1)

	// Handle transaction state changes for connection management
	if shouldRelease && isTransactionStart {
		logger.Debug("Transaction started - converting pooled connection to dedicated")
		client.SetBackendConnection(backend)
		backend.SetClientRef(client)
		connectionOK = false // Don't release in defer
		shouldRelease = false
	} else if !shouldRelease && isTransactionEnd && !client.IsListening() {
		logger.Debug("Transaction ended - releasing dedicated connection to pool")
		client.SetBackendConnection(nil)
		backend.SetClientRef(nil)
		p.releaseBackendConnection(backend)
	}

	return nil
}

// getBackendForClient gets the appropriate backend connection for a client
// Uses smart connection management: dedicated connections for transactions and listeners,
// pooled connections otherwise
func (p *WildcardPooler) getBackendForClient(client *ClientConnection) (*BackendConnection, bool, error) {
	logger := client.Logger()

	logger.Debug("Determining backend connection strategy",
		"in_transaction", client.IsInTransaction(),
		"is_listening", client.IsListening(),
		"has_existing_backend", client.GetBackendConnection() != nil)

	// Rule 1: If client is listening for notifications, use dedicated connection
	// LISTEN/NOTIFY requires a persistent connection
	if client.IsListening() {
		backend := client.GetBackendConnection()
		if backend == nil {
			logger.Debug("Creating dedicated backend connection for listening client")
			var err error
			backend, err = p.acquireBackendConnection(client.GetDatabase(), client.GetUser())
			if err != nil {
				return nil, false, err
			}
			client.SetBackendConnection(backend)
			backend.SetClientRef(client)
		}
		logger.Debug("Using dedicated backend connection for listener")
		return backend, false, nil // Never release listener connections
	}

	// Rule 2: If client is in a transaction, use dedicated connection
	// Transactions must execute on the same backend connection
	if client.IsInTransaction() {
		backend := client.GetBackendConnection()
		if backend == nil {
			logger.Debug("Creating dedicated backend connection for active transaction")
			var err error
			backend, err = p.acquireBackendConnection(client.GetDatabase(), client.GetUser())
			if err != nil {
				return nil, false, err
			}
			client.SetBackendConnection(backend)
			backend.SetClientRef(client)
		}
		logger.Debug("Using dedicated backend connection for transaction")
		return backend, false, nil // Don't release during transaction
	}

	// Rule 3: Outside of transactions and not listening, use pooled connections
	// This provides connection pooling efficiency for stateless queries
	logger.Debug("Acquiring pooled backend connection")
	backend, err := p.acquireBackendConnection(client.GetDatabase(), client.GetUser())
	if err != nil {
		return nil, false, err
	}
	return backend, true, nil // Release after use
}

// executeExtendedQuery handles extended query protocol (Parse/Bind/Execute/Sync)
func (p *WildcardPooler) executeExtendedQuery(client *ClientConnection, msgType byte, body []byte) error {
	logger := client.Logger().WithField("msg_type", string(msgType))

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
	logger := client.Logger().WithField("msg_type", string(msgType))

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
	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			if netErr, ok := err.(*net.OpError); ok && netErr.Timeout() {
				return fmt.Errorf("query timeout")
			}
			return err
		}

		if err := client.WriteMessage(msgType, body); err != nil {
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

// acquireBackendConnection gets a backend connection for a specific database and user
func (p *WildcardPooler) acquireBackendConnection(dbName string, username string) (*BackendConnection, error) {
	dbManager, err := p.getDatabaseManager(dbName, username)
	if err != nil {
		return nil, err
	}

	logger := p.logger.WithDatabase(dbName).WithUser(username)

	select {
	case conn := <-dbManager.backendPool:
		conn.SetInUse(true)
		atomic.AddInt64(&dbManager.stats.ActiveConnections, 1)
		atomic.AddInt64(&dbManager.stats.IdleConnections, -1)
		logger.Debug("Successfully acquired pooled connection")
		return conn, nil

	default:
		// Create new connection
		if atomic.LoadInt64(&dbManager.stats.TotalConnections) < int64(dbManager.config.MaxConnections) {
			conn, err := dbManager.createBackendConnection()
			if err != nil {
				return nil, fmt.Errorf("failed to create new connection: %w", err)
			}

			conn.SetInUse(true)
			atomic.AddInt64(&dbManager.stats.TotalConnections, 1)
			atomic.AddInt64(&dbManager.stats.ActiveConnections, 1)
			logger.Debug("Successfully created new backend connection")
			return conn, nil
		}

		logger.Error("Connection pool exhausted")
		return nil, fmt.Errorf("no available backend connections for database %s", dbName)
	}
}

func (p *WildcardPooler) createReplacementConnection(dbManager *DatabaseManager, logger *Logger) (*BackendConnection, error) {
	newConn, err := dbManager.createBackendConnection()
	if err != nil {
		logger.WithError(err).Error("Failed to create replacement connection")
		return nil, fmt.Errorf("failed to create replacement connection: %w", err)
	}

	newConn.SetInUse(true)
	atomic.AddInt64(&dbManager.stats.TotalConnections, 1)
	atomic.AddInt64(&dbManager.stats.ActiveConnections, 1)
	logger.Debug("Successfully created replacement connection")
	return newConn, nil
}

// getDatabaseManager retrieves or creates a database manager for a specific database and user
func (p *WildcardPooler) getDatabaseManager(dbName, user string) (*DatabaseManager, error) {
	logger := p.logger.WithDatabase(dbName).WithUser(user)

	// Try each target in priority order
	sortedTargets := p.getSortedTargets()

	for _, target := range sortedTargets {
		// Check if this target serves this database
		if !p.targetServesDatabase(target, dbName) {
			continue
		}

		// Check if pool exists for this target/database/user
		p.targetsMu.RLock()
		if targetMap, ok := p.targets[target.Name]; ok {
			if dbMap, ok := targetMap[dbName]; ok {
				if manager, ok := dbMap[user]; ok {
					logger.Debug("Found existing database manager",
						"target", target.Name)
					p.targetsMu.RUnlock()
					return manager, nil
				}
			}
		}
		p.targetsMu.RUnlock()
	}

	// No existing pool, needs authentication
	logger.Debug("No existing database manager found for this user")
	return nil, fmt.Errorf("database pool not created yet - needs authentication")
}

// addDatabaseWithCredentials adds a database pool with user credentials
func (p *WildcardPooler) addDatabaseWithCredentials(
	target *Target, dbName, user, password string, initialConn *BackendConnection) error {

	logger := p.logger.WithField("target", target.Name).
		WithField("database", dbName).
		WithField("user", user)

	p.targetsMu.Lock()
	defer p.targetsMu.Unlock()

	// Initialize nested maps if needed
	if p.targets[target.Name] == nil {
		p.targets[target.Name] = make(map[string]map[string]*DatabaseManager)
	}
	if p.targets[target.Name][dbName] == nil {
		p.targets[target.Name][dbName] = make(map[string]*DatabaseManager)
	}

	// Check if already exists (race condition protection)
	if manager, exists := p.targets[target.Name][dbName][user]; exists {
		logger.Debug("Pool already exists, adding connection to existing pool")
		// Add connection to existing pool
		select {
		case manager.backendPool <- initialConn:
			atomic.AddInt64(&manager.stats.TotalConnections, 1)
			atomic.AddInt64(&manager.stats.IdleConnections, 1)
		default:
			// Pool is full, close the connection
			initialConn.Close()
		}
		return nil
	}

	// Create new manager
	config := DatabaseConfig{
		Name:           dbName,
		Host:           target.Host,
		Port:           target.Port,
		User:           user,
		Password:       password,
		SSLMode:        target.SSLMode,
		SSLCAFile:      target.SSLCAFile,
		MaxConnections: target.MaxConnections,
		ConnectTimeout: target.ConnectTimeout,
		Parameters:     target.Parameters,
	}

	dbManager := &DatabaseManager{
		config:      config,
		target:      target,
		backendPool: make(chan *BackendConnection, config.MaxConnections),
		username:    user,
		password:    password,
	}

	// Add initial connection to the pool
	dbManager.backendPool <- initialConn
	atomic.AddInt64(&dbManager.stats.TotalConnections, 1)
	atomic.AddInt64(&dbManager.stats.IdleConnections, 1)

	p.targets[target.Name][dbName][user] = dbManager
	atomic.AddInt64(&p.stats.TotalDatabases, 1)

	logger.Info("Created database pool with client credentials and initial connection")
	return nil
}
