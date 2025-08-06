package main

import (
	"fmt"
	"io"
	"strings"
	"sync/atomic"
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
		logger.Debug("Handling query", "query", query)
		return p.handleQuery(client, query)

	case Parse, Bind, Execute, Sync:
		logger.Debug("Handling extended query protocol")
		return p.handleExtendedQuery(client, msgType, body)

	case Terminate:
		logger.Debug("Client terminating connection")
		return io.EOF

	default:
		logger.Debug("Forwarding unhandled message type to backend")
		// For unknown message types, try to forward them directly to backend
		return p.forwardUnknownMessage(client, msgType, body)
	}
}

// handleQuery handles a query from the client
func (p *WildcardPooler) handleQuery(client *ClientConnection, query string) error {
	logger := client.Logger().WithField("query", query)

	// Check if this is a LISTEN/UNLISTEN/NOTIFY command
	queryUpper := strings.ToUpper(strings.TrimSpace(query))

	if strings.HasPrefix(queryUpper, "LISTEN ") {
		return p.handleListen(client, query)
	} else if strings.HasPrefix(queryUpper, "UNLISTEN") {
		return p.handleUnlisten(client, query)
	} else if strings.HasPrefix(queryUpper, "NOTIFY ") {
		return p.handleNotify(client, query)
	}

	// Get backend connection
	backend := client.GetBackendConnection()
	if backend == nil {
		logger.Error("No backend connection available for client")
		return sendErrorResponse(client, "FATAL", "08003", "connection does not exist")
	}

	// Forward query to backend
	if err := p.forwardQueryToBackend(backend, query); err != nil {
		logger.WithError(err).Error("Failed to forward query to backend")
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	// Forward complete response from backend to client
	if err := p.forwardCompleteBackendResponse(client, backend); err != nil {
		logger.WithError(err).Error("Failed to forward backend response")
		return err
	}

	// Update transaction state based on query
	p.updateTransactionState(client, query)

	// Update statistics
	if dbManager, _ := p.getDatabaseManager(client.GetDatabase(), client.GetUser()); dbManager != nil {
		atomic.AddInt64(&dbManager.stats.QueriesExecuted, 1)
	}
	atomic.AddInt64(&p.stats.TotalQueries, 1)

	return nil
}

// handleExtendedQuery handles extended query protocol (Parse/Bind/Execute/Sync)
func (p *WildcardPooler) handleExtendedQuery(client *ClientConnection, msgType byte, body []byte) error {
	logger := client.Logger().WithField("msg_type", string(msgType))

	// Get backend connection
	backend := client.GetBackendConnection()
	if backend == nil {
		logger.Error("No backend connection available for extended query")
		return sendErrorResponse(client, "FATAL", "08003", "connection does not exist")
	}

	// Forward message to backend
	if err := backend.WriteMessage(msgType, body); err != nil {
		logger.WithError(err).Error("Failed to forward extended query message")
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	// Handle responses based on message type
	switch msgType {
	case Parse:
		// Parse expects ParseComplete (1) or ErrorResponse (E)
		return p.forwardSingleResponse(client, backend, "ParseComplete")

	case Bind:
		// Bind expects BindComplete (2) or ErrorResponse (E)
		return p.forwardSingleResponse(client, backend, "BindComplete")

	case Execute:
		// Execute can return multiple messages until CommandComplete or ErrorResponse
		return p.forwardExecuteResponse(client, backend)

	case Sync:
		// Sync expects ReadyForQuery (Z)
		return p.forwardUntilReady(client, backend)

	default:
		logger.Warn("Unknown extended query message type", "type", string(msgType))
		return p.forwardSingleResponse(client, backend, "Unknown")
	}
}

// forwardUnknownMessage forwards unknown message types directly
func (p *WildcardPooler) forwardUnknownMessage(client *ClientConnection, msgType byte, body []byte) error {
	logger := client.Logger().WithField("msg_type", string(msgType))

	backend := client.GetBackendConnection()
	if backend == nil {
		logger.Error("No backend connection for unknown message")
		return sendErrorResponse(client, "FATAL", "08003", "connection does not exist")
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
		logger.WithError(err).Error("Failed to read backend response")
		return err
	}

	logger.Debug("Forwarding single response", "msg_type", string(msgType), "body_len", len(body))

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
			logger.WithError(err).Error("Failed to read execute response")
			return err
		}

		logger.Debug("Forwarding execute response", "msg_type", string(msgType), "body_len", len(body))

		if err := client.WriteMessage(msgType, body); err != nil {
			logger.WithError(err).Error("Failed to forward execute response")
			return err
		}

		// Execute sequence ends with CommandComplete, EmptyQueryResponse, or ErrorResponse
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
			logger.WithError(err).Error("Failed to read response waiting for ready")
			return err
		}

		logger.Debug("Forwarding message waiting for ready", "msg_type", string(msgType), "body_len", len(body))

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
			logger.WithError(err).Error("Failed to read complete response")
			return err
		}

		logger.Debug("Forwarding complete response", "msg_type", string(msgType), "body_len", len(body))

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

// updateTransactionState updates the client's transaction state based on the query
func (p *WildcardPooler) updateTransactionState(client *ClientConnection, query string) {
	queryUpper := strings.ToUpper(strings.TrimSpace(query))

	// Check for transaction commands
	if strings.HasPrefix(queryUpper, "BEGIN") ||
		strings.HasPrefix(queryUpper, "START TRANSACTION") {
		client.SetInTransaction(true)
	} else if strings.HasPrefix(queryUpper, "COMMIT") ||
		strings.HasPrefix(queryUpper, "ROLLBACK") {
		client.SetInTransaction(false)
	}
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

	select {
	case conn := <-dbManager.backendPool:
		conn.SetInUse(true)
		atomic.AddInt64(&dbManager.stats.ActiveConnections, 1)
		atomic.AddInt64(&dbManager.stats.IdleConnections, -1)
		return conn, nil
	default:
		// Try to create a new connection if we haven't reached the limit
		if atomic.LoadInt64(&dbManager.stats.TotalConnections) < int64(dbManager.config.MaxConnections) {
			conn, err := dbManager.createBackendConnection()
			if err == nil {
				conn.SetInUse(true)
				atomic.AddInt64(&dbManager.stats.TotalConnections, 1)
				atomic.AddInt64(&dbManager.stats.ActiveConnections, 1)
				return conn, nil
			}
		}
		return nil, fmt.Errorf("no available backend connections for database %s", dbName)
	}
}

// getDatabaseManager finds or creates a database manager for the requested database
func (p *WildcardPooler) getDatabaseManager(dbName, user string) (*DatabaseManager, error) {
	// First check static databases
	p.databasesMu.RLock()
	if dbManager, exists := p.staticDatabases[dbName]; exists {
		p.databasesMu.RUnlock()
		return dbManager, nil
	}
	p.databasesMu.RUnlock()

	// Check dynamic databases
	for _, target := range p.wildcardTargets {
		key := fmt.Sprintf("%s:%s", target.Name, dbName)

		p.databasesMu.RLock()
		if dbManager, exists := p.dynamicDatabases[key]; exists {
			p.databasesMu.RUnlock()
			return dbManager, nil
		}
		p.databasesMu.RUnlock()
	}

	// If on-demand creation is enabled, try to create the database pool
	if p.config.AutoDiscovery.CreatePoolsOnDemand {
		return p.createDatabaseOnDemand(dbName, user)
	}

	return nil, fmt.Errorf("database not found: %s", dbName)
}

// createDatabaseOnDemand creates a database pool on-demand
func (p *WildcardPooler) createDatabaseOnDemand(dbName, user string) (*DatabaseManager, error) {
	logger := p.logger.WithDatabase(dbName).WithUser(user)
	logger.Debug("Creating database pool on demand")

	// Try each wildcard target to see if the database exists
	for _, target := range p.wildcardTargets {
		// Check user mapping
		dbUser, _ := p.resolveUserCredentials(target, user, dbName)
		if dbUser == "" {
			continue // No valid user mapping for this target
		}

		// Check if database exists on this target
		exists, err := p.checkDatabaseExists(target, dbName)
		if err != nil {
			logger.WithError(err).Warn("Failed to check database existence", "target", target.Name)
			continue
		}
		if !exists {
			logger.Debug("Database does not exist on target", "target", target.Name, "database", dbName)
			continue // Database doesn't exist on this target
		}

		// Database exists, create the pool
		if err := p.addDynamicDatabase(target, dbName); err != nil {
			logger.WithError(err).Warn("Failed to create on-demand pool", "target", target.Name)
			continue
		}

		// Return the newly created database manager
		key := fmt.Sprintf("%s:%s", target.Name, dbName)
		p.databasesMu.RLock()
		dbManager := p.dynamicDatabases[key]
		p.databasesMu.RUnlock()

		atomic.AddInt64(&p.stats.DatabasesCreated, 1)
		logger.Info("Created on-demand pool", "target", target.Name)
		return dbManager, nil
	}

	return nil, fmt.Errorf("database %s not found on any wildcard target", dbName)
}

// resolveUserCredentials resolves user credentials based on mapping rules
func (p *WildcardPooler) resolveUserCredentials(target *WildcardTarget, clientUser, dbName string) (string, string) {
	// Check user mappings
	for _, mapping := range target.UserMappings {
		if mapping.ClientUser == clientUser || mapping.ClientUser == "*" {
			// TODO: Implement database filter regex matching
			if mapping.DatabaseFilter == "" || mapping.DatabaseFilter == "*" {
				return mapping.DatabaseUser, mapping.DatabasePass
			}
		}
	}

	// Fall back to default credentials
	return target.DefaultUser, target.DefaultPassword
}
