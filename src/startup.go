package main

import (
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"
)

// handleStartupMessage handles the PostgreSQL startup protocol with wildcard support
func (p *WildcardPooler) handleStartupMessage(client *ClientConnection) error {
	// Read the first message - could be SSL request or startup message
	length, err := readUint32(client.reader)
	if err != nil {
		return fmt.Errorf("failed to read message length: %w", err)
	}

	p.logger.Debug("First message length", "length", length)

	// Check if this is an SSL request
	if length == 8 {
		// This might be an SSL request, read the next 4 bytes to confirm
		requestCode, err := readUint32(client.reader)
		if err != nil {
			return fmt.Errorf("failed to read request code: %w", err)
		}

		p.logger.Debug("Request code received", "code", requestCode)

		// SSL request magic number is 80877103 (0x04D2162F)
		if requestCode == SSLRequestCode {
			p.logger.Debug("SSL request received, rejecting SSL")

			// Respond with 'N' (SSL not supported)
			if err := client.writer.WriteByte('N'); err != nil {
				return fmt.Errorf("failed to send SSL rejection: %w", err)
			}
			if err := client.writer.Flush(); err != nil {
				return fmt.Errorf("failed to flush SSL rejection: %w", err)
			}

			// Now read the actual startup message
			return p.handleStartupMessage(client)
		} else if requestCode == CancelRequestCode {
			// Handle cancel request (not implemented in this example)
			return fmt.Errorf("cancel request not supported")
		} else {
			return fmt.Errorf("unknown request code: %d", requestCode)
		}
	}

	// This is a regular startup message, read the protocol version
	protocolVersion, err := readInt32(client.reader)
	if err != nil {
		return fmt.Errorf("failed to read protocol version: %w", err)
	}

	p.logger.Debug("Protocol version", "version", protocolVersion)

	// Validate protocol version (should be 196608 for PostgreSQL 3.0)
	if protocolVersion != ProtocolVersion30 {
		return fmt.Errorf("unsupported protocol version: %d, expected %d", protocolVersion, ProtocolVersion30)
	}

	// Read parameters (length includes the length field itself, so subtract 8 bytes for length + version)
	paramLength := int(length - 8)
	if paramLength < 0 {
		return fmt.Errorf("invalid startup message length: %d", length)
	}

	paramBytes := make([]byte, paramLength)
	if paramLength > 0 {
		if _, err := io.ReadFull(client.reader, paramBytes); err != nil {
			return fmt.Errorf("failed to read startup parameters: %w", err)
		}
	}

	// Parse parameters
	params := parseStartupParams(paramBytes)

	p.logger.Debug("Parsed startup parameters",
		"params", params,
		"param_bytes_len", len(paramBytes),
		"param_bytes_hex", fmt.Sprintf("%x", paramBytes))

	startupMsg := StartupMessage{
		ProtocolVersion: protocolVersion,
		Parameters:      params,
		User:            params["user"],
		Database:        params["database"],
	}

	// Validate required parameters
	if startupMsg.User == "" {
		return fmt.Errorf("missing required parameter: user")
	}
	if startupMsg.Database == "" {
		return fmt.Errorf("missing required parameter: database")
	}

	client.SetUser(startupMsg.User)
	client.SetDatabase(startupMsg.Database)

	logger := client.Logger().WithUser(startupMsg.User).WithDatabase(startupMsg.Database)
	logger.Info("Client startup",
		"protocol_version", protocolVersion,
		"remote_addr", client.RemoteAddr(),
		"all_params", params)

	// Try to find or create a database manager for the requested database
	dbManager, err := p.getDatabaseManager(startupMsg.Database, startupMsg.User)
	if err != nil {
		logger.WithError(err).Error("Database not available")
		return sendErrorResponse(client, "FATAL", "3D000",
			fmt.Sprintf("database \"%s\" does not exist or is not accessible", startupMsg.Database))
	}

	// Update last used time
	dbManager.mu.Lock()
	dbManager.lastUsed = time.Now()
	poolMode := dbManager.config.PoolMode
	if poolMode == "" {
		poolMode = p.config.Server.DefaultPoolMode
	}
	dbManager.mu.Unlock()

	// Set session mode based on pool configuration
	client.SetSessionMode(poolMode == "session")

	logger.Info("Client pool mode determined", "pool_mode", poolMode, "session_mode", poolMode == "session")

	// For session mode, establish backend connection now
	// For transaction/statement mode, connections are acquired per query/transaction
	if poolMode == "session" {
		backend, err := p.acquireBackendConnection(startupMsg.Database)
		if err != nil {
			logger.WithError(err).Error("Failed to acquire backend connection for session mode")
			return sendErrorResponse(client, "FATAL", "53300", "too many connections")
		}

		client.SetBackendConnection(backend)
		backend.SetClientRef(client)
		logger.Debug("Session mode backend connection established",
			"backend_addr", backend.RemoteAddr())
	} else {
		logger.Debug("Transaction/statement mode - backend connections will be acquired per query")
	}

	// Send authentication OK (simplified authentication)
	if err := sendAuthenticationOK(client); err != nil {
		return fmt.Errorf("failed to send authentication OK: %w", err)
	}

	// Send backend key data (dummy values for now)
	if err := sendBackendKeyData(client, 12345, 67890); err != nil {
		return fmt.Errorf("failed to send backend key data: %w", err)
	}

	// Send parameter status messages
	parameterStatuses := map[string]string{
		"server_version":              "13.0 (PgJoint)",
		"client_encoding":             "UTF8",
		"DateStyle":                   "ISO, MDY",
		"TimeZone":                    "UTC",
		"integer_datetimes":           "on",
		"is_superuser":                "off",
		"server_encoding":             "UTF8",
		"session_authorization":       startupMsg.User,
		"standard_conforming_strings": "on",
	}

	for name, value := range parameterStatuses {
		if err := sendParameterStatus(client, name, value); err != nil {
			return fmt.Errorf("failed to send parameter status %s: %w", name, err)
		}
	}

	// Send ready for query
	if err := sendReadyForQuery(client, 'I'); err != nil {
		return fmt.Errorf("failed to send ready for query: %w", err)
	}

	client.SetAuthenticated(true)
	logger.Info("Client authenticated successfully",
		"session_mode", client.IsSessionMode(),
		"has_backend", client.GetBackendConnection() != nil)
	return nil
}

// authenticateBackend authenticates with the PostgreSQL backend using raw TCP connection
func (dm *DatabaseManager) authenticateBackend(backend *BackendConnection) error {
	// Send startup message
	startupMsg := buildStartupMessage(dm.config.User, dm.config.Name)
	if _, err := backend.writer.Write(startupMsg); err != nil {
		return fmt.Errorf("failed to send startup message: %w", err)
	}
	if err := backend.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush startup message: %w", err)
	}

	// Read authentication response and consume ALL messages until ready
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
				continue // Continue reading until ReadyForQuery
			case AuthenticationMD5:
				if len(body) < 8 {
					return fmt.Errorf("invalid MD5 auth response")
				}

				// Handle MD5 authentication
				salt := body[4:8]
				response := buildMD5Response(dm.config.User, dm.config.Password, salt)

				// Send password message
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

				// Handle SCRAM flow inline to ensure we consume all messages
				if err := dm.handleSCRAMAuthInline(backend, saslData); err != nil {
					return err
				}
				continue
			case AuthenticationSASLContinue:
				return fmt.Errorf("unexpected SASL continue outside of SCRAM flow")
			case AuthenticationSASLFinal:
				return fmt.Errorf("unexpected SASL final outside of SCRAM flow")
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
			// Just consume these messages, don't store them
			continue

		case 'Z': // Ready for query - authentication is fully complete
			return nil

		case 'E': // Error response
			errorMsg := parseErrorMessage(body)
			return fmt.Errorf("backend authentication failed: %s", errorMsg)

		default:
			return fmt.Errorf("unexpected message type during auth: %c", msgType)
		}
	}
}

// handleSCRAMAuthInline handles SCRAM authentication inline during backend connection setup
func (dm *DatabaseManager) handleSCRAMAuthInline(backend *BackendConnection, saslData []byte) error {
	// This is a simplified version that just completes the SCRAM flow
	// without returning early, ensuring all messages are consumed
	return handleSCRAMAuth(backend, dm.config.User, dm.config.Password, saslData)
}

// createBackendConnection creates a new backend connection using raw TCP connection
func (dm *DatabaseManager) createBackendConnection() (*BackendConnection, error) {
	// Create TCP connection to PostgreSQL backend
	addr := fmt.Sprintf("%s:%d", dm.config.Host, dm.config.Port)

	conn, err := net.DialTimeout("tcp", addr, dm.config.ConnectTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	targetName := dm.config.Name
	if dm.wildcardTarget != nil {
		targetName = dm.wildcardTarget.Name
	}

	// Create backend connection with proper reader/writer
	backend := NewBackendConnection(conn, dm.config.Name, targetName)

	// Authenticate with the backend
	if err := dm.authenticateBackend(backend); err != nil {
		conn.Close()
		return nil, fmt.Errorf("authentication failed: %w", err)
	}

	return backend, nil
}

// initializeConnections initializes the minimum number of backend connections
func (dm *DatabaseManager) initializeConnections() error {
	for i := 0; i < dm.config.MinConnections; i++ {
		conn, err := dm.createBackendConnection()
		if err != nil {
			return fmt.Errorf("failed to create connection %d: %w", i+1, err)
		}

		select {
		case dm.backendPool <- conn:
			atomic.AddInt64(&dm.stats.TotalConnections, 1)
			atomic.AddInt64(&dm.stats.IdleConnections, 1)
		default:
			// This shouldn't happen since we're creating min connections
			conn.Close()
			return fmt.Errorf("failed to add connection to pool")
		}
	}

	return nil
}
