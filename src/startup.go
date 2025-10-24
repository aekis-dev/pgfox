package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
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
			// Get the database config to check SSL mode
			// We need to peek at the next startup message to get the database name
			// For now, check if ANY of our targets/databases support SSL

			sslSupported := p.isSSLSupported()

			if sslSupported {
				p.logger.Debug("SSL request received, accepting SSL")

				// Respond with 'S' (SSL supported)
				if err := client.writer.WriteByte('S'); err != nil {
					return fmt.Errorf("failed to send SSL acceptance: %w", err)
				}
				if err := client.writer.Flush(); err != nil {
					return fmt.Errorf("failed to flush SSL acceptance: %w", err)
				}

				// Upgrade connection to TLS
				if err := p.upgradeToTLS(client); err != nil {
					return fmt.Errorf("failed to upgrade to TLS: %w", err)
				}

				// Now read the actual startup message over TLS
				return p.handleStartupMessage(client)
			} else {
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
			}
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
		"database", startupMsg.Database,
		"user", startupMsg.User)

	// Always authenticate client with backend (true passthrough)
	logger.Debug("Requesting client authentication for passthrough")
	return p.authenticateClientWithBackend(client, startupMsg.User, startupMsg.Database)
}

// authenticateClientWithBackend performs passthrough authentication
func (p *WildcardPooler) authenticateClientWithBackend(client *ClientConnection, user, database string) error {
	logger := client.Logger()

	// Find target that serves this database (in priority order)
	var selectedTarget *Target

	for _, target := range p.getSortedTargets() {
		if p.targetServesDatabase(target, database) {
			selectedTarget = target
			logger.Debug("Selected target for database",
				"target", target.Name,
				"priority", target.Priority)
			break
		}
	}

	if selectedTarget == nil {
		logger.Warn("No target serves this database", "database", database)
		return sendErrorResponse(client, "FATAL", "3D000", "database not found")
	}

	// Request password from client using CleartextPassword
	logger.Debug("Requesting cleartext password from client")

	authRequest := make([]byte, 4)
	authRequest[0] = 0
	authRequest[1] = 0
	authRequest[2] = 0
	authRequest[3] = 3 // AuthenticationCleartextPassword

	if err := client.WriteMessage('R', authRequest); err != nil {
		return fmt.Errorf("failed to request password: %w", err)
	}

	// Read password response
	msgType, body, err := client.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read password: %w", err)
	}

	if msgType != 'p' {
		return fmt.Errorf("expected password message, got %c", msgType)
	}

	// Password is null-terminated
	password := string(body)
	if len(password) > 0 && password[len(password)-1] == 0 {
		password = password[:len(password)-1]
	}

	logger.Debug("Received client password, attempting backend connection")
	client.SetPassword(password)

	// Try to connect to backend with these credentials
	backendConn, err := p.createBackendConnectionWithCredentials(
		selectedTarget.Host, selectedTarget.Port, database, user, password, selectedTarget)
	if err != nil {
		logger.WithError(err).Error("Backend authentication failed")
		return sendErrorResponse(client, "FATAL", "28P01", "password authentication failed")
	}

	// Success! Backend accepted the credentials
	logger.Info("Backend authentication successful")

	// Get process ID and secret key
	processID := backendConn.GetProcessID()
	secretKey := backendConn.GetSecretKey()

	// Send AuthenticationOK to client
	if err := sendAuthenticationOK(client); err != nil {
		backendConn.Close()
		return fmt.Errorf("failed to send authentication OK: %w", err)
	}

	// Send backend key data
	if err := sendBackendKeyData(client, processID, secretKey); err != nil {
		backendConn.Close()
		return fmt.Errorf("failed to send backend key data: %w", err)
	}

	// Send parameter status messages
	if err := p.sendParameterStatuses(client, user); err != nil {
		backendConn.Close()
		return err
	}

	// Send ready for query
	if err := sendReadyForQuery(client, 'I'); err != nil {
		backendConn.Close()
		return fmt.Errorf("failed to send ready for query: %w", err)
	}

	// NOW create database pool with these credentials
	// Pass the authenticated connection to be added to the pool
	if err := p.addDatabaseWithCredentials(selectedTarget, database, user, password, backendConn); err != nil {
		logger.WithError(err).Error("Failed to create database pool")
		backendConn.Close()
		return sendErrorResponse(client, "FATAL", "53300", "too many connections")
	}

	logger.Debug("auth connection added to pool for reuse")

	client.SetAuthenticated(true)
	logger.Info("Client authenticated successfully with passthrough credentials")
	return nil
}

// sendParameterStatuses sends parameter status messages
func (p *WildcardPooler) sendParameterStatuses(client *ClientConnection, user string) error {
	parameterStatuses := map[string]string{
		"server_version":              "13.0 (PgFox)",
		"client_encoding":             "UTF8",
		"DateStyle":                   "ISO, MDY",
		"TimeZone":                    "UTC",
		"integer_datetimes":           "on",
		"is_superuser":                "off",
		"server_encoding":             "UTF8",
		"session_authorization":       user,
		"standard_conforming_strings": "on",
	}

	for name, value := range parameterStatuses {
		if err := sendParameterStatus(client, name, value); err != nil {
			return fmt.Errorf("failed to send parameter status %s: %w", name, err)
		}
	}

	return nil
}

// createBackendConnectionWithCredentials creates a backend connection with specific credentials
func (p *WildcardPooler) createBackendConnectionWithCredentials(
	host string, port int, database, user, password string, target *Target) (*BackendConnection, error) {

	addr := fmt.Sprintf("%s:%d", host, port)

	// Determine connect timeout
	var connectTimeout time.Duration
	if target != nil {
		connectTimeout = target.ConnectTimeout
	} else {
		connectTimeout = p.config.Server.ConnectTimeout
	}

	conn, err := net.DialTimeout("tcp", addr, connectTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	// Enable TCP keepalive to detect dead connections
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	// Handle SSL to backend if required
	if target != nil && target.SSLMode != "disable" {
		conn, err = p.upgradeBackendToTLS(conn, host, target.SSLMode, target.SSLCAFile)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to establish SSL to backend: %w", err)
		}
	}

	targetName := database
	if target != nil {
		targetName = target.Name
	}

	backend := NewBackendConnection(conn, database, targetName)

	// Send startup message
	startupMsg := buildStartupMessage(user, database)
	if _, err := backend.writer.Write(startupMsg); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to send startup message: %w", err)
	}
	if err := backend.writer.Flush(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to flush startup message: %w", err)
	}

	// Handle authentication with provided credentials
	if err := p.authenticateBackendWithPassword(backend, user, password); err != nil {
		conn.Close()
		return nil, fmt.Errorf("backend authentication failed: %w", err)
	}

	// CRITICAL: Verify buffer is clean after authentication
	buffered := backend.reader.Buffered()
	if buffered > 0 {
		p.logger.Error("Backend connection has buffered data after authentication",
			"bytes", buffered,
			"backend", backend.RemoteAddr())
		// Drain the buffer
		junk := make([]byte, buffered)
		if _, err := backend.reader.Read(junk); err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to drain buffer: %w", err)
		}
		p.logger.Debug("Drained buffered data after authentication", "bytes", buffered)
	}

	return backend, nil
}

// upgradeBackendToTLS upgrades a backend connection to TLS (pooler-level function)
func (p *WildcardPooler) upgradeBackendToTLS(conn net.Conn, host, sslMode, sslCAFile string) (net.Conn, error) {
	// Send SSL request to backend
	sslRequest := make([]byte, 8)
	binary.BigEndian.PutUint32(sslRequest[0:4], 8)
	binary.BigEndian.PutUint32(sslRequest[4:8], SSLRequestCode)

	if _, err := conn.Write(sslRequest); err != nil {
		return nil, fmt.Errorf("failed to send SSL request to backend: %w", err)
	}

	// Read response
	response := make([]byte, 1)
	if _, err := conn.Read(response); err != nil {
		return nil, fmt.Errorf("failed to read SSL response from backend: %w", err)
	}

	if response[0] == 'N' {
		// Backend doesn't support SSL
		if sslMode == "require" || sslMode == "verify-ca" || sslMode == "verify-full" {
			return nil, fmt.Errorf("backend does not support SSL but SSLMode is %s", sslMode)
		}
		// For "prefer", fall back to non-SSL
		return conn, nil
	}

	if response[0] != 'S' {
		return nil, fmt.Errorf("unexpected SSL response from backend: %c", response[0])
	}

	// Configure TLS for backend connection
	tlsConfig := &tls.Config{
		ServerName:         host,
		InsecureSkipVerify: sslMode == "require", // Only verify for verify-ca and verify-full
		MinVersion:         tls.VersionTLS12,
	}

	// Load CA certificate if specified and needed
	if (sslMode == "verify-ca" || sslMode == "verify-full") && sslCAFile != "" {
		caCert, err := os.ReadFile(sslCAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		tlsConfig.RootCAs = caCertPool
	}

	// Upgrade to TLS
	tlsConn := tls.Client(conn, tlsConfig)

	// Perform handshake
	if err := tlsConn.Handshake(); err != nil {
		return nil, fmt.Errorf("TLS handshake with backend failed: %w", err)
	}

	return tlsConn, nil
}

// authenticateBackendWithPassword authenticates with backend using provided credentials
func (p *WildcardPooler) authenticateBackendWithPassword(backend *BackendConnection, user, password string) error {
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
			case AuthenticationCleartextPassword:
				// Send cleartext password
				passMsg := []byte(password + "\x00")
				if err := backend.WriteMessage('p', passMsg); err != nil {
					return fmt.Errorf("failed to send password: %w", err)
				}
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
				if len(body) < 4 {
					return fmt.Errorf("invalid SASL auth response")
				}
				saslData := body[4:]
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
			return nil

		case 'E': // Error response
			errorMsg := parseErrorMessage(body)
			return fmt.Errorf("authentication failed: %s", errorMsg)
		}
	}
}

// authenticateBackend authenticates with the PostgreSQL backend using stored credentials
func (dm *DatabaseManager) authenticateBackend(backend *BackendConnection) error {
	// Send startup message with stored username
	startupMsg := buildStartupMessage(dm.username, dm.config.Name)
	if _, err := backend.writer.Write(startupMsg); err != nil {
		return fmt.Errorf("failed to send startup message: %w", err)
	}
	if err := backend.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush startup message: %w", err)
	}

	// Read authentication response and authenticate with stored password
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
			case AuthenticationCleartextPassword:
				passMsg := []byte(dm.password + "\x00")
				if err := backend.WriteMessage('p', passMsg); err != nil {
					return fmt.Errorf("failed to send password: %w", err)
				}
			case AuthenticationMD5:
				if len(body) < 8 {
					return fmt.Errorf("invalid MD5 auth response")
				}
				salt := body[4:8]
				response := buildMD5Response(dm.username, dm.password, salt)
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
				return handleSCRAMAuth(backend, dm.username, dm.password, saslData)
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
			return fmt.Errorf("authentication failed: %s", errorMsg)

		default:
			return fmt.Errorf("unexpected message type during auth: %c", msgType)
		}
	}
}

// createBackendConnection creates a new backend connection using raw TCP connection
func (dm *DatabaseManager) createBackendConnection() (*BackendConnection, error) {
	// Create TCP connection to PostgreSQL backend
	addr := fmt.Sprintf("%s:%d", dm.config.Host, dm.config.Port)

	conn, err := net.DialTimeout("tcp", addr, dm.config.ConnectTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	// Enable TCP keepalive to detect dead connections
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		// Enable keepalive
		tcpConn.SetKeepAlive(true)
		// Send keepalive probes every 30 seconds
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	// Handle SSL to backend if required
	if dm.config.SSLMode != "disable" {
		conn, err = dm.upgradeBackendToTLS(conn, addr)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to establish SSL to backend: %w", err)
		}
	}

	// Use target name if available, otherwise use database name
	targetName := dm.config.Name
	if dm.target != nil {
		targetName = dm.target.Name
	}

	// Create backend connection with proper reader/writer
	backend := NewBackendConnection(conn, dm.config.Name, targetName)

	// Authenticate with the backend
	if err := dm.authenticateBackend(backend); err != nil {
		conn.Close()
		return nil, fmt.Errorf("authentication failed: %w", err)
	}

	// CRITICAL: Verify buffer is clean after authentication
	buffered := backend.reader.Buffered()
	if buffered > 0 {
		// This shouldn't happen, but drain it if it does
		junk := make([]byte, buffered)
		if _, err := backend.reader.Read(junk); err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to drain buffer: %w", err)
		}
	}

	return backend, nil
}

// upgradeBackendToTLS upgrades a backend connection to TLS (DatabaseManager method)
func (dm *DatabaseManager) upgradeBackendToTLS(conn net.Conn, addr string) (net.Conn, error) {
	// Send SSL request to backend
	sslRequest := make([]byte, 8)
	binary.BigEndian.PutUint32(sslRequest[0:4], 8)
	binary.BigEndian.PutUint32(sslRequest[4:8], SSLRequestCode)

	if _, err := conn.Write(sslRequest); err != nil {
		return nil, fmt.Errorf("failed to send SSL request to backend: %w", err)
	}

	// Read response
	response := make([]byte, 1)
	if _, err := conn.Read(response); err != nil {
		return nil, fmt.Errorf("failed to read SSL response from backend: %w", err)
	}

	if response[0] == 'N' {
		// Backend doesn't support SSL
		if dm.config.SSLMode == "require" || dm.config.SSLMode == "verify-ca" || dm.config.SSLMode == "verify-full" {
			return nil, fmt.Errorf("backend does not support SSL but SSLMode is %s", dm.config.SSLMode)
		}
		// For "prefer", fall back to non-SSL
		return conn, nil
	}

	if response[0] != 'S' {
		return nil, fmt.Errorf("unexpected SSL response from backend: %c", response[0])
	}

	// Configure TLS for backend connection
	tlsConfig := &tls.Config{
		ServerName:         dm.config.Host,
		InsecureSkipVerify: dm.config.SSLMode == "require", // Only verify for verify-ca and verify-full
		MinVersion:         tls.VersionTLS12,
	}

	// Load CA certificate if specified and needed
	if (dm.config.SSLMode == "verify-ca" || dm.config.SSLMode == "verify-full") && dm.config.SSLCAFile != "" {
		caCert, err := os.ReadFile(dm.config.SSLCAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		tlsConfig.RootCAs = caCertPool
	}

	// Upgrade to TLS
	tlsConn := tls.Client(conn, tlsConfig)

	// Perform handshake
	if err := tlsConn.Handshake(); err != nil {
		return nil, fmt.Errorf("TLS handshake with backend failed: %w", err)
	}

	return tlsConn, nil
}
