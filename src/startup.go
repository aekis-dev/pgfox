package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

// handleStartupMessage handles the PostgreSQL startup protocol.
func (p *Server) handleStartupMessage(client *ClientConnection) error {
	length, err := readUint32(client.reader)
	if err != nil {
		return fmt.Errorf("failed to read message length: %w", err)
	}

	p.logger.Debug("First message length", "length", length)

	if length == 8 {
		requestCode, err := readUint32(client.reader)
		if err != nil {
			return fmt.Errorf("failed to read request code: %w", err)
		}

		p.logger.Debug("Request code received", "code", requestCode)

		if requestCode == SSLRequestCode {
			if p.isSSLSupported() {
				p.logger.Debug("SSL request received, accepting")
				if err := client.writer.WriteByte('S'); err != nil {
					return fmt.Errorf("failed to send SSL acceptance: %w", err)
				}
				if err := client.writer.Flush(); err != nil {
					return fmt.Errorf("failed to flush SSL acceptance: %w", err)
				}
				if err := p.upgradeToTLS(client); err != nil {
					return fmt.Errorf("failed to upgrade to TLS: %w", err)
				}
				return p.handleStartupMessage(client)
			}

			p.logger.Debug("SSL not configured, rejecting")
			if err := client.writer.WriteByte('N'); err != nil {
				return fmt.Errorf("failed to send SSL rejection: %w", err)
			}
			if err := client.writer.Flush(); err != nil {
				return fmt.Errorf("failed to flush SSL rejection: %w", err)
			}
			return p.handleStartupMessage(client)

		} else if requestCode == CancelRequestCode {
			return p.handleCancelRequest(client)
		}

		return fmt.Errorf("unknown request code: %d", requestCode)
	}

	protocolVersion, err := readInt32(client.reader)
	if err != nil {
		return fmt.Errorf("failed to read protocol version: %w", err)
	}

	if protocolVersion != ProtocolVersion30 {
		return fmt.Errorf("unsupported protocol version: %d, expected %d", protocolVersion, ProtocolVersion30)
	}

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

	params := parseStartupParams(paramBytes)

	p.logger.Debug("Parsed startup parameters", "params", params)

	startupMsg := StartupMessage{
		ProtocolVersion: protocolVersion,
		Parameters:      params,
		User:            params["user"],
		Database:        params["database"],
	}

	if startupMsg.User == "" {
		return fmt.Errorf("missing required parameter: user")
	}
	if startupMsg.Database == "" {
		return fmt.Errorf("missing required parameter: database")
	}

	client.SetUser(startupMsg.User)
	client.SetDatabase(startupMsg.Database)

	logger := client.Logger().WithUser(startupMsg.User).WithDatabase(startupMsg.Database)
	logger.Debug("Client startup",
		"remote_addr", client.RemoteAddr(),
		"database", startupMsg.Database,
		"user", startupMsg.User)

	return p.authenticateClientWithBackend(client, startupMsg.User, startupMsg.Database)
}

// authenticateClientWithBackend authenticates a client using SCRAM-SHA-256,
// then ensures the backend pool for this (database, user) exists.
// No backend connection is opened here — the target goroutine owns all
// connection creation. Parameters come from target.params captured at startup.
func (p *Server) authenticateClientWithBackend(client *ClientConnection, user, database string) error {
	logger := client.Logger()

	// Get or create the pool — target selection is handled inside getPool.
	pool := p.getPool(database, user)
	if pool == nil {
		logger.Warn("No target serves database", "database", database)
		return sendErrorResponse(client, "FATAL", "3D000", "database not found")
	}

	target := pool.target

	// Check access control rules before doing any crypto work.
	if err := target.checkAccess(client.RemoteAddr(), user, database); err != nil {
		logger.Warn("Access denied", "user", user, "database", database, "client", client.RemoteAddr(), "target", target.Name)
		return sendErrorResponse(client, "FATAL", "28000", "access denied")
	}

	// Fetch live SCRAM verifier from pg_shadow via target.conn.
	verifier, err := p.getSCRAMVerifier(target, user)
	if err != nil {
		logger.WithError(err).Error("Failed to fetch SCRAM verifier")
		return sendErrorResponse(client, "FATAL", "28P01", "authentication failed")
	}

	// Run SCRAM-SHA-256 server exchange with the client.
	if err := p.handleClientSCRAM(client, user, verifier); err != nil {
		logger.WithError(err).Warn("Client SCRAM authentication failed")
		return sendErrorResponse(client, "FATAL", "28P01", "password authentication failed")
	}
	logger.Debug("Client SCRAM authentication successful")

	// Send authentication success to the client. The client will block
	// naturally on borrowConn when it sends its first query if no connection
	// is available yet — no need to wait here.
	if err := sendAuthenticationOK(client); err != nil {
		return fmt.Errorf("failed to send AuthenticationOK: %w", err)
	}
	if err := sendBackendKeyData(client, 0, 0); err != nil {
		return fmt.Errorf("failed to send BackendKeyData: %w", err)
	}
	if err := p.sendTargetParameterStatuses(client, user, target); err != nil {
		return err
	}
	if err := sendReadyForQuery(client, 'I'); err != nil {
		return fmt.Errorf("failed to send ReadyForQuery: %w", err)
	}

	client.SetAuthenticated(true)
	logger.Debug("Client authenticated",
		"target", target.Name,
		"database", database,
		"total_pool", pool.totalConnections(),
		"active_clients", atomic.LoadInt64(&p.stats.ActiveClients))

	return nil
}

// sendTargetParameterStatuses sends ParameterStatus messages to the client
// using the values captured from the target's privileged connection at startup.
// session_authorization is overridden to reflect the authenticated client user.
func (p *Server) sendTargetParameterStatuses(client *ClientConnection, user string, target *Target) error {
	overrides := map[string]string{"session_authorization": user}

	for name, value := range target.params {
		if ov, ok := overrides[name]; ok {
			value = ov
		}
		if err := sendParameterStatus(client, name, value); err != nil {
			return fmt.Errorf("failed to send parameter status %s: %w", name, err)
		}
	}
	for name, value := range overrides {
		if _, sent := target.params[name]; !sent {
			if err := sendParameterStatus(client, name, value); err != nil {
				return fmt.Errorf("failed to send parameter status %s: %w", name, err)
			}
		}
	}
	return nil
}

// getSCRAMVerifier fetches the SCRAM-SHA-256 verifier for a user from pg_authid
// via the target's privileged connection (target.conn).
func (p *Server) getSCRAMVerifier(target *Target, username string) (*SCRAMVerifier, error) {
	if target.conn == nil {
		return nil, fmt.Errorf("privileged connection not ready for target %s", target.Name)
	}

	query := fmt.Sprintf(
		"SELECT rolpassword FROM pg_authid WHERE rolname = '%s'",
		escapeSingleQuote(username))

	if err := target.conn.WriteMessage('Q', []byte(query+"\x00")); err != nil {
		return nil, fmt.Errorf("failed to send pg_shadow query: %w", err)
	}

	if err := target.conn.conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return nil, fmt.Errorf("failed to set read deadline: %w", err)
	}
	defer target.conn.conn.SetReadDeadline(time.Time{})

	var rolpassword string
	found := false

	for {
		msgType, body, err := target.conn.ReadMessage()
		if err != nil {
			return nil, fmt.Errorf("failed to read pg_shadow response: %w", err)
		}
		switch msgType {
		case 'T':
		case 'D':
			if len(body) < 2 {
				continue
			}
			pos := 2
			if pos+4 > len(body) {
				continue
			}
			colLen := int(int32(body[pos])<<24 | int32(body[pos+1])<<16 |
				int32(body[pos+2])<<8 | int32(body[pos+3]))
			pos += 4
			if colLen < 0 || pos+colLen > len(body) {
				continue
			}
			rolpassword = string(body[pos : pos+colLen])
			found = true
		case 'C':
		case 'Z':
			if !found {
				return nil, fmt.Errorf("user %q not found in pg_authid", username)
			}
			return parseSCRAMVerifier(rolpassword)
		case 'E':
			return nil, fmt.Errorf("pg_shadow query error: %s", parseErrorMessage(body))
		case 'N':
			continue
		}
	}
}

// createCertBackendConnection opens a new backend connection using TLS client
// certificate auth (verify-full). Used by auth for the seed connection and by
// the pool manager goroutine for all subsequent connections.
func (p *Server) createCertBackendConnection(target *Target, database, user string, cert tls.Certificate) (*BackendConnection, error) {
	addr := fmt.Sprintf("%s:%d", target.Host, target.Port)

	conn, err := net.DialTimeout("tcp", addr, target.ConnectTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	tlsCfg, err := p.backendTLSConfig(target.Host, cert)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}

	tlsConn, err := p.upgradeToCertTLS(conn, tlsCfg)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("TLS upgrade failed: %w", err)
	}

	backend := NewBackendConnection(tlsConn, database, target.Name, user, p.config.Server.MaxMessageSize)

	startupMsg := buildStartupMessage(user, database)
	if _, err := tlsConn.Write(startupMsg); err != nil {
		tlsConn.Close()
		return nil, fmt.Errorf("failed to send startup message: %w", err)
	}

	if err := p.drainBackendStartup(backend); err != nil {
		tlsConn.Close()
		return nil, fmt.Errorf("backend startup failed: %w", err)
	}

	return backend, nil
}

// upgradeToCertTLS sends the PostgreSQL SSL request and upgrades to TLS.
func (p *Server) upgradeToCertTLS(conn net.Conn, tlsCfg *tls.Config) (net.Conn, error) {
	sslRequest := make([]byte, 8)
	binary.BigEndian.PutUint32(sslRequest[0:4], 8)
	binary.BigEndian.PutUint32(sslRequest[4:8], SSLRequestCode)
	if _, err := conn.Write(sslRequest); err != nil {
		return nil, fmt.Errorf("failed to send SSL request: %w", err)
	}

	response := make([]byte, 1)
	if _, err := conn.Read(response); err != nil {
		return nil, fmt.Errorf("failed to read SSL response: %w", err)
	}
	if response[0] != 'S' {
		conn.Close()
		return nil, fmt.Errorf(
			"backend rejected SSL (response=%q) — ensure postgresql.conf has ssl=on "+
				"and pg_hba.conf uses hostssl with clientcert=verify-full", string(response))
	}

	tlsConn := tls.Client(conn, tlsCfg)
	if err := tlsConn.Handshake(); err != nil {
		return nil, fmt.Errorf("TLS handshake failed: %w", err)
	}
	return tlsConn, nil
}

// drainBackendStartup reads ParameterStatus, BackendKeyData, ReadyForQuery
// after the startup message. Cert-auth connections receive AuthenticationOK
// without a challenge.
func (p *Server) drainBackendStartup(backend *BackendConnection) error {
	authComplete := false
	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			return fmt.Errorf("failed to read backend startup: %w", err)
		}
		switch msgType {
		case 'R':
			if len(body) < 4 {
				return fmt.Errorf("invalid auth response")
			}
			authType := uint32(body[0])<<24 | uint32(body[1])<<16 | uint32(body[2])<<8 | uint32(body[3])
			if authType == AuthenticationOK {
				authComplete = true
				continue
			}
			return fmt.Errorf("unexpected auth type %d on cert connection — check pg_hba.conf", authType)
		case 'S':
			parts := splitNullTerminated(body)
			if len(parts) == 2 {
				backend.parameters[parts[0]] = parts[1]
			}
		case 'K':
			if len(body) >= 8 {
				processID := int32(body[0])<<24 | int32(body[1])<<16 | int32(body[2])<<8 | int32(body[3])
				secretKey := int32(body[4])<<24 | int32(body[5])<<16 | int32(body[6])<<8 | int32(body[7])
				backend.SetProcessID(processID)
				backend.SetSecretKey(secretKey)
			}
		case 'Z':
			if !authComplete {
				return fmt.Errorf("received ReadyForQuery before AuthenticationOK")
			}
			return nil
		case 'E':
			return fmt.Errorf("backend error during startup: %s", parseErrorMessage(body))
		case 'N':
			continue
		}
	}
}

// handleClientSCRAM runs the full SCRAM-SHA-256 server exchange with a client.
func (p *Server) handleClientSCRAM(client *ClientConnection, user string, verifier *SCRAMVerifier) error {
	logger := client.Logger()

	// Step 1: Advertise SCRAM-SHA-256.
	mechanism := "SCRAM-SHA-256"
	saslBody := make([]byte, 4+len(mechanism)+2)
	saslBody[3] = byte(AuthenticationSASL)
	copy(saslBody[4:], mechanism)
	if err := client.WriteMessage('R', saslBody); err != nil {
		return fmt.Errorf("failed to send AuthSASL: %w", err)
	}

	// Step 2: Read client-first-message.
	msgType, body, err := client.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read SASLInitialResponse: %w", err)
	}
	if msgType != 'p' {
		return fmt.Errorf("expected SASLInitialResponse, got %c", msgType)
	}

	clientFirst, err := parseSASLInitialResponse(body)
	if err != nil {
		return fmt.Errorf("bad SASLInitialResponse: %w", err)
	}

	clientFirstBare, err := extractClientFirstBare(clientFirst)
	if err != nil {
		return fmt.Errorf("bad client-first-message: %w", err)
	}

	clientNonce, err := extractNonce(clientFirstBare)
	if err != nil {
		return fmt.Errorf("failed to extract client nonce: %w", err)
	}

	// Step 3: Send server-first-message.
	serverNonceSuffix := make([]byte, 18)
	if _, err := rand.Read(serverNonceSuffix); err != nil {
		return fmt.Errorf("failed to generate server nonce: %w", err)
	}
	serverNonce := clientNonce + base64.StdEncoding.EncodeToString(serverNonceSuffix)
	saltB64 := base64.StdEncoding.EncodeToString(verifier.Salt)
	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d", serverNonce, saltB64, verifier.Iterations)

	saslContBody := make([]byte, 4+len(serverFirst))
	saslContBody[3] = byte(AuthenticationSASLContinue)
	copy(saslContBody[4:], serverFirst)
	if err := client.WriteMessage('R', saslContBody); err != nil {
		return fmt.Errorf("failed to send AuthSASLContinue: %w", err)
	}

	// Step 4: Read client-final-message.
	msgType, body, err = client.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read SASLResponse: %w", err)
	}
	if msgType != 'p' {
		return fmt.Errorf("expected SASLResponse, got %c", msgType)
	}
	clientFinal := string(body)

	// Step 5: Verify client proof.
	clientFinalWithoutProof, clientProofB64, err := splitClientFinal(clientFinal)
	if err != nil {
		return fmt.Errorf("bad client-final-message: %w", err)
	}

	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof

	clientProof, err := base64.StdEncoding.DecodeString(clientProofB64)
	if err != nil {
		return fmt.Errorf("bad client proof encoding: %w", err)
	}

	clientSig := scramHMAC(verifier.StoredKey, []byte(authMessage))
	clientKey := scramXOR(clientProof, clientSig)

	if !hmac.Equal(scramHash(clientKey), verifier.StoredKey) {
		return fmt.Errorf("client proof verification failed")
	}

	// Step 6: Send server-final-message with server signature.
	serverSig := scramHMAC(verifier.ServerKey, []byte(authMessage))
	serverFinalMsg := "v=" + base64.StdEncoding.EncodeToString(serverSig)

	saslFinalBody := make([]byte, 4+len(serverFinalMsg))
	saslFinalBody[3] = byte(AuthenticationSASLFinal)
	copy(saslFinalBody[4:], serverFinalMsg)
	if err := client.WriteMessage('R', saslFinalBody); err != nil {
		return fmt.Errorf("failed to send AuthSASLFinal: %w", err)
	}

	logger.Debug("SCRAM exchange complete", "user", user)
	return nil
}

// handleCancelRequest processes a PostgreSQL cancel request.
func (p *Server) handleCancelRequest(client *ClientConnection) error {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(client.reader, buf); err != nil {
		return fmt.Errorf("failed to read cancel data: %w", err)
	}

	processID := int32(buf[0])<<24 | int32(buf[1])<<16 | int32(buf[2])<<8 | int32(buf[3])
	secretKey := int32(buf[4])<<24 | int32(buf[5])<<16 | int32(buf[6])<<8 | int32(buf[7])

	p.logger.Debug("Cancel request received", "process_id", processID)

	cancelTarget, backend := p.findBackendByKey(processID, secretKey)
	if backend == nil {
		p.logger.Debug("Cancel: no matching backend", "process_id", processID)
		return nil
	}

	addr := fmt.Sprintf("%s:%d", cancelTarget.Host, cancelTarget.Port)
	cancelConn, err := net.DialTimeout("tcp", addr, p.config.Server.ConnectTimeout)
	if err != nil {
		p.logger.WithError(err).Warn("Cancel: failed to reach backend")
		return nil
	}
	defer cancelConn.Close()

	msg := make([]byte, 16)
	binary.BigEndian.PutUint32(msg[0:4], 16)
	binary.BigEndian.PutUint32(msg[4:8], CancelRequestCode)
	binary.BigEndian.PutUint32(msg[8:12], uint32(processID))
	binary.BigEndian.PutUint32(msg[12:16], uint32(secretKey))

	if _, err := cancelConn.Write(msg); err != nil {
		p.logger.WithError(err).Warn("Cancel: failed to send")
		return nil
	}

	p.logger.Debug("Cancel forwarded", "process_id", processID, "backend", addr)
	return nil
}

// --- SCRAM helper functions ---

func scramHMAC(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func scramHash(data []byte) []byte {
	h := sha256.New()
	h.Write(data)
	return h.Sum(nil)
}

func scramXOR(a, b []byte) []byte {
	out := make([]byte, len(a))
	for i := range out {
		out[i] = a[i] ^ b[i]
	}
	return out
}

// parseSASLInitialResponse extracts the client-first-message from the body.
func parseSASLInitialResponse(body []byte) (string, error) {
	if len(body) == 0 {
		return "", fmt.Errorf("empty SASLInitialResponse")
	}
	nullIdx := -1
	for i, b := range body {
		if b == 0 {
			nullIdx = i
			break
		}
	}
	if nullIdx >= 0 && nullIdx < len(body)-5 {
		pos := nullIdx + 1
		if pos+4 > len(body) {
			return "", fmt.Errorf("truncated SASLInitialResponse")
		}
		msgLen := int(int32(body[pos])<<24 | int32(body[pos+1])<<16 |
			int32(body[pos+2])<<8 | int32(body[pos+3]))
		pos += 4
		if msgLen < 0 {
			return "", fmt.Errorf("empty client-first-message")
		}
		if pos+msgLen > len(body) {
			return "", fmt.Errorf("truncated client-first-message")
		}
		return string(body[pos : pos+msgLen]), nil
	}
	return string(body), nil
}

// extractClientFirstBare strips the GS2 header from the client-first-message.
func extractClientFirstBare(clientFirst string) (string, error) {
	idx := strings.Index(clientFirst, ",,")
	if idx < 0 {
		return "", fmt.Errorf("no GS2 header in client-first-message")
	}
	return clientFirst[idx+2:], nil
}

// extractNonce returns the r= value from a SCRAM message.
func extractNonce(msg string) (string, error) {
	for _, part := range strings.Split(msg, ",") {
		if strings.HasPrefix(part, "r=") {
			return part[2:], nil
		}
	}
	return "", fmt.Errorf("no nonce (r=) in: %s", msg)
}

// splitClientFinal splits client-final into (without-proof, proof-base64).
func splitClientFinal(clientFinal string) (string, string, error) {
	idx := strings.LastIndex(clientFinal, ",p=")
	if idx < 0 {
		return "", "", fmt.Errorf("no proof (p=) in client-final-message")
	}
	return clientFinal[:idx], clientFinal[idx+3:], nil
}

// splitNullTerminated splits b on null bytes, returning non-empty parts.
func splitNullTerminated(b []byte) []string {
	var parts []string
	start := 0
	for i, c := range b {
		if c == 0 {
			if i > start {
				parts = append(parts, string(b[start:i]))
			}
			start = i + 1
		}
	}
	if start < len(b) {
		parts = append(parts, string(b[start:]))
	}
	return parts
}

// escapeSingleQuote escapes single quotes in a string for use in SQL literals.
func escapeSingleQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// authenticateBackendWithPassword authenticates with backend using credentials.
// Kept for any future path that needs password-based backend auth.
func (p *Server) authenticateBackendWithPassword(backend *BackendConnection, user, password string) error {
	authComplete := false

	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			return fmt.Errorf("failed to read auth response: %w", err)
		}

		switch msgType {
		case 'R':
			if len(body) < 4 {
				return fmt.Errorf("invalid authentication response")
			}
			authType := uint32(body[0])<<24 | uint32(body[1])<<16 | uint32(body[2])<<8 | uint32(body[3])
			switch authType {
			case AuthenticationOK:
				authComplete = true
				continue
			case AuthenticationCleartextPassword:
				passMsg := []byte(password + "\x00")
				if err := backend.WriteMessage('p', passMsg); err != nil {
					return fmt.Errorf("failed to send password: %w", err)
				}
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
				continue
			case AuthenticationSASL:
				if len(body) < 4 {
					return fmt.Errorf("invalid SASL auth response")
				}
				return handleSCRAMAuth(backend, user, password, body[4:])
			default:
				return fmt.Errorf("unsupported authentication type: %d", authType)
			}

		case 'K':
			if !authComplete {
				return fmt.Errorf("received BackendKeyData before AuthenticationOK")
			}
			if len(body) >= 8 {
				processID := int32(body[0])<<24 | int32(body[1])<<16 | int32(body[2])<<8 | int32(body[3])
				secretKey := int32(body[4])<<24 | int32(body[5])<<16 | int32(body[6])<<8 | int32(body[7])
				backend.SetProcessID(processID)
				backend.SetSecretKey(secretKey)
			}
			continue

		case 'S':
			continue

		case 'Z':
			if !authComplete {
				return fmt.Errorf("received ReadyForQuery before AuthenticationOK")
			}
			return nil

		case 'E':
			return fmt.Errorf("authentication failed: %s", parseErrorMessage(body))

		case 'N':
			continue

		default:
			return fmt.Errorf("unexpected message type during auth: %c", msgType)
		}
	}
}

// getAuthTypeName returns a human-readable name for a PostgreSQL auth type.
func getAuthTypeName(authType uint32) string {
	switch authType {
	case 0:
		return "AuthenticationOK"
	case 3:
		return "AuthenticationCleartextPassword"
	case 5:
		return "AuthenticationMD5"
	case 10:
		return "AuthenticationSASL"
	case 11:
		return "AuthenticationSASLContinue"
	case 12:
		return "AuthenticationSASLFinal"
	default:
		return fmt.Sprintf("Unknown(%d)", authType)
	}
}
