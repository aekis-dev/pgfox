package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/pbkdf2"
)

// SCRAM-SHA-256 authentication implementation
type SCRAMAuth struct {
	username       string
	password       string
	clientNonce    string
	serverNonce    string
	salt           []byte
	iterations     int
	clientFirstMsg string
	serverFirstMsg string
	authMessage    string
}

// NewSCRAMAuth creates a new SCRAM-SHA-256 authenticator
func NewSCRAMAuth(username, password string) *SCRAMAuth {
	// Generate random client nonce
	nonce := make([]byte, 24)
	rand.Read(nonce)
	clientNonce := base64.StdEncoding.EncodeToString(nonce)

	return &SCRAMAuth{
		username:    username,
		password:    password,
		clientNonce: clientNonce,
	}
}

// BuildInitialResponse builds the SCRAM-SHA-256 initial client response
func (s *SCRAMAuth) BuildInitialResponse() []byte {
	// Client first message: "n,,n=username,r=clientnonce"
	s.clientFirstMsg = fmt.Sprintf("n=%s,r=%s", s.username, s.clientNonce)
	initialResponse := "n,," + s.clientFirstMsg
	return []byte(initialResponse)
}

// ProcessServerFirst processes the server's first response
func (s *SCRAMAuth) ProcessServerFirst(serverResponse []byte) ([]byte, error) {
	s.serverFirstMsg = string(serverResponse)

	// Parse server first message: "r=servernonce,s=salt,i=iterations"
	parts := strings.Split(s.serverFirstMsg, ",")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid server first message")
	}

	// Extract server nonce
	if !strings.HasPrefix(parts[0], "r=") {
		return nil, fmt.Errorf("invalid server nonce")
	}
	s.serverNonce = parts[0][2:]
	if !strings.HasPrefix(s.serverNonce, s.clientNonce) {
		return nil, fmt.Errorf("server nonce doesn't contain client nonce")
	}

	// Extract salt
	if !strings.HasPrefix(parts[1], "s=") {
		return nil, fmt.Errorf("invalid salt")
	}
	saltB64 := parts[1][2:]
	var err error
	s.salt, err = base64.StdEncoding.DecodeString(saltB64)
	if err != nil {
		return nil, fmt.Errorf("failed to decode salt: %w", err)
	}

	// Extract iterations
	if !strings.HasPrefix(parts[2], "i=") {
		return nil, fmt.Errorf("invalid iterations")
	}
	iterStr := parts[2][2:]
	s.iterations, err = strconv.Atoi(iterStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse iterations: %w", err)
	}

	// Build client final message
	return s.buildClientFinal()
}

// buildClientFinal builds the client final message
func (s *SCRAMAuth) buildClientFinal() ([]byte, error) {
	// Channel binding: "c=biws" (base64 of "n,,")
	channelBinding := "c=biws"

	// Client final without proof
	clientFinalWithoutProof := fmt.Sprintf("%s,r=%s", channelBinding, s.serverNonce)

	// Auth message
	s.authMessage = fmt.Sprintf("%s,%s,%s", s.clientFirstMsg, s.serverFirstMsg, clientFinalWithoutProof)

	// Calculate salted password
	saltedPassword := s.pbkdf2Hash([]byte(s.password), s.salt, s.iterations, 32)

	// Calculate client key
	clientKey := s.hmacSHA256(saltedPassword, []byte("Client Key"))

	// Calculate stored key
	storedKey := s.sha256Hash(clientKey)

	// Calculate client signature
	clientSignature := s.hmacSHA256(storedKey, []byte(s.authMessage))

	// Calculate client proof
	clientProof := s.xor(clientKey, clientSignature)
	clientProofB64 := base64.StdEncoding.EncodeToString(clientProof)

	// Build final message
	clientFinal := fmt.Sprintf("%s,p=%s", clientFinalWithoutProof, clientProofB64)

	return []byte(clientFinal), nil
}

// VerifyServerFinal verifies the server's final message
func (s *SCRAMAuth) VerifyServerFinal(serverFinal []byte) error {
	serverFinalStr := string(serverFinal)

	// Server final should be "v=serverSignature"
	if !strings.HasPrefix(serverFinalStr, "v=") {
		return fmt.Errorf("invalid server final message")
	}

	serverSignatureB64 := serverFinalStr[2:]
	serverSignature, err := base64.StdEncoding.DecodeString(serverSignatureB64)
	if err != nil {
		return fmt.Errorf("failed to decode server signature: %w", err)
	}

	// Calculate expected server signature
	saltedPassword := s.pbkdf2Hash([]byte(s.password), s.salt, s.iterations, 32)
	serverKey := s.hmacSHA256(saltedPassword, []byte("Server Key"))
	expectedServerSignature := s.hmacSHA256(serverKey, []byte(s.authMessage))

	// Verify signatures match
	if !hmac.Equal(serverSignature, expectedServerSignature) {
		return fmt.Errorf("server signature verification failed")
	}

	return nil
}

// parseSASLMechanisms parses the list of supported SASL mechanisms from AuthenticationSASL message
func parseSASLMechanisms(data []byte) []string {
	var mechanisms []string

	i := 0
	start := 0

	for i < len(data) {
		if data[i] == 0 {
			if i > start {
				mechanism := string(data[start:i])
				if mechanism != "" {
					mechanisms = append(mechanisms, mechanism)
				}
			}
			start = i + 1
		}
		i++
	}

	// Handle case where data doesn't end with null terminator
	if start < len(data) {
		mechanism := string(data[start:])
		if mechanism != "" {
			mechanisms = append(mechanisms, mechanism)
		}
	}

	return mechanisms
}

// Helper functions for SCRAM-SHA-256

func (s *SCRAMAuth) pbkdf2Hash(password, salt []byte, iterations, keyLen int) []byte {
	return pbkdf2.Key(password, salt, iterations, keyLen, sha256.New)
}

func (s *SCRAMAuth) hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func (s *SCRAMAuth) sha256Hash(data []byte) []byte {
	h := sha256.New()
	h.Write(data)
	return h.Sum(nil)
}

func (s *SCRAMAuth) xor(a, b []byte) []byte {
	result := make([]byte, len(a))
	for i := range result {
		result[i] = a[i] ^ b[i]
	}
	return result
}

// handleSCRAMAuth handles SCRAM-SHA-256 authentication
func handleSCRAMAuth(backend *BackendConnection, username, password string, saslData []byte) error {
	// Parse supported SASL mechanisms
	mechanisms := parseSASLMechanisms(saslData)

	// Check if SCRAM-SHA-256 is supported
	scramSupported := false
	for _, mech := range mechanisms {
		if mech == "SCRAM-SHA-256" {
			scramSupported = true
			break
		}
	}

	if !scramSupported {
		return fmt.Errorf("server does not support SCRAM-SHA-256, supported mechanisms: %v", mechanisms)
	}

	scram := NewSCRAMAuth(username, password)

	// Send initial response
	initialResponse := scram.BuildInitialResponse()

	// Build SASL initial response message
	mechanism := "SCRAM-SHA-256"
	msgLen := len(mechanism) + 1 + 4 + len(initialResponse)
	msg := make([]byte, msgLen)

	pos := 0
	copy(msg[pos:], mechanism)
	pos += len(mechanism)
	msg[pos] = 0
	pos++

	msg[pos] = byte(len(initialResponse) >> 24)
	msg[pos+1] = byte(len(initialResponse) >> 16)
	msg[pos+2] = byte(len(initialResponse) >> 8)
	msg[pos+3] = byte(len(initialResponse))
	pos += 4

	copy(msg[pos:], initialResponse)

	if err := backend.WriteMessage('p', msg); err != nil {
		return fmt.Errorf("failed to send SASL initial response: %w", err)
	}

	// Read server first response
	msgType, body, err := backend.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read SASL continue: %w", err)
	}

	if msgType == 'E' {
		errorMsg := parseErrorMessage(body)
		return fmt.Errorf("SCRAM authentication failed: %s", errorMsg)
	}

	if msgType != 'R' {
		return fmt.Errorf("expected authentication response, got %c", msgType)
	}

	if len(body) < 4 {
		return fmt.Errorf("invalid SASL continue response")
	}

	authType := uint32(body[0])<<24 | uint32(body[1])<<16 | uint32(body[2])<<8 | uint32(body[3])
	if authType != AuthenticationSASLContinue {
		return fmt.Errorf("expected SASL continue, got auth type %d", authType)
	}

	serverFirst := body[4:]
	clientFinal, err := scram.ProcessServerFirst(serverFirst)
	if err != nil {
		return fmt.Errorf("failed to process server first: %w", err)
	}

	// Send client final response
	if err := backend.WriteMessage('p', clientFinal); err != nil {
		return fmt.Errorf("failed to send client final: %w", err)
	}

	// Read server final response
	msgType, body, err = backend.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read server final: %w", err)
	}

	if msgType == 'E' {
		errorMsg := parseErrorMessage(body)
		return fmt.Errorf("SCRAM final authentication failed: %s", errorMsg)
	}

	if msgType != 'R' {
		return fmt.Errorf("expected authentication response, got %c", msgType)
	}

	if len(body) < 4 {
		return fmt.Errorf("invalid server final response")
	}

	authType = uint32(body[0])<<24 | uint32(body[1])<<16 | uint32(body[2])<<8 | uint32(body[3])
	if authType != AuthenticationSASLFinal {
		return fmt.Errorf("expected SASL final, got auth type %d", authType)
	}

	serverFinal := body[4:]
	if err := scram.VerifyServerFinal(serverFinal); err != nil {
		return fmt.Errorf("server verification failed: %w", err)
	}

	// CRITICAL: SCRAM auth is complete, but we MUST continue reading
	// until we get ReadyForQuery. The server will send:
	// - AuthenticationOK (R with type=0)
	// - ParameterStatus (S) messages
	// - BackendKeyData (K)
	// - ReadyForQuery (Z)

	authComplete := false

	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			return fmt.Errorf("failed to read post-SCRAM message: %w", err)
		}

		switch msgType {
		case 'R': // Should be AuthenticationOK
			if len(body) >= 4 {
				authType := uint32(body[0])<<24 | uint32(body[1])<<16 | uint32(body[2])<<8 | uint32(body[3])
				if authType == AuthenticationOK {
					authComplete = true
					continue
				}
			}
			return fmt.Errorf("unexpected auth message after SCRAM")

		case 'K': // Backend key data
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

		case 'S': // Parameter status
			if !authComplete {
				return fmt.Errorf("received ParameterStatus before AuthenticationOK")
			}
			continue

		case 'Z': // Ready for query - DONE!
			if !authComplete {
				return fmt.Errorf("received ReadyForQuery before AuthenticationOK")
			}
			return nil

		case 'E':
			errorMsg := parseErrorMessage(body)
			return fmt.Errorf("error after SCRAM: %s", errorMsg)

		case 'N':
			continue

		default:
			return fmt.Errorf("unexpected message type after SCRAM: %c", msgType)
		}
	}
}
