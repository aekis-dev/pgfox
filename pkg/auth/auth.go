package auth

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

// ParseSASLMechanisms parses the list of supported SASL mechanisms from AuthenticationSASL message
func ParseSASLMechanisms(data []byte) []string {
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

// SCRAMVerifier holds the server-side SCRAM-SHA-256 verification data
// retrieved from pg_authid.rolpassword. PgFox uses this to act as a
// proper SCRAM server when authenticating clients without needing plaintext.
type SCRAMVerifier struct {
	Iterations int
	Salt       []byte // raw bytes decoded from base64
	StoredKey  []byte // HMAC-SHA-256(SHA-256(ClientKey), "")
	ServerKey  []byte // HMAC-SHA-256(SaltedPassword, "Server Key")
}

// ParseSCRAMVerifier parses a PostgreSQL SCRAM-SHA-256 verifier string from
// pg_authid.rolpassword. The format is:
//
//	SCRAM-SHA-256$<iterations>:<salt-base64>$<StoredKey-base64>:<ServerKey-base64>
func ParseSCRAMVerifier(rolpassword string) (*SCRAMVerifier, error) {
	const prefix = "SCRAM-SHA-256$"
	if !strings.HasPrefix(rolpassword, prefix) {
		return nil, fmt.Errorf("not a SCRAM-SHA-256 verifier: unexpected prefix")
	}

	rest := rolpassword[len(prefix):]

	// Split on "$" to separate "iterations:salt" from "StoredKey:ServerKey"
	parts := strings.SplitN(rest, "$", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid SCRAM verifier: missing $ separator")
	}

	// Parse iterations and salt
	iterSalt := strings.SplitN(parts[0], ":", 2)
	if len(iterSalt) != 2 {
		return nil, fmt.Errorf("invalid SCRAM verifier: missing iterations:salt separator")
	}

	iterations, err := strconv.Atoi(iterSalt[0])
	if err != nil {
		return nil, fmt.Errorf("invalid SCRAM verifier: bad iterations %q: %w", iterSalt[0], err)
	}

	salt, err := base64.StdEncoding.DecodeString(iterSalt[1])
	if err != nil {
		return nil, fmt.Errorf("invalid SCRAM verifier: bad salt base64: %w", err)
	}

	// Parse StoredKey and ServerKey
	keys := strings.SplitN(parts[1], ":", 2)
	if len(keys) != 2 {
		return nil, fmt.Errorf("invalid SCRAM verifier: missing StoredKey:ServerKey separator")
	}

	storedKey, err := base64.StdEncoding.DecodeString(keys[0])
	if err != nil {
		return nil, fmt.Errorf("invalid SCRAM verifier: bad StoredKey base64: %w", err)
	}

	serverKey, err := base64.StdEncoding.DecodeString(keys[1])
	if err != nil {
		return nil, fmt.Errorf("invalid SCRAM verifier: bad ServerKey base64: %w", err)
	}

	return &SCRAMVerifier{
		Iterations: iterations,
		Salt:       salt,
		StoredKey:  storedKey,
		ServerKey:  serverKey,
	}, nil
}
