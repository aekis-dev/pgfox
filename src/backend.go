package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// BackendConnection represents a connection to PostgreSQL backend
type BackendConnection struct {
	conn           net.Conn
	reader         *bufio.Reader
	writer         *bufio.Writer
	dbName         string
	username       string
	targetName     string
	inUse          bool
	createdAt      time.Time
	lastUsedAt     time.Time
	processID      int32
	secretKey      int32
	clientRef      *ClientConnection
	maxMessageSize int               // maximum allowed PostgreSQL message body size in bytes
	parameters     map[string]string // ParameterStatus values received during backend auth
	mu             sync.Mutex
}

// NewBackendConnection creates a new backend connection
func NewBackendConnection(conn net.Conn, dbName, targetName, username string, maxMessageSize int) *BackendConnection {
	return &BackendConnection{
		conn:           conn,
		reader:         bufio.NewReader(conn),
		writer:         bufio.NewWriter(conn),
		dbName:         dbName,
		username:       username,
		targetName:     targetName,
		createdAt:      time.Now(),
		lastUsedAt:     time.Now(),
		maxMessageSize: maxMessageSize,
		parameters:     make(map[string]string),
	}
}

// Close closes the backend connection
func (b *BackendConnection) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.conn != nil {
		addr := b.conn.RemoteAddr()
		err := b.conn.Close()
		if err != nil {
			// Log with context about which connection closed
			return fmt.Errorf("failed to close backend %v: %w", addr, err)
		}
		// Explicitly log successful close
		return nil
	}
	return nil
}

// IsInUse returns whether the connection is currently in use
func (b *BackendConnection) IsInUse() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inUse
}

// SetInUse sets the in-use status
func (b *BackendConnection) SetInUse(inUse bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.inUse = inUse
	if !inUse {
		// Update last used time when released
		b.lastUsedAt = time.Now()
	}
}

// GetDatabase returns the database name
func (b *BackendConnection) GetDatabase() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dbName
}

// GetTarget returns the target name
func (b *BackendConnection) GetTarget() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.targetName
}

// CreatedAt returns when the connection was created
func (b *BackendConnection) CreatedAt() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.createdAt
}

// LastUsedAt returns when the connection was last used
func (b *BackendConnection) LastUsedAt() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastUsedAt
}

// GetProcessID returns the backend process ID
func (b *BackendConnection) GetProcessID() int32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.processID
}

// SetProcessID sets the backend process ID
func (b *BackendConnection) SetProcessID(processID int32) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.processID = processID
}

// GetSecretKey returns the backend secret key
func (b *BackendConnection) GetSecretKey() int32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.secretKey
}

// SetSecretKey sets the backend secret key
func (b *BackendConnection) SetSecretKey(secretKey int32) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.secretKey = secretKey
}

// GetClientRef returns the associated client connection
func (b *BackendConnection) GetClientRef() *ClientConnection {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.clientRef
}

// SetClientRef sets the associated client connection
func (b *BackendConnection) SetClientRef(client *ClientConnection) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.clientRef = client
}

// WriteMessage writes a message to the backend
func (b *BackendConnection) WriteMessage(msgType byte, body []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Validate message parameters
	if len(body) > b.maxMessageSize {
		return fmt.Errorf("message body %d bytes exceeds max_message_size %d", len(body), b.maxMessageSize)
	}

	// Write message type
	if err := b.writer.WriteByte(msgType); err != nil {
		return fmt.Errorf("failed to write message type %c: %w", msgType, err)
	}

	// Write message length (including length field itself)
	length := uint32(len(body) + 4)
	if err := writeUint32(b.writer, length); err != nil {
		return fmt.Errorf("failed to write message length %d: %w", length, err)
	}

	// Write message body
	if len(body) > 0 {
		if _, err := b.writer.Write(body); err != nil {
			return fmt.Errorf("failed to write message body (%d bytes): %w", len(body), err)
		}
	}

	if err := b.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush message type=%c len=%d: %w", msgType, length, err)
	}

	return nil
}

// ReadMessage reads a message from the backend
func (b *BackendConnection) ReadMessage() (byte, []byte, error) {
	// Read message type
	msgType, err := b.reader.ReadByte()
	if err != nil {
		if err == io.EOF {
			return 0, nil, fmt.Errorf("backend connection closed")
		}
		return 0, nil, fmt.Errorf("failed to read message type: %w", err)
	}

	// Read message length
	length, err := readUint32(b.reader)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read message length after type %c: %w", msgType, err)
	}

	// Validate message length
	if length < 4 {
		return 0, nil, fmt.Errorf("invalid message length %d for type %c (must be >= 4)", length, msgType)
	}

	if length > uint32(b.maxMessageSize) {
		return 0, nil, fmt.Errorf("message length %d exceeds max_message_size %d for type %c", length, b.maxMessageSize, msgType)
	}

	// Read message body (length includes the length field itself)
	bodyLength := int(length - 4)
	body := make([]byte, bodyLength)
	if bodyLength > 0 {
		if _, err := io.ReadFull(b.reader, body); err != nil {
			return 0, nil, fmt.Errorf("failed to read message body (%d bytes) for type %c: %w", bodyLength, msgType, err)
		}
	}

	return msgType, body, nil
}

// RemoteAddr returns the backend's remote address
func (b *BackendConnection) RemoteAddr() net.Addr {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn != nil {
		return b.conn.RemoteAddr()
	}
	return nil
}

// LocalAddr returns the backend's local address
func (b *BackendConnection) LocalAddr() net.Addr {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn != nil {
		return b.conn.LocalAddr()
	}
	return nil
}

// IsAlive checks if the connection is still alive
func (b *BackendConnection) IsAlive() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.conn == nil {
		return false
	}

	// Quick non-blocking check
	netConn, ok := b.conn.(net.Conn)
	if !ok {
		return false
	}

	// Set immediate deadline
	if err := netConn.SetReadDeadline(time.Now().Add(1 * time.Millisecond)); err != nil {
		return false
	}
	defer netConn.SetReadDeadline(time.Time{})

	// Try to read one byte
	one := make([]byte, 1)
	_, err := netConn.Read(one)

	// Check if timeout (good - no data means alive and idle)
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}

	// Any other error = closed
	return false
}

func (b *BackendConnection) Peek(n int) ([]byte, error) {
	return b.reader.Peek(n)
}
