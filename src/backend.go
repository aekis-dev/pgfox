package main

import (
	"bufio"
	"bytes"
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
	targetName     string
	inUse          bool
	createdAt      time.Time
	lastUsedAt     time.Time
	processID      int32
	secretKey      int32
	isListening    bool
	listenChannels map[string]bool
	clientRef      *ClientConnection
	mu             sync.Mutex
}

// NewBackendConnection creates a new backend connection
func NewBackendConnection(conn net.Conn, dbName, targetName string) *BackendConnection {
	return &BackendConnection{
		conn:           conn,
		reader:         bufio.NewReader(conn),
		writer:         bufio.NewWriter(conn),
		dbName:         dbName,
		targetName:     targetName,
		createdAt:      time.Now(),
		lastUsedAt:     time.Now(),
		listenChannels: make(map[string]bool),
	}
}

// Close closes the backend connection
func (b *BackendConnection) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.conn != nil {
		return b.conn.Close()
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

// IsListening returns whether the connection is listening for notifications
func (b *BackendConnection) IsListening() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.isListening
}

// SetListening sets the listening status
func (b *BackendConnection) SetListening(listening bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.isListening = listening
}

// AddListenChannel adds a channel to the listen set
func (b *BackendConnection) AddListenChannel(channel string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.listenChannels[channel] = true
}

// RemoveListenChannel removes a channel from the listen set
func (b *BackendConnection) RemoveListenChannel(channel string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.listenChannels, channel)
}

// GetListenChannels returns a copy of the listen channels
func (b *BackendConnection) GetListenChannels() map[string]bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	channels := make(map[string]bool)
	for ch, active := range b.listenChannels {
		channels[ch] = active
	}
	return channels
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
	if len(body) > 1024*1024 { // 1MB limit
		return fmt.Errorf("message body too large: %d bytes", len(body))
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

	if length > 1024*1024*10 { // 10MB limit for large result sets
		return 0, nil, fmt.Errorf("message length %d too large for type %c", length, msgType)
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
	n, err := netConn.Read(one)

	if err == nil && n > 0 {
		// Unexpected data - put it back and consider connection alive
		b.reader = bufio.NewReader(io.MultiReader(bytes.NewReader(one), netConn))
		return true
	}

	// Check if timeout (good - no data means alive)
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}

	// Any other error = closed
	return false
}
