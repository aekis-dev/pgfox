package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// ClientConnection represents a client connection
type ClientConnection struct {
	conn           net.Conn
	reader         *bufio.Reader
	writer         *bufio.Writer
	backendConn    *BackendConnection
	authenticated  bool
	database       string
	user           string
	inTransaction  bool
	isListening    bool
	sessionMode    bool // Track if this client is in session mode
	listenChannels map[string]bool
	logger         *Logger
	mu             sync.Mutex
	connectedAt    time.Time
	lastActivity   time.Time
}

// NewClientConnection creates a new client connection
func NewClientConnection(conn net.Conn, logger *Logger) *ClientConnection {
	now := time.Now()
	return &ClientConnection{
		conn:           conn,
		reader:         bufio.NewReader(conn),
		writer:         bufio.NewWriter(conn),
		listenChannels: make(map[string]bool),
		logger:         logger,
		connectedAt:    now,
		lastActivity:   now,
	}
}

// RemoteAddr returns the client's remote address
func (c *ClientConnection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Close closes the client connection
func (c *ClientConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If we have a dedicated backend connection, close it
	if c.backendConn != nil {
		c.backendConn.Close()
		c.backendConn = nil
	}

	return c.conn.Close()
}

// IsAuthenticated returns whether the client is authenticated
func (c *ClientConnection) IsAuthenticated() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.authenticated
}

// SetAuthenticated sets the authentication status
func (c *ClientConnection) SetAuthenticated(auth bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.authenticated = auth
	if auth {
		c.lastActivity = time.Now()
	}
}

// GetDatabase returns the requested database name
func (c *ClientConnection) GetDatabase() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.database
}

// SetDatabase sets the requested database name
func (c *ClientConnection) SetDatabase(database string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.database = database
}

// GetUser returns the client username
func (c *ClientConnection) GetUser() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.user
}

// SetUser sets the client username
func (c *ClientConnection) SetUser(user string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.user = user
}

// IsInTransaction returns whether the client is in a transaction
func (c *ClientConnection) IsInTransaction() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inTransaction
}

// SetInTransaction sets the transaction state
func (c *ClientConnection) SetInTransaction(inTx bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inTransaction = inTx
	c.lastActivity = time.Now()

	// Log transaction state changes for debugging
	if inTx {
		c.logger.Debug("Client entered transaction")
	} else {
		c.logger.Debug("Client exited transaction")
	}
}

// IsSessionMode returns whether the client is in session mode
func (c *ClientConnection) IsSessionMode() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionMode
}

// SetSessionMode sets the session mode
func (c *ClientConnection) SetSessionMode(sessionMode bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessionMode = sessionMode
	if sessionMode {
		c.logger.Debug("Client set to session mode")
	}
}

// IsListening returns whether the client is listening for notifications
func (c *ClientConnection) IsListening() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isListening
}

// SetListening sets the listening state
func (c *ClientConnection) SetListening(listening bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.isListening = listening
}

// AddListenChannel adds a channel to the listen set
func (c *ClientConnection) AddListenChannel(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listenChannels[channel] = true
	c.isListening = true
	c.lastActivity = time.Now()
}

// RemoveListenChannel removes a channel from the listen set
func (c *ClientConnection) RemoveListenChannel(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.listenChannels, channel)
	if len(c.listenChannels) == 0 {
		c.isListening = false
	}
}

// GetListenChannels returns a copy of the listen channels
func (c *ClientConnection) GetListenChannels() map[string]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	channels := make(map[string]bool)
	for ch, active := range c.listenChannels {
		channels[ch] = active
	}
	return channels
}

// ClearListenChannels removes all listen channels
func (c *ClientConnection) ClearListenChannels() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listenChannels = make(map[string]bool)
	c.isListening = false
}

// GetBackendConnection returns the associated backend connection
func (c *ClientConnection) GetBackendConnection() *BackendConnection {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.backendConn
}

// SetBackendConnection sets the associated backend connection
func (c *ClientConnection) SetBackendConnection(conn *BackendConnection) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.backendConn != nil && c.backendConn != conn {
		c.logger.Debug("Replacing existing backend connection")
		// Don't close the old connection here - let the pooler handle it
	}

	c.backendConn = conn
	c.lastActivity = time.Now()

	if conn != nil {
		c.logger.Debug("Backend connection assigned",
			"backend_addr", conn.RemoteAddr(),
			"backend_db", conn.GetDatabase())
	}
}

// ShouldKeepBackendConnection returns true if the backend connection should be kept
func (c *ClientConnection) ShouldKeepBackendConnection() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Keep connection if in session mode, transaction, or listening
	return c.sessionMode || c.inTransaction || c.isListening
}

// WriteMessage writes a message to the client
func (c *ClientConnection) WriteMessage(msgType byte, body []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update activity timestamp
	c.lastActivity = time.Now()

	// Validate message parameters
	if len(body) > 1024*1024*10 { // 10MB limit
		return fmt.Errorf("message body too large: %d bytes", len(body))
	}

	// Write message type
	if err := c.writer.WriteByte(msgType); err != nil {
		return fmt.Errorf("failed to write message type %c to client: %w", msgType, err)
	}

	// Write message length (including length field itself)
	length := uint32(len(body) + 4)
	if err := writeUint32(c.writer, length); err != nil {
		return fmt.Errorf("failed to write message length %d to client: %w", length, err)
	}

	// Write message body
	if len(body) > 0 {
		if _, err := c.writer.Write(body); err != nil {
			return fmt.Errorf("failed to write message body (%d bytes) to client: %w", len(body), err)
		}
	}

	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush message type=%c len=%d to client: %w", msgType, length, err)
	}

	return nil
}

// Enhanced ReadMessage for client connections
func (c *ClientConnection) ReadMessage() (byte, []byte, error) {
	// Read message type
	msgType, err := c.reader.ReadByte()
	if err != nil {
		if err == io.EOF {
			return 0, nil, io.EOF
		}
		return 0, nil, fmt.Errorf("failed to read message type from client: %w", err)
	}

	// Read message length
	length, err := readUint32(c.reader)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read message length after type %c from client: %w", msgType, err)
	}

	// Validate message length
	if length < 4 {
		return 0, nil, fmt.Errorf("invalid message length %d for type %c from client (must be >= 4)", length, msgType)
	}

	if length > 1024*1024 { // 1MB limit for client messages
		return 0, nil, fmt.Errorf("message length %d too large for type %c from client", length, msgType)
	}

	// Read message body (length includes the length field itself)
	bodyLength := int(length - 4)
	body := make([]byte, bodyLength)
	if bodyLength > 0 {
		if _, err := io.ReadFull(c.reader, body); err != nil {
			return 0, nil, fmt.Errorf("failed to read message body (%d bytes) for type %c from client: %w", bodyLength, msgType, err)
		}
	}

	// Update activity timestamp
	c.mu.Lock()
	c.lastActivity = time.Now()
	c.mu.Unlock()

	return msgType, body, nil
}

// GetConnectedAt returns when the client connected
func (c *ClientConnection) GetConnectedAt() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connectedAt
}

// GetLastActivity returns the last activity time
func (c *ClientConnection) GetLastActivity() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastActivity
}

// GetConnectionDuration returns how long the client has been connected
func (c *ClientConnection) GetConnectionDuration() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Since(c.connectedAt)
}

// GetIdleDuration returns how long the client has been idle
func (c *ClientConnection) GetIdleDuration() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Since(c.lastActivity)
}

// GetConnectionInfo returns connection information for debugging
func (c *ClientConnection) GetConnectionInfo() map[string]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	info := map[string]interface{}{
		"remote_addr":         c.conn.RemoteAddr().String(),
		"database":            c.database,
		"user":                c.user,
		"authenticated":       c.authenticated,
		"in_transaction":      c.inTransaction,
		"is_listening":        c.isListening,
		"session_mode":        c.sessionMode,
		"connected_at":        c.connectedAt,
		"last_activity":       c.lastActivity,
		"connection_duration": time.Since(c.connectedAt),
		"idle_duration":       time.Since(c.lastActivity),
		"listen_channels":     len(c.listenChannels),
	}

	if c.backendConn != nil {
		info["has_backend"] = true
		info["backend_addr"] = c.backendConn.RemoteAddr().String()
		info["backend_db"] = c.backendConn.GetDatabase()
	} else {
		info["has_backend"] = false
	}

	return info
}

// Logger returns the client's logger
func (c *ClientConnection) Logger() *Logger {
	return c.logger
}
