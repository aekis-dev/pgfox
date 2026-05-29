package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ClientConnection represents a client connection.
//
// Lock discipline — two separate locks instead of one coarse mutex:
//
//   - writeMu: serialises concurrent writers on the underlying TCP connection.
//     Only WriteMessage acquires it. The listen-monitor goroutine is the one
//     concurrent writer; every other write is from the owning client goroutine.
//
//   - sharedMu: protects the small set of fields that genuinely cross goroutine
//     boundaries: backendConn, lastActivity, listenChannels/isListening.
//
// All other fields (authenticated, database, user, password, inTransaction,
// namedStmts, connectedAt, stmtNameMap/stmtRevMap) are owned exclusively by
// the single goroutine handling this client and need no locking.
type ClientConnection struct {
	conn           net.Conn
	reader         *bufio.Reader
	writer         *bufio.Writer
	backendConn    *BackendConnection
	authenticated  bool
	database       string
	user           string
	password       string
	inTransaction  bool
	namedStmts     int // count of named prepared statements active on pinned backend
	isListening    bool
	listenChannels map[Channel]bool
	logger         *Logger
	writeMu        sync.Mutex
	sharedMu       sync.Mutex
	connectedAt    time.Time
	lastActivity   time.Time

	maxMessageSize int // maximum allowed PostgreSQL message body size in bytes

	// stmtNameMap maps client-visible statement names to internal pgfox hashes.
	// stmtRevMap is the reverse: hash → client name. Both are written only from
	// the goroutine handling this client connection, so no lock is needed.
	stmtNameMap map[string]string // clientName → hash
	stmtRevMap  map[string]string // hash → clientName

	// msgsSent is incremented atomically for metrics. Kept here for cache
	// locality; the owning goroutine is the sole writer so atomic is enough.
	msgsSent int64
}

// NewClientConnection creates a new client connection
func NewClientConnection(conn net.Conn, logger *Logger, maxMessageSize int) *ClientConnection {
	now := time.Now()
	return &ClientConnection{
		conn:           conn,
		reader:         bufio.NewReader(conn),
		writer:         bufio.NewWriter(conn),
		listenChannels: make(map[Channel]bool),
		logger:         logger,
		connectedAt:    now,
		lastActivity:   now,
		maxMessageSize: maxMessageSize,
		stmtNameMap:    make(map[string]string),
		stmtRevMap:     make(map[string]string),
	}
}

// RemoteAddr returns the client's remote address
func (c *ClientConnection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Close closes the client connection
func (c *ClientConnection) Close() error {
	c.sharedMu.Lock()
	bc := c.backendConn
	c.backendConn = nil
	c.sharedMu.Unlock()

	if bc != nil {
		bc.Close()
	}
	return c.conn.Close()
}

func (c *ClientConnection) IsAuthenticated() bool   { return c.authenticated }
func (c *ClientConnection) SetAuthenticated(v bool) { c.authenticated = v }

func (c *ClientConnection) GetDatabase() string   { return c.database }
func (c *ClientConnection) SetDatabase(v string)  { c.database = v }
func (c *ClientConnection) GetUser() string       { return c.user }
func (c *ClientConnection) SetUser(v string)      { c.user = v }
func (c *ClientConnection) SetPassword(v string)  { c.password = v }
func (c *ClientConnection) GetPassword() string   { return c.password }
func (c *ClientConnection) IsInTransaction() bool { return c.inTransaction }

func (c *ClientConnection) AddNamedStatement() { c.namedStmts++ }
func (c *ClientConnection) RemoveNamedStatement() {
	if c.namedStmts > 0 {
		c.namedStmts--
	}
}
func (c *ClientConnection) HasNamedStatements() bool { return c.namedStmts > 0 }

func (c *ClientConnection) SetInTransaction(inTx bool) {
	c.inTransaction = inTx
	if inTx {
		c.logger.Debug("Client entered transaction")
	} else {
		c.logger.Debug("Client exited transaction")
	}
}

func (c *ClientConnection) GetConnectedAt() time.Time { return c.connectedAt }

// --- sharedMu-protected accessors ---

func (c *ClientConnection) GetLastActivity() time.Time {
	c.sharedMu.Lock()
	t := c.lastActivity
	c.sharedMu.Unlock()
	return t
}

func (c *ClientConnection) GetBackendConnection() *BackendConnection {
	c.sharedMu.Lock()
	bc := c.backendConn
	c.sharedMu.Unlock()
	return bc
}

func (c *ClientConnection) SetBackendConnection(conn *BackendConnection) {
	c.sharedMu.Lock()
	if c.backendConn != nil && c.backendConn != conn {
		if conn != nil {
			c.logger.Debug("Replacing existing backend connection")
		} else {
			c.logger.Debug("Clearing backend connection reference")
		}
	}
	c.backendConn = conn
	c.lastActivity = time.Now()
	c.sharedMu.Unlock()

	if conn != nil {
		c.logger.Debug("Backend connection assigned",
			"backend_addr", conn.RemoteAddr(),
			"backend_db", conn.GetDatabase())
	}
}

func (c *ClientConnection) ShouldKeepBackendConnection() bool {
	return c.inTransaction // single-goroutine field, no lock needed
}

// --- Listen channel accessors (sharedMu) ---

func (c *ClientConnection) IsListening() bool {
	c.sharedMu.Lock()
	v := c.isListening
	c.sharedMu.Unlock()
	return v
}

func (c *ClientConnection) SetListening(v bool) {
	c.sharedMu.Lock()
	c.isListening = v
	c.sharedMu.Unlock()
}

func (c *ClientConnection) AddListenChannel(ch Channel) {
	c.sharedMu.Lock()
	c.listenChannels[ch] = true
	c.isListening = true
	c.lastActivity = time.Now()
	c.sharedMu.Unlock()
}

func (c *ClientConnection) RemoveListenChannel(ch Channel) {
	c.sharedMu.Lock()
	delete(c.listenChannels, ch)
	if len(c.listenChannels) == 0 {
		c.isListening = false
	}
	c.sharedMu.Unlock()
}

func (c *ClientConnection) GetListenChannels() map[Channel]bool {
	c.sharedMu.Lock()
	out := make(map[Channel]bool, len(c.listenChannels))
	for ch, active := range c.listenChannels {
		out[ch] = active
	}
	c.sharedMu.Unlock()
	return out
}

func (c *ClientConnection) ClearListenChannels() {
	c.sharedMu.Lock()
	c.listenChannels = make(map[Channel]bool)
	c.isListening = false
	c.sharedMu.Unlock()
}

// --- I/O ---

// WriteMessage writes a PostgreSQL protocol message to the client.
// writeMu serialises concurrent callers (owning goroutine + listen fan-out).
func (c *ClientConnection) WriteMessage(msgType byte, body []byte) error {
	if len(body) > c.maxMessageSize {
		return fmt.Errorf("message body too large: %d bytes", len(body))
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := c.writer.WriteByte(msgType); err != nil {
		return fmt.Errorf("failed to write message type %c to client: %w", msgType, err)
	}
	length := uint32(len(body) + 4)
	if err := writeUint32(c.writer, length); err != nil {
		return fmt.Errorf("failed to write message length %d to client: %w", length, err)
	}
	if len(body) > 0 {
		if _, err := c.writer.Write(body); err != nil {
			return fmt.Errorf("failed to write message body (%d bytes) to client: %w", len(body), err)
		}
	}
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush message type=%c len=%d to client: %w", msgType, length, err)
	}

	atomic.AddInt64(&c.msgsSent, 1)

	c.sharedMu.Lock()
	c.lastActivity = time.Now()
	c.sharedMu.Unlock()

	return nil
}

// ReadMessage reads a PostgreSQL protocol message from the client.
// Only the owning goroutine calls this — no locking needed.
func (c *ClientConnection) ReadMessage() (byte, []byte, error) {
	msgType, err := c.reader.ReadByte()
	if err != nil {
		if err == io.EOF {
			return 0, nil, io.EOF
		}
		return 0, nil, fmt.Errorf("failed to read message type from client: %w", err)
	}

	length, err := readUint32(c.reader)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read message length after type %c from client: %w", msgType, err)
	}
	if length < 4 {
		return 0, nil, fmt.Errorf("invalid message length %d for type %c from client (must be >= 4)", length, msgType)
	}
	if length > uint32(c.maxMessageSize) {
		return 0, nil, fmt.Errorf("message length %d exceeds max_message_size %d for type %c from client", length, c.maxMessageSize, msgType)
	}

	bodyLength := int(length - 4)
	body := make([]byte, bodyLength)
	if bodyLength > 0 {
		if _, err := io.ReadFull(c.reader, body); err != nil {
			return 0, nil, fmt.Errorf("failed to read message body (%d bytes) for type %c from client: %w", bodyLength, msgType, err)
		}
	}

	c.sharedMu.Lock()
	c.lastActivity = time.Now()
	c.sharedMu.Unlock()

	return msgType, body, nil
}

// Logger returns the client's logger.
func (c *ClientConnection) Logger() *Logger { return c.logger }

// --- Prepared statement name mapping (single-goroutine, no lock) ---

// --- Prepared statement name mapping ---
// These methods are NOT guarded by c.mu because they are only ever called
// from the single goroutine that owns this client connection.

// MapStmtName registers a mapping from a client-visible name to an internal
// pgfox hash, and the reverse. Replaces any previous mapping for clientName.
func (c *ClientConnection) MapStmtName(clientName, hash string) {
	// Remove any previous reverse entry for this clientName.
	if old, ok := c.stmtNameMap[clientName]; ok {
		delete(c.stmtRevMap, old)
	}
	c.stmtNameMap[clientName] = hash
	c.stmtRevMap[hash] = clientName
}

// LookupInternalName returns the internal hash for a client-visible statement
// name, or ("", false) if not found.
func (c *ClientConnection) LookupInternalName(clientName string) (string, bool) {
	hash, ok := c.stmtNameMap[clientName]
	return hash, ok
}

// LookupClientName returns the client-visible name for an internal hash,
// or ("", false) if not found.
func (c *ClientConnection) LookupClientName(hash string) (string, bool) {
	name, ok := c.stmtRevMap[hash]
	return name, ok
}

// UnmapStmtName removes the mapping for clientName and its reverse entry.
func (c *ClientConnection) UnmapStmtName(clientName string) {
	if hash, ok := c.stmtNameMap[clientName]; ok {
		delete(c.stmtRevMap, hash)
	}
	delete(c.stmtNameMap, clientName)
}
