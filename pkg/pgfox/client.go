package pgfox

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aekis-dev/pgfox/pkg/logger"
)

// Client represents a client connection.
//
// Lock discipline — two separate locks instead of one coarse mutex:
//
//   - writeMu: serialises concurrent writers on the underlying TCP connection.
//     Only WriteMessage acquires it. The listen-monitor goroutine is the one
//     concurrent writer; every other write is from the owning client goroutine.
//
//   - sharedMu: protects the small set of fields that genuinely cross goroutine
//     boundaries: backend, lastActivity, listenChannels/isListening.
//
// All other fields (authenticated, database, user, password, inTransaction,
// namedStmts, connectedAt, stmtNameMap) are owned exclusively by
// the single goroutine handling this client and need no locking.
type Client struct {
	conn           net.Conn
	reader         *bufio.Reader
	writer         *bufio.Writer
	backend        *Backend
	authenticated  bool
	database       string
	user           string
	password       string
	inTransaction  bool
	namedStmts     int // count of named prepared statements active on pinned backend
	isListening    bool
	listenChannels map[Channel]bool
	logger         *logger.Logger
	writeMu        sync.Mutex
	sharedMu       sync.Mutex
	connectedAt    time.Time
	lastActivity   time.Time

	maxMessageSize int // maximum allowed PostgreSQL message body size in bytes

	// stmtNameMap maps client-visible statement names to internal pgfox hashes.
	// Written only from the goroutine handling this client, so no lock is needed.
	stmtNameMap map[string]string // clientName → hash

	// Transaction-deferred LISTEN/UNLISTEN support. PostgreSQL applies
	// LISTEN/UNLISTEN at COMMIT and discards them on ROLLBACK; pendingListens
	// accumulates them (in order) while a transaction is open. lastTxStatus and
	// lastCommandTag mirror the most recent ReadyForQuery status byte and
	// CommandComplete tag written to the client, and are read when a transaction
	// resolves to decide commit-vs-rollback. All guarded by sharedMu.
	pendingListens []pendingListen
	lastTxStatus   byte
	lastCommandTag string

	// Cancellation: pgfox assigns each client its own (pid, secret) and sends
	// it as BackendKeyData. A CancelRequest carrying this pair is mapped back to
	// this client, whose currently-executing backend is then sent a real
	// CancelRequest. activeBackend is the backend running the client's in-flight
	// query (set while awaiting a backend response), read from the separate
	// goroutine that handles the incoming CancelRequest.
	cancelPID     int32
	cancelSecret  int32
	activeBackend atomic.Pointer[Backend]

	// msgsSent is incremented atomically for metrics. Kept here for cache
	// locality; the owning goroutine is the sole writer so atomic is enough.
	msgsSent int64
}

// NewClient creates a new client connection
func NewClient(conn net.Conn, logger *logger.Logger, maxMessageSize int) *Client {
	now := time.Now()
	return &Client{
		conn:           conn,
		reader:         bufio.NewReader(conn),
		writer:         bufio.NewWriter(conn),
		listenChannels: make(map[Channel]bool),
		logger:         logger,
		connectedAt:    now,
		lastActivity:   now,
		maxMessageSize: maxMessageSize,
		stmtNameMap:    make(map[string]string),
		lastTxStatus:   'I',
	}
}

// RemoteAddr returns the client's remote address
func (c *Client) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Close closes the client connection
func (c *Client) Close() error {
	c.sharedMu.Lock()
	bc := c.backend
	c.backend = nil
	c.sharedMu.Unlock()

	if bc != nil {
		bc.Close()
	}
	return c.conn.Close()
}

func (c *Client) IsAuthenticated() bool   { return c.authenticated }
func (c *Client) SetAuthenticated(v bool) { c.authenticated = v }

func (c *Client) GetDatabase() string   { return c.database }
func (c *Client) SetDatabase(v string)  { c.database = v }
func (c *Client) GetUser() string       { return c.user }
func (c *Client) SetUser(v string)      { c.user = v }
func (c *Client) SetPassword(v string)  { c.password = v }
func (c *Client) GetPassword() string   { return c.password }
func (c *Client) IsInTransaction() bool { return c.inTransaction }

func (c *Client) AddNamedStatement() { c.namedStmts++ }
func (c *Client) RemoveNamedStatement() {
	if c.namedStmts > 0 {
		c.namedStmts--
	}
}
func (c *Client) HasNamedStatements() bool { return c.namedStmts > 0 }

// --- Cancellation key + active backend ---

// SetCancelKey records the pgfox-assigned cancel identifiers for this client.
func (c *Client) SetCancelKey(pid, secret int32) {
	c.cancelPID = pid
	c.cancelSecret = secret
}

// CancelPID returns the client's pgfox-assigned cancel process id.
func (c *Client) CancelPID() int32 { return c.cancelPID }

// CancelSecret returns the client's pgfox-assigned cancel secret.
func (c *Client) CancelSecret() int32 { return c.cancelSecret }

// SetActiveBackend records (or clears, with nil) the backend currently running
// this client's query. Safe to call/read across goroutines.
func (c *Client) SetActiveBackend(b *Backend) { c.activeBackend.Store(b) }

// ActiveBackend returns the backend currently running this client's query, or
// nil if the client has no query in flight.
func (c *Client) ActiveBackend() *Backend { return c.activeBackend.Load() }

// --- Transaction-deferred LISTEN/UNLISTEN (sharedMu) ---

// LastTxStatus returns the most recent ReadyForQuery transaction status byte
// written to the client ('I', 'T', or 'E'). It reflects the real PostgreSQL
// transaction state, unlike IsInTransaction which is also set for named-
// statement pinning.
func (c *Client) LastTxStatus() byte {
	c.sharedMu.Lock()
	defer c.sharedMu.Unlock()
	return c.lastTxStatus
}

// LastCommandTag returns the most recent CommandComplete tag written to the
// client (e.g. "COMMIT", "ROLLBACK"). Used to decide whether a transaction that
// just reached idle committed or rolled back.
func (c *Client) LastCommandTag() string {
	c.sharedMu.Lock()
	defer c.sharedMu.Unlock()
	return c.lastCommandTag
}

// BufferListen records a LISTEN/UNLISTEN action to be applied when the current
// transaction commits (or discarded if it rolls back).
func (c *Client) BufferListen(kind listenKind, ch Channel) {
	c.sharedMu.Lock()
	c.pendingListens = append(c.pendingListens, pendingListen{kind: kind, channel: ch})
	c.sharedMu.Unlock()
}

// HasPendingListens reports whether any LISTEN/UNLISTEN actions are buffered.
func (c *Client) HasPendingListens() bool {
	c.sharedMu.Lock()
	defer c.sharedMu.Unlock()
	return len(c.pendingListens) > 0
}

// TakePendingListens returns the buffered actions (in order) and clears the buffer.
func (c *Client) TakePendingListens() []pendingListen {
	c.sharedMu.Lock()
	defer c.sharedMu.Unlock()
	out := c.pendingListens
	c.pendingListens = nil
	return out
}

func (c *Client) SetInTransaction(inTx bool) {
	c.inTransaction = inTx
	if inTx {
		c.logger.Debug("Client entered transaction")
	} else {
		c.logger.Debug("Client exited transaction")
	}
}

func (c *Client) GetConnectedAt() time.Time { return c.connectedAt }

// --- sharedMu-protected accessors ---

func (c *Client) GetLastActivity() time.Time {
	c.sharedMu.Lock()
	t := c.lastActivity
	c.sharedMu.Unlock()
	return t
}

func (c *Client) GetBackend() *Backend {
	c.sharedMu.Lock()
	bc := c.backend
	c.sharedMu.Unlock()
	return bc
}

func (c *Client) SetBackend(conn *Backend) {
	c.sharedMu.Lock()
	if c.backend != nil && c.backend != conn {
		if conn != nil {
			c.logger.Debug("Replacing existing backend connection")
		} else {
			c.logger.Debug("Clearing backend connection reference")
		}
	}
	c.backend = conn
	c.lastActivity = time.Now()
	c.sharedMu.Unlock()

	if conn != nil {
		c.logger.Debug("Queue connection assigned",
			"backend_addr", conn.RemoteAddr(),
			"backend_db", conn.GetDatabase())
	}
}

func (c *Client) ShouldKeepBackend() bool {
	// Pinned for any reason (real transaction or named statements). The backend
	// pointer is the source of truth for pinning; inTransaction tracks only a
	// real SQL transaction.
	return c.GetBackend() != nil
}

// --- Listen channel accessors (sharedMu) ---

func (c *Client) IsListening() bool {
	c.sharedMu.Lock()
	v := c.isListening
	c.sharedMu.Unlock()
	return v
}

func (c *Client) SetListening(v bool) {
	c.sharedMu.Lock()
	c.isListening = v
	c.sharedMu.Unlock()
}

func (c *Client) AddListenChannel(ch Channel) {
	c.sharedMu.Lock()
	c.listenChannels[ch] = true
	c.isListening = true
	c.lastActivity = time.Now()
	c.sharedMu.Unlock()
}

func (c *Client) RemoveListenChannel(ch Channel) {
	c.sharedMu.Lock()
	delete(c.listenChannels, ch)
	if len(c.listenChannels) == 0 {
		c.isListening = false
	}
	c.sharedMu.Unlock()
}

func (c *Client) GetListenChannels() map[Channel]bool {
	c.sharedMu.Lock()
	out := make(map[Channel]bool, len(c.listenChannels))
	for ch, active := range c.listenChannels {
		out[ch] = active
	}
	c.sharedMu.Unlock()
	return out
}

func (c *Client) ClearListenChannels() {
	c.sharedMu.Lock()
	c.listenChannels = make(map[Channel]bool)
	c.isListening = false
	c.sharedMu.Unlock()
}

// --- I/O ---

// WriteMessage writes a PostgreSQL protocol message to the client.
// writeMu serialises concurrent callers (owning goroutine + listen fan-out).
func (c *Client) WriteMessage(msgType byte, body []byte) error {
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
	// Mirror transaction-relevant protocol state for deferred LISTEN/UNLISTEN.
	switch msgType {
	case 'C': // CommandComplete — body is the null-terminated command tag.
		c.lastCommandTag = cString(body)
	case 'Z': // ReadyForQuery — body[0] is the transaction status byte.
		if len(body) > 0 {
			c.lastTxStatus = body[0]
		}
	}
	c.sharedMu.Unlock()

	return nil
}

// cString returns the bytes of b up to the first NUL, as a string.
func cString(b []byte) string {
	for i, ch := range b {
		if ch == 0 {
			return string(b[:i])
		}
	}
	return string(b)
}

// ReadMessage reads a PostgreSQL protocol message from the client.
// Only the owning goroutine calls this — no locking needed.
func (c *Client) ReadMessage() (byte, []byte, error) {
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
func (c *Client) Logger() *logger.Logger { return c.logger }

// --- Prepared statement name mapping (single-goroutine, no lock) ---

// --- Prepared statement name mapping ---
// These methods are NOT guarded by c.Mu because they are only ever called
// from the single goroutine that owns this client connection.

// MapStmtName registers a mapping from a client-visible name to an internal
// pgfox hash, and the reverse. Replaces any previous mapping for clientName.
func (c *Client) MapStmtName(clientName, hash string) {
	c.stmtNameMap[clientName] = hash
}

// LookupInternalName returns the internal hash for a client-visible statement
// name, or ("", false) if not found.
func (c *Client) LookupInternalName(clientName string) (string, bool) {
	hash, ok := c.stmtNameMap[clientName]
	return hash, ok
}

// UnmapStmtName removes the mapping for clientName and its reverse entry.
func (c *Client) UnmapStmtName(clientName string) {
	delete(c.stmtNameMap, clientName)
}

// SendAuthenticationOK sends authentication OK message
func (c *Client) SendAuthenticationOK() error {
	body := make([]byte, 4)
	binary.BigEndian.PutUint32(body, AuthenticationOK)
	return c.WriteMessage('R', body)
}

// SendAuthenticationMD5 sends an MD5 password challenge to the client.
// The 4-byte salt is randomly generated by the caller.
func (c *Client) SendAuthenticationMD5(salt [4]byte) error {
	body := make([]byte, 8)
	binary.BigEndian.PutUint32(body[0:4], AuthenticationMD5)
	copy(body[4:8], salt[:])
	return c.WriteMessage('R', body)
}

// SendAuthenticationCleartext sends a cleartext password challenge to the client.
func (c *Client) SendAuthenticationCleartext() error {
	body := make([]byte, 4)
	binary.BigEndian.PutUint32(body, AuthenticationCleartextPassword)
	return c.WriteMessage('R', body)
}

// sendBackendKeyData sends backend key data message
func (c *Client) SendBackendKeyData(processID, secretKey int32) error {
	body := make([]byte, 8)
	binary.BigEndian.PutUint32(body[0:4], uint32(processID))
	binary.BigEndian.PutUint32(body[4:8], uint32(secretKey))
	return c.WriteMessage('K', body)
}

// SendParameterStatus sends parameter status message
func (c *Client) SendParameterStatus(name, value string) error {
	body := name + "\x00" + value + "\x00"
	return c.WriteMessage('S', []byte(body))
}

// SendReadyForQuery sends ready for query message
func (c *Client) SendReadyForQuery(status byte) error {
	body := []byte{status}
	return c.WriteMessage('Z', body)
}

// SendErrorResponse sends error response message
func (c *Client) SendErrorResponse(severity, code, message string) error {
	body := fmt.Sprintf("S%s\x00C%s\x00M%s\x00\x00", severity, code, message)
	return c.WriteMessage('E', []byte(body))
}

// SendCommandComplete sends command complete message
func (c *Client) SendCommandComplete(command string) error {
	body := command + "\x00"
	return c.WriteMessage('C', []byte(body))
}

// SendNotificationToClient sends a notification message to a client
func (c *Client) SendNotificationToClient(notification NotificationMessage) error {
	channelBytes := []byte(notification.Channel)
	payloadBytes := []byte(notification.Payload)
	bodyLen := 4 + len(channelBytes) + 1 + len(payloadBytes) + 1

	body := make([]byte, bodyLen)
	binary.BigEndian.PutUint32(body[0:4], uint32(notification.ProcessID))

	pos := 4
	copy(body[pos:], channelBytes)
	pos += len(channelBytes)
	body[pos] = 0 // null terminator
	pos++

	copy(body[pos:], payloadBytes)
	pos += len(payloadBytes)
	body[pos] = 0 // null terminator

	return c.WriteMessage('A', body)
}
