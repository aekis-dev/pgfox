package pgfox

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aekis-dev/pgfox/pkg/auth"
)

// Backend represents a connection to a PostgreSQL backend.
//
// Lock discipline:
//
//   - No lock on WriteMessage / ReadMessage. A Backend is owned by
//     exactly one goroutine while inUse=true (the client handler). The listen
//     monitor owns its dedicated backend exclusively. There is never concurrent
//     I/O on a single Backend.
//
//   - sharedMu protects only the fields read by goroutines other than the
//     current owner: inUse, lastUsedAt, client. processID and secretKey are
//     written once at startup and then read-only — they use atomic int32 to
//     avoid locking on the cancel-request scan path.
//
//   - deployedStmts, parameters are single-owner and need no lock.
type Backend struct {
	Conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	dbName     string
	username   string
	targetName string
	Pool       *Pool
	createdAt  time.Time

	// sharedMu protects inUse, lastUsedAt, client — the fields that cross
	// the ownership boundary (target goroutine ↔ client goroutine).
	sharedMu   sync.Mutex
	inUse      bool
	lastUsedAt time.Time
	client     *Client

	// processID and secretKey are written once during startup and read from
	// multiple goroutines (cancel scan). Atomic removes the need for sharedMu
	// on the hot cancel-lookup path.
	processID int32 // accessed via atomic
	secretKey int32 // accessed via atomic

	maxMessageSize int
	parameters     map[string]string

	// deployedStmts tracks which prepared statement hashes have been
	// successfully deployed (Parse acknowledged) on this specific connection.
	// Single-owner while inUse; no lock needed.
	deployedStmts map[string]bool
}

// NewBackend creates a new backend connection.
func NewBackend(conn net.Conn, dbName, targetName, username string, maxMessageSize int) *Backend {
	return &Backend{
		Conn:           conn,
		reader:         bufio.NewReader(conn),
		writer:         bufio.NewWriter(conn),
		dbName:         dbName,
		username:       username,
		targetName:     targetName,
		createdAt:      time.Now(),
		lastUsedAt:     time.Now(),
		maxMessageSize: maxMessageSize,
		parameters:     make(map[string]string),
		deployedStmts:  make(map[string]bool),
	}
}

// Close closes the backend connection.
func (b *Backend) Close() error {
	if b.Conn != nil {
		addr := b.Conn.RemoteAddr()
		if err := b.Conn.Close(); err != nil {
			return fmt.Errorf("failed to close backend %v: %w", addr, err)
		}
	}
	return nil
}

// --- sharedMu-protected accessors ---

func (b *Backend) IsInUse() bool {
	b.sharedMu.Lock()
	v := b.inUse
	b.sharedMu.Unlock()
	return v
}

func (b *Backend) SetInUse(inUse bool) {
	b.sharedMu.Lock()
	b.inUse = inUse
	if !inUse {
		b.lastUsedAt = time.Now()
	}
	b.sharedMu.Unlock()
}

func (b *Backend) LastUsedAt() time.Time {
	b.sharedMu.Lock()
	t := b.lastUsedAt
	b.sharedMu.Unlock()
	return t
}

func (b *Backend) GetClient() *Client {
	b.sharedMu.Lock()
	c := b.client
	b.sharedMu.Unlock()
	return c
}

func (b *Backend) SetClient(c *Client) {
	b.sharedMu.Lock()
	b.client = c
	b.sharedMu.Unlock()
}

// --- Atomic accessors (processID / secretKey) ---

func (b *Backend) GetProcessID() int32 {
	return atomic.LoadInt32(&b.processID)
}

func (b *Backend) SetProcessID(v int32) {
	atomic.StoreInt32(&b.processID, v)
}

func (b *Backend) GetSecretKey() int32 {
	return atomic.LoadInt32(&b.secretKey)
}

func (b *Backend) SetSecretKey(v int32) {
	atomic.StoreInt32(&b.secretKey, v)
}

// --- Lock-free accessors (immutable after construction) ---

func (b *Backend) GetDatabase() string  { return b.dbName }
func (b *Backend) GetTarget() string    { return b.targetName }
func (b *Backend) CreatedAt() time.Time { return b.createdAt }

func (b *Backend) RemoteAddr() net.Addr {
	if b.Conn != nil {
		return b.Conn.RemoteAddr()
	}
	return nil
}

func (b *Backend) LocalAddr() net.Addr {
	if b.Conn != nil {
		return b.Conn.LocalAddr()
	}
	return nil
}

// --- I/O (no lock — single owner while inUse) ---

// WriteMessage writes a message to the backend.
// Must only be called by the goroutine that currently owns this connection.
func (b *Backend) WriteMessage(msgType byte, body []byte) error {
	if len(body) > b.maxMessageSize {
		return fmt.Errorf("message body %d bytes exceeds max_message_size %d", len(body), b.maxMessageSize)
	}
	if err := b.writer.WriteByte(msgType); err != nil {
		return fmt.Errorf("failed to write message type %c: %w", msgType, err)
	}
	length := uint32(len(body) + 4)
	if err := writeUint32(b.writer, length); err != nil {
		return fmt.Errorf("failed to write message length %d: %w", length, err)
	}
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

// ReadMessage reads a message from the backend.
// Must only be called by the goroutine that currently owns this connection.
//
// The returned body slice is borrowed from msgBodyPool. Callers MUST call
// PutMsgBody(body) when they are done reading all fields from it. If the body
// needs to outlive the immediate dispatch, call cloneMsgBody first and put
// the original back immediately.
func (b *Backend) ReadMessage() (byte, []byte, error) {
	msgType, err := b.reader.ReadByte()
	if err != nil {
		if err == io.EOF {
			return 0, nil, fmt.Errorf("backend connection closed")
		}
		return 0, nil, fmt.Errorf("failed to read message type: %w", err)
	}
	length, err := readUint32(b.reader)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read message length after type %c: %w", msgType, err)
	}
	if length < 4 {
		return 0, nil, fmt.Errorf("invalid message length %d for type %c (must be >= 4)", length, msgType)
	}
	if length > uint32(b.maxMessageSize) {
		return 0, nil, fmt.Errorf("message length %d exceeds max_message_size %d for type %c", length, b.maxMessageSize, msgType)
	}
	bodyLength := int(length - 4)

	var body []byte
	if bodyLength > 0 {
		if bodyLength > msgBodyPoolMax {
			// Oversized — allocate directly; no Pool churn.
			body = make([]byte, bodyLength)
		} else {
			bp := getMsgBody(bodyLength)
			*bp = (*bp)[:bodyLength]
			body = *bp
		}
		if _, err := io.ReadFull(b.reader, body); err != nil {
			PutMsgBody(body)
			return 0, nil, fmt.Errorf("failed to read message body (%d bytes) for type %c: %w", bodyLength, msgType, err)
		}
	}
	return msgType, body, nil
}

// PutMsgBody returns a backend ReadMessage body to the Pool.
// Call this exactly once per body returned by Backend.ReadMessage,
// after all fields have been read from it.
func PutMsgBody(body []byte) {
	if cap(body) == 0 || cap(body) > msgBodyPoolMax {
		return
	}
	bp := &body
	putMsgBody(bp)
}

func (b *Backend) Peek(n int) ([]byte, error) {
	return b.reader.Peek(n)
}

// Release signals the target that a connection is dead and should be replaced.
func (b *Backend) Release() {
	b.sharedMu.Lock()
	c := b.client
	b.client = nil
	b.sharedMu.Unlock()

	if c != nil {
		c.SetBackend(nil)
		c.SetInTransaction(false)
	}
	if b.Pool != nil {
		b.Pool.Target.CloseCh <- b
	} else {
		b.Close()
	}
}

// Return returns a backend connection to its Pool directly from the calling
// goroutine, bypassing the target goroutine's event loop for the common healthy
// case. This eliminates the serialisation bottleneck where all concurrent
// returns queue through the single target goroutine.
//
// If the connection is dead it is handed to the target via closeCh for proper
// bookkeeping (totalOpen decrement, All removal). If the backendPool
// channel is unexpectedly full (which should not happen given the channel is
// sized to MaxConnections), it is also sent to closeCh rather than leaked.
func (b *Backend) Return() {
	b.SetInUse(false)
	b.SetClient(nil)

	// No liveness probe here: it cost up to 1ms per return on the hot path and,
	// worse, a raw Read could consume a pending byte off the socket (bypassing
	// b.reader) and corrupt the stream. A connection that died while idle is
	// detected lazily on next use (ReadMessage/WriteMessage error → Release).
	// Direct deposit into the Pool channel — no target goroutine involvement.
	select {
	case b.Pool.Queue <- b:
		// Update cancel-lookup index and wake any waiting borrowers.
		b.Pool.Target.BackendIndex.Store(b.GetProcessID(), b)
		b.Pool.Target.signalConnReady()
	default:
		// backendPool full — let target goroutine close it cleanly.
		b.Pool.Target.CloseCh <- b
	}
}

// HasStmt returns true if hash has been deployed on this connection.
func (b *Backend) HasStmt(hash string) bool {
	return b.deployedStmts[hash]
}

// MarkStmt records that hash has been successfully deployed on this connection.
func (b *Backend) MarkStmt(hash string) {
	b.deployedStmts[hash] = true
}

// HandleSCRAMAuth handles SCRAM-SHA-256 authentication
func (b *Backend) HandleSCRAMAuth(username, password string, saslData []byte) error {
	// Parse supported SASL mechanisms
	mechanisms := auth.ParseSASLMechanisms(saslData)

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

	scram := auth.NewSCRAMAuth(username, password)

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

	if err := b.WriteMessage('p', msg); err != nil {
		return fmt.Errorf("failed to send SASL initial response: %w", err)
	}

	// Read server first response
	msgType, body, err := b.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read SASL continue: %w", err)
	}

	if msgType == 'E' {
		errorMsg := ParseErrorMessage(body)
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
	if err := b.WriteMessage('p', clientFinal); err != nil {
		return fmt.Errorf("failed to send client final: %w", err)
	}

	// Read server final response
	msgType, body, err = b.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read server final: %w", err)
	}

	if msgType == 'E' {
		errorMsg := ParseErrorMessage(body)
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
		msgType, body, err := b.ReadMessage()
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

		case 'K': // Queue key data
			if !authComplete {
				return fmt.Errorf("received BackendKeyData before AuthenticationOK")
			}
			if len(body) >= 8 {
				processID := int32(body[0])<<24 | int32(body[1])<<16 | int32(body[2])<<8 | int32(body[3])
				secretKey := int32(body[4])<<24 | int32(body[5])<<16 | int32(body[6])<<8 | int32(body[7])
				b.SetProcessID(processID)
				b.SetSecretKey(secretKey)
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
			errorMsg := ParseErrorMessage(body)
			return fmt.Errorf("error after SCRAM: %s", errorMsg)

		case 'N':
			continue

		default:
			return fmt.Errorf("unexpected message type after SCRAM: %c", msgType)
		}
	}
}
