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

// BackendConnection represents a connection to a PostgreSQL backend.
//
// Lock discipline:
//
//   - No lock on WriteMessage / ReadMessage. A BackendConnection is owned by
//     exactly one goroutine while inUse=true (the client handler). The listen
//     monitor owns its dedicated backend exclusively. There is never concurrent
//     I/O on a single BackendConnection.
//
//   - sharedMu protects only the fields read by goroutines other than the
//     current owner: inUse, lastUsedAt, client. processID and secretKey are
//     written once at startup and then read-only — they use atomic int32 to
//     avoid locking on the cancel-request scan path.
//
//   - deployedStmts, parameters are single-owner and need no lock.
type BackendConnection struct {
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	dbName     string
	username   string
	targetName string
	pool       *Pool
	createdAt  time.Time

	// sharedMu protects inUse, lastUsedAt, client — the fields that cross
	// the ownership boundary (target goroutine ↔ client goroutine).
	sharedMu   sync.Mutex
	inUse      bool
	lastUsedAt time.Time
	client     *ClientConnection

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

// NewBackendConnection creates a new backend connection.
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
		deployedStmts:  make(map[string]bool),
	}
}

// Close closes the backend connection.
func (b *BackendConnection) Close() error {
	if b.conn != nil {
		addr := b.conn.RemoteAddr()
		if err := b.conn.Close(); err != nil {
			return fmt.Errorf("failed to close backend %v: %w", addr, err)
		}
	}
	return nil
}

// --- sharedMu-protected accessors ---

func (b *BackendConnection) IsInUse() bool {
	b.sharedMu.Lock()
	v := b.inUse
	b.sharedMu.Unlock()
	return v
}

func (b *BackendConnection) SetInUse(inUse bool) {
	b.sharedMu.Lock()
	b.inUse = inUse
	if !inUse {
		b.lastUsedAt = time.Now()
	}
	b.sharedMu.Unlock()
}

func (b *BackendConnection) LastUsedAt() time.Time {
	b.sharedMu.Lock()
	t := b.lastUsedAt
	b.sharedMu.Unlock()
	return t
}

func (b *BackendConnection) GetClient() *ClientConnection {
	b.sharedMu.Lock()
	c := b.client
	b.sharedMu.Unlock()
	return c
}

func (b *BackendConnection) SetClient(c *ClientConnection) {
	b.sharedMu.Lock()
	b.client = c
	b.sharedMu.Unlock()
}

// --- Atomic accessors (processID / secretKey) ---

func (b *BackendConnection) GetProcessID() int32 {
	return atomic.LoadInt32(&b.processID)
}

func (b *BackendConnection) SetProcessID(v int32) {
	atomic.StoreInt32(&b.processID, v)
}

func (b *BackendConnection) GetSecretKey() int32 {
	return atomic.LoadInt32(&b.secretKey)
}

func (b *BackendConnection) SetSecretKey(v int32) {
	atomic.StoreInt32(&b.secretKey, v)
}

// --- Lock-free accessors (immutable after construction) ---

func (b *BackendConnection) GetDatabase() string  { return b.dbName }
func (b *BackendConnection) GetTarget() string    { return b.targetName }
func (b *BackendConnection) CreatedAt() time.Time { return b.createdAt }

func (b *BackendConnection) RemoteAddr() net.Addr {
	if b.conn != nil {
		return b.conn.RemoteAddr()
	}
	return nil
}

func (b *BackendConnection) LocalAddr() net.Addr {
	if b.conn != nil {
		return b.conn.LocalAddr()
	}
	return nil
}

// --- I/O (no lock — single owner while inUse) ---

// WriteMessage writes a message to the backend.
// Must only be called by the goroutine that currently owns this connection.
func (b *BackendConnection) WriteMessage(msgType byte, body []byte) error {
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
func (b *BackendConnection) ReadMessage() (byte, []byte, error) {
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
			// Oversized — allocate directly; no pool churn.
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

// PutMsgBody returns a backend ReadMessage body to the pool.
// Call this exactly once per body returned by BackendConnection.ReadMessage,
// after all fields have been read from it.
func PutMsgBody(body []byte) {
	if cap(body) == 0 || cap(body) > msgBodyPoolMax {
		return
	}
	bp := &body
	putMsgBody(bp)
}

// IsAlive checks if the connection is still alive via a non-blocking TCP peek.
func (b *BackendConnection) IsAlive() bool {
	if b.conn == nil {
		return false
	}
	netConn, ok := b.conn.(net.Conn)
	if !ok {
		return false
	}
	if err := netConn.SetReadDeadline(time.Now().Add(1 * time.Millisecond)); err != nil {
		return false
	}
	defer netConn.SetReadDeadline(time.Time{})
	one := make([]byte, 1)
	_, err := netConn.Read(one)
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}
	return false
}

func (b *BackendConnection) Peek(n int) ([]byte, error) {
	return b.reader.Peek(n)
}

// Release signals the target that a connection is dead and should be replaced.
func (b *BackendConnection) Release() {
	b.sharedMu.Lock()
	c := b.client
	b.client = nil
	b.sharedMu.Unlock()

	if c != nil {
		c.SetBackendConnection(nil)
		c.SetInTransaction(false)
	}
	if b.pool != nil {
		b.pool.target.closeCh <- b
	} else {
		b.Close()
	}
}

// HasStmt returns true if hash has been deployed on this connection.
func (b *BackendConnection) HasStmt(hash string) bool {
	return b.deployedStmts[hash]
}

// MarkStmt records that hash has been successfully deployed on this connection.
func (b *BackendConnection) MarkStmt(hash string) {
	b.deployedStmts[hash] = true
}
