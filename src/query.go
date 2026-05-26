package main

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

// handleClientMessage dispatches a message from a client.
func (p *Server) handleClientMessage(client *ClientConnection) error {
	msgType, body, err := client.ReadMessage()
	if err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("failed to read client message: %w", err)
	}

	logger := client.Logger().WithField("msg_type", string(msgType)).WithField("body_len", len(body))
	logger.Debug("Received client message")

	switch msgType {
	case Query:
		query := string(body)
		if len(query) > 0 && query[len(query)-1] == 0 {
			query = query[:len(query)-1]
		}
		return p.executeQuery(client, query)

	case Parse, Bind, Execute, Sync:
		return p.executeExtendedQuery(client, msgType, body)

	case Terminate:
		logger.Debug("Client terminating connection")
		return io.EOF

	default:
		logger.Debug("Forwarding unhandled message type")
		return p.executeExtendedQuery(client, msgType, body)
	}
}

// executeQuery handles a simple query ('Q') from the client.
//
// Transaction state is driven entirely by the ReadyForQuery status byte that
// PostgreSQL sends at the end of every response — not by string-matching the
// query text. Status values:
//
//	'I' — idle (no transaction)
//	'T' — inside a transaction block
//	'E' — inside a failed transaction block
//
// Pin/unpin logic:
//   - Backend reports 'T' or 'E': pin the connection to this client so all
//     subsequent queries in the same transaction go to the same backend.
//   - Backend reports 'I' and the client had a pinned connection: unpin and
//     return to pool (transaction ended via COMMIT/ROLLBACK).
func (p *Server) executeQuery(client *ClientConnection, query string) error {
	logger := client.Logger()

	queryUpper := strings.ToUpper(strings.TrimSpace(query))

	if strings.HasPrefix(queryUpper, "LISTEN ") {
		return p.handleListen(client, query)
	} else if strings.HasPrefix(queryUpper, "UNLISTEN") {
		return p.handleUnlisten(client, query)
	} else if strings.HasPrefix(queryUpper, "NOTIFY ") || p.containsPgNotify(queryUpper) {
		return p.handleNotify(client, query)
	}

	// pinned: the connection is already dedicated to this client's transaction.
	// borrowed: we took a connection from the pool for this single query.
	pinned := client.IsInTransaction()

	var backend *BackendConnection

	if pinned {
		backend = client.GetBackendConnection()
		if backend == nil {
			// Defensive: transaction flag set but no pinned conn — borrow one.
			logger.Warn("In transaction but no pinned backend, borrowing from pool")
			pool := p.getPool(client.GetDatabase(), client.GetUser())
			if pool == nil {
				return sendErrorResponse(client, "FATAL", "53300", "no pool available")
			}
			var err error
			backend, err = pool.borrowConn(p.ctx)
			if err != nil {
				return sendErrorResponse(client, "FATAL", "53300", "too many connections")
			}
			client.SetBackendConnection(backend)
			backend.SetClient(client)
		}
	} else {
		pool := p.getPool(client.GetDatabase(), client.GetUser())
		if pool == nil {
			return sendErrorResponse(client, "FATAL", "53300", "no pool available")
		}
		var err error
		backend, err = pool.borrowConn(p.ctx)
		if err != nil {
			logger.WithError(err).Error("Failed to borrow backend connection")
			return sendErrorResponse(client, "FATAL", "53300", "too many connections")
		}
	}

	if p.config.Server.QueryTimeout > 0 {
		if conn, ok := backend.conn.(net.Conn); ok {
			conn.SetReadDeadline(time.Now().Add(p.config.Server.QueryTimeout))
			defer conn.SetReadDeadline(time.Time{})
		}
	}

	if err := p.forwardQueryToBackend(backend, query); err != nil {
		logger.WithError(err).Error("Failed to forward query to backend")
		backend.Release()
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	txStatus, err := p.forwardCompleteBackendResponse(client, backend)
	if err != nil {
		logger.WithError(err).Error("Failed to forward backend response")
		backend.Release()
		return err
	}

	// Update stats.
	if pool := p.getPool(client.GetDatabase(), client.GetUser()); pool != nil {
		atomic.AddInt64(&pool.stats.QueriesExecuted, 1)
	}
	atomic.AddInt64(&p.stats.TotalQueries, 1)

	// Apply transaction state from PostgreSQL's authoritative status byte.
	p.reconcileConn(client, backend, txStatus, pinned, logger)

	return nil
}

// reconcileConn pins or unpins the backend connection based on the ReadyForQuery
// status byte. This is the single authoritative place that manages pin/unpin.
//
//	txStatus 'T' or 'E' — PostgreSQL is inside a transaction: pin.
//	txStatus 'I'         — PostgreSQL is idle: unpin (return to pool) or just
//	                       return the borrowed connection normally.
func (p *Server) reconcileConn(
	client *ClientConnection,
	backend *BackendConnection,
	txStatus byte,
	wasPinned bool,
	logger *Logger,
) {
	switch txStatus {
	case 'T', 'E':
		// Inside a transaction — pin if not already pinned.
		if !wasPinned {
			logger.Debug("Transaction opened — pinning connection", "tx_status", string(txStatus))
			client.SetBackendConnection(backend)
			backend.SetClient(client)
			client.SetInTransaction(true)
		}
		// Already pinned — nothing to change.

	case 'I':
		if wasPinned {
			// Transaction closed — unpin and return to pool.
			logger.Debug("Transaction closed — returning connection to pool")
			client.SetBackendConnection(nil)
			backend.SetClient(nil)
			client.SetInTransaction(false)
			backend.pool.target.returnCh <- backend
		} else {
			// Regular borrowed connection — return to pool.
			backend.pool.target.returnCh <- backend
		}

	default:
		// Unknown status — return safely.
		logger.Warn("Unknown ReadyForQuery status, returning connection", "status", string(txStatus))
		if wasPinned {
			client.SetBackendConnection(nil)
			backend.SetClient(nil)
			client.SetInTransaction(false)
		}
		backend.pool.target.returnCh <- backend
	}
}

// executeExtendedQuery handles extended query protocol messages
// (Parse/Bind/Execute/Sync and any other message types).
//
// Extended queries require a pinned backend because Parse creates a prepared
// statement and Bind/Execute reference it — all must go to the same backend.
// We pin on the first extended message and unpin when Sync returns 'I'.
func (p *Server) executeExtendedQuery(client *ClientConnection, msgType byte, body []byte) error {
	logger := client.Logger().WithField("msg_type", string(msgType))

	// Ensure we have a pinned backend for this extended query sequence.
	backend := client.GetBackendConnection()
	wasPinned := backend != nil

	if backend == nil {
		pool := p.getPool(client.GetDatabase(), client.GetUser())
		if pool == nil {
			return sendErrorResponse(client, "FATAL", "53300", "no pool available")
		}
		var err error
		backend, err = pool.borrowConn(p.ctx)
		if err != nil {
			logger.WithError(err).Error("Failed to borrow backend for extended query")
			return sendErrorResponse(client, "FATAL", "53300", "too many connections")
		}
		client.SetBackendConnection(backend)
		backend.SetClient(client)
		client.SetInTransaction(true) // treat as in-transaction until Sync says otherwise
		logger.Debug("Pinned backend for extended query sequence")
	}

	if p.config.Server.QueryTimeout > 0 {
		if conn, ok := backend.conn.(net.Conn); ok {
			conn.SetReadDeadline(time.Now().Add(p.config.Server.QueryTimeout))
			defer conn.SetReadDeadline(time.Time{})
		}
	}

	if err := backend.WriteMessage(msgType, body); err != nil {
		logger.WithError(err).Error("Failed to forward extended query message")
		backend.Release()
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	var txStatus byte
	var err error

	switch msgType {
	case Parse:
		err = p.forwardSingleResponse(client, backend, "ParseComplete")
	case Bind:
		err = p.forwardSingleResponse(client, backend, "BindComplete")
	case Execute:
		err = p.forwardExecuteResponse(client, backend)
	case Sync:
		txStatus, err = p.forwardUntilReady(client, backend)
	default:
		logger.Warn("Unknown extended query message type", "type", string(msgType))
		err = p.forwardSingleResponse(client, backend, "Unknown")
	}

	if err != nil {
		backend.Release()
		return err
	}

	// Only Sync gives us the authoritative transaction status.
	// For Parse/Bind/Execute we keep the connection pinned.
	if msgType == Sync && txStatus != 0 {
		p.reconcileConn(client, backend, txStatus, wasPinned, logger)
	}

	return nil
}

// forwardQueryToBackend sends a simple query to the backend.
func (p *Server) forwardQueryToBackend(backend *BackendConnection, query string) error {
	return backend.WriteMessage('Q', []byte(query+"\x00"))
}

// forwardCompleteBackendResponse forwards all messages until ReadyForQuery,
// returning the transaction status byte from the ReadyForQuery message.
//
//	'I' — idle (not in a transaction)
//	'T' — in a transaction block
//	'E' — in a failed transaction block
func (p *Server) forwardCompleteBackendResponse(client *ClientConnection, backend *BackendConnection) (byte, error) {
	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			if netErr, ok := err.(*net.OpError); ok && netErr.Timeout() {
				return 0, fmt.Errorf("query timeout")
			}
			return 0, err
		}

		if err := client.WriteMessage(msgType, body); err != nil {
			if isClientGone(err) {
				return 0, err
			}
			return 0, err
		}

		if msgType == 'Z' { // ReadyForQuery
			if len(body) > 0 {
				return body[0], nil
			}
			return 'I', nil
		}
	}
}

// forwardSingleResponse forwards a single response message from backend to client.
func (p *Server) forwardSingleResponse(client *ClientConnection, backend *BackendConnection, expectedType string) error {
	logger := client.Logger().WithField("expected", expectedType)

	msgType, body, err := backend.ReadMessage()
	if err != nil {
		if netErr, ok := err.(*net.OpError); ok && netErr.Timeout() {
			logger.WithError(err).Warn("Timeout reading single response")
			return sendErrorResponse(client, "ERROR", "57014", "query timeout")
		}
		logger.WithError(err).Error("Failed to read backend response")
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	if msgType == NotificationResponse {
		p.handleNotificationResponse(body)
		return p.forwardSingleResponse(client, backend, expectedType)
	}

	if err := client.WriteMessage(msgType, body); err != nil {
		logger.WithError(err).Error("Failed to forward response to client")
		return err
	}

	return nil
}

// forwardExecuteResponse forwards an Execute response (multiple messages ending
// with CommandComplete, EmptyQueryResponse, ErrorResponse, or PortalSuspended).
func (p *Server) forwardExecuteResponse(client *ClientConnection, backend *BackendConnection) error {
	logger := client.Logger()

	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			if netErr, ok := err.(*net.OpError); ok && netErr.Timeout() {
				logger.WithError(err).Warn("Timeout reading execute response")
				return sendErrorResponse(client, "ERROR", "57014", "query timeout")
			}
			logger.WithError(err).Error("Failed to read execute response")
			return sendErrorResponse(client, "ERROR", "08006", "connection failure")
		}

		if err := client.WriteMessage(msgType, body); err != nil {
			logger.WithError(err).Error("Failed to forward execute response")
			return err
		}

		switch msgType {
		case 'C', 'I', 'E', 's': // CommandComplete, EmptyQueryResponse, ErrorResponse, PortalSuspended
			return nil
		}
	}
}

// forwardUntilReady forwards messages until ReadyForQuery, returning the
// transaction status byte.
func (p *Server) forwardUntilReady(client *ClientConnection, backend *BackendConnection) (byte, error) {
	logger := client.Logger()

	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			if netErr, ok := err.(*net.OpError); ok && netErr.Timeout() {
				logger.WithError(err).Warn("Timeout waiting for ready")
				return 0, sendErrorResponse(client, "ERROR", "57014", "query timeout")
			}
			logger.WithError(err).Error("Failed to read response waiting for ready")
			return 0, sendErrorResponse(client, "ERROR", "08006", "connection failure")
		}

		if err := client.WriteMessage(msgType, body); err != nil {
			logger.WithError(err).Error("Failed to forward message waiting for ready")
			return 0, err
		}

		if msgType == 'Z' { // ReadyForQuery
			if len(body) > 0 {
				return body[0], nil
			}
			return 'I', nil
		}
	}
}

// containsPgNotify checks if a query contains a pg_notify function call.
func (p *Server) containsPgNotify(queryUpper string) bool {
	normalized := strings.ReplaceAll(queryUpper, " ", "")
	patterns := []string{
		"PG_NOTIFY(",
		"\"PG_NOTIFY\"(",
		"'PG_NOTIFY'(",
	}
	for _, pattern := range patterns {
		if strings.Contains(normalized, pattern) {
			return true
		}
		spaced := strings.ReplaceAll(pattern, "(", " (")
		if strings.Contains(queryUpper, spaced) {
			return true
		}
	}
	return false
}
