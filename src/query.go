package main

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

// parseStatementName extracts the prepared statement name from a Parse message body.
// The body starts with a null-terminated statement name — empty means unnamed.
func parseStatementName(body []byte) string {
	for i, b := range body {
		if b == 0 {
			return string(body[:i])
		}
	}
	return ""
}

// parseCloseTarget extracts the close type ('S'=statement, 'P'=portal) and name
// from a Close message body.
func parseCloseTarget(body []byte) (byte, string) {
	if len(body) < 2 {
		return 0, ""
	}
	closeType := body[0]
	for i, b := range body[1:] {
		if b == 0 {
			return closeType, string(body[1 : 1+i])
		}
	}
	return closeType, ""
}

// pipelineMsg holds a single buffered extended query protocol message.
type pipelineMsg struct {
	msgType byte
	body    []byte
}

// handleClientMessage reads client messages and dispatches them.
//
// For simple query protocol ('Q') each message is self-contained.
//
// For extended query protocol the client pipelines multiple messages
// (Parse, Bind, Execute, Sync) without waiting for individual responses.
// We buffer the full pipeline up to Sync or Flush before forwarding to
// the backend — PostgreSQL won't respond until it receives Sync or Flush.
func (p *Server) handleClientMessage(client *ClientConnection) error {
	msgType, body, err := client.ReadMessage()
	if err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("failed to read client message: %w", err)
	}

	logger := client.Logger()
	logger.Debug("Received client message",
		"msg_type", string(msgType),
		"body_len", len(body))

	switch msgType {
	case Query:
		query := string(body)
		if len(query) > 0 && query[len(query)-1] == 0 {
			query = query[:len(query)-1]
		}
		return p.executeQuery(client, query)

	case Terminate:
		logger.Debug("Client terminating connection")
		return io.EOF

	default:
		// Extended query protocol — buffer full pipeline until Sync or Flush.
		pipeline := []pipelineMsg{{msgType, body}}
		complete := msgType == Sync || msgType == 'H'

		for !complete {
			next, nextBody, err := client.ReadMessage()
			if err != nil {
				return err
			}
			if next == Terminate {
				return io.EOF
			}
			pipeline = append(pipeline, pipelineMsg{next, nextBody})
			complete = next == Sync || next == 'H'
		}

		return p.executeExtendedPipeline(client, pipeline)
	}
}

// executeExtendedPipeline handles a complete buffered extended query pipeline.
// Inspects Parse messages to track named prepared statements, and Close messages
// to release them. The connection stays pinned as long as named statements exist
// or a transaction is open — regardless of what ReadyForQuery reports.
func (p *Server) executeExtendedPipeline(client *ClientConnection, pipeline []pipelineMsg) error {
	logger := client.Logger()

	// Inspect the pipeline before forwarding — track named statement lifecycle.
	for _, m := range pipeline {
		switch m.msgType {
		case Parse:
			name := parseStatementName(m.body)
			if name != "" {
				client.AddNamedStatement()
				logger.Debug("Named prepared statement created", "name", name,
					"total", client.namedStmts)
			}
		case 'C': // Close
			closeType, name := parseCloseTarget(m.body)
			if closeType == 'S' && name != "" {
				client.RemoveNamedStatement()
				logger.Debug("Named prepared statement closed", "name", name,
					"total", client.namedStmts)
			}
		}
	}

	// Acquire or reuse pinned backend.
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
		client.SetInTransaction(true)
		logger.Debug("Pinned backend for extended query pipeline",
			"messages", len(pipeline))
	}

	if p.config.Server.QueryTimeout > 0 {
		if conn, ok := backend.conn.(net.Conn); ok {
			conn.SetReadDeadline(time.Now().Add(p.config.Server.QueryTimeout))
			defer conn.SetReadDeadline(time.Time{})
		}
	}

	// Forward all buffered messages to the backend in one pass.
	for _, m := range pipeline {
		if err := backend.WriteMessage(m.msgType, m.body); err != nil {
			logger.WithError(err).Error("Failed to forward extended query message",
				"msg_type", string(m.msgType))
			backend.Release()
			return sendErrorResponse(client, "ERROR", "08006", "connection failure")
		}
	}

	lastMsg := pipeline[len(pipeline)-1].msgType

	if lastMsg == Sync {
		// Sync — read until ReadyForQuery, then decide whether to unpin.
		txStatus, err := p.forwardUntilReady(client, backend)
		if err != nil {
			backend.Release()
			return err
		}
		p.reconcileConn(client, backend, txStatus, wasPinned, logger)
	} else {
		// Flush ('H') — forward until the Describe response ends (no Z after Flush).
		if err := p.drainFlushResponse(client, backend); err != nil {
			backend.Release()
			return err
		}
	}

	return nil
}

// executeQuery handles a simple query ('Q') from the client.
// Transaction state is driven by the ReadyForQuery status byte.
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

	pinned := client.IsInTransaction()

	var backend *BackendConnection

	if pinned {
		backend = client.GetBackendConnection()
		if backend == nil {
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

	if pool := p.getPool(client.GetDatabase(), client.GetUser()); pool != nil {
		atomic.AddInt64(&pool.stats.QueriesExecuted, 1)
	}
	atomic.AddInt64(&p.stats.TotalQueries, 1)

	p.reconcileConn(client, backend, txStatus, pinned, logger)

	return nil
}

// reconcileConn pins or unpins the backend connection based on ReadyForQuery
// status and whether named prepared statements are still active.
//
// A connection stays pinned if:
//   - PostgreSQL reports 'T' or 'E' (inside a transaction), OR
//   - Named prepared statements exist on this backend (must go to same backend)
func (p *Server) reconcileConn(
	client *ClientConnection,
	backend *BackendConnection,
	txStatus byte,
	wasPinned bool,
	logger *Logger,
) {
	hasNamedStmts := client.HasNamedStatements()

	switch txStatus {
	case 'T', 'E':
		if !wasPinned {
			logger.Debug("Transaction opened — pinning connection", "tx_status", string(txStatus))
			client.SetBackendConnection(backend)
			backend.SetClient(client)
			client.SetInTransaction(true)
		}

	case 'I':
		if hasNamedStmts {
			// Keep pinned — named statements live on this specific backend.
			if !wasPinned {
				client.SetBackendConnection(backend)
				backend.SetClient(client)
				client.SetInTransaction(true)
			}
			logger.Debug("Keeping connection pinned for named statements",
				"count", client.namedStmts)
		} else if wasPinned {
			logger.Debug("Transaction closed — returning connection to pool")
			client.SetBackendConnection(nil)
			backend.SetClient(nil)
			client.SetInTransaction(false)
			backend.pool.target.returnCh <- backend
		} else {
			backend.pool.target.returnCh <- backend
		}

	default:
		logger.Warn("Unknown ReadyForQuery status, returning connection",
			"status", string(txStatus))
		if wasPinned && !hasNamedStmts {
			client.SetBackendConnection(nil)
			backend.SetClient(nil)
			client.SetInTransaction(false)
		}
		backend.pool.target.returnCh <- backend
	}
}

// drainFlushResponse reads backend responses after a Flush message.
// PostgreSQL does NOT send ReadyForQuery after Flush — only after Sync.
// The response to Parse+Describe+Flush ends with RowDescription('T') or NoData('n').
func (p *Server) drainFlushResponse(client *ClientConnection, backend *BackendConnection) error {
	logger := client.Logger()
	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			logger.WithError(err).Error("Failed to read response after flush")
			return sendErrorResponse(client, "ERROR", "08006", "connection failure")
		}

		if err := client.WriteMessage(msgType, body); err != nil {
			return err
		}

		// RowDescription or NoData ends the Describe response.
		// ErrorResponse ends a failed stage.
		// Connection stays pinned — client will send Bind+Execute+Sync next.
		switch msgType {
		case 'T', 'n', 'E':
			return nil
		}
	}
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

		if msgType == 'Z' {
			if len(body) > 0 {
				return body[0], nil
			}
			return 'I', nil
		}
	}
}

// forwardUntilReady forwards messages until ReadyForQuery, returning the
// transaction status byte. Used after Sync in executeExtendedPipeline.
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

		if msgType == 'Z' {
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
