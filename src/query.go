package main

import (
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"
)

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
//
// For each Parse message in the pipeline:
//   - If the query can be parameterized, we register it in the target cache
//     and remap the client's statement name to the internal hash-based name.
//   - If not, we pass it through as-is and track the client's name for pinning.
//
// For Bind and Describe messages that reference a client statement name, we
// rewrite the name to the internal hash-based name before forwarding.
//
// For Close messages that close a client-named statement, we rewrite and unmap.
//
// Connection pinning rules (unchanged from before):
//   - Named statements from the client that we can't remap → pin
//   - Open transactions (ReadyForQuery reports 'T' or 'E') → pin
//   - All other cases → return to pool after Sync
func (p *Server) executeExtendedPipeline(client *ClientConnection, pipeline []pipelineMsg) error {
	logger := client.Logger()

	// --- Phase 1: inspect and rewrite the pipeline ---
	// We track whether any message requires pinning (non-remappable named stmt).
	requiresPin := false

	rewritten := make([]pipelineMsg, len(pipeline))
	copy(rewritten, pipeline)

	for i, m := range pipeline {
		switch m.msgType {
		case 'P': // Parse
			clientName := ParseBodyStatementName(m.body)
			querySQL := ParseBodyQuery(m.body)

			result, _ := ParameterizeQuery(querySQL)

			if result != nil {
				// We can manage this statement in the cache.
				pool := p.getPool(client.GetDatabase(), client.GetUser())
				if pool != nil {
					entry, isNew := pool.target.stmtCache.GetOrRegister(
						result.Hash, result.CanonicalSQL, querySQL, len(result.Values))
					if isNew {
						logger.Debug("Registered prepared statement via extended protocol",
							"hash", result.Hash,
							"client_name", clientName)
					}
					entry.RecordExecution()
				}

				internalName := StmtName(result.Hash)

				if clientName != "" {
					// Named statement: register the mapping so Bind/Close can find it.
					client.MapStmtName(clientName, result.Hash)
					// Count it as a named stmt for pinning logic — we still need
					// this backend if the client plans to Bind later in a different
					// pipeline. BUT because we have the cache, a different backend
					// can serve the Bind as long as it also has the stmt deployed.
					// We track it but don't force-pin here; reconcileConn decides.
					client.AddNamedStatement()
					logger.Debug("Mapped client named statement",
						"client_name", clientName,
						"internal_name", internalName)
				}

				// Rewrite the Parse body to use the internal name.
				rewritten[i].body = RewriteParseBodyName(m.body, internalName)

			} else {
				// Can't parameterize — pass through with original name.
				// If it's a named statement, we must pin to this specific backend.
				if clientName != "" {
					requiresPin = true
					client.AddNamedStatement()
					logger.Debug("Named statement passthrough (non-parameterizable), pinning",
						"client_name", clientName)
				}
			}

		case 'B': // Bind
			clientStmtName := BindBodyStatementName(m.body)
			if clientStmtName != "" {
				if hash, ok := client.LookupInternalName(clientStmtName); ok {
					rewritten[i].body = RewriteBindBodyName(m.body, StmtName(hash))
				}
				// If not in map, it's a passthrough named stmt — leave as-is.
			}

		case 'D': // Describe
			descType, descName := DescribeBodyTarget(m.body)
			if descType == 'S' && descName != "" {
				if hash, ok := client.LookupInternalName(descName); ok {
					rewritten[i].body = RewriteDescribeBodyName(m.body, StmtName(hash))
				}
			}

		case 'C': // Close
			closeType, closeName := CloseBodyTarget(m.body)
			if closeType == 'S' && closeName != "" {
				if hash, ok := client.LookupInternalName(closeName); ok {
					rewritten[i].body = RewriteCloseBodyName(m.body, StmtName(hash))
					client.UnmapStmtName(closeName)
				}
				client.RemoveNamedStatement()
				logger.Debug("Named statement closed", "name", closeName)
			} else if closeType == 'S' && closeName == "" {
				// Closing the unnamed statement — no mapping needed.
			}
		}
	}

	// --- Phase 2: acquire backend ---
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
		if requiresPin {
			// Non-remappable named statement: pin immediately.
			client.SetBackendConnection(backend)
			backend.SetClient(client)
			client.SetInTransaction(true)
		}
	}

	if p.config.Server.QueryTimeout > 0 {
		if conn, ok := backend.conn.(net.Conn); ok {
			conn.SetReadDeadline(time.Now().Add(p.config.Server.QueryTimeout))
			defer conn.SetReadDeadline(time.Time{})
		}
	}

	// --- Phase 3: forward the rewritten pipeline ---
	// Parse messages that were remapped to internal names will be deployed on
	// this backend if not already present — the backend responds with ParseComplete.
	// --- Phase 4: forward the rewritten pipeline ---
	for _, m := range rewritten {
		if err := backend.WriteMessage(m.msgType, m.body); err != nil {
			logger.WithError(err).Error("Failed to forward extended query message",
				"msg_type", string(m.msgType))
			backend.Release()
			return sendErrorResponse(client, "ERROR", "08006", "connection failure")
		}
	}

	// --- Phase 5: read responses ---
	lastMsg := rewritten[len(rewritten)-1].msgType

	if lastMsg == 'S' { // Sync
		txStatus, err := p.forwardExtendedResponse(client, backend, logger)
		if err != nil {
			backend.Release()
			return err
		}
		p.reconcileConn(client, backend, txStatus, wasPinned, logger)
	} else { // Flush ('H')
		if err := p.drainFlushResponse(client, backend); err != nil {
			backend.Release()
			return err
		}
		// After Flush, connection stays available; client will send more messages.
		// If we acquired it for this pipeline without pinning, keep it associated
		// temporarily — the next Sync will trigger reconcileConn.
		if !wasPinned && !requiresPin {
			client.SetBackendConnection(backend)
			backend.SetClient(client)
		}
	}

	return nil
}

// forwardExtendedResponse reads backend responses after a Sync in the extended
// protocol, forwarding all messages to the client. It also records ParseComplete
// events to mark statements as deployed on the backend.
//
// This is separate from forwardPreparedResponse because in the extended protocol
// the client expects to receive ParseComplete and BindComplete messages.
func (p *Server) forwardExtendedResponse(client *ClientConnection, backend *BackendConnection, logger *Logger) (byte, error) {
	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			if netErr, ok := err.(*net.OpError); ok && netErr.Timeout() {
				return 0, sendErrorResponse(client, "ERROR", "57014", "query timeout")
			}
			return 0, sendErrorResponse(client, "ERROR", "08006", "connection failure")
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

// executeQuery handles a simple query ('Q') from the client.
// Transaction state is driven by the ReadyForQuery status byte.
func (p *Server) executeQuery(client *ClientConnection, query string) error {
	logger := client.Logger()

	// Single parse pass: classify special commands AND attempt parameterization
	// simultaneously — avoids the previous double-parse overhead.
	cmd, result := ClassifyAndParameterize(query)

	switch cmd {
	case SimpleQueryListen:
		return p.handleListen(client, query)
	case SimpleQueryUnlisten:
		return p.handleUnlisten(client, query)
	case SimpleQueryNotify:
		return p.handleNotify(client, query)
	}

	pinned := client.IsInTransaction()

	// Serve through the prepared statement cache when not in a transaction
	// and the query was successfully parameterized.
	if !pinned && result != nil {
		return p.executeAsPrepared(client, query, result, logger)
	}

	// --- Fallback: simple query protocol (original path) ---
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

// executeAsPrepared serves a simple query through the extended protocol using
// the target-level prepared statement cache. It:
//  1. Registers the canonical statement in the target cache (or finds existing).
//  2. Borrows a backend connection from the pool.
//  3. Sends Parse to the backend only if this backend hasn't seen this statement.
//  4. Sends Bind (text params, binary results) + Execute + Sync.
//  5. Forwards all responses to the client, translating to what the client
//     expects from a simple query (CommandComplete + ReadyForQuery).
//  6. Returns the backend to the pool (no pinning — no transaction opened).
func (p *Server) executeAsPrepared(client *ClientConnection, originalSQL string, result *ParameterizeResult, logger *Logger) error {
	pool := p.getPool(client.GetDatabase(), client.GetUser())
	if pool == nil {
		return sendErrorResponse(client, "FATAL", "53300", "no pool available")
	}

	// Register in the target cache.
	entry, isNew := pool.target.stmtCache.GetOrRegister(
		result.Hash, result.CanonicalSQL, originalSQL, len(result.Values))
	if isNew {
		logger.Debug("Registered new prepared statement",
			"hash", result.Hash,
			"params", entry.ParamCount,
			"sql", result.CanonicalSQL)
	}

	backend, err := pool.borrowConn(p.ctx)
	if err != nil {
		logger.WithError(err).Error("Failed to borrow backend for prepared query")
		return sendErrorResponse(client, "FATAL", "53300", "too many connections")
	}

	if p.config.Server.QueryTimeout > 0 {
		if conn, ok := backend.conn.(net.Conn); ok {
			conn.SetReadDeadline(time.Now().Add(p.config.Server.QueryTimeout))
			defer conn.SetReadDeadline(time.Time{})
		}
	}

	stmtName := StmtName(result.Hash)

	// Deploy the prepared statement if this backend hasn't seen it yet.
	if !backend.HasStmt(result.Hash) {
		parseBody := BuildParseBody(stmtName, result.CanonicalSQL, nil)
		if err := backend.WriteMessage('P', parseBody); err != nil {
			backend.Release()
			return sendErrorResponse(client, "ERROR", "08006", "connection failure")
		}
	}

	// Bind: text format for all params, binary format for all result columns.
	bindBody := BuildBindBody(
		"", // unnamed portal
		stmtName,
		nil, // all params in text format (we extracted them as text)
		result.Values,
		[]int16{1}, // binary results for all columns
	)
	if err := backend.WriteMessage('B', bindBody); err != nil {
		backend.Release()
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	// Execute the unnamed portal.
	execBody := BuildExecuteBody("", 0)
	if err := backend.WriteMessage('E', execBody); err != nil {
		backend.Release()
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	// Sync — backend will now respond and send ReadyForQuery.
	if err := backend.WriteMessage('S', SyncBody); err != nil {
		backend.Release()
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	// Read responses. If we just sent Parse for the first time, the first
	// response will be ParseComplete ('1') before BindComplete ('2').
	// We translate the extended protocol response back to what the simple
	// query client expects, forwarding everything except ParseComplete and
	// BindComplete (which don't exist in simple query protocol).
	txStatus, err := p.forwardPreparedResponse(client, backend, result.Hash, entry, logger)
	if err != nil {
		backend.Release()
		return err
	}

	atomic.AddInt64(&pool.stats.QueriesExecuted, 1)
	atomic.AddInt64(&p.stats.TotalQueries, 1)
	entry.RecordExecution()

	// Prepared statement execution is always stateless from the pool's perspective
	// (ReadyForQuery will report 'I' since we didn't open a transaction).
	p.reconcileConn(client, backend, txStatus, false, logger)

	return nil
}

// forwardPreparedResponse reads backend responses after Parse+Bind+Execute+Sync
// and forwards them to the client, translating extended protocol messages to
// what a simple query client expects.
//
// Handles:
//   - ParseComplete ('1')   → consumed, not forwarded; marks stmt deployed
//   - BindComplete ('2')    → consumed, not forwarded
//   - DataRow ('D')         → forwarded as-is (binary data from backend)
//   - RowDescription ('T')  → forwarded as-is
//   - CommandComplete ('C') → forwarded as-is
//   - ErrorResponse ('E')   → forwarded as-is
//   - ReadyForQuery ('Z')   → forwarded as-is, loop exits
func (p *Server) forwardPreparedResponse(
	client *ClientConnection,
	backend *BackendConnection,
	hash string,
	entry *CachedStmt,
	logger *Logger,
) (byte, error) {
	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			if netErr, ok := err.(*net.OpError); ok && netErr.Timeout() {
				return 0, sendErrorResponse(client, "ERROR", "57014", "query timeout")
			}
			return 0, sendErrorResponse(client, "ERROR", "08006", "connection failure")
		}

		switch msgType {
		case '1': // ParseComplete — statement successfully deployed on this backend
			if !backend.HasStmt(hash) {
				backend.MarkStmt(hash)
				entry.RecordDeploy()
				logger.Debug("Prepared statement deployed on backend", "hash", hash)
			}
			// Do not forward to client — simple query protocol has no ParseComplete.

		case '2': // BindComplete
			// Do not forward to client — simple query protocol has no BindComplete.

		case 'Z': // ReadyForQuery
			if err := client.WriteMessage(msgType, body); err != nil {
				return 0, err
			}
			if len(body) > 0 {
				return body[0], nil
			}
			return 'I', nil

		default:
			// DataRow, RowDescription, CommandComplete, ErrorResponse, NoticeResponse, etc.
			// Forward everything else as-is.
			if err := client.WriteMessage(msgType, body); err != nil {
				if isClientGone(err) {
					return 0, err
				}
				return 0, err
			}
		}
	}
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
// Kept as a thin wrapper over forwardExtendedResponse for callers in
// listen_notify.go and similar that don't need the extended name-remapping path.
func (p *Server) forwardUntilReady(client *ClientConnection, backend *BackendConnection) (byte, error) {
	return p.forwardExtendedResponse(client, backend, client.Logger())
}
