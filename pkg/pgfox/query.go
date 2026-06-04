package pgfox

import (
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/aekis-dev/pgfox/pkg/logger"
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
func (p *Server) HandleClientMessage(client *Client) error {
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
		// Borrow a pooled slice; executeExtendedPipeline returns it to the Pool.
		pp := getPipeline()
		*pp = append(*pp, pipelineMsg{msgType, body})
		complete := msgType == Sync || msgType == 'H'

		for !complete {
			next, nextBody, err := client.ReadMessage()
			if err != nil {
				putPipeline(pp)
				return err
			}
			if next == Terminate {
				putPipeline(pp)
				return io.EOF
			}
			*pp = append(*pp, pipelineMsg{next, nextBody})
			complete = next == Sync || next == 'H'
		}

		return p.executeExtendedPipeline(client, pp)
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
//   - All other cases → return to Pool after Sync
func (p *Server) executeExtendedPipeline(client *Client, pp *[]pipelineMsg) error {
	pipeline := *pp
	logger := client.Logger()

	// --- Phase 1: inspect and rewrite the pipeline ---
	// We track whether any message requires pinning (non-remappable named stmt).
	requiresPin := false

	// remappedParses maps pipeline index → hash for Parse messages that were
	// rewritten to internal names. Phase 4 uses this to skip forwarding the
	// Parse to a backend that already has the statement deployed.
	remappedParses := make(map[int]string, 4)

	// bindRequiredHashes collects hashes that Bind messages reference but have
	// no corresponding Parse in this pipeline. After Phase 2 (backend is known),
	// we synthesize Parse messages for any hash the backend hasn't deployed yet.
	bindRequiredHashes := make(map[string]bool, 4)

	// specialCmd and specialSQL are set when a Parse message contains a
	// LISTEN/UNLISTEN/NOTIFY statement. The pipeline is drained without
	// forwarding to any backend and then routed to the appropriate handler.
	var specialCmd SimpleQueryCommand
	var specialSQL string

	// Borrow a second pooled slice for the rewritten pipeline, then return both
	// when this function exits regardless of error path.
	rp := getPipeline()
	*rp = (*rp)[:len(pipeline)]
	copy(*rp, pipeline)
	rewritten := *rp
	defer func() {
		putPipeline(pp)
		putPipeline(rp)
	}()

	for i, m := range pipeline {
		switch m.msgType {
		case 'P': // Parse
			clientName := ParseBodyStatementName(m.body)
			querySQL := ParseBodyQuery(m.body)

			// Classify the SQL regardless of statement name so we can intercept
			// LISTEN/UNLISTEN/NOTIFY even when sent via the extended protocol
			// (e.g. asyncpg.execute("LISTEN chan") uses unnamed prepared stmts).
			if cmd, _ := ClassifyAndParameterize(querySQL); cmd != SimpleQueryOther {
				// NOTIFY inside a transaction must run on the pinned backend (it
				// fires at COMMIT); intercept it only in autocommit. When pinned,
				// fall through so it is forwarded to the pinned backend as an
				// ordinary statement, preserving transaction semantics. LISTEN and
				// UNLISTEN are always handled by the monitor machinery.
				if cmd == SimpleQueryNotify && client.IsInTransaction() {
					// fall through to the normal statement handling below
				} else {
					specialCmd = cmd
					specialSQL = querySQL
					break // drain the rest of the pipeline, then dispatch below
				}
			}

			// The unnamed statement ("") is per-connection and implicitly replaced
			// on every Parse. It cannot be shared across backends via the cache —
			// pass it through completely unchanged and pin to this backend.
			if clientName == "" {
				requiresPin = true
				break
			}

			result, _ := ParameterizeQuery(querySQL)

			if result != nil {
				// Named statement that we can manage in the shared cache.
				pool := p.getPool(client.GetDatabase(), client.GetUser())
				if pool != nil {
					entry, isNew := pool.Target.StmtCache.GetOrRegister(
						result.Hash, result.CanonicalSQL, querySQL, len(result.Values))
					if isNew {
						logger.Debug("Registered prepared statement via extended protocol",
							"hash", result.Hash,
							"client_name", clientName)
					}
					entry.RecordExecution()
				}

				internalName := StmtName(result.Hash)

				// Register name→hash so subsequent Bind/Describe/Close can find it.
				// Do NOT call AddNamedStatement — remapped stmts live in the target
				// cache and can be served by any backend, so they must not pin.
				client.MapStmtName(clientName, result.Hash)
				logger.Debug("Mapped client named statement",
					"client_name", clientName,
					"internal_name", internalName)

				// Rewrite the Parse body to use the internal name, and record the
				// hash so phase 4 can skip sending it if this backend already has it.
				rewritten[i].body = RewriteParseBodyName(m.body, internalName)
				remappedParses[i] = result.Hash

			} else {
				// Can't cache — pass through and pin to this backend.
				requiresPin = true
				client.AddNamedStatement()
				logger.Debug("Named statement passthrough (non-parameterizable), pinning",
					"client_name", clientName)
			}

		case 'B': // Bind
			clientStmtName := BindBodyStatementName(m.body)
			if clientStmtName != "" {
				if hash, ok := client.LookupInternalName(clientStmtName); ok {
					rewritten[i].body = RewriteBindBodyName(m.body, StmtName(hash))
					// Track hashes referenced by Bind so phase 3.5 can deploy them on
					// backends that don't have the statement yet (e.g. asyncpg re-using
					// a cached statement that was prepared on a different backend).
					bindRequiredHashes[hash] = true
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
				if _, ok := client.LookupInternalName(closeName); ok {
					// Remapped statement — pgfox owns the backend lifetime of pfx_<hash>.
					// Do NOT forward Close('S','pfx_hash') to the backend: that would
					// evict the statement from this backend while deployedStmts still
					// says it's deployed, causing InvalidSQLStatementNameError on reuse.
					// Instead rewrite to Close('S','') — closing the unnamed statement
					// is a no-op if it is empty, and PostgreSQL still sends CloseComplete.
					rewritten[i].body = RewriteCloseBodyName(m.body, "")
					client.UnmapStmtName(closeName)
					logger.Debug("Remapped named statement close intercepted", "name", closeName)
				} else {
					// Passthrough named statement — was counted in namedStmts.
					client.RemoveNamedStatement()
					logger.Debug("Passthrough named statement closed", "name", closeName)
				}
			} else if closeType == 'S' && closeName == "" {
				// Closing the unnamed statement — no mapping needed.
			}
		}
	}

	// --- Phase 1.5: intercept LISTEN/UNLISTEN/NOTIFY from extended protocol ---
	// If the Parse contained one of these special commands, drain the rest of
	// the buffered pipeline (no backend involvement) and dispatch to the handler.
	// We send CommandComplete + ReadyForQuery for the Sync case; for Flush we
	// send nothing extra (client will send Bind+Execute+Sync next, which we also
	// intercept via the same specialCmd path if it re-parses, or just discard).
	if specialCmd != SimpleQueryOther {
		// LISTEN/UNLISTEN/NOTIFY arrived via extended protocol (e.g. asyncpg
		// execute()). The pipeline may have ended with Flush (H), meaning
		// the client will immediately send a follow-up Bind+Execute+Sync pipeline
		// expecting to execute the prepared statement. Since we intercepted the
		// Parse and never sent it to any backend, we must consume and discard
		// that follow-up pipeline and respond synthetically, otherwise the client
		// will send B referencing a non-existent unnamed prepared statement.
		if pipeline[len(pipeline)-1].msgType == 'H' {
			// Pipeline ended with Flush (H): asyncpg sends P+D+H then B+E+S as
			// two separate pipelines. We must respond to both synthetically.
			//
			// Response to P (Parse): ParseComplete
			if err := client.WriteMessage('1', nil); err != nil {
				return err
			}
			// Response to D(S) (Describe statement): ParameterDescription (0 params) + NoData
			paramDesc := []byte{0, 0} // int16 = 0 parameters
			if err := client.WriteMessage('t', paramDesc); err != nil {
				return err
			}
			if err := client.WriteMessage('n', nil); err != nil { // NoData
				return err
			}
			// No ReadyForQuery after Flush — client continues with B+E+S.
			// Drain the follow-up Bind+Execute+Sync pipeline from the wire.
			for {
				mt, _, err := client.ReadMessage()
				if err != nil {
					return err
				}
				if mt == 'S' || mt == 'H' {
					break
				}
			}
			// Response to B+E+S: BindComplete + CommandComplete + ReadyForQuery.
			if err := client.WriteMessage('2', nil); err != nil { // BindComplete
				return err
			}
		}
		// For Sync-terminated pipelines (or after handling the Flush follow-up),
		// dispatch to the handler which sends CommandComplete + ReadyForQuery.
		switch specialCmd {
		case SimpleQueryListen:
			return p.handleListen(client, specialSQL)
		case SimpleQueryUnlisten:
			return p.handleUnlisten(client, specialSQL)
		case SimpleQueryNotify:
			return p.handleNotify(client, specialSQL)
		}
	}

	// --- Phase 2: acquire backend ---
	backend := client.GetBackend()
	wasPinned := backend != nil

	if backend == nil {
		pool := p.getPool(client.GetDatabase(), client.GetUser())
		if pool == nil {
			return client.SendErrorResponse("FATAL", "53300", "no Pool available")
		}
		var err error
		backend, err = pool.borrowConn(p.Context)
		if err != nil {
			logger.WithError(err).Error("Failed to borrow backend for extended query")
			return client.SendErrorResponse("FATAL", "53300", "too many connections")
		}
		if requiresPin {
			// Non-remappable named/unnamed statement: pin immediately. NOTE:
			// SetInTransaction(true) is reused here purely as the "backend is
			// pinned to this client" mechanism — there may be no real SQL
			// transaction. inTransaction doubles as the pin flag throughout
			// (reconcileConn, simple-query routing). Logic that needs the real
			// transaction state (e.g. deferred LISTEN/UNLISTEN) must use
			// LastTxStatus(), not IsInTransaction(). Cleanly separating "pinned"
			// from "in transaction" is a deliberate follow-up refactor.
			client.SetBackend(backend)
			backend.SetClient(client)
			client.SetInTransaction(true)
		}
	}

	if p.Config.Server.QueryTimeout > 0 {
		if conn, ok := backend.Conn.(net.Conn); ok {
			conn.SetReadDeadline(time.Now().Add(p.Config.Server.QueryTimeout))
			defer conn.SetReadDeadline(time.Time{})
		}
	}

	// --- Phase 3.5: inject missing Parses for Bind-referenced statements ---
	// When a client pipeline contains Bind but no Parse (e.g. asyncpg re-using a
	// cached statement across a different backend connection), the backend may not
	// have the statement deployed. Synthesize and send Parse + wait for ParseComplete
	// now, before forwarding the rest of the pipeline.
	var sentParseHashes []string
	for hash := range bindRequiredHashes {
		// Skip if this hash also has a Parse in the pipeline (handled in phase 4).
		hasParseInPipeline := false
		for _, h := range remappedParses {
			if h == hash {
				hasParseInPipeline = true
				break
			}
		}
		if hasParseInPipeline {
			continue
		}
		if backend.HasStmt(hash) {
			continue
		}
		// Fetch the canonical SQL from the stmt cache.
		pool := p.getPool(client.GetDatabase(), client.GetUser())
		if pool == nil {
			backend.Release()
			return client.SendErrorResponse("ERROR", "26000", "prepared statement not found")
		}
		entry := pool.Target.StmtCache.Get(hash)
		if entry == nil {
			backend.Release()
			return client.SendErrorResponse("ERROR", "26000", "prepared statement not found")
		}
		// Send Parse + Sync to deploy the statement.
		parseBody := BuildParseBody(StmtName(hash), entry.CanonicalSQL, nil)
		if err := backend.WriteMessage('P', parseBody); err != nil {
			backend.Release()
			return client.SendErrorResponse("ERROR", "08006", "connection failure")
		}
		if err := backend.WriteMessage('S', []byte{}); err != nil {
			backend.Release()
			return client.SendErrorResponse("ERROR", "08006", "connection failure")
		}
		// Read until ReadyForQuery, expecting ParseComplete then ReadyForQuery.
		for {
			mt, body, err := backend.ReadMessage()
			if err != nil {
				backend.Release()
				return client.SendErrorResponse("ERROR", "08006", "connection failure")
			}
			PutMsgBody(body)
			if mt == '1' { // ParseComplete
				backend.MarkStmt(hash)
				logger.Debug("Synthesized Parse deployed", "hash", hash)
			} else if mt == 'E' { // ErrorResponse
				backend.Release()
				return client.SendErrorResponse("ERROR", "26000", "failed to deploy prepared statement")
			} else if mt == 'Z' { // ReadyForQuery
				break
			}
		}
	}

	// --- Phase 4: forward the rewritten pipeline ---
	// For remapped Parse messages, skip if the backend already has the statement
	// deployed — sending Parse twice for the same name is a PostgreSQL error.
	for i, m := range rewritten {
		if m.msgType == 'P' {
			if hash, ok := remappedParses[i]; ok {
				if backend.HasStmt(hash) {
					// Already deployed — skip Parse to backend, but synthesize
					// ParseComplete to the client (it sent Parse and expects a response).
					if err := client.WriteMessage('1', nil); err != nil {
						backend.Release()
						return err
					}
					continue
				}
				// Will be sent — record hash so we can mark it on ParseComplete.
				sentParseHashes = append(sentParseHashes, hash)
			}
		}
		if err := backend.WriteMessage(m.msgType, m.body); err != nil {
			logger.WithError(err).Error("Failed to forward extended query message",
				"msg_type", string(m.msgType))
			backend.Release()
			return client.SendErrorResponse("ERROR", "08006", "connection failure")
		}
	}

	// --- Phase 5: read responses ---
	lastMsg := rewritten[len(rewritten)-1].msgType

	if lastMsg == 'S' { // Sync
		txStatus, err := p.forwardExtendedResponse(client, backend, sentParseHashes, logger)
		if err != nil {
			backend.Release()
			return err
		}
		// wasPinned tracks state before this pipeline. If we pinned during this
		// pipeline (requiresPin=true), reconcileConn must also treat it as pinned
		// so it correctly unsets SetBackend on a clean 'I' status.
		p.reconcileConn(client, backend, txStatus, wasPinned || requiresPin, logger)
	} else { // Flush ('H')
		if err := p.drainFlushResponse(client, backend, sentParseHashes); err != nil {
			backend.Release()
			return err
		}
		// After Flush, connection stays available; client will send more messages.
		// If we acquired it for this pipeline without pinning, keep it associated
		// temporarily — the next Sync will trigger reconcileConn.
		if !wasPinned && !requiresPin {
			client.SetBackend(backend)
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
func (p *Server) forwardExtendedResponse(client *Client, backend *Backend, sentParseHashes []string, logger *logger.Logger) (byte, error) {
	// sentParseHashes lists, in order, the hashes of Parse messages that were
	// actually forwarded to the backend this pipeline. ParseComplete responses
	// arrive in that same order; we mark each hash deployed as we see them.
	parseIdx := 0

	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			if netErr, ok := err.(*net.OpError); ok && netErr.Timeout() {
				return 0, client.SendErrorResponse("ERROR", "57014", "query timeout")
			}
			return 0, client.SendErrorResponse("ERROR", "08006", "connection failure")
		}

		// Each ParseComplete corresponds to a forwarded Parse in pipeline order.
		if msgType == '1' && parseIdx < len(sentParseHashes) {
			backend.MarkStmt(sentParseHashes[parseIdx])
			parseIdx++
		}

		writeErr := client.WriteMessage(msgType, body)
		// Extract status before returning body to Pool.
		var status byte
		if msgType == 'Z' && len(body) > 0 {
			status = body[0]
		}
		PutMsgBody(body)
		if writeErr != nil {
			if isClientGone(writeErr) {
				return 0, writeErr
			}
			return 0, writeErr
		}

		if msgType == 'Z' { // ReadyForQuery
			if status != 0 {
				return status, nil
			}
			return 'I', nil
		}
	}
}

// executeQuery handles a simple query ('Q') from the client.
// Transaction state is driven by the ReadyForQuery status byte.
func (p *Server) executeQuery(client *Client, query string) error {
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
		// NOTIFY inside a transaction must run on the pinned backend so the
		// notification fires at COMMIT and ReadyForQuery keeps reporting 'T'.
		// Borrowing a one-shot backend (handleNotify) is correct only in
		// autocommit; in a transaction we fall through to the pinned-backend
		// path below.
		if !client.IsInTransaction() {
			return p.handleNotify(client, query)
		}
	}

	pinned := client.IsInTransaction()

	// Serve through the prepared statement cache when not in a transaction
	// and the query was successfully parameterized.
	if !pinned && result != nil {
		return p.executeAsPrepared(client, query, result, logger)
	}

	// --- Fallback: simple query protocol (original path) ---
	var backend *Backend

	if pinned {
		backend = client.GetBackend()
		if backend == nil {
			logger.Warn("In transaction but no pinned backend, borrowing from Pool")
			pool := p.getPool(client.GetDatabase(), client.GetUser())
			if pool == nil {
				return client.SendErrorResponse("FATAL", "53300", "no Pool available")
			}
			var err error
			backend, err = pool.borrowConn(p.Context)
			if err != nil {
				return client.SendErrorResponse("FATAL", "53300", "too many connections")
			}
			client.SetBackend(backend)
			backend.SetClient(client)
		}
	} else {
		pool := p.getPool(client.GetDatabase(), client.GetUser())
		if pool == nil {
			return client.SendErrorResponse("FATAL", "53300", "no Pool available")
		}
		var err error
		backend, err = pool.borrowConn(p.Context)
		if err != nil {
			logger.WithError(err).Error("Failed to borrow backend connection")
			return client.SendErrorResponse("FATAL", "53300", "too many connections")
		}
	}

	if p.Config.Server.QueryTimeout > 0 {
		if conn, ok := backend.Conn.(net.Conn); ok {
			conn.SetReadDeadline(time.Now().Add(p.Config.Server.QueryTimeout))
			defer conn.SetReadDeadline(time.Time{})
		}
	}

	if err := p.forwardQueryToBackend(backend, query); err != nil {
		logger.WithError(err).Error("Failed to forward query to backend")
		backend.Release()
		return client.SendErrorResponse("ERROR", "08006", "connection failure")
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
	atomic.AddInt64(&p.GlobalStats.TotalQueries, 1)

	p.reconcileConn(client, backend, txStatus, pinned, logger)

	return nil
}

// executeAsPrepared serves a simple query through the extended protocol using
// the target-level prepared statement cache. It:
//  1. Registers the canonical statement in the target cache (or finds existing).
//  2. Borrows a backend connection from the Pool.
//  3. Sends Parse to the backend only if this backend hasn't seen this statement.
//  4. Sends Bind (text params, binary results) + Execute + Sync.
//  5. Forwards all responses to the client, translating to what the client
//     expects from a simple query (CommandComplete + ReadyForQuery).
//  6. Returns the backend to the Pool (no pinning — no transaction opened).
func (p *Server) executeAsPrepared(client *Client, originalSQL string, result *ParameterizeResult, logger *logger.Logger) error {
	pool := p.getPool(client.GetDatabase(), client.GetUser())
	if pool == nil {
		return client.SendErrorResponse("FATAL", "53300", "no Pool available")
	}

	// Register in the target cache.
	entry, isNew := pool.Target.StmtCache.GetOrRegister(
		result.Hash, result.CanonicalSQL, originalSQL, len(result.Values))
	if isNew {
		logger.Debug("Registered new prepared statement",
			"hash", result.Hash,
			"params", entry.ParamCount,
			"sql", result.CanonicalSQL)
	}

	backend, err := pool.borrowConn(p.Context)
	if err != nil {
		logger.WithError(err).Error("Failed to borrow backend for prepared query")
		return client.SendErrorResponse("FATAL", "53300", "too many connections")
	}

	if p.Config.Server.QueryTimeout > 0 {
		if conn, ok := backend.Conn.(net.Conn); ok {
			conn.SetReadDeadline(time.Now().Add(p.Config.Server.QueryTimeout))
			defer conn.SetReadDeadline(time.Time{})
		}
	}

	stmtName := StmtName(result.Hash)

	// Deploy the prepared statement if this backend hasn't seen it yet.
	if !backend.HasStmt(result.Hash) {
		parseBody := BuildParseBody(stmtName, result.CanonicalSQL, nil)
		if err := backend.WriteMessage('P', parseBody); err != nil {
			backend.Release()
			return client.SendErrorResponse("ERROR", "08006", "connection failure")
		}
	}

	// Bind: all params text, and request results in TEXT format.
	//
	// Invariant: pgfox always asks the backend for the result format the client
	// will consume, and forwards the bytes verbatim. A simple-query client
	// issued 'Q' and can only read text, so we request text here. (The extended
	// protocol path forwards the client's own Bind, so binary clients there get
	// binary.) This keeps pgfox free of any value-level type knowledge — every
	// forward is a byte copy, never a transcode.
	bindBody := BuildBindBody(
		"", // unnamed portal
		stmtName,
		nil, // all params in text format (we extracted them as text)
		result.Values,
		nil, // result formats: none specified = all text
	)
	if err := backend.WriteMessage('B', bindBody); err != nil {
		backend.Release()
		return client.SendErrorResponse("ERROR", "08006", "connection failure")
	}

	// Describe the unnamed portal. Execute never emits a RowDescription, but a
	// simple-query client requires one before any DataRow. The portal Describe
	// makes the backend send RowDescription (row-returning) or NoData (not),
	// which forwardPreparedResponse forwards / swallows respectively.
	if err := backend.WriteMessage('D', BuildDescribePortal("")); err != nil {
		backend.Release()
		return client.SendErrorResponse("ERROR", "08006", "connection failure")
	}

	// Execute the unnamed portal.
	execBody := BuildExecuteBody("", 0)
	if err := backend.WriteMessage('E', execBody); err != nil {
		backend.Release()
		return client.SendErrorResponse("ERROR", "08006", "connection failure")
	}

	// Sync — backend will now respond and send ReadyForQuery.
	if err := backend.WriteMessage('S', SyncBody); err != nil {
		backend.Release()
		return client.SendErrorResponse("ERROR", "08006", "connection failure")
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
	atomic.AddInt64(&p.GlobalStats.TotalQueries, 1)
	entry.RecordExecution()

	// Prepared statement execution is always stateless from the Pool's perspective
	// (ReadyForQuery will report 'I' since we didn't open a transaction).
	p.reconcileConn(client, backend, txStatus, false, logger)

	return nil
}

// forwardPreparedResponse reads backend responses after Parse+Bind+Describe+
// Execute+Sync and forwards them to the client, translating extended protocol
// messages to what a simple query client expects.
//
// Handles:
//   - ParseComplete ('1')   → consumed, not forwarded; marks stmt deployed
//   - BindComplete ('2')    → consumed, not forwarded
//   - NoData ('n')          → consumed, not forwarded (from the portal Describe)
//   - RowDescription ('T')  → forwarded as-is (text format, from the Describe)
//   - DataRow ('D')         → forwarded as-is (text data from backend)
//   - CommandComplete ('C') → forwarded as-is
//   - ErrorResponse ('E')   → forwarded as-is
//   - ReadyForQuery ('Z')   → forwarded as-is, loop exits
func (p *Server) forwardPreparedResponse(
	client *Client,
	backend *Backend,
	hash string,
	entry *CachedStmt,
	logger *logger.Logger,
) (byte, error) {
	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			if netErr, ok := err.(*net.OpError); ok && netErr.Timeout() {
				return 0, client.SendErrorResponse("ERROR", "57014", "query timeout")
			}
			return 0, client.SendErrorResponse("ERROR", "08006", "connection failure")
		}

		switch msgType {
		case '1': // ParseComplete — statement successfully deployed on this backend
			if !backend.HasStmt(hash) {
				backend.MarkStmt(hash)
				entry.RecordDeploy()
				logger.Debug("Prepared statement deployed on backend", "hash", hash)
			}
			PutMsgBody(body) // not forwarded to client

		case '2': // BindComplete
			PutMsgBody(body) // not forwarded to client

		case 'n': // NoData — from the portal Describe on a non-row-returning
			// statement (e.g. INSERT without RETURNING). Simple query protocol
			// has no NoData message, so consume it without forwarding. The
			// matching CommandComplete still flows through the default case.
			PutMsgBody(body)

		case 'Z': // ReadyForQuery
			var status byte
			if len(body) > 0 {
				status = body[0]
			}
			writeErr := client.WriteMessage(msgType, body)
			PutMsgBody(body)
			if writeErr != nil {
				return 0, writeErr
			}
			if status != 0 {
				return status, nil
			}
			return 'I', nil

		default:
			// DataRow, RowDescription, CommandComplete, ErrorResponse, NoticeResponse, etc.
			writeErr := client.WriteMessage(msgType, body)
			PutMsgBody(body)
			if writeErr != nil {
				if isClientGone(writeErr) {
					return 0, writeErr
				}
				return 0, writeErr
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
	client *Client,
	backend *Backend,
	txStatus byte,
	wasPinned bool,
	logger *logger.Logger,
) {
	hasNamedStmts := client.HasNamedStatements()

	switch txStatus {
	case 'T', 'E':
		if !wasPinned {
			logger.Debug("Transaction opened — pinning connection", "tx_status", string(txStatus))
			client.SetBackend(backend)
			backend.SetClient(client)
			client.SetInTransaction(true)
		}

	case 'I':
		// A transaction reaching idle resolves any LISTEN/UNLISTEN buffered
		// during it (applied on COMMIT, discarded otherwise). No-op if none.
		p.resolvePendingListens(client)

		if hasNamedStmts {
			// Keep pinned — named statements live on this specific backend.
			if !wasPinned {
				client.SetBackend(backend)
				backend.SetClient(client)
				client.SetInTransaction(true)
			}
			logger.Debug("Keeping connection pinned for named statements",
				"count", client.namedStmts)
		} else if wasPinned {
			logger.Debug("Transaction closed — returning connection to Pool")
			client.SetBackend(nil)
			backend.SetClient(nil)
			client.SetInTransaction(false)
			backend.Return()
		} else {
			backend.Return()
		}

	default:
		logger.Warn("Unknown ReadyForQuery status, returning connection",
			"status", string(txStatus))
		if wasPinned && !hasNamedStmts {
			client.SetBackend(nil)
			backend.SetClient(nil)
			client.SetInTransaction(false)
		}
		backend.Return()
	}
}

// drainFlushResponse reads backend responses after a Flush message.
// PostgreSQL does NOT send ReadyForQuery after Flush — only after Sync.
// The response to Parse+Describe+Flush ends with RowDescription('T') or NoData('n').
func (p *Server) drainFlushResponse(client *Client, backend *Backend, sentParseHashes []string) error {
	logger := client.Logger()
	// ParseComplete responses arrive in the same order as the Parse messages
	// that were sent. Mark each hash as deployed so phase 3.5 does not
	// re-inject a Parse on the same backend for the subsequent Bind pipeline.
	parseIdx := 0
	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			logger.WithError(err).Error("Failed to read response after flush")
			return client.SendErrorResponse("ERROR", "08006", "connection failure")
		}

		if msgType == '1' && parseIdx < len(sentParseHashes) {
			backend.MarkStmt(sentParseHashes[parseIdx])
			parseIdx++
		}

		writeErr := client.WriteMessage(msgType, body)
		done := msgType == 'T' || msgType == 'n' || msgType == 'E'
		PutMsgBody(body)
		if writeErr != nil {
			return writeErr
		}
		// RowDescription or NoData ends the Describe response.
		// ErrorResponse ends a failed stage.
		// Connection stays pinned — client will send Bind+Execute+Sync next.
		if done {
			return nil
		}
	}
}

// forwardQueryToBackend sends a simple query to the backend.
func (p *Server) forwardQueryToBackend(backend *Backend, query string) error {
	return backend.WriteMessage('Q', []byte(query+"\x00"))
}

// forwardCompleteBackendResponse forwards all messages until ReadyForQuery,
// returning the transaction status byte from the ReadyForQuery message.
//
//	'I' — idle (not in a transaction)
//	'T' — in a transaction block
//	'E' — in a failed transaction block
func (p *Server) forwardCompleteBackendResponse(client *Client, backend *Backend) (byte, error) {
	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			if netErr, ok := err.(*net.OpError); ok && netErr.Timeout() {
				return 0, fmt.Errorf("query timeout")
			}
			return 0, err
		}

		var status byte
		if msgType == 'Z' && len(body) > 0 {
			status = body[0]
		}
		writeErr := client.WriteMessage(msgType, body)
		PutMsgBody(body)
		if writeErr != nil {
			if isClientGone(writeErr) {
				return 0, writeErr
			}
			return 0, writeErr
		}

		if msgType == 'Z' {
			if status != 0 {
				return status, nil
			}
			return 'I', nil
		}
	}
}

// forwardUntilReady forwards messages until ReadyForQuery, returning the
// transaction status byte. Used after Sync in executeExtendedPipeline.
// Kept as a thin wrapper over forwardExtendedResponse for callers in
// listen_notify.go and similar that don't need the extended name-remapping path.
func (p *Server) forwardUntilReady(client *Client, backend *Backend) (byte, error) {
	// No remapped Parse messages — pass nil so no MarkStmt calls are made.
	return p.forwardExtendedResponse(client, backend, nil, client.Logger())
}
