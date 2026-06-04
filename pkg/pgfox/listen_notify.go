package pgfox

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// Channel identifies a unique PostgreSQL notification channel within a database.
type Channel struct {
	Database string
	Name     string
}

// listenKind classifies a transaction-deferred LISTEN/UNLISTEN action.
type listenKind int

const (
	listenKindListen listenKind = iota
	listenKindUnlisten
	listenKindUnlistenAll
)

// pendingListen is a LISTEN/UNLISTEN action buffered while a transaction is
// open. PostgreSQL applies such actions at COMMIT and discards them at ROLLBACK.
type pendingListen struct {
	kind    listenKind
	channel Channel
}

// Listen monitors one PostgreSQL channel for a database.
// It owns exactly one dedicated backend connection and one goroutine.
// All clients subscribed to this channel share the single backend connection.
type Listen struct {
	Channel Channel
	Backend *Backend
	Clients map[*Client]bool
	Mu      sync.RWMutex
	Done    chan struct{}
}

// addClient registers a client as a subscriber.
func (l *Listen) addClient(client *Client) {
	l.Mu.Lock()
	defer l.Mu.Unlock()
	l.Clients[client] = true
}

// removeClient unregisters a client. Returns true if the monitor is now empty.
func (l *Listen) removeClient(client *Client) bool {
	l.Mu.Lock()
	defer l.Mu.Unlock()
	delete(l.Clients, client)
	return len(l.Clients) == 0
}

// clientCount returns the number of subscribed clients.
func (l *Listen) clientCount() int {
	l.Mu.RLock()
	defer l.Mu.RUnlock()
	return len(l.Clients)
}

// copyClients returns a snapshot of the current client set.
func (l *Listen) copyClients() []*Client {
	l.Mu.RLock()
	defer l.Mu.RUnlock()
	out := make([]*Client, 0, len(l.Clients))
	for c := range l.Clients {
		out = append(out, c)
	}
	return out
}

// run is the goroutine that owns the backend connection exclusively.
// It blocks on ReadMessage and fans notifications out to all subscribed clients.
func (l *Listen) run(p *Server) {
	defer p.Wg.Done()
	defer close(l.Done)

	logger := p.Logger.
		WithField("component", "listen").
		WithField("database", l.Channel.Database).
		WithField("channel", l.Channel.Name)

	logger.Info("Listen monitor started")

	for {
		l.Mu.RLock()
		backend := l.Backend
		l.Mu.RUnlock()

		if backend == nil {
			logger.Debug("Listen monitor stopping — backend cleared")
			return
		}

		msgType, body, err := backend.ReadMessage()
		if err != nil {
			if p.Context.Err() != nil {
				logger.Debug("Listen monitor stopping — server shutdown")
				return
			}

			// No clients left means tearDownListen closed the connection.
			if l.clientCount() == 0 {
				logger.Debug("Listen monitor stopping — no clients remaining")
				return
			}

			logger.WithError(err).Warn("Queue connection lost, attempting reconnect")

			newBackend, reconnErr := p.reconnectListen(l)
			if reconnErr == nil {
				logger.Info("Reconnected listen monitor successfully")
				l.Mu.Lock()
				l.Backend = newBackend
				l.Mu.Unlock()
				continue
			}

			logger.WithError(reconnErr).Error("Reconnect failed, tearing down")
			p.failListen(l, fmt.Errorf("lost connection to database %q: %w", l.Channel.Database, reconnErr))
			return
		}

		switch msgType {
		case 'A': // NotificationResponse
			notification := parseNotificationResponse(body)
			if notification == nil {
				logger.Warn("Received unparseable notification")
				continue
			}
			logger.Debug("Notification received, fanning out", "payload", notification.Payload)
			l.fanOut(p, *notification)

		case 'Z', 'S', 'N': // ReadyForQuery, ParameterStatus, NoticeResponse
			continue
		case 'E':
			logger.Warn("Error from listen backend", "error", ParseErrorMessage(body))
		default:
			logger.Warn("Unexpected message in listen monitor", "type", string([]byte{msgType}))
		}
	}
}

// fanOut sends a notification to every subscribed client.
func (l *Listen) fanOut(p *Server, notification NotificationMessage) {
	logger := p.Logger.
		WithField("channel", notification.Channel).
		WithField("payload", notification.Payload)

	clients := l.copyClients()
	sent, failed := 0, 0

	for _, client := range clients {
		if err := client.SendNotificationToClient(notification); err != nil {
			logger.WithError(err).Warn("Failed to deliver notification, removing client",
				"client", client.RemoteAddr())
			p.RemoveClientFromListen(l.Channel, client)
			failed++
		} else {
			sent++
		}
	}

	atomic.AddInt64(&p.GlobalStats.NotificationsSent, int64(sent))
	logger.Debug("Notification fan-out complete", "sent", sent, "failed", failed)
}

// --- Server-level listen management ---

// getOrCreateListen returns the existing monitor or creates a new one.
// The backend connection is opened via Pool.newConn(p) — bypassing backendPool
// so it never competes with query connections for Pool slots.
func (p *Server) getOrCreateListen(ch Channel, client *Client) (*Listen, bool, error) {
	// Fast path: monitor already exists.
	p.ListenersMu.RLock()
	if l, ok := p.Listeners[ch]; ok {
		p.ListenersMu.RUnlock()
		l.addClient(client)
		return l, false, nil
	}
	p.ListenersMu.RUnlock()

	// Slow path: open a dedicated connection directly, outside the Pool channel.
	pool := p.getPool(ch.Database, client.GetUser())
	if pool == nil {
		return nil, false, fmt.Errorf("no Pool for database %s", ch.Database)
	}

	// Budget check — include both Pool and listen connections.
	// Use the atomic listen counter; totalOpen is read via the atomic snapshot
	// taken by Stats() to avoid a data race with the target goroutine.
	listenOpen := int(atomic.LoadInt32(&pool.Target.ListenOpen))
	totalOpen := int(pool.Target.AtomicTotalOpen.Load())
	if totalOpen+listenOpen >= pool.Target.MaxConnections {
		return nil, false, fmt.Errorf("target %s at connection limit", pool.Target.Name)
	}

	backend, err := pool.newConn(p)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open listen connection: %w", err)
	}
	atomic.AddInt32(&pool.Target.ListenOpen, 1)

	listenQuery := fmt.Sprintf("LISTEN %q", ch.Name)
	if err := backend.WriteMessage('Q', []byte(listenQuery+"\x00")); err != nil {
		backend.Close()
		atomic.AddInt32(&pool.Target.ListenOpen, -1)
		return nil, false, fmt.Errorf("failed to send LISTEN to backend: %w", err)
	}

	if err := p.drainUntilReady(backend); err != nil {
		backend.Close()
		atomic.AddInt32(&pool.Target.ListenOpen, -1)
		return nil, false, fmt.Errorf("backend rejected LISTEN: %w", err)
	}

	// Write lock only for registration — race guard re-checks under lock.
	p.ListenersMu.Lock()

	// Race guard: another goroutine may have created the monitor while we
	// were acquiring the backend connection.
	if existing, ok := p.Listeners[ch]; ok {
		p.ListenersMu.Unlock()
		backend.Close()
		atomic.AddInt32(&pool.Target.ListenOpen, -1)
		existing.addClient(client)
		return existing, false, nil
	}

	l := &Listen{
		Channel: ch,
		Backend: backend,
		Clients: map[*Client]bool{client: true},
		Done:    make(chan struct{}),
	}

	p.Listeners[ch] = l
	p.ListenersMu.Unlock()

	p.Wg.Add(1)
	go l.run(p)

	p.Logger.Info("Created listen monitor",
		"database", ch.Database,
		"channel", ch.Name)

	return l, true, nil
}

// removeClientFromListen removes a client from a channel's monitor.
// If the monitor becomes empty it is torn down.
func (p *Server) RemoveClientFromListen(ch Channel, client *Client) {
	p.ListenersMu.Lock()
	l, ok := p.Listeners[ch]
	if !ok {
		p.ListenersMu.Unlock()
		return
	}

	empty := l.removeClient(client)
	if empty {
		delete(p.Listeners, ch)
	}
	p.ListenersMu.Unlock()

	if empty {
		p.tearDownListen(l)
	}
}

// tearDownListen gracefully shuts down a Listen monitor.
func (p *Server) tearDownListen(l *Listen) {
	logger := p.Logger.
		WithField("database", l.Channel.Database).
		WithField("channel", l.Channel.Name)

	logger.Info("Tearing down listen monitor")

	l.Mu.Lock()
	backend := l.Backend
	l.Backend = nil
	l.Mu.Unlock()

	if backend != nil {
		// Best-effort UNLISTEN before closing.
		unlistenQuery := fmt.Sprintf("UNLISTEN %q\x00", l.Channel.Name)
		_ = backend.WriteMessage('Q', []byte(unlistenQuery))
		backend.Close()
		if backend.Pool != nil {
			atomic.AddInt32(&backend.Pool.Target.ListenOpen, -1)
		}
	}

	<-l.Done
	logger.Info("Listen monitor torn down")
}

// failListen notifies all subscribed clients with a fatal error and tears down.
func (p *Server) failListen(l *Listen, err error) {
	p.ListenersMu.Lock()
	delete(p.Listeners, l.Channel)
	p.ListenersMu.Unlock()

	clients := l.copyClients()
	msg := fmt.Sprintf("lost connection to PostgreSQL on channel %q: %v", l.Channel.Name, err)

	for _, client := range clients {
		client.RemoveListenChannel(l.Channel)
		_ = client.SendErrorResponse("FATAL", "57P01", msg)
		client.Close()
	}

	p.Logger.Error("Listen monitor failed, all clients disconnected",
		"database", l.Channel.Database,
		"channel", l.Channel.Name,
		"clients", len(clients))
}

// reconnectListen creates a fresh backend connection for an existing monitor
// and re-issues LISTEN on it.
func (p *Server) reconnectListen(l *Listen) (*Backend, error) {
	l.Mu.RLock()
	var sample *Client
	for c := range l.Clients {
		sample = c
		break
	}
	l.Mu.RUnlock()

	if sample == nil {
		return nil, fmt.Errorf("no clients to reconnect for")
	}

	pool := p.getPool(l.Channel.Database, sample.GetUser())
	if pool == nil {
		return nil, fmt.Errorf("no Pool for database %s", l.Channel.Database)
	}

	backend, err := pool.newConn(p)
	if err != nil {
		return nil, fmt.Errorf("failed to open reconnect connection: %w", err)
	}

	listenQuery := fmt.Sprintf("LISTEN %q\x00", l.Channel.Name)
	if err := backend.WriteMessage('Q', []byte(listenQuery)); err != nil {
		backend.Close()
		return nil, fmt.Errorf("failed to send LISTEN on reconnect: %w", err)
	}

	if err := p.drainUntilReady(backend); err != nil {
		backend.Close()
		return nil, fmt.Errorf("backend rejected LISTEN on reconnect: %w", err)
	}

	return backend, nil
}

// drainUntilReady reads messages from a backend until ReadyForQuery.
func (p *Server) drainUntilReady(backend *Backend) error {
	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			return fmt.Errorf("read error while waiting for ready: %w", err)
		}
		switch msgType {
		case 'Z':
			return nil
		case 'E':
			return fmt.Errorf("backend error: %s", ParseErrorMessage(body))
		}
	}
}

// --- Cleanup and stats ---

// cleanupClientListeners removes a client from all listen monitors it belongs to.
func (p *Server) cleanupClientListeners(client *Client) {
	channels := client.GetListenChannels()
	for ch := range channels {
		p.RemoveClientFromListen(ch, client)
	}
}

// shutdownListeners tears down all active listen monitors during server shutdown.
func (p *Server) shutdownListeners() {
	p.ListenersMu.Lock()
	listens := make([]*Listen, 0, len(p.Listeners))
	for _, l := range p.Listeners {
		listens = append(listens, l)
	}
	p.Listeners = make(map[Channel]*Listen)
	p.ListenersMu.Unlock()

	for _, l := range listens {
		l.Mu.Lock()
		backend := l.Backend
		l.Backend = nil
		l.Mu.Unlock()

		if backend != nil {
			backend.Close()
			if backend.Pool != nil {
				atomic.AddInt32(&backend.Pool.Target.ListenOpen, -1)
			}
		}
	}

	for _, l := range listens {
		<-l.Done
	}

	p.Logger.Info("All listen monitors shut down", "count", len(listens))
}

// handleListen handles a LISTEN command from a client.
func (p *Server) handleListen(client *Client, query string) error {
	logger := client.Logger()

	parts := strings.Fields(query)
	if len(parts) < 2 {
		return client.SendErrorResponse("ERROR", "42601", "syntax error in LISTEN command")
	}
	// Handle both quoted ("my_channel") and unquoted (my_channel) names.
	// Also strip trailing semicolons sent by some clients.
	channelName := strings.TrimRight(strings.Trim(parts[1], `"`), ";")

	ch := Channel{
		Database: client.GetDatabase(),
		Name:     channelName,
	}

	// Transactional LISTEN: PostgreSQL defers the subscription to COMMIT and
	// discards it on ROLLBACK. Buffer it and reply with the correct in-
	// transaction status; resolvePendingListens applies it when the transaction
	// ends. LastTxStatus (not IsInTransaction) is used so a backend pinned only
	// for named statements is not mistaken for an open transaction.
	if status := client.LastTxStatus(); status == 'T' || status == 'E' {
		if status == 'E' {
			if err := client.SendErrorResponse("ERROR", "25P02",
				"current transaction is aborted, commands ignored until end of transaction block"); err != nil {
				return err
			}
			return client.SendReadyForQuery('E')
		}
		client.BufferListen(listenKindListen, ch)
		logger.Debug("Buffered LISTEN until commit", "channel", channelName)
		if err := client.SendCommandComplete("LISTEN"); err != nil {
			return err
		}
		return client.SendReadyForQuery('T')
	}

	l, isNew, err := p.getOrCreateListen(ch, client)
	if err != nil {
		logger.WithError(err).Error("Failed to set up listen monitor")
		return client.SendErrorResponse("ERROR", "08006", "could not establish listen connection")
	}

	client.AddListenChannel(ch)

	if isNew {
		logger.Info("Created new listen monitor", "channel", channelName)
	} else {
		logger.Info("Joined existing listen monitor", "channel", channelName,
			"total_clients", l.clientCount())
	}

	if err := client.SendCommandComplete("LISTEN"); err != nil {
		return err
	}
	return client.SendReadyForQuery('I')
}

// handleUnlisten handles an UNLISTEN command from a client.
func (p *Server) handleUnlisten(client *Client, query string) error {
	logger := client.Logger()

	parts := strings.Fields(query)

	// Transactional UNLISTEN: deferred to COMMIT, discarded on ROLLBACK.
	if status := client.LastTxStatus(); status == 'T' || status == 'E' {
		if status == 'E' {
			if err := client.SendErrorResponse("ERROR", "25P02",
				"current transaction is aborted, commands ignored until end of transaction block"); err != nil {
				return err
			}
			return client.SendReadyForQuery('E')
		}
		if len(parts) >= 2 && parts[1] == "*" {
			client.BufferListen(listenKindUnlistenAll, Channel{Database: client.GetDatabase()})
		} else if len(parts) >= 2 {
			channelName := strings.TrimRight(strings.Trim(parts[1], `"`), ";")
			client.BufferListen(listenKindUnlisten, Channel{Database: client.GetDatabase(), Name: channelName})
		}
		logger.Debug("Buffered UNLISTEN until commit")
		if err := client.SendCommandComplete("UNLISTEN"); err != nil {
			return err
		}
		return client.SendReadyForQuery('T')
	}

	if len(parts) >= 2 && strings.ToUpper(parts[1]) == "*" {
		channels := client.GetListenChannels()
		for ch := range channels {
			p.RemoveClientFromListen(ch, client)
		}
		client.ClearListenChannels()
		logger.Info("UNLISTEN * — left all channels", "count", len(channels))
	} else if len(parts) >= 2 {
		channelName := strings.TrimRight(strings.Trim(parts[1], `"`), ";")
		ch := Channel{
			Database: client.GetDatabase(),
			Name:     channelName,
		}
		p.RemoveClientFromListen(ch, client)
		client.RemoveListenChannel(ch)
		logger.Info("UNLISTEN", "channel", channelName)
	}

	if err := client.SendCommandComplete("UNLISTEN"); err != nil {
		return err
	}
	return client.SendReadyForQuery('I')
}

// resolvePendingListens applies or discards the LISTEN/UNLISTEN actions buffered
// during a transaction, based on its outcome. It is called when a transaction
// reaches idle ('I'). If the transaction-ending command completed as "COMMIT"
// the actions are applied in order; otherwise (ROLLBACK, failed COMMIT, error)
// they are discarded — matching PostgreSQL's transactional LISTEN semantics.
//
// Granularity is whole-transaction: savepoint-level rollback of a LISTEN
// (ROLLBACK TO SAVEPOINT) is not modelled — a LISTEN survives until the outer
// transaction's outcome. This is a deliberate limitation for the pooler.
//
// Note: the actions are applied just after the COMMIT's ReadyForQuery has been
// forwarded to the client, so there is a small asynchronous window before the
// subscription is live. This is invisible in practice since notifications are
// inherently asynchronous.
func (p *Server) resolvePendingListens(client *Client) {
	if !client.HasPendingListens() {
		return
	}
	actions := client.TakePendingListens()

	if !strings.EqualFold(client.LastCommandTag(), "COMMIT") {
		client.Logger().Debug("Transaction did not commit — discarding buffered LISTEN/UNLISTEN",
			"count", len(actions))
		return
	}

	for _, a := range actions {
		switch a.kind {
		case listenKindListen:
			if _, _, err := p.getOrCreateListen(a.channel, client); err != nil {
				client.Logger().WithError(err).Warn("Failed to apply deferred LISTEN at commit",
					"channel", a.channel.Name)
				continue
			}
			client.AddListenChannel(a.channel)
		case listenKindUnlisten:
			p.RemoveClientFromListen(a.channel, client)
			client.RemoveListenChannel(a.channel)
		case listenKindUnlistenAll:
			for ch := range client.GetListenChannels() {
				p.RemoveClientFromListen(ch, client)
			}
			client.ClearListenChannels()
		}
	}
}

// handleNotify handles NOTIFY commands and pg_notify() function calls.
func (p *Server) handleNotify(client *Client, query string) error {
	logger := client.Logger()

	pool := p.getPool(client.GetDatabase(), client.GetUser())
	if pool == nil {
		return client.SendErrorResponse("FATAL", "53300", "no Pool available")
	}

	backend, err := pool.borrowConn(p.Context)
	if err != nil {
		logger.WithError(err).Error("Failed to borrow backend for NOTIFY")
		return client.SendErrorResponse("FATAL", "53300", "too many connections")
	}

	if err := p.forwardQueryToBackend(backend, query); err != nil {
		logger.WithError(err).Error("Failed to send NOTIFY to backend")
		backend.Release()
		return client.SendErrorResponse("ERROR", "08006", "connection failure")
	}

	if _, err := p.forwardCompleteBackendResponse(client, backend); err != nil {
		backend.Release()
		if isClientGone(err) {
			return err
		}
		logger.WithError(err).Error("Failed to forward NOTIFY response")
		return err
	}

	// NOTIFY is always outside a transaction from the Pool's perspective —
	// we borrowed for one query and return immediately regardless of status.
	backend.Return()

	logger.Debug("NOTIFY executed successfully")
	return nil
}

// handleNotificationResponse routes an unexpected notification on a pooled
// connection to the appropriate listen monitor.
func (p *Server) handleNotificationResponse(body []byte) {
	notification := parseNotificationResponse(body)
	if notification == nil {
		p.Logger.Error("Failed to parse notification response")
		return
	}

	p.ListenersMu.RLock()
	var matched []*Listen
	for key, l := range p.Listeners {
		if key.Name == notification.Channel {
			matched = append(matched, l)
		}
	}
	p.ListenersMu.RUnlock()

	for _, l := range matched {
		l.fanOut(p, *notification)
	}
}

// registerListener and unregisterListener are shims for the metrics path.
func (p *Server) registerListener(_ Channel, _ *Client) {}
func (p *Server) unregisterListener(ch Channel, client *Client) {
	p.RemoveClientFromListen(ch, client)
}

// getListenerStats returns per-channel client counts for metrics.
func (p *Server) GetListenerStats() map[string]int {
	p.ListenersMu.RLock()
	defer p.ListenersMu.RUnlock()

	stats := make(map[string]int)
	for ch, l := range p.Listeners {
		if count := l.clientCount(); count > 0 {
			stats[ch.Database+"/"+ch.Name] = count
		}
	}
	return stats
}
