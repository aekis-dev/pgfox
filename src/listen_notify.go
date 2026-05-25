package main

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

// Listen monitors one PostgreSQL channel for a database.
// It owns exactly one backend connection and one goroutine.
// All clients subscribed to this channel share the single backend connection.
type Listen struct {
	channel Channel
	backend *BackendConnection
	clients map[*ClientConnection]bool
	mu      sync.RWMutex
	done    chan struct{}
}

// addClient registers a client as a subscriber of this listen monitor.
func (l *Listen) addClient(client *ClientConnection) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.clients[client] = true
}

// removeClient unregisters a client. Returns true if the monitor is now empty.
func (l *Listen) removeClient(client *ClientConnection) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.clients, client)
	return len(l.clients) == 0
}

// clientCount returns the number of subscribed clients.
func (l *Listen) clientCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.clients)
}

// copyClients returns a snapshot of the current client set.
func (l *Listen) copyClients() []*ClientConnection {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]*ClientConnection, 0, len(l.clients))
	for c := range l.clients {
		out = append(out, c)
	}
	return out
}

// run is the goroutine that owns the backend connection exclusively.
// It blocks on ReadMessage and fans out notifications to all subscribed clients.
// On backend failure it attempts one reconnect; if that fails it tears down.
func (l *Listen) run(p *Server) {
	defer p.wg.Done()
	defer close(l.done)

	logger := p.logger.
		WithField("component", "listen").
		WithField("database", l.channel.Database).
		WithField("channel", l.channel.Name)

	logger.Info("Listen monitor started")

	for {
		msgType, body, err := l.backend.ReadMessage()
		if err != nil {
			if p.ctx.Err() != nil {
				logger.Debug("Listen monitor stopping — server shutdown")
				return
			}

			logger.WithError(err).Warn("Backend connection lost, attempting reconnect")

			newBackend, reconnErr := p.reconnectListen(l)
			if reconnErr == nil {
				logger.Info("Reconnected listen monitor successfully")
				l.backend = newBackend
				continue
			}

			logger.WithError(reconnErr).Error("Reconnect failed, notifying clients and tearing down")
			p.failListen(l, fmt.Errorf("lost connection to database %q: %w", l.channel.Database, reconnErr))
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

		case 'Z', 'S', 'N': // ReadyForQuery, ParameterStatus, NoticeResponse — normal idle traffic
			continue

		case 'E':
			logger.Warn("Error from listen backend", "error", parseErrorMessage(body))

		default:
			logger.Warn("Unexpected message in listen monitor", "type", string([]byte{msgType}))
		}
	}
}

// fanOut sends a notification to every subscribed client.
func (l *Listen) fanOut(p *Server, notification NotificationMessage) {
	logger := p.logger.
		WithField("channel", notification.Channel).
		WithField("payload", notification.Payload)

	clients := l.copyClients()
	sent, failed := 0, 0

	for _, client := range clients {
		if err := sendNotificationToClient(client, notification); err != nil {
			logger.WithError(err).Warn("Failed to deliver notification, removing client",
				"client", client.RemoteAddr())
			p.removeClientFromListen(l.channel, client)
			failed++
		} else {
			sent++
		}
	}

	atomic.AddInt64(&p.stats.NotificationsSent, int64(sent))
	logger.Debug("Notification fan-out complete", "sent", sent, "failed", failed)
}

// --- Server-level listen management ---

// getOrCreateListen returns the existing Listen monitor for the given channel,
// or creates a new one. The lock is held only for the lookup and final
// registration — the backend connection is acquired outside the lock.
func (p *Server) getOrCreateListen(ch Channel, client *ClientConnection) (*Listen, bool, error) {
	// Fast path: monitor already exists.
	p.listenersMu.RLock()
	if l, ok := p.listeners[ch]; ok {
		p.listenersMu.RUnlock()
		l.addClient(client)
		return l, false, nil
	}
	p.listenersMu.RUnlock()

	// Slow path: create a new monitor.
	// Acquire a backend connection BEFORE taking the write lock so we don't
	// block all listener operations while waiting for a pool connection.
	backend, err := p.borrowConn(client)
	if err != nil {
		return nil, false, fmt.Errorf("failed to acquire backend for listen monitor: %w", err)
	}

	listenQuery := fmt.Sprintf("LISTEN %s", ch.Name)
	if err := backend.WriteMessage('Q', []byte(listenQuery+"\x00")); err != nil {
		p.returnOrCloseListenConn(backend, ch.Database, client.GetUser(), false)
		return nil, false, fmt.Errorf("failed to send LISTEN to backend: %w", err)
	}

	if err := p.drainUntilReady(backend); err != nil {
		p.returnOrCloseListenConn(backend, ch.Database, client.GetUser(), false)
		return nil, false, fmt.Errorf("backend rejected LISTEN: %w", err)
	}

	// Take write lock only to register the monitor.
	p.listenersMu.Lock()

	// Race guard: another goroutine may have created the monitor while we
	// were acquiring the backend connection.
	if existing, ok := p.listeners[ch]; ok {
		p.listenersMu.Unlock()
		// We don't need our backend — return it to the pool.
		p.returnOrCloseListenConn(backend, ch.Database, client.GetUser(), true)
		existing.addClient(client)
		return existing, false, nil
	}

	l := &Listen{
		channel: ch,
		backend: backend,
		clients: map[*ClientConnection]bool{client: true},
		done:    make(chan struct{}),
	}

	p.listeners[ch] = l
	p.listenersMu.Unlock()

	p.wg.Add(1)
	go l.run(p)

	p.logger.Info("Created listen monitor", "database", ch.Database, "channel", ch.Name)

	return l, true, nil
}

// returnOrCloseListenConn returns a backend connection to its pool (healthy=true)
// or signals the pool manager to close it (healthy=false).
func (p *Server) returnOrCloseListenConn(backend *BackendConnection, dbName, user string, healthy bool) {
	pool := p.getPool(dbName, user)
	if pool == nil {
		backend.Close()
		return
	}
	if healthy {
		pool.returnCh <- backend
	} else {
		pool.closeCh <- backend
	}
}

// removeClientFromListen removes a client from a channel's monitor.
// If the monitor becomes empty it is torn down.
func (p *Server) removeClientFromListen(ch Channel, client *ClientConnection) {
	p.listenersMu.Lock()
	l, ok := p.listeners[ch]
	if !ok {
		p.listenersMu.Unlock()
		return
	}

	empty := l.removeClient(client)
	if empty {
		delete(p.listeners, ch)
	}
	p.listenersMu.Unlock()

	if empty {
		p.tearDownListen(l)
	}
}

// tearDownListen gracefully shuts down a Listen monitor.
func (p *Server) tearDownListen(l *Listen) {
	logger := p.logger.
		WithField("database", l.channel.Database).
		WithField("channel", l.channel.Name)

	logger.Info("Tearing down listen monitor")

	l.mu.Lock()
	backend := l.backend
	l.backend = nil
	l.mu.Unlock()

	if backend != nil {
		// Best-effort UNLISTEN before closing.
		unlistenQuery := fmt.Sprintf("UNLISTEN %s\x00", l.channel.Name)
		_ = backend.WriteMessage('Q', []byte(unlistenQuery))
		// Close via pool manager so stats stay consistent.
		p.returnOrCloseListenConn(backend, l.channel.Database, "", false)
	}

	<-l.done
	logger.Info("Listen monitor torn down")
}

// failListen notifies all subscribed clients with a fatal error and tears down.
func (p *Server) failListen(l *Listen, err error) {
	p.listenersMu.Lock()
	delete(p.listeners, l.channel)
	p.listenersMu.Unlock()

	clients := l.copyClients()
	msg := fmt.Sprintf("lost connection to PostgreSQL on channel %q: %v", l.channel.Name, err)

	for _, client := range clients {
		client.RemoveListenChannel(l.channel)
		_ = sendErrorResponse(client, "FATAL", "57P01", msg)
		client.Close()
	}

	p.logger.Error("Listen monitor failed, all clients disconnected",
		"database", l.channel.Database,
		"channel", l.channel.Name,
		"clients", len(clients))
}

// reconnectListen creates a fresh backend connection for an existing monitor
// and re-issues LISTEN on it.
func (p *Server) reconnectListen(l *Listen) (*BackendConnection, error) {
	l.mu.RLock()
	var sample *ClientConnection
	for c := range l.clients {
		sample = c
		break
	}
	l.mu.RUnlock()

	if sample == nil {
		return nil, fmt.Errorf("no clients to reconnect for")
	}

	backend, err := p.borrowConn(sample)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire backend: %w", err)
	}

	listenQuery := fmt.Sprintf("LISTEN %s\x00", l.channel.Name)
	if err := backend.WriteMessage('Q', []byte(listenQuery)); err != nil {
		p.returnOrCloseListenConn(backend, l.channel.Database, sample.GetUser(), false)
		return nil, fmt.Errorf("failed to send LISTEN on reconnect: %w", err)
	}

	if err := p.drainUntilReady(backend); err != nil {
		p.returnOrCloseListenConn(backend, l.channel.Database, sample.GetUser(), false)
		return nil, fmt.Errorf("backend rejected LISTEN on reconnect: %w", err)
	}

	return backend, nil
}

// drainUntilReady reads messages from a backend until ReadyForQuery.
func (p *Server) drainUntilReady(backend *BackendConnection) error {
	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			return fmt.Errorf("read error while waiting for ready: %w", err)
		}
		switch msgType {
		case 'Z':
			return nil
		case 'E':
			return fmt.Errorf("backend error: %s", parseErrorMessage(body))
		}
	}
}

// --- Cleanup and stats ---

// cleanupClientListeners removes a client from all listen monitors it belongs to.
func (p *Server) cleanupClientListeners(client *ClientConnection) {
	channels := client.GetListenChannels()
	for ch := range channels {
		p.removeClientFromListen(ch, client)
	}
}

// shutdownListeners tears down all active listen monitors during server shutdown.
func (p *Server) shutdownListeners() {
	p.listenersMu.Lock()
	listens := make([]*Listen, 0, len(p.listeners))
	for _, l := range p.listeners {
		listens = append(listens, l)
	}
	p.listeners = make(map[Channel]*Listen)
	p.listenersMu.Unlock()

	for _, l := range listens {
		l.mu.Lock()
		backend := l.backend
		l.backend = nil
		l.mu.Unlock()

		if backend != nil {
			backend.Close()
		}
	}

	for _, l := range listens {
		<-l.done
	}

	p.logger.Info("All listen monitors shut down", "count", len(listens))
}

// handleListen handles a LISTEN command from a client.
func (p *Server) handleListen(client *ClientConnection, query string) error {
	logger := client.Logger()

	parts := strings.Fields(query)
	if len(parts) < 2 {
		return sendErrorResponse(client, "ERROR", "42601", "syntax error in LISTEN command")
	}
	channelName := strings.Trim(parts[1], "\"';")

	ch := Channel{
		Database: client.GetDatabase(),
		Name:     channelName,
	}

	l, isNew, err := p.getOrCreateListen(ch, client)
	if err != nil {
		logger.WithError(err).Error("Failed to set up listen monitor")
		return sendErrorResponse(client, "ERROR", "08006", "could not establish listen connection")
	}

	client.AddListenChannel(ch)

	if isNew {
		logger.Info("Created new listen monitor", "channel", channelName)
	} else {
		logger.Info("Joined existing listen monitor", "channel", channelName,
			"total_clients", l.clientCount())
	}

	if err := sendCommandComplete(client, "LISTEN"); err != nil {
		return err
	}
	return sendReadyForQuery(client, 'I')
}

// handleUnlisten handles an UNLISTEN command from a client.
func (p *Server) handleUnlisten(client *ClientConnection, query string) error {
	logger := client.Logger()

	parts := strings.Fields(query)

	if len(parts) >= 2 && strings.ToUpper(parts[1]) == "*" {
		channels := client.GetListenChannels()
		for ch := range channels {
			p.removeClientFromListen(ch, client)
		}
		client.ClearListenChannels()
		logger.Info("UNLISTEN * — left all channels", "count", len(channels))
	} else if len(parts) >= 2 {
		channelName := strings.Trim(parts[1], "\"';")
		ch := Channel{
			Database: client.GetDatabase(),
			Name:     channelName,
		}
		p.removeClientFromListen(ch, client)
		client.RemoveListenChannel(ch)
		logger.Info("UNLISTEN", "channel", channelName)
	}

	if err := sendCommandComplete(client, "UNLISTEN"); err != nil {
		return err
	}
	return sendReadyForQuery(client, 'I')
}

// handleNotify handles NOTIFY commands and pg_notify() function calls.
func (p *Server) handleNotify(client *ClientConnection, query string) error {
	logger := client.Logger()

	backend, err := p.borrowConn(client)
	if err != nil {
		logger.WithError(err).Error("Failed to borrow backend for NOTIFY")
		return sendErrorResponse(client, "FATAL", "53300", "too many connections")
	}

	if err := p.forwardQueryToBackend(backend, query); err != nil {
		logger.WithError(err).Error("Failed to send NOTIFY to backend")
		p.closeConn(client, backend)
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	if _, err := p.forwardCompleteBackendResponse(client, backend); err != nil {
		logger.WithError(err).Error("Failed to forward NOTIFY response")
		p.closeConn(client, backend)
		return err
	}

	// NOTIFY is always outside a transaction from the pool's perspective —
	// we borrowed for one query and return immediately regardless of status.
	p.returnConn(client, backend)

	logger.Debug("NOTIFY executed successfully")
	return nil
}

// handleNotificationResponse routes an unexpected notification on a pooled
// connection to the appropriate listen monitor.
func (p *Server) handleNotificationResponse(body []byte) {
	notification := parseNotificationResponse(body)
	if notification == nil {
		p.logger.Error("Failed to parse notification response")
		return
	}

	p.listenersMu.RLock()
	var matched []*Listen
	for key, l := range p.listeners {
		if key.Name == notification.Channel {
			matched = append(matched, l)
		}
	}
	p.listenersMu.RUnlock()

	for _, l := range matched {
		l.fanOut(p, *notification)
	}
}

// registerListener and unregisterListener are shims for the metrics path.
func (p *Server) registerListener(_ Channel, _ *ClientConnection) {}
func (p *Server) unregisterListener(ch Channel, client *ClientConnection) {
	p.removeClientFromListen(ch, client)
}

// getListenerStats returns per-channel client counts for metrics.
func (p *Server) getListenerStats() map[string]int {
	p.listenersMu.RLock()
	defer p.listenersMu.RUnlock()

	stats := make(map[string]int)
	for ch, l := range p.listeners {
		if count := l.clientCount(); count > 0 {
			stats[ch.Database+"/"+ch.Name] = count
		}
	}
	return stats
}
