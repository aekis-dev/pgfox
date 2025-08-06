package main

import (
	"net"
	"strings"
	"sync/atomic"
	"time"
)

// handleListen handles LISTEN commands
func (p *WildcardPooler) handleListen(client *ClientConnection, query string) error {
	logger := client.Logger().WithField("query", query)
	logger.Debug("Handling LISTEN command")

	// Parse channel name from LISTEN command
	parts := strings.Fields(strings.ToUpper(query))
	if len(parts) < 2 {
		return sendErrorResponse(client, "ERROR", "42601", "syntax error in LISTEN command")
	}

	channel := strings.Trim(parts[1], "\"';")

	// Ensure client has a dedicated backend connection for listening
	if client.GetBackendConnection() == nil {
		backend, err := p.acquireListeningBackendConnection(client.GetDatabase())
		if err != nil {
			return sendErrorResponse(client, "FATAL", "53300", "too many connections")
		}
		client.SetBackendConnection(backend)
		backend.SetClientRef(client)
		backend.SetListening(true)
	}

	backend := client.GetBackendConnection()

	// Execute LISTEN on backend
	if err := p.forwardQueryToBackend(backend, query); err != nil {
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	// Forward response
	if err := p.forwardBackendResponse(client, backend); err != nil {
		return err
	}

	// Register client as listener for this channel
	client.AddListenChannel(channel)
	backend.AddListenChannel(channel)
	p.registerListener(channel, client)

	// Start listening for notifications on this backend connection if not already started
	if !backend.IsListening() {
		backend.SetListening(true)
		p.wg.Add(1)
		go p.listenForNotifications(backend)
	}

	logger.Info("Client listening on channel", "channel", channel)
	return nil
}

// handleUnlisten handles UNLISTEN commands
func (p *WildcardPooler) handleUnlisten(client *ClientConnection, query string) error {
	logger := client.Logger().WithField("query", query)
	logger.Debug("Handling UNLISTEN command")

	backend := client.GetBackendConnection()
	if backend == nil {
		return sendCommandComplete(client, "UNLISTEN")
	}

	// Execute UNLISTEN on backend
	if err := p.forwardQueryToBackend(backend, query); err != nil {
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	// Forward response
	if err := p.forwardBackendResponse(client, backend); err != nil {
		return err
	}

	// Parse channel name and unregister
	parts := strings.Fields(strings.ToUpper(query))
	if len(parts) >= 2 && parts[1] != "*" {
		channel := strings.Trim(parts[1], "\"';")
		p.unregisterListener(channel, client)
		client.RemoveListenChannel(channel)
		backend.RemoveListenChannel(channel)
		logger.Info("Client unlistened from channel", "channel", channel)
	} else {
		// UNLISTEN * - unregister from all channels
		channels := client.GetListenChannels()
		for channel := range channels {
			p.unregisterListener(channel, client)
		}
		client.ClearListenChannels()
		// Clear backend channels too
		backend.listenChannels = make(map[string]bool)
		logger.Info("Client unlistened from all channels")
	}

	return nil
}

// handleNotify handles NOTIFY commands
func (p *WildcardPooler) handleNotify(client *ClientConnection, query string) error {
	logger := client.Logger().WithField("query", query)
	logger.Debug("Handling NOTIFY command")

	// Get any available backend connection to execute NOTIFY
	backend := client.GetBackendConnection()
	if backend == nil {
		var err error
		backend, err = p.acquireBackendConnection(client.GetDatabase())
		if err != nil {
			return sendErrorResponse(client, "FATAL", "53300", "too many connections")
		}
		defer p.releaseBackendConnection(backend)
	}

	// Execute NOTIFY on backend
	if err := p.forwardQueryToBackend(backend, query); err != nil {
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	// Forward response
	return p.forwardBackendResponse(client, backend)
}

// acquireListeningBackendConnection gets a backend connection for listening
func (p *WildcardPooler) acquireListeningBackendConnection(dbName string) (*BackendConnection, error) {
	// For listening connections, we create dedicated connections
	dbManager, err := p.getDatabaseManager(dbName, "")
	if err != nil {
		return nil, err
	}

	// Create a new dedicated connection for listening
	conn, err := dbManager.createBackendConnection()
	if err != nil {
		return nil, err
	}

	atomic.AddInt64(&dbManager.stats.TotalConnections, 1)
	atomic.AddInt64(&dbManager.stats.ActiveConnections, 1)

	return conn, nil
}

// listenForNotifications listens for notifications on a backend connection
func (p *WildcardPooler) listenForNotifications(backend *BackendConnection) {
	defer p.wg.Done()

	logger := p.logger.WithTarget(backend.GetTarget()).WithDatabase(backend.GetDatabase())
	logger.Debug("Starting notification listener")

	for {
		select {
		case <-p.ctx.Done():
			logger.Debug("Notification listener stopping due to context cancellation")
			return
		default:
			// Set a read timeout to check for context cancellation
			if conn, ok := backend.conn.(net.Conn); ok {
				conn.SetReadDeadline(time.Now().Add(time.Second))
			}

			msgType, body, err := backend.ReadMessage()
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue // Timeout, check context and continue
				}
				logger.WithError(err).Error("Notification listener error")
				return
			}

			// Clear the deadline for future reads
			if conn, ok := backend.conn.(net.Conn); ok {
				conn.SetReadDeadline(time.Time{})
			}

			switch msgType {
			case NotificationResponse:
				p.handleNotificationResponse(body)
			case ReadyForQuery:
				// Backend is ready for more queries
				continue
			default:
				// Forward other messages to the client if available
				if backend.GetClientRef() != nil {
					if err := backend.GetClientRef().WriteMessage(msgType, body); err != nil {
						logger.WithError(err).Error("Failed to forward message to client")
					}
				}
			}
		}
	}
}

// handleNotificationResponse processes a notification from the backend
func (p *WildcardPooler) handleNotificationResponse(body []byte) {
	notification := parseNotificationResponse(body)
	if notification == nil {
		p.logger.Error("Failed to parse notification response")
		return
	}

	logger := p.logger.WithField("channel", notification.Channel).WithField("payload", notification.Payload)
	logger.Debug("Received notification")

	// Forward notification to all listening clients
	p.forwardNotificationToClients(*notification)
}

// forwardNotificationToClients forwards a notification to all listening clients
func (p *WildcardPooler) forwardNotificationToClients(notification NotificationMessage) {
	p.listenersMu.RLock()
	clients, exists := p.listeners[notification.Channel]
	if !exists {
		p.listenersMu.RUnlock()
		return
	}

	// Make a copy of the clients map to avoid holding the lock during I/O
	clientsCopy := make(map[*ClientConnection]bool)
	for client, active := range clients {
		if active {
			clientsCopy[client] = true
		}
	}
	p.listenersMu.RUnlock()

	// Send notification to each client
	sentCount := 0
	for client := range clientsCopy {
		if err := sendNotificationToClient(client, notification); err != nil {
			client.Logger().WithError(err).Error("Failed to send notification to client")
			// Remove failed client from listeners
			p.unregisterListener(notification.Channel, client)
		} else {
			sentCount++
		}
	}

	if sentCount > 0 {
		atomic.AddInt64(&p.stats.NotificationsSent, int64(sentCount))
		p.logger.Debug("Forwarded notification to clients",
			"channel", notification.Channel,
			"clients", sentCount)
	}
}

// getListeningClients returns all clients listening on a specific channel
func (p *WildcardPooler) getListeningClients(channel string) []*ClientConnection {
	p.listenersMu.RLock()
	defer p.listenersMu.RUnlock()

	clients, exists := p.listeners[channel]
	if !exists {
		return nil
	}

	result := make([]*ClientConnection, 0, len(clients))
	for client, active := range clients {
		if active {
			result = append(result, client)
		}
	}

	return result
}

// getListeningChannels returns all channels that have active listeners
func (p *WildcardPooler) getListeningChannels() []string {
	p.listenersMu.RLock()
	defer p.listenersMu.RUnlock()

	channels := make([]string, 0, len(p.listeners))
	for channel, clients := range p.listeners {
		if len(clients) > 0 {
			channels = append(channels, channel)
		}
	}

	return channels
}

// getListenerStats returns statistics about listeners
func (p *WildcardPooler) getListenerStats() map[string]int {
	p.listenersMu.RLock()
	defer p.listenersMu.RUnlock()

	stats := make(map[string]int)
	for channel, clients := range p.listeners {
		activeClients := 0
		for _, active := range clients {
			if active {
				activeClients++
			}
		}
		if activeClients > 0 {
			stats[channel] = activeClients
		}
	}

	return stats
}

// isClientListening checks if a client is listening on any channels
func (p *WildcardPooler) isClientListening(client *ClientConnection) bool {
	return len(client.GetListenChannels()) > 0
}

// stopListeningOnBackend stops listening on a backend connection
func (p *WildcardPooler) stopListeningOnBackend(backend *BackendConnection) {
	if !backend.IsListening() {
		return
	}

	backend.SetListening(false)

	// The notification listener goroutine will exit when it detects
	// that the backend is no longer listening or when the context is cancelled
}
