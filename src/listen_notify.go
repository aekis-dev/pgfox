package main

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

// handleListen handles LISTEN commands
func (p *WildcardPooler) handleListen(client *ClientConnection, query string) error {
	logger := client.Logger().WithField("query", query)
	logger.Info("Handling LISTEN command")

	// Parse channel name from LISTEN command - preserve original case
	parts := strings.Fields(query) // Don't uppercase the original query
	if len(parts) < 2 {
		return sendErrorResponse(client, "ERROR", "42601", "syntax error in LISTEN command")
	}

	// Extract channel name and clean quotes, but preserve case
	channel := strings.Trim(parts[1], "\"';")

	// Check if this is actually a LISTEN command by checking the first part
	if !strings.EqualFold(parts[0], "LISTEN") {
		return sendErrorResponse(client, "ERROR", "42601", "not a LISTEN command")
	}

	logger.Info("Parsed LISTEN channel", "channel", channel)

	// For LISTEN, we need a dedicated connection that won't be shared
	// Use the existing backend connection or create a new one
	backend := client.GetBackendConnection()
	if backend == nil {
		logger.Info("No existing backend connection, creating new one for LISTEN")
		var err error
		backend, err = p.acquireListeningBackendConnection(client.GetDatabase())
		if err != nil {
			return sendErrorResponse(client, "FATAL", "53300", "too many connections")
		}
		client.SetBackendConnection(backend)
		backend.SetClientRef(client)
	} else {
		logger.Info("Using existing backend connection for LISTEN", "backend_addr", backend.RemoteAddr())
	}

	// Execute LISTEN on backend
	logger.Info("Forwarding LISTEN command to backend")
	if err := p.forwardQueryToBackend(backend, query); err != nil {
		logger.WithError(err).Error("Failed to forward LISTEN to backend")
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	// Forward response
	logger.Info("Forwarding LISTEN response from backend")
	if err := p.forwardCompleteBackendResponse(client, backend); err != nil {
		logger.WithError(err).Error("Failed to forward LISTEN response")
		return err
	}

	// Register client as listener for this channel (preserve original case)
	logger.Info("Registering client as listener", "channel", channel)
	client.AddListenChannel(channel)
	backend.AddListenChannel(channel)
	p.registerListener(channel, client)

	// DON'T start async notification listener here - it interferes with query processing
	// Instead, we'll use a different approach where the normal query processor
	// handles notifications when they arrive during regular message flow

	logger.Info("LISTEN command completed successfully", "channel", channel)
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
	if err := p.forwardCompleteBackendResponse(client, backend); err != nil {
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
		backend.mu.Lock()
		backend.listenChannels = make(map[string]bool)
		backend.mu.Unlock()
		logger.Info("Client unlistened from all channels")
	}

	return nil
}

// handleNotify handles NOTIFY commands and pg_notify() function calls
func (p *WildcardPooler) handleNotify(client *ClientConnection, query string) error {
	logger := client.Logger().WithField("query", query)
	logger.Debug("Handling NOTIFY/pg_notify command")

	// Use the client's existing backend connection
	backend := client.GetBackendConnection()
	if backend == nil {
		return sendErrorResponse(client, "FATAL", "08003", "connection does not exist")
	}

	// Execute NOTIFY/pg_notify on backend
	if err := p.forwardQueryToBackend(backend, query); err != nil {
		return sendErrorResponse(client, "ERROR", "08006", "connection failure")
	}

	// Forward response - pg_notify() will return query results, NOTIFY returns CommandComplete
	if err := p.forwardCompleteBackendResponse(client, backend); err != nil {
		return err
	}

	// Extract channel information for logging
	queryUpper := strings.ToUpper(strings.TrimSpace(query))
	var channelInfo string
	if strings.Contains(queryUpper, "PG_NOTIFY(") {
		// Try to extract channel from pg_notify('channel', 'payload')
		if start := strings.Index(queryUpper, "PG_NOTIFY("); start != -1 {
			if end := strings.Index(queryUpper[start:], ")"); end != -1 {
				channelInfo = queryUpper[start : start+end+1]
			}
		}
	}

	logger.Info("Executed notification command", "channel_info", channelInfo, "listening_clients", len(p.getListeningChannels()))

	// IMPORTANT: Since PostgreSQL notifications only work within the same session,
	// we need to manually trigger notification checking on all listening backends
	p.triggerNotificationCheck()

	return nil
}

// triggerNotificationCheck sends a lightweight query to all listening backends
// to trigger them to check for notifications
func (p *WildcardPooler) triggerNotificationCheck() {
	logger := p.logger.WithField("component", "notify_trigger")

	p.listenersMu.RLock()
	totalListeners := 0
	for _, clients := range p.listeners {
		totalListeners += len(clients)
	}

	if totalListeners == 0 {
		p.listenersMu.RUnlock()
		logger.Debug("No listeners to trigger")
		return
	}

	// Get unique backend connections that have listeners
	backendConnections := make(map[*BackendConnection]bool)
	for _, clients := range p.listeners {
		for client := range clients {
			if backend := client.GetBackendConnection(); backend != nil {
				backendConnections[backend] = true
			}
		}
	}
	p.listenersMu.RUnlock()

	logger.Info("Triggering notification check on listening backends",
		"backend_count", len(backendConnections),
		"total_listeners", totalListeners)

	// Send a simple query to each listening backend to trigger notification processing
	for backend := range backendConnections {
		go p.triggerBackendNotificationCheck(backend)
	}
}

// triggerBackendNotificationCheck sends a lightweight query to trigger notification processing
func (p *WildcardPooler) triggerBackendNotificationCheck(backend *BackendConnection) {
	logger := p.logger.WithField("backend", backend.RemoteAddr())
	logger.Debug("Triggering notification check on backend")

	// Use a simple, fast query that won't interfere with the backend
	// This will cause PostgreSQL to process any pending notifications
	triggerQuery := "SELECT 1"

	// Set a short timeout for this trigger
	if conn, ok := backend.conn.(net.Conn); ok {
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		defer conn.SetReadDeadline(time.Time{})
	}

	// Send the trigger query
	if err := p.forwardQueryToBackend(backend, triggerQuery); err != nil {
		logger.WithError(err).Debug("Failed to send notification trigger query")
		return
	}

	// Read the response and handle any notifications that come with it
	for {
		msgType, body, err := backend.ReadMessage()
		if err != nil {
			logger.WithError(err).Debug("Failed to read notification trigger response")
			return
		}

		// Handle notifications that arrive during this trigger
		if msgType == NotificationResponse { // 'A'
			logger.Info("Received notification during trigger check")
			p.handleNotificationResponse(body)
			continue
		}

		// Process the trigger query response normally but don't forward to client
		if msgType == 'Z' { // ReadyForQuery - trigger complete
			logger.Debug("Notification trigger completed")
			return
		}
		if msgType == 'E' { // ErrorResponse
			logger.Debug("Notification trigger error")
			return
		}

		// Skip other messages (RowDescription, DataRow, CommandComplete)
	}
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

// listenForNotificationsAsync listens for notifications on a backend connection asynchronously
func (p *WildcardPooler) listenForNotificationsAsync(backend *BackendConnection) {
	defer p.wg.Done()

	logger := p.logger.WithTarget(backend.GetTarget()).WithDatabase(backend.GetDatabase())
	logger.Debug("Starting async notification listener")

	// Create a separate context for this listener
	ctx, cancel := context.WithCancel(p.ctx)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			logger.Debug("Async notification listener stopping due to context cancellation")
			return
		default:
			// Set a read timeout to periodically check for context cancellation
			if netConn, ok := backend.conn.(net.Conn); ok {
				netConn.SetReadDeadline(time.Now().Add(1 * time.Second))
			}

			// Try to read a message
			msgType, body, err := backend.ReadMessage()

			// Clear deadline
			if netConn, ok := backend.conn.(net.Conn); ok {
				netConn.SetReadDeadline(time.Time{})
			}

			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue // Timeout, check context and continue
				}
				if !isContextCancelled(ctx) {
					logger.WithError(err).Error("Async notification listener error")
				}
				return
			}

			logger.Debug("Notification listener received message", "type", string(msgType), "body_len", len(body))

			// Handle different message types
			switch msgType {
			case NotificationResponse: // 'A'
				logger.Debug("Processing notification response")
				p.handleNotificationResponse(body)
			case ReadyForQuery: // 'Z'
				// Backend is ready for more queries
				logger.Debug("Backend ready for query in notification listener")
				continue
			case ErrorResponse: // 'E'
				errorMsg := parseErrorMessage(body)
				logger.WithError(fmt.Errorf(errorMsg)).Error("Backend error in notification listener")
				continue
			default:
				// For other messages, we need to check if there's a client waiting for this response
				clientRef := backend.GetClientRef()
				if clientRef != nil {
					logger.Debug("Forwarding non-notification message to client", "type", string(msgType))
					if err := clientRef.WriteMessage(msgType, body); err != nil {
						logger.WithError(err).Error("Failed to forward message to client in notification listener")
					}
				} else {
					logger.Debug("Received message with no client ref", "type", string(msgType))
				}
			}
		}
	}
}

// registerListener registers a client as a listener for a channel
func (p *WildcardPooler) registerListener(channel string, client *ClientConnection) {
	p.listenersMu.Lock()
	defer p.listenersMu.Unlock()

	if p.listeners[channel] == nil {
		p.listeners[channel] = make(map[*ClientConnection]bool)
	}
	p.listeners[channel][client] = true

	p.logger.Info("Registered listener",
		"channel", channel,
		"client", client.RemoteAddr(),
		"total_listeners_for_channel", len(p.listeners[channel]),
		"total_channels", len(p.listeners))
}

// unregisterListener unregisters a client from a channel
func (p *WildcardPooler) unregisterListener(channel string, client *ClientConnection) {
	p.listenersMu.Lock()
	defer p.listenersMu.Unlock()

	if clients, exists := p.listeners[channel]; exists {
		delete(clients, client)
		if len(clients) == 0 {
			delete(p.listeners, channel)
			p.logger.Info("Removed empty channel", "channel", channel)
		} else {
			p.logger.Info("Unregistered listener",
				"channel", channel,
				"client", client.RemoteAddr(),
				"remaining_listeners", len(clients))
		}
	}
}

// isContextCancelled checks if context is cancelled
func isContextCancelled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// handleNotificationResponse processes a notification from the backend
func (p *WildcardPooler) handleNotificationResponse(body []byte) {
	logger := p.logger.WithField("component", "notification")
	logger.Debug("Processing notification response", "body_hex", fmt.Sprintf("%x", body), "body_len", len(body))

	notification := parseNotificationResponse(body)
	if notification == nil {
		logger.Error("Failed to parse notification response", "body", string(body))
		return
	}

	logger.Info("Parsed notification successfully",
		"channel", notification.Channel,
		"payload", notification.Payload,
		"process_id", notification.ProcessID)

	// Forward notification to all listening clients
	p.forwardNotificationToClients(*notification)
}

// forwardNotificationToClients forwards a notification to all listening clients
func (p *WildcardPooler) forwardNotificationToClients(notification NotificationMessage) {
	logger := p.logger.WithField("channel", notification.Channel).WithField("payload", notification.Payload)

	p.listenersMu.RLock()
	clients, exists := p.listeners[notification.Channel]
	if !exists {
		p.listenersMu.RUnlock()
		logger.Debug("No listeners for notification channel")
		return
	}

	// Make a copy of the clients map to avoid holding the lock during I/O
	clientsCopy := make(map[*ClientConnection]bool)
	for client, active := range clients {
		if active {
			clientsCopy[client] = true
		}
	}
	totalListeners := len(clientsCopy)
	p.listenersMu.RUnlock()

	logger.Debug("Forwarding notification to listeners", "listener_count", totalListeners)

	// Send notification to each client
	sentCount := 0
	failedCount := 0
	for client := range clientsCopy {
		if err := sendNotificationToClient(client, notification); err != nil {
			client.Logger().WithError(err).Error("Failed to send notification to client")
			// Remove failed client from listeners
			p.unregisterListener(notification.Channel, client)
			failedCount++
		} else {
			sentCount++
		}
	}

	if sentCount > 0 {
		atomic.AddInt64(&p.stats.NotificationsSent, int64(sentCount))
		logger.Info("Successfully forwarded notification",
			"channel", notification.Channel,
			"sent", sentCount,
			"failed", failedCount,
			"total_listeners", totalListeners)
	} else {
		logger.Warn("Failed to forward notification to any listeners",
			"channel", notification.Channel,
			"failed", failedCount,
			"total_listeners", totalListeners)
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
	// The async notification listener goroutine will exit when the context is cancelled
}
