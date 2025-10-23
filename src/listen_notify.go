package main

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// NotificationMonitor manages a single backend connection that handles all LISTEN commands
// for a specific database. Multiple clients can subscribe to channels through this monitor.
type NotificationMonitor struct {
	pooler           *WildcardPooler
	dbName           string
	username         string
	backend          *BackendConnection
	listenedChannels map[string]int                        // channel -> subscriber count
	subscribers      map[string]map[*ClientConnection]bool // channel -> clients
	mu               sync.RWMutex
	ctx              context.Context
	cancel           context.CancelFunc
	logger           *Logger
	active           atomic.Bool
	reconnectBackoff time.Duration
}

// NewNotificationMonitor creates a new notification monitor for a database
func NewNotificationMonitor(pooler *WildcardPooler, dbName, username string) *NotificationMonitor {
	ctx, cancel := context.WithCancel(pooler.ctx)

	return &NotificationMonitor{
		pooler:           pooler,
		dbName:           dbName,
		username:         username,
		listenedChannels: make(map[string]int),
		subscribers:      make(map[string]map[*ClientConnection]bool),
		ctx:              ctx,
		cancel:           cancel,
		logger:           pooler.logger.WithField("component", "notification_monitor").WithDatabase(dbName).WithUser(username),
		reconnectBackoff: 1 * time.Second,
	}
}

// Start initializes the backend connection and starts listening
func (nm *NotificationMonitor) Start() error {
	nm.logger.Info("Starting notification monitor")

	// Create dedicated backend connection
	if err := nm.createBackendConnection(); err != nil {
		return fmt.Errorf("failed to create backend connection: %w", err)
	}

	nm.active.Store(true)

	// Start the notification listener goroutine
	go nm.listenLoop()

	return nil
}

// Stop shuts down the notification monitor
func (nm *NotificationMonitor) Stop() {
	nm.logger.Info("Stopping notification monitor")
	nm.active.Store(false)
	nm.cancel()

	if nm.backend != nil {
		nm.backend.Close()
	}
}

// AddSubscriber adds a client subscription to a channel
func (nm *NotificationMonitor) AddSubscriber(client *ClientConnection, channel string) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	nm.logger.Info("Adding subscriber", "channel", channel, "client", client.RemoteAddr())

	// Initialize subscribers map for this channel if needed
	if nm.subscribers[channel] == nil {
		nm.subscribers[channel] = make(map[*ClientConnection]bool)
	}

	// Add client to subscribers
	nm.subscribers[channel][client] = true

	// If this is the first subscriber, send LISTEN to backend
	if nm.listenedChannels[channel] == 0 {
		if err := nm.sendListen(channel); err != nil {
			delete(nm.subscribers[channel], client)
			if len(nm.subscribers[channel]) == 0 {
				delete(nm.subscribers, channel)
			}
			return fmt.Errorf("failed to send LISTEN: %w", err)
		}
	}

	nm.listenedChannels[channel]++

	nm.logger.Info("Subscriber added",
		"channel", channel,
		"total_subscribers", nm.listenedChannels[channel])

	return nil
}

// RemoveSubscriber removes a client subscription from a channel
func (nm *NotificationMonitor) RemoveSubscriber(client *ClientConnection, channel string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	nm.logger.Info("Removing subscriber", "channel", channel, "client", client.RemoteAddr())

	if nm.subscribers[channel] == nil {
		return
	}

	delete(nm.subscribers[channel], client)
	nm.listenedChannels[channel]--

	// If no more subscribers, send UNLISTEN to backend
	if nm.listenedChannels[channel] == 0 {
		nm.sendUnlisten(channel)
		delete(nm.subscribers, channel)
		delete(nm.listenedChannels, channel)

		nm.logger.Info("Last subscriber removed, sent UNLISTEN", "channel", channel)
	} else {
		nm.logger.Info("Subscriber removed",
			"channel", channel,
			"remaining_subscribers", nm.listenedChannels[channel])
	}
}

// RemoveClient removes a client from all channels
func (nm *NotificationMonitor) RemoveClient(client *ClientConnection) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	nm.logger.Info("Removing client from all channels", "client", client.RemoteAddr())

	// Find all channels this client is subscribed to
	channelsToRemove := make([]string, 0)
	for channel, clients := range nm.subscribers {
		if clients[client] {
			channelsToRemove = append(channelsToRemove, channel)
		}
	}

	// Remove from each channel
	for _, channel := range channelsToRemove {
		delete(nm.subscribers[channel], client)
		nm.listenedChannels[channel]--

		// If no more subscribers, send UNLISTEN
		if nm.listenedChannels[channel] == 0 {
			nm.sendUnlisten(channel)
			delete(nm.subscribers, channel)
			delete(nm.listenedChannels, channel)
		}
	}

	nm.logger.Info("Client removed from all channels",
		"client", client.RemoteAddr(),
		"channels_removed", len(channelsToRemove))
}

// GetSubscriberCount returns the number of subscribers for a channel
func (nm *NotificationMonitor) GetSubscriberCount(channel string) int {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return nm.listenedChannels[channel]
}

// GetTotalSubscribers returns the total number of unique subscribers
func (nm *NotificationMonitor) GetTotalSubscribers() int {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	uniqueClients := make(map[*ClientConnection]bool)
	for _, clients := range nm.subscribers {
		for client := range clients {
			uniqueClients[client] = true
		}
	}
	return len(uniqueClients)
}

// GetChannels returns all channels being listened to
func (nm *NotificationMonitor) GetChannels() []string {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	channels := make([]string, 0, len(nm.listenedChannels))
	for channel := range nm.listenedChannels {
		channels = append(channels, channel)
	}
	return channels
}

// createBackendConnection creates the backend connection for this monitor
func (nm *NotificationMonitor) createBackendConnection() error {
	dbManager, err := nm.pooler.getDatabaseManager(nm.dbName, nm.username)
	if err != nil {
		return fmt.Errorf("failed to get database manager: %w", err)
	}

	// Create a dedicated connection (not from pool)
	backend, err := dbManager.createBackendConnection()
	if err != nil {
		return fmt.Errorf("failed to create backend connection: %w", err)
	}

	nm.backend = backend
	nm.backend.SetListening(true)

	nm.logger.Info("Backend connection created", "backend_addr", backend.RemoteAddr())

	return nil
}

// sendListen sends a LISTEN command to the backend
func (nm *NotificationMonitor) sendListen(channel string) error {
	if nm.backend == nil {
		return fmt.Errorf("backend connection not available")
	}

	query := fmt.Sprintf("LISTEN %s", channel)
	queryMsg := []byte(query + "\x00")

	nm.logger.Debug("Sending LISTEN to backend", "channel", channel)

	if err := nm.backend.WriteMessage('Q', queryMsg); err != nil {
		return fmt.Errorf("failed to send LISTEN: %w", err)
	}

	// Read response (should get CommandComplete + ReadyForQuery)
	for {
		msgType, _, err := nm.backend.ReadMessage()
		if err != nil {
			return fmt.Errorf("failed to read LISTEN response: %w", err)
		}

		if msgType == 'E' { // ErrorResponse
			return fmt.Errorf("backend returned error for LISTEN")
		}

		if msgType == 'Z' { // ReadyForQuery
			break
		}
	}

	nm.logger.Debug("LISTEN successful", "channel", channel)
	return nil
}

// sendUnlisten sends an UNLISTEN command to the backend
func (nm *NotificationMonitor) sendUnlisten(channel string) error {
	if nm.backend == nil {
		return fmt.Errorf("backend connection not available")
	}

	query := fmt.Sprintf("UNLISTEN %s", channel)
	queryMsg := []byte(query + "\x00")

	nm.logger.Debug("Sending UNLISTEN to backend", "channel", channel)

	if err := nm.backend.WriteMessage('Q', queryMsg); err != nil {
		nm.logger.WithError(err).Warn("Failed to send UNLISTEN (non-fatal)")
		return nil // Non-fatal, backend might be dead
	}

	// Read response
	for {
		msgType, _, err := nm.backend.ReadMessage()
		if err != nil {
			nm.logger.WithError(err).Warn("Failed to read UNLISTEN response (non-fatal)")
			return nil
		}

		if msgType == 'Z' { // ReadyForQuery
			break
		}
	}

	nm.logger.Debug("UNLISTEN successful", "channel", channel)
	return nil
}

// listenLoop continuously reads messages from the backend
func (nm *NotificationMonitor) listenLoop() {
	nm.logger.Info("Starting listen loop")

	for nm.active.Load() {
		select {
		case <-nm.ctx.Done():
			nm.logger.Info("Listen loop stopping due to context cancellation")
			return
		default:
			if err := nm.readAndProcessMessage(); err != nil {
				nm.logger.WithError(err).Error("Error in listen loop")

				// Attempt to reconnect
				if nm.active.Load() {
					nm.handleConnectionFailure()
				}
				return
			}
		}
	}

	nm.logger.Info("Listen loop stopped")
}

// readAndProcessMessage reads and processes a single message from backend
func (nm *NotificationMonitor) readAndProcessMessage() error {
	if nm.backend == nil {
		return fmt.Errorf("backend connection not available")
	}

	// Set read deadline for periodic context checking
	if conn, ok := nm.backend.conn.(net.Conn); ok {
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	}

	msgType, body, err := nm.backend.ReadMessage()

	// Clear deadline
	if conn, ok := nm.backend.conn.(net.Conn); ok {
		conn.SetReadDeadline(time.Time{})
	}

	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return nil // Just timeout, continue
		}
		return fmt.Errorf("failed to read message: %w", err)
	}

	switch msgType {
	case 'A': // NotificationResponse
		nm.handleNotification(body)

	case 'Z': // ReadyForQuery
		// Backend is idle, continue listening

	case 'E': // ErrorResponse
		errorMsg := parseErrorMessage(body)
		nm.logger.WithError(fmt.Errorf(errorMsg)).Warn("Backend error message")

	case 'S': // ParameterStatus
		// Ignore parameter status updates

	default:
		nm.logger.Warn("Unexpected message type in listen loop", "type", string(msgType))
	}

	return nil
}

// handleNotification processes a notification and forwards to subscribers
func (nm *NotificationMonitor) handleNotification(body []byte) {
	notification := parseNotificationResponse(body)
	if notification == nil {
		nm.logger.Error("Failed to parse notification", "body_len", len(body))
		return
	}

	nm.logger.Info("Received notification",
		"channel", notification.Channel,
		"payload_len", len(notification.Payload),
		"process_id", notification.ProcessID)

	nm.mu.RLock()
	subscribers, exists := nm.subscribers[notification.Channel]
	if !exists {
		nm.mu.RUnlock()
		nm.logger.Warn("Received notification for channel with no subscribers",
			"channel", notification.Channel)
		return
	}

	// Copy subscribers to avoid holding lock during I/O
	clientsCopy := make([]*ClientConnection, 0, len(subscribers))
	for client := range subscribers {
		clientsCopy = append(clientsCopy, client)
	}
	nm.mu.RUnlock()

	// Forward to all subscribers
	successCount := 0
	failedClients := make([]*ClientConnection, 0)

	for _, client := range clientsCopy {
		if err := sendNotificationToClient(client, *notification); err != nil {
			nm.logger.WithError(err).Warn("Failed to send notification to client",
				"client", client.RemoteAddr())
			failedClients = append(failedClients, client)
		} else {
			successCount++
		}
	}

	// Remove failed clients
	if len(failedClients) > 0 {
		for _, client := range failedClients {
			nm.RemoveClient(client)
		}
	}

	// Update statistics
	atomic.AddInt64(&nm.pooler.stats.NotificationsSent, int64(successCount))

	nm.logger.Info("Notification forwarded",
		"channel", notification.Channel,
		"successful", successCount,
		"failed", len(failedClients),
		"total_subscribers", len(clientsCopy))
}

// handleConnectionFailure attempts to reconnect after connection failure
func (nm *NotificationMonitor) handleConnectionFailure() {
	nm.logger.Warn("Backend connection failed, attempting to reconnect")

	// Close old connection
	if nm.backend != nil {
		nm.backend.Close()
		nm.backend = nil
	}

	// Get channels we need to re-LISTEN to
	nm.mu.RLock()
	channelsToResubscribe := make([]string, 0, len(nm.listenedChannels))
	for channel := range nm.listenedChannels {
		channelsToResubscribe = append(channelsToResubscribe, channel)
	}
	nm.mu.RUnlock()

	// Attempt reconnection with exponential backoff
	for attempt := 1; nm.active.Load(); attempt++ {
		select {
		case <-nm.ctx.Done():
			return
		case <-time.After(nm.reconnectBackoff):
			nm.logger.Info("Attempting to reconnect", "attempt", attempt)

			if err := nm.createBackendConnection(); err != nil {
				nm.logger.WithError(err).Warn("Reconnection failed")

				// Exponential backoff (max 30 seconds)
				nm.reconnectBackoff *= 2
				if nm.reconnectBackoff > 30*time.Second {
					nm.reconnectBackoff = 30 * time.Second
				}
				continue
			}

			// Reconnection successful, re-LISTEN to all channels
			nm.logger.Info("Reconnected, re-subscribing to channels",
				"channel_count", len(channelsToResubscribe))

			nm.mu.Lock()
			for _, channel := range channelsToResubscribe {
				if err := nm.sendListen(channel); err != nil {
					nm.logger.WithError(err).Error("Failed to re-LISTEN to channel",
						"channel", channel)
				}
			}
			nm.mu.Unlock()

			// Reset backoff
			nm.reconnectBackoff = 1 * time.Second

			// Restart listen loop
			go nm.listenLoop()
			return
		}
	}
}

// handleListen handles LISTEN commands using the notification monitor
func (p *WildcardPooler) handleListen(client *ClientConnection, query string) error {
	logger := client.Logger().WithField("query", query)
	logger.Info("Handling LISTEN command")

	// Parse channel name
	parts := strings.Fields(query)
	if len(parts) < 2 {
		return sendErrorResponse(client, "ERROR", "42601", "syntax error in LISTEN command")
	}
	channel := strings.Trim(parts[1], "\"';")

	// Get or create notification monitor for this database/user
	monitor, err := p.getOrCreateNotificationMonitor(client.GetDatabase(), client.GetUser())
	if err != nil {
		logger.WithError(err).Error("Failed to get notification monitor")
		return sendErrorResponse(client, "FATAL", "53300", "too many connections")
	}

	// Add client as subscriber
	if err := monitor.AddSubscriber(client, channel); err != nil {
		logger.WithError(err).Error("Failed to add subscriber")
		return sendErrorResponse(client, "ERROR", "08006", "failed to subscribe to channel")
	}

	// Mark client as listening
	client.AddListenChannel(channel)

	// Send success response to client
	if err := sendCommandComplete(client, "LISTEN"); err != nil {
		return err
	}
	if err := sendReadyForQuery(client, 'I'); err != nil {
		return err
	}

	logger.Info("LISTEN successful", "channel", channel)
	return nil
}

// handleUnlisten handles UNLISTEN commands
func (p *WildcardPooler) handleUnlisten(client *ClientConnection, query string) error {
	logger := client.Logger().WithField("query", query)
	logger.Debug("Handling UNLISTEN command")

	// Parse channel name
	parts := strings.Fields(strings.ToUpper(query))

	monitor, err := p.getOrCreateNotificationMonitor(client.GetDatabase(), client.GetUser())
	if err != nil {
		// No monitor means no subscriptions, just return OK
		return sendCommandComplete(client, "UNLISTEN")
	}

	if len(parts) >= 2 && parts[1] == "*" {
		// UNLISTEN * - remove from all channels
		monitor.RemoveClient(client)
		client.ClearListenChannels()
	} else if len(parts) >= 2 {
		// UNLISTEN specific channel
		channel := strings.Trim(parts[1], "\"';")
		monitor.RemoveSubscriber(client, channel)
		client.RemoveListenChannel(channel)
	}

	if err := sendCommandComplete(client, "UNLISTEN"); err != nil {
		return err
	}
	return sendReadyForQuery(client, 'I')
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
