package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// APIVersion is the protocol version for API compatibility
	// This should match the server's expected version (derived from Python package version 0.8.x)
	APIVersion = "0.8"

	// Timeouts
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
)

// Callbacks holds success/error callbacks for a request
type Callbacks struct {
	OnSuccess func(result any)
	OnError   func(err any)
}

// Connection manages a WebSocket connection to the server
type Connection struct {
	host      string
	secure    bool
	workspace string
	sessionID string
	logger    *slog.Logger

	mu        sync.Mutex
	conn      *websocket.Conn
	connected bool
	lastID    int
	requests  map[int]Callbacks
	handlers  map[string]func(params []any) error

	// Callbacks
	onSession func(executionIDs []string)

	// Channels
	sendCh      chan any
	recvCh      chan serverMessage
	doneCh      chan struct{}
	reconnectCh chan struct{}
}

type serverMessage struct {
	msgType int
	payload any
	err     error
}

// NewConnection creates a new connection to the server
func NewConnection(host string, secure bool, workspace, sessionID string, logger *slog.Logger) *Connection {
	if logger == nil {
		logger = slog.Default()
	}
	return &Connection{
		host:        host,
		secure:      secure,
		workspace:   workspace,
		sessionID:   sessionID,
		logger:      logger,
		requests:    make(map[int]Callbacks),
		handlers:    make(map[string]func(params []any) error),
		sendCh:      make(chan any, 100),
		recvCh:      make(chan serverMessage, 100),
		doneCh:      make(chan struct{}),
		reconnectCh: make(chan struct{}, 1),
	}
}

// SetOnSession registers a callback for when a session message is received
func (c *Connection) SetOnSession(handler func(executionIDs []string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onSession = handler
}

// RegisterHandler registers a handler for a command from the server
func (c *Connection) RegisterHandler(command string, handler func(params []any) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers[command] = handler
}

// Connect establishes the WebSocket connection
func (c *Connection) Connect(ctx context.Context) error {
	scheme := "ws"
	if c.secure {
		scheme = "wss"
	}

	params := url.Values{}
	params.Set("workspaceId", c.workspace)
	params.Set("version", APIVersion)

	u := url.URL{
		Scheme:   scheme,
		Host:     c.host,
		Path:     "/worker",
		RawQuery: params.Encode(),
	}

	// Build subprotocols for authentication
	encodedToken := base64.RawURLEncoding.EncodeToString([]byte(c.sessionID))
	subprotocols := []string{fmt.Sprintf("session.%s", encodedToken), "v1"}

	dialer := websocket.Dialer{
		Subprotocols: subprotocols,
	}

	c.logger.Info("connecting to server", "url", u.String())

	conn, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.connected = true
	c.mu.Unlock()

	c.logger.Info("connected to server")

	return nil
}

// Run starts the connection read/write loops
func (c *Connection) Run(ctx context.Context) error {
	// Start read/write goroutines
	go c.readLoop(ctx)
	go c.writeLoop(ctx)

	// Main loop: handle received messages and reconnection
	for {
		select {
		case <-ctx.Done():
			// Close connection to unblock readLoop
			c.mu.Lock()
			if c.conn != nil {
				_ = c.conn.Close()
				c.conn = nil
			}
			c.connected = false
			c.mu.Unlock()
			return ctx.Err()

		case <-c.doneCh:
			return nil

		case msg := <-c.recvCh:
			if msg.err != nil {
				c.logger.Error("receive error", "error", msg.err)
				// Mark as disconnected and fail pending requests
				c.failPendingRequests(msg.err)
				return msg.err
			}
			if err := c.handleMessage(msg.msgType, msg.payload); err != nil {
				c.logger.Error("failed to handle message", "error", err)
			}
		}
	}
}

func (c *Connection) readLoop(ctx context.Context) {
	for {
		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()

		if conn == nil {
			return
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			select {
			case c.recvCh <- serverMessage{err: err}:
			case <-ctx.Done():
			}
			return
		}

		msgType, payload, err := ParseServerMessage(message)
		if err != nil {
			c.logger.Error("failed to parse message", "error", err)
			continue
		}

		select {
		case c.recvCh <- serverMessage{msgType: msgType, payload: payload}:
		case <-ctx.Done():
			return
		}
	}
}

func (c *Connection) writeLoop(ctx context.Context) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-c.sendCh:
			c.mu.Lock()
			conn := c.conn
			c.mu.Unlock()

			if conn == nil {
				continue
			}

			data, err := json.Marshal(msg)
			if err != nil {
				c.logger.Error("failed to marshal message", "error", err)
				continue
			}

			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				c.logger.Error("failed to write message", "error", err)
				return
			}

		case <-ticker.C:
			c.mu.Lock()
			conn := c.conn
			c.mu.Unlock()

			if conn == nil {
				continue
			}

			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Connection) handleMessage(msgType int, payload any) error {
	switch msgType {
	case MsgTypeSession:
		session, ok := payload.(SessionMessage)
		if !ok {
			return fmt.Errorf("invalid session message type")
		}
		c.logger.Info("received session", "session_id", session.SessionID, "execution_ids", len(session.ExecutionIDs))
		c.mu.Lock()
		handler := c.onSession
		c.mu.Unlock()
		if handler != nil {
			handler(session.ExecutionIDs)
		}
		return nil

	case MsgTypeCommand:
		cmd, ok := payload.(CommandMessage)
		if !ok {
			return fmt.Errorf("invalid command type")
		}
		c.mu.Lock()
		handler, exists := c.handlers[cmd.Command]
		c.mu.Unlock()
		if !exists {
			return fmt.Errorf("unknown command: %s", cmd.Command)
		}
		return handler(cmd.Params)

	case MsgTypeSuccess:
		resp, ok := payload.(SuccessResponse)
		if !ok {
			return fmt.Errorf("invalid success response type")
		}
		c.mu.Lock()
		cb, exists := c.requests[resp.RequestID]
		if exists {
			delete(c.requests, resp.RequestID)
		}
		c.mu.Unlock()
		if exists && cb.OnSuccess != nil {
			cb.OnSuccess(resp.Result)
		}
		return nil

	case MsgTypeError:
		resp, ok := payload.(ErrorResponse)
		if !ok {
			return fmt.Errorf("invalid error response type")
		}
		c.mu.Lock()
		cb, exists := c.requests[resp.RequestID]
		if exists {
			delete(c.requests, resp.RequestID)
		}
		c.mu.Unlock()
		if exists && cb.OnError != nil {
			cb.OnError(resp.Error)
		}
		return nil

	default:
		return fmt.Errorf("unknown message type: %d", msgType)
	}
}

// Notify sends a notification to the server (no response expected)
func (c *Connection) Notify(request string, params ...any) error {
	req := Request{
		Request: request,
		Params:  params,
	}

	select {
	case c.sendCh <- req:
		return nil
	default:
		return fmt.Errorf("send queue full")
	}
}

// Request sends a request to the server and waits for a response
func (c *Connection) Request(ctx context.Context, request string, params ...any) (any, error) {
	c.mu.Lock()
	c.lastID++
	id := c.lastID
	c.mu.Unlock()

	resultCh := make(chan any, 1)
	errCh := make(chan any, 1)

	callbacks := Callbacks{
		OnSuccess: func(result any) {
			resultCh <- result
		},
		OnError: func(err any) {
			errCh <- err
		},
	}

	c.mu.Lock()
	c.requests[id] = callbacks
	c.mu.Unlock()

	req := Request{
		Request: request,
		Params:  params,
		ID:      &id,
	}

	select {
	case c.sendCh <- req:
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.requests, id)
		c.mu.Unlock()
		return nil, ctx.Err()
	}

	select {
	case result := <-resultCh:
		return result, nil
	case err := <-errCh:
		return nil, fmt.Errorf("server error: %v", err)
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.requests, id)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Close closes the connection and fails all pending requests
func (c *Connection) Close() error {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.connected = false

	// Fail all pending requests
	pendingRequests := c.requests
	c.requests = make(map[int]Callbacks)
	c.mu.Unlock()

	// Close the underlying connection
	var closeErr error
	if conn != nil {
		closeErr = conn.Close()
	}

	// Notify all pending requests of the failure
	for _, cb := range pendingRequests {
		if cb.OnError != nil {
			cb.OnError("connection closed")
		}
	}

	select {
	case c.doneCh <- struct{}{}:
	default:
	}

	return closeErr
}

// IsConnected returns whether the connection is active
func (c *Connection) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

// failPendingRequests marks connection as disconnected and fails all pending requests
func (c *Connection) failPendingRequests(err error) {
	c.mu.Lock()
	c.connected = false
	pendingRequests := c.requests
	c.requests = make(map[int]Callbacks)
	c.mu.Unlock()

	errMsg := "connection lost"
	if err != nil {
		errMsg = err.Error()
	}

	for _, cb := range pendingRequests {
		if cb.OnError != nil {
			cb.OnError(errMsg)
		}
	}
}
