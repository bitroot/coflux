package log

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Entry represents a single log entry
type Entry struct {
	RunID       string         // Run ID (external)
	ExecutionID string         // Execution ID (external, string representation of int)
	WorkspaceID string         // Workspace ID (external, string representation of int)
	Timestamp   int64          // Unix timestamp in milliseconds
	Level       int            // Log level as integer (0=debug, 1=info, 2=warning, 3=error)
	Template    *string        // Optional message template
	Values      map[string]any // Structured values (nil for plain message logs)
	Message     string         // Plain message (used when Values is nil)
}

// Store is the interface for log storage backends
type Store interface {
	// Log records a log entry
	Log(entry Entry) error
	// Flush sends any buffered entries
	Flush() error
	// Close flushes and closes the store
	Close() error
}

// HTTPStore implements Store using HTTP POST with batching
type HTTPStore struct {
	baseURL       string
	client        *http.Client
	batchSize     int
	flushInterval time.Duration
	logger        *slog.Logger

	mu      sync.Mutex
	buffer  []Entry
	timer   *time.Timer
	closed  bool
	flushCh chan struct{}
	doneCh  chan struct{}
}

// NewHTTPStore creates a new HTTP log store
func NewHTTPStore(baseURL string, batchSize int, flushInterval time.Duration, logger *slog.Logger) *HTTPStore {
	if logger == nil {
		logger = slog.Default()
	}
	s := &HTTPStore{
		baseURL:       baseURL,
		client:        &http.Client{Timeout: 30 * time.Second},
		batchSize:     batchSize,
		flushInterval: flushInterval,
		logger:        logger,
		buffer:        make([]Entry, 0, batchSize),
		flushCh:       make(chan struct{}, 1),
		doneCh:        make(chan struct{}),
	}

	// Start background flush goroutine
	go s.flushLoop()

	return s
}

// Log records a log entry
func (s *HTTPStore) Log(entry Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("store is closed")
	}

	// Add timestamp if not set
	if entry.Timestamp == 0 {
		entry.Timestamp = time.Now().UnixMilli()
	}

	s.buffer = append(s.buffer, entry)

	// Flush if batch is full or flush interval is zero (immediate mode)
	if len(s.buffer) >= s.batchSize || s.flushInterval == 0 {
		s.triggerFlush()
	} else if s.timer == nil {
		// Start timer for flush interval
		s.timer = time.AfterFunc(s.flushInterval, func() {
			s.mu.Lock()
			s.timer = nil
			s.mu.Unlock()
			s.triggerFlush()
		})
	}

	return nil
}

func (s *HTTPStore) triggerFlush() {
	select {
	case s.flushCh <- struct{}{}:
	default:
	}
}

func (s *HTTPStore) flushLoop() {
	for {
		select {
		case <-s.flushCh:
			s.doFlush()
		case <-s.doneCh:
			return
		}
	}
}

func (s *HTTPStore) doFlush() {
	s.mu.Lock()
	if len(s.buffer) == 0 {
		s.mu.Unlock()
		return
	}

	// Take the buffer
	entries := s.buffer
	s.buffer = make([]Entry, 0, s.batchSize)

	// Cancel any pending timer
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	s.mu.Unlock()

	// Send to server
	if err := s.send(entries); err != nil {
		s.logger.Error("failed to send logs", "error", err, "count", len(entries))
		// Put entries back in buffer on failure
		s.mu.Lock()
		s.buffer = append(entries, s.buffer...)
		s.mu.Unlock()
	}
}

func (s *HTTPStore) send(entries []Entry) error {
	// Format entries for the server API
	messages := make([]map[string]any, len(entries))
	for i, e := range entries {
		msg := map[string]any{
			"runId":       e.RunID,
			"executionId": e.ExecutionID,
			"workspaceId": e.WorkspaceID,
			"timestamp":   e.Timestamp,
			"level":       e.Level,
		}
		if e.Template != nil {
			msg["template"] = *e.Template
		}
		if e.Values != nil {
			msg["values"] = e.Values
		} else if e.Message != "" {
			// Convert plain message to structured value format
			msg["template"] = e.Message
			msg["values"] = map[string]any{}
		} else {
			msg["values"] = map[string]any{}
		}
		messages[i] = msg
	}

	body, err := json.Marshal(map[string]any{
		"messages": messages,
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	return nil
}

// Flush sends any buffered entries
func (s *HTTPStore) Flush() error {
	s.doFlush()
	return nil
}

// Close flushes and closes the store
func (s *HTTPStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	if s.timer != nil {
		s.timer.Stop()
	}
	s.mu.Unlock()

	// Final flush
	s.doFlush()

	close(s.doneCh)
	return nil
}

// NoopStore is a store that discards all logs
type NoopStore struct{}

func (NoopStore) Log(entry Entry) error { return nil }
func (NoopStore) Flush() error          { return nil }
func (NoopStore) Close() error          { return nil }

// LevelToInt converts a log level string to integer
// 0=debug, 1=info, 2=warning, 3=error
func LevelToInt(level string) int {
	switch level {
	case "debug":
		return 0
	case "info":
		return 1
	case "warning", "warn":
		return 2
	case "error":
		return 3
	default:
		return 1 // default to info
	}
}
