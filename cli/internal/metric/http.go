package metric

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

// HTTPStore implements Store using HTTP POST with batching
type HTTPStore struct {
	baseURL       string
	token         string
	project       string
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

// NewHTTPStore creates a new HTTP metric store
func NewHTTPStore(baseURL string, token string, project string, batchSize int, flushInterval time.Duration, logger *slog.Logger) *HTTPStore {
	if logger == nil {
		logger = slog.Default()
	}
	s := &HTTPStore{
		baseURL:       baseURL,
		token:         token,
		project:       project,
		client:        &http.Client{Timeout: 30 * time.Second},
		batchSize:     batchSize,
		flushInterval: flushInterval,
		logger:        logger,
		buffer:        make([]Entry, 0, batchSize),
		flushCh:       make(chan struct{}, 1),
		doneCh:        make(chan struct{}),
	}

	go s.flushLoop()

	return s
}

// Record writes metric entries to the buffer
func (s *HTTPStore) Record(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("store is closed")
	}

	s.buffer = append(s.buffer, entries...)

	if len(s.buffer) >= s.batchSize || s.flushInterval == 0 {
		s.triggerFlush()
	} else if s.timer == nil {
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

	entries := s.buffer
	s.buffer = make([]Entry, 0, s.batchSize)

	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	s.mu.Unlock()

	if err := s.send(entries); err != nil {
		s.logger.Error("failed to send metrics", "error", err, "count", len(entries))
		s.mu.Lock()
		s.buffer = append(entries, s.buffer...)
		s.mu.Unlock()
	}
}

func (s *HTTPStore) send(entries []Entry) error {
	items := make([]map[string]any, len(entries))
	for i, e := range entries {
		items[i] = map[string]any{
			"runId":       e.RunID,
			"executionId": e.ExecutionID,
			"workspaceId": e.WorkspaceID,
			"key":         e.Key,
			"value":       e.Value,
			"at":          e.At,
		}
	}

	body, err := json.Marshal(map[string]any{
		"entries": items,
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
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	if s.project != "" {
		req.Header.Set("X-Project", s.project)
	}

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

	s.doFlush()

	close(s.doneCh)
	return nil
}
