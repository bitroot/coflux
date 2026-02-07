package log

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// LogEntry represents a log entry returned from the server
type LogEntry struct {
	ExecutionID string         `json:"executionId"`
	WorkspaceID string         `json:"workspaceId"`
	Timestamp   int64          `json:"timestamp"`
	Level       int            `json:"level"`
	Template    string         `json:"template"`
	Values      map[string]any `json:"values"`
}

// QueryResult holds the result of a log query
type QueryResult struct {
	Logs   []LogEntry `json:"logs"`
	Cursor string     `json:"cursor"`
}

// Client reads logs from the server's GET /logs endpoint
type Client struct {
	baseURL string
	client  *http.Client
}

// NewClient creates a new log reader client.
// baseURL should be the full URL to the /logs endpoint (e.g. "http://host:port/logs").
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 30 * time.Second},
	}
}

// Query fetches logs for a run (one-shot JSON mode)
func (c *Client) Query(ctx context.Context, params url.Values) (*QueryResult, error) {
	reqURL := c.baseURL + "?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	var result QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	return &result, nil
}

// Stream connects to the SSE endpoint and calls callback for each batch of log entries.
// It blocks until the context is cancelled or an error occurs.
func (c *Client) Stream(ctx context.Context, params url.Values, callback func([]LogEntry) error) error {
	reqURL := c.baseURL + "?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	// Use a client without timeout for streaming
	streamClient := &http.Client{}
	resp, err := streamClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := line[len("data: "):]

		var entries []LogEntry
		if err := json.Unmarshal([]byte(data), &entries); err != nil {
			continue
		}
		if len(entries) > 0 {
			if err := callback(entries); err != nil {
				return err
			}
		}
	}

	if err := scanner.Err(); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("stream read error: %w", err)
	}

	return nil
}

// LevelName returns a human-readable name for a log level integer.
// 0=debug, 1=stdout, 2=info, 3=stderr, 4=warning, 5=error
func LevelName(level int) string {
	switch level {
	case 0:
		return "DEBUG"
	case 1:
		return "STDOUT"
	case 2:
		return "INFO"
	case 3:
		return "STDERR"
	case 4:
		return "WARN"
	case 5:
		return "ERROR"
	default:
		return fmt.Sprintf("L%d", level)
	}
}
