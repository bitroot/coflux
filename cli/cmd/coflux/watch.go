package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	topicalclient "github.com/bitroot/coflux/cli/internal/topical"
	topical "github.com/joefreeman/topical/client_go"
	"golang.org/x/term"
)

// watchTopics connects to the topical server, subscribes to the given topics,
// and calls render whenever any topic updates. The render function receives
// the current data for each topic (by index) and returns lines to display.
// Return nil from render to skip drawing (e.g. when waiting for all topics).
// The helper handles terminal cursor management, truncation, and signal handling.
func watchTopics(ctx context.Context, host string, secure bool, token string, topics []string, render func(data []map[string]any) []string) error {
	client, err := topicalclient.Connect(ctx, host, secure, token)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer client.Close()

	subs := make([]*topical.Subscription, len(topics))
	for i, topic := range topics {
		subs[i] = client.Subscribe(topic, nil)
		defer subs[i].Unsubscribe()
	}

	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	data := make([]map[string]any, len(topics))
	linesDrawn := 0

	maxLines := 0
	if _, h, err := term.GetSize(int(os.Stdout.Fd())); err == nil && h > 0 {
		maxLines = h - 1
	}

	doRender := func() {
		lines := render(data)
		if lines == nil {
			return
		}

		if linesDrawn > 0 {
			fmt.Printf("\033[%dA", linesDrawn)
		}

		totalRows := len(lines) - 1 // exclude header
		truncated := 0
		if maxLines > 0 && len(lines) > maxLines {
			truncated = len(lines) - (maxLines - 1)
			lines = lines[:maxLines-1]
		}
		for _, line := range lines {
			fmt.Printf("\r%s\033[K\n", line)
		}
		if truncated > 0 {
			fmt.Printf("\r%s… %d more (%d total)%s\033[K\n", colorDim, truncated, totalRows, colorReset)
		}
		outputLines := len(lines)
		if truncated > 0 {
			outputLines++
		}
		// Clear from cursor to end of screen
		fmt.Print("\033[J")
		linesDrawn = outputLines
	}

	// Merge all subscription channels into a single event stream
	type topicEvent struct {
		index int
		data  map[string]any
		err   error
	}

	events := make(chan topicEvent)
	for i, sub := range subs {
		go func(idx int, s *topical.Subscription) {
			for {
				select {
				case value, ok := <-s.Values():
					if !ok {
						events <- topicEvent{index: idx, err: fmt.Errorf("subscription closed unexpectedly")}
						return
					}
					if d, ok := value.(map[string]any); ok {
						events <- topicEvent{index: idx, data: d}
					}
				case err, ok := <-s.Err():
					if !ok {
						events <- topicEvent{index: idx, err: fmt.Errorf("subscription closed unexpectedly")}
					} else {
						events <- topicEvent{index: idx, err: fmt.Errorf("subscription error: %w", err)}
					}
					return
				}
			}
		}(i, sub)
	}

	for {
		select {
		case ev := <-events:
			if ev.err != nil {
				return ev.err
			}
			data[ev.index] = ev.data
			doRender()
		case <-sigCtx.Done():
			return nil
		}
	}
}
