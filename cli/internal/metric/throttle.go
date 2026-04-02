package metric

import (
	"sync"
	"time"
)

// Throttle wraps a Store and rate-limits metric recording per key per execution.
// Uses a hybrid leading+trailing edge approach:
//   - First point (or first after quiet period): sent immediately
//   - Subsequent points within window: buffer last value, send when window closes
//   - On flush/close: emit any buffered trailing values
type Throttle struct {
	inner    Store
	window   time.Duration
	mu       sync.Mutex
	pending  map[throttleKey]*throttleState
	closed   bool
	closeCh  chan struct{}
	flushMu  sync.Mutex
}

type throttleKey struct {
	executionID string
	key         string
}

type throttleState struct {
	lastSent time.Time
	trailing *Entry // buffered trailing-edge entry, nil if none pending
	timer    *time.Timer
}

// NewThrottle creates a throttled store wrapper.
// rate is max points per key per second (e.g., 10 means 100ms window).
func NewThrottle(inner Store, rate float64) *Throttle {
	window := time.Duration(float64(time.Second) / rate)
	return &Throttle{
		inner:   inner,
		window:  window,
		pending: make(map[throttleKey]*throttleState),
		closeCh: make(chan struct{}),
	}
}

// Record processes entries through the throttle
func (t *Throttle) Record(entries []Entry) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return t.inner.Record(entries)
	}

	var toSend []Entry

	for i := range entries {
		entry := &entries[i]
		tk := throttleKey{executionID: entry.ExecutionID, key: entry.Key}

		st, exists := t.pending[tk]
		if !exists {
			// First point for this key — leading edge, send immediately
			st = &throttleState{lastSent: time.Now()}
			t.pending[tk] = st
			toSend = append(toSend, *entry)
			continue
		}

		elapsed := time.Since(st.lastSent)
		if elapsed >= t.window {
			// Quiet period exceeded window — treat as leading edge
			st.lastSent = time.Now()
			if st.timer != nil {
				st.timer.Stop()
				st.timer = nil
			}
			st.trailing = nil
			toSend = append(toSend, *entry)
		} else {
			// Within window — buffer as trailing edge
			entryCopy := *entry
			st.trailing = &entryCopy

			if st.timer == nil {
				remaining := t.window - elapsed
				st.timer = time.AfterFunc(remaining, func() {
					t.flushTrailing(tk)
				})
			}
		}
	}

	if len(toSend) > 0 {
		return t.inner.Record(toSend)
	}
	return nil
}

func (t *Throttle) flushTrailing(tk throttleKey) {
	t.mu.Lock()
	st, exists := t.pending[tk]
	if !exists || st.trailing == nil {
		t.mu.Unlock()
		return
	}

	entry := *st.trailing
	st.trailing = nil
	st.timer = nil
	st.lastSent = time.Now()
	t.mu.Unlock()

	// Send outside lock
	_ = t.inner.Record([]Entry{entry})
}

// Flush sends any buffered trailing entries and flushes the inner store
func (t *Throttle) Flush() error {
	t.flushMu.Lock()
	defer t.flushMu.Unlock()

	t.mu.Lock()
	var trailing []Entry
	for _, st := range t.pending {
		if st.trailing != nil {
			trailing = append(trailing, *st.trailing)
			st.trailing = nil
			if st.timer != nil {
				st.timer.Stop()
				st.timer = nil
			}
			st.lastSent = time.Now()
		}
	}
	t.mu.Unlock()

	if len(trailing) > 0 {
		if err := t.inner.Record(trailing); err != nil {
			return err
		}
	}

	return t.inner.Flush()
}

// Close flushes all pending entries and closes the inner store
func (t *Throttle) Close() error {
	t.mu.Lock()
	t.closed = true
	// Stop all timers
	for _, st := range t.pending {
		if st.timer != nil {
			st.timer.Stop()
		}
	}
	t.mu.Unlock()

	// Flush trailing entries
	if err := t.Flush(); err != nil {
		return err
	}

	return t.inner.Close()
}

// RemoveExecution cleans up throttle state for a finished execution
func (t *Throttle) RemoveExecution(executionID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for tk, st := range t.pending {
		if tk.executionID == executionID {
			if st.timer != nil {
				st.timer.Stop()
			}
			// Flush trailing entry synchronously
			if st.trailing != nil {
				go func(entry Entry) {
					_ = t.inner.Record([]Entry{entry})
				}(*st.trailing)
			}
			delete(t.pending, tk)
		}
	}
}
