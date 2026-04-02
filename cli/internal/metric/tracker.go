package metric

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type atMode int

const (
	atModeUnknown atMode = iota
	atModeAuto           // auto-computed from time since execution start
	atModeCustom         // user-provided
)

type keyState struct {
	mode   atMode
	lastAt float64
	warned bool
}

// Tracker enforces at-value consistency and auto-computes time-based at values.
// It tracks per-execution, per-key state.
type Tracker struct {
	mu     sync.Mutex
	execs  map[string]*executionTracker // executionID -> tracker
	logger *slog.Logger
}

type executionTracker struct {
	startTime time.Time
	keys      map[string]*keyState
}

// NewTracker creates a new metric tracker
func NewTracker(logger *slog.Logger) *Tracker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Tracker{
		execs:  make(map[string]*executionTracker),
		logger: logger,
	}
}

// RegisterExecution registers an execution with its start time.
// Must be called before Process for that execution.
func (t *Tracker) RegisterExecution(executionID string, startTime time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.execs[executionID] = &executionTracker{
		startTime: startTime,
		keys:      make(map[string]*keyState),
	}
}

// UnregisterExecution removes tracking state for an execution.
func (t *Tracker) UnregisterExecution(executionID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.execs, executionID)
}

// Process validates and resolves the at value for a metric entry.
// Returns the resolved at value and whether the entry should be recorded.
// If at is nil, auto-at (time since execution start) is used.
func (t *Tracker) Process(executionID, key string, at *float64) (float64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	exec, ok := t.execs[executionID]
	if !ok {
		// Unknown execution — use current time as fallback
		return float64(time.Now().UnixMilli()) / 1000.0, true
	}

	ks, exists := exec.keys[key]
	if !exists {
		ks = &keyState{mode: atModeUnknown}
		exec.keys[key] = ks
	}

	var resolvedAt float64

	if at == nil {
		// Auto-at mode
		if ks.mode == atModeCustom {
			// Consistency violation: was custom, now auto
			if !ks.warned {
				t.logger.Warn("metric key switched from custom to auto at, using auto",
					"execution_id", executionID, "key", key)
				ks.warned = true
			}
		}
		ks.mode = atModeAuto
		resolvedAt = time.Since(exec.startTime).Seconds()
	} else {
		// Custom-at mode
		if ks.mode == atModeAuto {
			// Consistency violation: was auto, now custom
			if !ks.warned {
				t.logger.Warn("metric key switched from auto to custom at, using auto",
					"execution_id", executionID, "key", key)
				ks.warned = true
			}
			// Fall back to auto
			ks.mode = atModeAuto
			resolvedAt = time.Since(exec.startTime).Seconds()
		} else {
			ks.mode = atModeCustom
			resolvedAt = *at

			// Check monotonicity
			if exists && resolvedAt <= ks.lastAt {
				if !ks.warned {
					t.logger.Warn(fmt.Sprintf("metric at value not increasing (%.4f <= %.4f), dropping",
						resolvedAt, ks.lastAt),
						"execution_id", executionID, "key", key)
					ks.warned = true
				}
				return 0, false
			}
		}
	}

	ks.lastAt = resolvedAt
	return resolvedAt, true
}
