// Worker-side idle-timeout enforcement for producer streams.
//
// Each registered stream with a configured ``timeout_ms`` gets a
// ``time.Timer`` that resets on every ``stream_append`` and clears on
// ``stream_close`` (or when the producer execution ends). When the
// timer fires, the owning pool reports a close with reason="timeout"
// to the server and tells the adapter to stop producing via a
// ``stream_force_close`` notification.
//
// The timer lives in the CLI (rather than the server) so the check is
// local to the process producing the stream, without a server
// round-trip. Server-side we just record the outcome.

package pool

import (
	"sync"
	"time"
)

type streamKey struct {
	executionID string
	index       int
}

// streamTimer is one active idle-timeout for a single stream.
type streamTimer struct {
	timeout time.Duration
	timer   *time.Timer
}

// streamTimers is a concurrency-safe registry of active stream timers
// for a pool. All mutations take the lock; the underlying timer
// callback runs on its own goroutine (scheduled by time.AfterFunc),
// so it must not hold the lock while invoking fireFn.
type streamTimers struct {
	mu     sync.Mutex
	timers map[streamKey]*streamTimer
	fireFn func(key streamKey)
}

func newStreamTimers(fireFn func(key streamKey)) *streamTimers {
	return &streamTimers{
		timers: make(map[streamKey]*streamTimer),
		fireFn: fireFn,
	}
}

// Register starts a new idle-timeout timer for the given stream.
// “timeoutMs“ <= 0 is a no-op (no timeout configured). Safe to call
// even if a timer already exists for the key — the existing one is
// stopped first (defensive; the adapter shouldn't double-register).
func (s *streamTimers) Register(key streamKey, timeoutMs int) {
	if timeoutMs <= 0 {
		return
	}
	d := time.Duration(timeoutMs) * time.Millisecond

	s.mu.Lock()
	if existing, ok := s.timers[key]; ok {
		existing.timer.Stop()
	}
	st := &streamTimer{timeout: d}
	st.timer = time.AfterFunc(d, func() { s.fire(key) })
	s.timers[key] = st
	s.mu.Unlock()
}

// Reset restarts the countdown for a stream. No-op if no timer was
// registered (stream has no timeout configured, or was already
// cleared).
func (s *streamTimers) Reset(key streamKey) {
	s.mu.Lock()
	st, ok := s.timers[key]
	s.mu.Unlock()
	if !ok {
		return
	}
	// time.Timer.Reset is safe to call on a timer that has already
	// stopped; we pre-stop to avoid the rare race where the timer
	// fires between Stop() and Reset() — Reset alone would schedule a
	// second fire.
	st.timer.Stop()
	st.timer.Reset(st.timeout)
}

// Clear stops and removes the timer for a stream. Returns true if a
// timer existed. Safe to call from the timer callback (fire already
// removed the entry before invoking fireFn).
func (s *streamTimers) Clear(key streamKey) bool {
	s.mu.Lock()
	st, ok := s.timers[key]
	if ok {
		delete(s.timers, key)
	}
	s.mu.Unlock()
	if !ok {
		return false
	}
	st.timer.Stop()
	return true
}

// ClearExecution removes every timer owned by the given execution.
// Used on execution end to drop any lingering timers in bulk.
func (s *streamTimers) ClearExecution(executionID string) {
	s.mu.Lock()
	toStop := make([]*streamTimer, 0)
	for k, st := range s.timers {
		if k.executionID == executionID {
			toStop = append(toStop, st)
			delete(s.timers, k)
		}
	}
	s.mu.Unlock()
	for _, st := range toStop {
		st.timer.Stop()
	}
}

// fire is invoked by time.AfterFunc when the timeout elapses. Removes
// the entry before invoking the callback, so any Clear/Reset arriving
// from a concurrent close/append is harmless.
func (s *streamTimers) fire(key streamKey) {
	s.mu.Lock()
	_, ok := s.timers[key]
	if ok {
		delete(s.timers, key)
	}
	s.mu.Unlock()
	if !ok {
		// Cleared before we fired — nothing to do.
		return
	}
	s.fireFn(key)
}
