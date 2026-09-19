package pool

import (
	"sync"
	"testing"
	"time"
)

// The registry is driven entirely by in-process timers, so these use real
// durations — but a short one against a much longer observation window,
// with nothing but the Go scheduler able to get in the way. That is a
// different proposition from racing the same behaviour across a server,
// a worker and four round trips.
const (
	tick = 30 * time.Millisecond
	// Long enough that a timer which was going to fire, has.
	settle = 10 * tick
)

// fired records which keys the registry reported, for assertions.
type fired struct {
	mu   sync.Mutex
	keys []streamKey
}

func (f *fired) record(key streamKey) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys = append(f.keys, key)
}

func (f *fired) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.keys)
}

func newFixture() (*streamTimers, *fired, streamKey) {
	f := &fired{}
	return newStreamTimers(f.record), f, streamKey{executionID: "R1:1:1", index: 0}
}

func TestRegisteredTimerFires(t *testing.T) {
	s, f, key := newFixture()
	s.Register(key, int(tick/time.Millisecond))

	time.Sleep(settle)
	if got := f.count(); got != 1 {
		t.Fatalf("expected the idle timeout to fire once, got %d", got)
	}
}

func TestZeroTimeoutNeverFires(t *testing.T) {
	s, f, key := newFixture()
	s.Register(key, 0)

	time.Sleep(settle)
	if got := f.count(); got != 0 {
		t.Fatalf("a stream with no configured timeout should never fire, got %d", got)
	}
}

// The rule a suspended consumer relies on: while the countdown is paused,
// the producer is not idle however long it waits.
func TestPausedTimerDoesNotFire(t *testing.T) {
	s, f, key := newFixture()
	s.Register(key, int(tick/time.Millisecond))
	s.SetPaused(key, true)

	time.Sleep(settle)
	if got := f.count(); got != 0 {
		t.Fatalf("a paused countdown should not fire, got %d", got)
	}
}

// Resuming starts a fresh full-length window rather than whatever was
// left of the old one — the same thing a resumed producer gets.
func TestResumingRestartsAFullWindow(t *testing.T) {
	s, f, key := newFixture()
	s.Register(key, int(tick/time.Millisecond))
	s.SetPaused(key, true)
	time.Sleep(settle)

	s.SetPaused(key, false)
	if got := f.count(); got != 0 {
		t.Fatalf("resuming should not fire immediately, got %d", got)
	}

	time.Sleep(settle)
	if got := f.count(); got != 1 {
		t.Fatalf("expected one fire after the fresh window, got %d", got)
	}
}

// An append while paused must not quietly restart the countdown: the
// consumers are still asleep, so the producer is still not idle.
func TestResetIsANoOpWhilePaused(t *testing.T) {
	s, f, key := newFixture()
	s.Register(key, int(tick/time.Millisecond))
	s.SetPaused(key, true)
	s.Reset(key)

	time.Sleep(settle)
	if got := f.count(); got != 0 {
		t.Fatalf("an append while paused should leave the countdown stopped, got %d", got)
	}
}

func TestClearStopsTheTimer(t *testing.T) {
	s, f, key := newFixture()
	s.Register(key, int(tick/time.Millisecond))

	if !s.Clear(key) {
		t.Fatal("Clear should report that a timer existed")
	}
	if s.Clear(key) {
		t.Fatal("Clear should report no timer the second time")
	}

	time.Sleep(settle)
	if got := f.count(); got != 0 {
		t.Fatalf("a cleared timer should not fire, got %d", got)
	}
}

func TestClearExecutionStopsEveryStreamItOwns(t *testing.T) {
	s, f, _ := newFixture()
	s.Register(streamKey{executionID: "R1:1:1", index: 0}, int(tick/time.Millisecond))
	s.Register(streamKey{executionID: "R1:1:1", index: 1}, int(tick/time.Millisecond))
	other := streamKey{executionID: "R2:1:1", index: 0}
	s.Register(other, int(tick/time.Millisecond))

	s.ClearExecution("R1:1:1")

	time.Sleep(settle)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.keys) != 1 || f.keys[0] != other {
		t.Fatalf("expected only the other execution's stream to fire, got %v", f.keys)
	}
}

// Pausing an unregistered stream is a no-op rather than a panic: a pause
// can arrive after the stream was closed.
func TestPausingAnUnknownStreamIsHarmless(t *testing.T) {
	s, _, key := newFixture()
	s.SetPaused(key, true)
	s.Reset(key)
}

// The pool's entry point is what the worker calls when the server sends
// `stream_timer_pause`; covered here so the wiring from that command to
// the right stream's countdown is not left to an end-to-end test.
func TestPoolSetStreamTimerPausedAddressesTheRightStream(t *testing.T) {
	f := &fired{}
	p := &Pool{}
	p.streamTimers = newStreamTimers(f.record)

	paused := streamKey{executionID: "R1:1:1", index: 0}
	other := streamKey{executionID: "R1:1:1", index: 1}
	p.streamTimers.Register(paused, int(tick/time.Millisecond))
	p.streamTimers.Register(other, int(tick/time.Millisecond))

	p.SetStreamTimerPaused("R1:1:1", 0, true)

	time.Sleep(settle)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.keys) != 1 || f.keys[0] != other {
		t.Fatalf("expected only the unpaused stream to fire, got %v", f.keys)
	}
}
