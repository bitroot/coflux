// Package checkpoint buffers and coalesces checkpoint writes on their way
// from an adapter to the server.
//
// Unlike metrics, checkpoints are semantic state rather than telemetry: the
// last write before an execution suspends or terminates has to reach the
// server before the successor starts, or the successor resumes from a stale
// value. Buffering therefore comes with synchronous, acknowledged flush
// points rather than a best-effort background drain.
package checkpoint

import (
	"context"
	"sync"
	"time"

	"github.com/bitroot/coflux/cli/internal/adapter"
)

// DefaultInterval is the minimum gap between checkpoint deltas reaching the
// server for a given execution. Writes arriving within a window are coalesced
// (last write wins per name) and delivered when it closes.
//
// Slower than the metric default because a checkpoint value can be
// blob-backed: coalescing here means a value superseded before anything could
// read it is never uploaded at all.
const DefaultInterval = 500 * time.Millisecond

// Sink delivers a coalesced delta to the server.
type Sink interface {
	SetCheckpoints(ctx context.Context, executionID string, set map[string]*adapter.Value, reset []string) error
}

// entry is the most recent operation buffered for a name. A name is either
// set to a value or reset, never both — a later operation replaces an earlier
// one, so the delta only ever describes the net effect.
type entry struct {
	value *adapter.Value
	reset bool
}

type state struct {
	pending  map[string]entry
	lastSent time.Time
	timer    *time.Timer

	// Held across delivery so a timer-driven flush and an explicit Flush
	// can't interleave and land deltas out of order — the later snapshot
	// must not be overtaken by the earlier one, or a superseded value
	// becomes the stored one.
	sending sync.Mutex
}

// Throttle coalesces per-execution checkpoint deltas.
type Throttle struct {
	sink     Sink
	interval time.Duration

	mu    sync.Mutex
	execs map[string]*state
}

func NewThrottle(sink Sink, interval time.Duration) *Throttle {
	if interval <= 0 {
		interval = DefaultInterval
	}
	return &Throttle{
		sink:     sink,
		interval: interval,
		execs:    make(map[string]*state),
	}
}

// Record buffers a delta. The first write for an execution (or the first
// after a quiet period longer than the interval) is delivered immediately;
// subsequent writes are coalesced and delivered when the window closes.
func (t *Throttle) Record(ctx context.Context, executionID string, set map[string]*adapter.Value, reset []string) error {
	t.mu.Lock()
	st, existed := t.execs[executionID]
	if !existed {
		st = &state{pending: make(map[string]entry)}
		t.execs[executionID] = st
	}

	for name, value := range set {
		st.pending[name] = entry{value: value}
	}
	for _, name := range reset {
		st.pending[name] = entry{reset: true}
	}

	// Leading edge: deliver immediately if this is the first write or the
	// previous one is already older than the window.
	immediate := !existed || time.Since(st.lastSent) >= t.interval
	if !immediate && st.timer == nil {
		remaining := t.interval - time.Since(st.lastSent)
		st.timer = time.AfterFunc(remaining, func() {
			_ = t.deliver(context.WithoutCancel(ctx), executionID, st)
		})
	}
	t.mu.Unlock()

	if immediate {
		return t.deliver(ctx, executionID, st)
	}
	return nil
}

// Flush delivers anything buffered for the execution and returns once the
// server has acknowledged it. Safe to call when nothing is pending.
func (t *Throttle) Flush(ctx context.Context, executionID string) error {
	t.mu.Lock()
	st, ok := t.execs[executionID]
	t.mu.Unlock()

	if !ok {
		return nil
	}
	return t.deliver(ctx, executionID, st)
}

// Remove drops buffered state for a finished execution. Callers are expected
// to have flushed first — anything still pending here can no longer be read
// by the server, since the execution has terminated.
func (t *Throttle) Remove(executionID string) {
	t.mu.Lock()
	st, ok := t.execs[executionID]
	if ok {
		if st.timer != nil {
			st.timer.Stop()
			st.timer = nil
		}
		delete(t.execs, executionID)
	}
	t.mu.Unlock()
}

// deliver snapshots the pending delta and sends it.
//
// The send lock is taken *before* t.mu, and t.mu is never held while
// acquiring it, so concurrent callers serialise into a consistent order:
// whoever holds the send lock snapshots last and therefore sends last.
func (t *Throttle) deliver(ctx context.Context, executionID string, st *state) error {
	st.sending.Lock()
	defer st.sending.Unlock()

	t.mu.Lock()
	if st.timer != nil {
		st.timer.Stop()
		st.timer = nil
	}

	if len(st.pending) == 0 {
		t.mu.Unlock()
		return nil
	}

	set := make(map[string]*adapter.Value, len(st.pending))
	var reset []string
	for name, e := range st.pending {
		if e.reset {
			reset = append(reset, name)
		} else {
			set[name] = e.value
		}
	}
	sent := st.pending
	st.pending = make(map[string]entry)
	st.lastSent = time.Now()
	t.mu.Unlock()

	err := t.sink.SetCheckpoints(ctx, executionID, set, reset)
	if err != nil {
		// Put the delta back so a later write or an explicit flush retries
		// it. Names written while this send was in flight are newer and win.
		t.mu.Lock()
		for name, e := range sent {
			if _, superseded := st.pending[name]; !superseded {
				st.pending[name] = e
			}
		}
		t.mu.Unlock()
	}
	return err
}
