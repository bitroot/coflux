"""Where checkpoint deltas get cut.

Checkpoint state is one snapshot of a step's progress rather than a set of
independent cells: the server stores an execution's row-set as a complete
snapshot and applies each delta in a transaction. So what decides whether
the successor of a crashed or suspended execution reads a *coherent* state
is where the deltas are cut — and one cut at an arbitrary point describes a
state the execution was never in.

That only matters for state derived from something a replay can't re-read.
A result resolves again; a stream item doesn't, so its position is recorded
in a cursor, and the cursor and whatever the body derived from the item have
to reach the server together. The context holds writes back while such an
item is in hand and publishes them with the advance that consumes it.

These cover the context's half of that — the holding, the coalescing, and
what becomes of held writes when the item is abandoned instead. Which
subscription takes a hold, and when, is in ``test_stream_iteration.py``.
"""

from __future__ import annotations

import pytest

from coflux import protocol
from coflux.context import ExecutorContext

CURSOR = "_cursor/Eproducer_0/0,None,1"


class _Wire:
    """Records the deltas reaching the worker, values unwrapped."""

    def __init__(self):
        self.deltas = []

    def send(self, execution_id, set_=None, reset=None):
        self.deltas.append(
            (
                {name: value["value"] for name, value in (set_ or {}).items()},
                list(reset or []),
            )
        )


@pytest.fixture
def wire(monkeypatch):
    w = _Wire()
    monkeypatch.setattr(protocol, "send_checkpoint_update", w.send)
    return w


@pytest.fixture
def ctx():
    return ExecutorContext("Econsumer")


def test_writes_go_out_immediately_when_nothing_is_held(ctx, wire):
    """The ordinary case — a body not consuming a stream is always at a
    point it could resume from, so nothing is deferred."""
    ctx.checkpoint_set("cursor", 4)
    ctx.checkpoint_set("cursor", 5)

    assert wire.deltas == [({"cursor": 4}, []), ({"cursor": 5}, [])]


def test_a_hold_publishes_one_delta_at_the_release(ctx, wire):
    """The whole point: derived state and the cursor advance land together
    or not at all."""
    ctx.hold_checkpoints()
    ctx.checkpoint_set("total", 7)
    ctx.checkpoint_set(CURSOR, 1)
    assert wire.deltas == []

    ctx.release_checkpoints(publish=True)

    assert wire.deltas == [({"total": 7, CURSOR: 1}, [])]


def test_the_last_write_to_a_name_wins(ctx, wire):
    """Held writes coalesce, the way the worker-side throttle does: the
    delta describes the net effect, not the history."""
    ctx.hold_checkpoints()
    ctx.checkpoint_set("total", 1)
    ctx.checkpoint_set("total", 2)
    ctx.checkpoint_reset("total")
    ctx.checkpoint_set("total", 3)
    ctx.release_checkpoints(publish=True)

    assert wire.deltas == [({"total": 3}, [])]


def test_a_reset_supersedes_a_held_write(ctx, wire):
    """A name is either set or reset in the delta, never both."""
    ctx.hold_checkpoints()
    ctx.checkpoint_set("total", 1)
    ctx.checkpoint_reset("total")
    ctx.release_checkpoints(publish=True)

    assert wire.deltas == [({}, ["total"])]


def test_an_abandoned_item_discards_what_the_body_derived(ctx, wire):
    """Nothing is recorded, because the item it came from was never
    consumed and will be delivered again."""
    ctx.hold_checkpoints()
    ctx.checkpoint_set("total", 7)
    ctx.release_checkpoints(publish=False)

    assert wire.deltas == []
    # The execution still sees its own writes; only the record is dropped.
    assert ctx.checkpoint_get("total") == 7


def test_nested_holds_publish_at_the_outermost_release(ctx, wire):
    """A body iterating two streams is only somewhere it could resume from
    once every cursor involved is up to date."""
    ctx.hold_checkpoints()
    ctx.checkpoint_set("outer", 1)
    ctx.hold_checkpoints()
    ctx.checkpoint_set("inner", 2)

    ctx.release_checkpoints(publish=True)
    assert wire.deltas == []

    ctx.release_checkpoints(publish=True)
    assert wire.deltas == [({"outer": 1, "inner": 2}, [])]


def test_an_abandoned_inner_item_discards_the_enclosing_writes_too(ctx, wire):
    """Not over-eager: an enclosing hold means that iteration hasn't
    advanced its own cursor either, so everything pending derives from an
    item that is still unconsumed."""
    ctx.hold_checkpoints()
    ctx.checkpoint_set("outer", 1)
    ctx.hold_checkpoints()
    ctx.checkpoint_set("inner", 2)

    ctx.release_checkpoints(publish=False)
    ctx.release_checkpoints(publish=True)

    assert wire.deltas == []


def test_writes_are_immediate_again_after_the_release(ctx, wire):
    ctx.hold_checkpoints()
    ctx.release_checkpoints(publish=True)
    ctx.checkpoint_set("cursor", 1)

    assert wire.deltas == [({"cursor": 1}, [])]


def test_an_unbalanced_release_is_a_no_op(ctx, wire):
    """The count can't go negative — one that did would leave every later
    write held for the rest of the execution."""
    ctx.release_checkpoints(publish=True)
    ctx.checkpoint_set("cursor", 1)

    assert wire.deltas == [({"cursor": 1}, [])]


def test_flush_publishes_what_is_held(ctx, wire, monkeypatch):
    """An explicit flush is the caller declaring the point consistent —
    the escape hatch for state that has to be durable before a side
    effect, including inside a loop body."""
    monkeypatch.setattr(protocol, "request_flush", lambda execution_id: "R1")
    monkeypatch.setattr(ctx, "_wait_response", lambda request_id: None)

    ctx.hold_checkpoints()
    ctx.checkpoint_set("sent", True)
    ctx.flush()

    assert wire.deltas == [({"sent": True}, [])]

    # The flush cut a delta; it didn't end the item.
    ctx.checkpoint_set("total", 1)
    assert len(wire.deltas) == 1
