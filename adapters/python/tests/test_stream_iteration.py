"""Lifecycle tests for the consumer-side stream iterator.

These exercise the real adapter (``coflux.streams``) in-process, with a
fake worker standing in for the far side of the adapter protocol: no
server, no CLI, nothing on disk.

That's the mirror image of the repo-root ``tests/`` suite, which puppets
the *adapter* (via the ``support/adapter.py`` shim) to test the real CLI
and server. Both drive one side of the adapter/worker boundary and assert
on the messages crossing it; which side is faked is what decides where a
test belongs. Anything about what the adapter decides to send — acks,
unsubscribes, when a subscription is released — belongs here, because the
root suite substitutes a shim for exactly that logic and so can never
observe it.

The behaviour under test is that abandoning a stream mid-iteration
releases the subscription, the way abandoning a generator closes it. It
matters beyond tidiness: a subscription that stays open stops advancing
its acknowledged position, and the server measures a producer's
``buffer`` against the *slowest* subscriber — so a consumer that walks
away from a bounded stream pins its producer until the idle timeout
fires, or forever if none was configured.
"""

import asyncio
import gc

import pytest

from coflux import protocol, streams
from coflux.errors import Suspending
from coflux.models import Stream

PRODUCER_STREAM_ID = "Eproducer_0"


class _FakeContext:
    """Stands in for ``ExecutorContext`` — only what subscriptions reach for.

    ``suspense_timeout`` is ``None`` by default, which is the no-cursor
    path: no checkpoint is written and iteration blocks indefinitely. The
    resume tests set it to opt into cursor tracking.
    """

    def __init__(self):
        self.execution_id = "Econsumer"
        self.suspense_timeout = None
        self.checkpoints = {}
        self.suspended = []
        self.probes = []
        self.available = False
        self._occurrences = {}

    def next_cursor_occurrence(self, name):
        occurrence = self._occurrences.get(name, 0)
        self._occurrences[name] = occurrence + 1
        return occurrence

    def checkpoint_get(self, name):
        if name not in self.checkpoints:
            raise KeyError(name)
        return self.checkpoints[name]

    def checkpoint_set(self, name, value):
        self.checkpoints[name] = value

    def stream_available(self, stream_id, sequence):
        """Stands in for the server's answer. Tests set ``available`` to
        say what it should report; the default is "nothing there", which
        is what makes a consumer suspend."""
        self.probes.append((stream_id, sequence))
        return self.available

    def suspend_execution(self, delay=None, stream_wait=None):
        self.suspended.append((delay, stream_wait))
        raise Suspending(None, stream_wait)


class _FakeDispatcher:
    def __init__(self):
        self.closed = False

    def is_closed(self):
        return self.closed

    def register_notification(self, method, handler):
        pass

    def add_close_callback(self, callback):
        pass


class _Harness:
    """Stands in for the server.

    Delivers a fixed backlog (and optionally a closure) synchronously in
    response to the subscribe, so iteration never blocks on the queue and
    the tests stay single-threaded.
    """

    def __init__(self, registry, dispatcher):
        self.registry = registry
        self.dispatcher = dispatcher
        self.subscribes = []
        self.strides = []
        self.unsubscribes = []
        self.acks = []
        self.context = _FakeContext()
        self._items = []
        self._close = None

    def serve(self, items, close=None):
        """Queue what the server should push once subscribed. ``close`` is
        a reason string, or None to leave the stream open."""
        self._items = items
        self._close = close

    def on_subscribe(
        self,
        execution_id,
        subscription_id,
        stream_id,
        from_sequence,
        prefetch,
        stride=None,
    ):
        self.subscribes.append(subscription_id)
        self.strides.append(stride)
        if self._items:
            self.registry._on_items(
                {"subscription_id": subscription_id, "items": list(self._items)}
            )
        if self._close is not None:
            self.registry._on_closed(
                {"subscription_id": subscription_id, "reason": self._close}
            )

    def on_unsubscribe(self, execution_id, subscription_id):
        self.unsubscribes.append(subscription_id)

    def on_ack(self, execution_id, subscription_id, count, sequence):
        self.acks.append((subscription_id, count, sequence))


@pytest.fixture
def harness(monkeypatch):
    # A fresh registry per test: the real one is a process-wide singleton
    # whose subscription ids and installed-handler flag would leak between
    # tests.
    registry = streams.StreamRegistry()
    dispatcher = _FakeDispatcher()
    h = _Harness(registry, dispatcher)

    monkeypatch.setattr(streams, "_registry_instance", registry)
    monkeypatch.setattr(streams, "get_dispatcher", lambda: dispatcher)
    monkeypatch.setattr(streams, "get_context", lambda: h.context)
    # Values go on the wire as tagged envelopes; the lifecycle is what's
    # under test, so keep them opaque.
    monkeypatch.setattr(streams, "deserialize_value", lambda value: value)
    monkeypatch.setattr(protocol, "send_stream_subscribe", h.on_subscribe)
    monkeypatch.setattr(protocol, "send_stream_unsubscribe", h.on_unsubscribe)
    monkeypatch.setattr(protocol, "send_stream_ack", h.on_ack)
    return h


def test_break_releases_subscription(harness):
    """Breaking out of the loop unsubscribes.

    Nothing here keeps a reference to the iterator — ``Stream.__iter__``
    creates a fresh one and only the ``for`` holds it, so ``break`` drops
    the last reference exactly as it would for a generator.
    """
    harness.serve([[0, "a"], [1, "b"], [2, "c"]])

    seen = []
    for value in Stream(PRODUCER_STREAM_ID):
        seen.append(value)
        if len(seen) == 2:
            break
    gc.collect()

    assert seen == ["a", "b"]
    assert harness.unsubscribes == harness.subscribes


def test_exception_in_loop_body_releases_subscription(harness):
    """An exception escaping the loop body releases it too — the analogue
    of a generator being closed when its consumer unwinds."""
    harness.serve([[0, "a"], [1, "b"]])

    with pytest.raises(RuntimeError):
        for _value in Stream(PRODUCER_STREAM_ID):
            raise RuntimeError("boom")
    # The traceback keeps the raising frame (and so the iterator) alive
    # until the `with` block exits and the exception is cleared.
    gc.collect()

    assert harness.unsubscribes == harness.subscribes


def test_dropping_the_iterator_releases_subscription(harness):
    """No loop at all — just letting the handle go out of scope."""
    harness.serve([[0, "a"]])

    iterator = iter(Stream(PRODUCER_STREAM_ID))
    assert next(iterator) == "a"
    assert harness.unsubscribes == []

    del iterator
    gc.collect()

    assert harness.unsubscribes == harness.subscribes


def test_registry_holds_iterators_weakly(harness):
    """The mechanism the above relies on: the registry must not keep
    iterators alive, or ``__del__`` would never run."""
    iterator = iter(Stream(PRODUCER_STREAM_ID))
    (subscription_id,) = harness.subscribes
    assert harness.registry._iterators.get(subscription_id) is iterator

    del iterator
    gc.collect()

    assert harness.registry._iterators.get(subscription_id) is None


def test_exhaustion_unsubscribes_exactly_once(harness):
    """Running to completion must not double-send: ``__next__`` releases on
    the terminal close, and ``__del__`` later calls ``close`` again."""
    harness.serve([[0, "a"], [1, "b"]], close="complete")

    seen = list(Stream(PRODUCER_STREAM_ID))
    gc.collect()

    assert seen == ["a", "b"]
    assert harness.unsubscribes == harness.subscribes
    assert len(harness.unsubscribes) == 1


def test_close_is_idempotent_and_ends_iteration(harness):
    """``close`` mirrors ``generator.close()``: repeatable, and a later
    ``next`` raises ``StopIteration`` rather than blocking on the queue."""
    iterator = iter(Stream(PRODUCER_STREAM_ID))

    iterator.close()
    iterator.close()

    assert len(harness.unsubscribes) == 1
    with pytest.raises(StopIteration):
        next(iterator)


def test_context_manager_releases_subscription(harness):
    """The explicit form, for consumers that want a visible scope."""
    harness.serve([[0, "a"], [1, "b"], [2, "c"]])

    seen = []
    with iter(Stream(PRODUCER_STREAM_ID)) as items:
        for value in items:
            seen.append(value)
            break

    assert seen == ["a"]
    assert harness.unsubscribes == harness.subscribes


def test_close_skips_roundtrip_when_dispatcher_gone(harness):
    """With stdin gone there's no one to receive the unsubscribe; stdout
    may still be writable, so the send has to be suppressed rather than
    attempted and swallowed."""
    iterator = iter(Stream(PRODUCER_STREAM_ID))
    harness.dispatcher.closed = True

    iterator.close()

    assert harness.unsubscribes == []


# --- async iteration ---------------------------------------------------------
#
# `async for` opens the same subscription and keeps the same accounting;
# only the waiting differs. These use `asyncio.run` rather than an async
# test plugin — the harness is synchronous, so a coroutine per test is
# all that's needed.


def test_async_iteration_drains_the_stream(harness):
    """The baseline: `async for` sees the same items as `for`, and
    releases the subscription exactly once on exhaustion."""
    harness.serve([[0, "a"], [1, "b"], [2, "c"]], close="complete")

    async def consume():
        return [value async for value in Stream(PRODUCER_STREAM_ID)]

    seen = asyncio.run(consume())
    gc.collect()

    assert seen == ["a", "b", "c"]
    assert harness.unsubscribes == harness.subscribes
    assert len(harness.unsubscribes) == 1


def test_async_break_releases_subscription(harness):
    """Leaving an `async for` early releases it, same as `break` does for
    the sync iterator."""
    harness.serve([[0, "a"], [1, "b"], [2, "c"]])

    async def consume():
        seen = []
        async for value in Stream(PRODUCER_STREAM_ID):
            seen.append(value)
            if len(seen) == 2:
                break
        return seen

    seen = asyncio.run(consume())
    gc.collect()

    assert seen == ["a", "b"]
    assert harness.unsubscribes == harness.subscribes


def test_async_cancellation_does_not_drop_an_item(harness):
    """A cancelled read must not consume the item it was waiting for.

    This is the reason the async path delivers through the event loop
    instead of offloading the blocking iterator to a thread: a cancelled
    `to_thread(next, ...)` leaves the thread blocked in the queue, so the
    item it eventually takes is discarded and the ack accounting is left
    pointing at an item no one will retire.
    """

    async def consume():
        iterator = Stream(PRODUCER_STREAM_ID).__aiter__()
        (subscription_id,) = harness.subscribes

        # Nothing served yet, so this suspends.
        pending = asyncio.ensure_future(iterator.__anext__())
        await asyncio.sleep(0)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending

        # The item arrives after the reader gave up; the next read gets it.
        harness.registry._on_items(
            {"subscription_id": subscription_id, "items": [[0, "a"]]}
        )
        return await iterator.__anext__()

    assert asyncio.run(consume()) == "a"


def test_async_ack_flushed_before_suspending(harness):
    """Progress is reported before the reader waits.

    Acks are batched, but a `buffer=0` producer may be blocked on exactly
    the acknowledgement being held back — so suspending without flushing
    first would deadlock the pair.
    """
    harness.serve([[0, "a"], [1, "b"]])

    async def consume():
        iterator = Stream(PRODUCER_STREAM_ID).__aiter__()
        (subscription_id,) = harness.subscribes

        assert await iterator.__anext__() == "a"
        assert await iterator.__anext__() == "b"
        # Two items in, both below the batch threshold: nothing sent yet.
        assert harness.acks == []

        pending = asyncio.ensure_future(iterator.__anext__())
        await asyncio.sleep(0)
        assert harness.acks == [(subscription_id, 2, 1)]

        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending

    asyncio.run(consume())


def test_async_close_is_idempotent_and_ends_iteration(harness):
    """`aclose` mirrors `close`: repeatable, and a later read raises
    `StopAsyncIteration` rather than waiting forever."""

    async def consume():
        iterator = Stream(PRODUCER_STREAM_ID).__aiter__()

        await iterator.aclose()
        await iterator.aclose()

        assert len(harness.unsubscribes) == 1
        with pytest.raises(StopAsyncIteration):
            await iterator.__anext__()

    asyncio.run(consume())


def test_async_context_manager_releases_subscription(harness):
    """The explicit scope, for readers that outlive their loop body."""
    harness.serve([[0, "a"], [1, "b"]])

    async def consume():
        seen = []
        async with Stream(PRODUCER_STREAM_ID).__aiter__() as items:
            async for value in items:
                seen.append(value)
                break
        return seen

    seen = asyncio.run(consume())

    assert seen == ["a"]
    assert harness.unsubscribes == harness.subscribes


def test_async_iterator_requires_a_running_loop(harness):
    """Reaching for `async for` outside async code should say so, rather
    than failing somewhere further in."""
    with pytest.raises(RuntimeError, match="running event loop"):
        Stream(PRODUCER_STREAM_ID).__aiter__()


# --- Resuming from a checkpoint cursor ---------------------------------------
#
# A subscription opened inside a `cf.suspense` scope keeps its position in
# an adapter-managed checkpoint, so the execution that resumes the step
# carries on rather than re-reading from sequence 0. Outside such a scope
# none of this engages and iteration behaves exactly as it always has.

CURSOR = f"_cursor/{PRODUCER_STREAM_ID}/0,None,1"


def test_no_cursor_outside_a_suspense_scope(harness):
    """The default path writes no checkpoint and never suspends."""
    harness.serve([[0, "a"], [1, "b"]], close="complete")

    assert list(Stream(PRODUCER_STREAM_ID)) == ["a", "b"]
    assert harness.context.checkpoints == {}
    assert harness.context.suspended == []


def test_cursor_advances_as_items_are_consumed(harness):
    harness.context.suspense_timeout = 30
    harness.serve([[0, "a"], [1, "b"], [2, "c"]], close="complete")

    assert list(Stream(PRODUCER_STREAM_ID)) == ["a", "b", "c"]
    assert harness.context.checkpoints == {CURSOR: 3}


def test_cursor_lags_the_item_in_hand(harness):
    """An item counts as consumed only once the caller comes back for the
    next one — the same boundary the acknowledgement uses, so a suspension
    mid-body replays that item rather than skipping it."""
    harness.context.suspense_timeout = 30
    harness.serve([[0, "a"], [1, "b"]])

    iterator = iter(Stream(PRODUCER_STREAM_ID))
    assert next(iterator) == "a"
    # "a" is in hand, not yet retired.
    assert harness.context.checkpoints == {}
    assert next(iterator) == "b"
    assert harness.context.checkpoints == {CURSOR: 1}


def test_resume_subscribes_at_the_cursor(harness):
    """The server starts delivery at the cursor, rather than the consumer
    reading a backlog in order to discard it."""
    harness.context.suspense_timeout = 30
    harness.context.checkpoints[CURSOR] = 2
    harness.serve([[2, "c"]], close="complete")

    assert list(Stream(PRODUCER_STREAM_ID)) == ["c"]
    assert harness.strides == [{"start": 2, "stop": None, "step": 1}]
    # Counting continues from where it resumed.
    assert harness.context.checkpoints == {CURSOR: 3}


def test_resume_of_a_partition_counts_its_own_items(harness):
    """A partition consumer's cursor counts items of its view, not raw
    sequences, so resuming is a plain slice on the same view."""
    harness.context.suspense_timeout = 30
    name = f"_cursor/{PRODUCER_STREAM_ID}/1,None,4"
    harness.context.checkpoints[name] = 3
    harness.serve([], close="complete")

    assert list(Stream(PRODUCER_STREAM_ID).partition(4, 1)) == []
    # Item 3 of the view sits at sequence 1 + 3*4.
    assert harness.strides == [{"start": 13, "stop": None, "step": 4}]


def test_identical_views_get_distinct_cursors(harness):
    """Content addressing can't separate two loops over the same view, so
    they fall back to an occurrence counter."""
    harness.context.suspense_timeout = 30
    harness.serve([[0, "a"]], close="complete")

    assert list(Stream(PRODUCER_STREAM_ID)) == ["a"]
    assert list(Stream(PRODUCER_STREAM_ID)) == ["a"]

    assert harness.context.checkpoints == {CURSOR: 1, f"{CURSOR}#1": 1}


def test_idle_stream_suspends_and_releases_the_subscription(harness):
    """Nothing arrives within the timeout, so the execution gives up its
    worker slot — after unsubscribing, since an abandoned subscription
    would pin the producer's backpressure watermark."""
    harness.context.suspense_timeout = 0.01
    harness.serve([])

    with pytest.raises(Suspending):
        list(Stream(PRODUCER_STREAM_ID))

    # The local wait expired, then the server confirmed there was nothing.
    assert harness.context.probes == [(PRODUCER_STREAM_ID, 0)]
    # No delay — the successor is gated on the stream instead, and the
    # server releases it when the next item lands or the stream closes.
    assert harness.context.suspended == [(None, (PRODUCER_STREAM_ID, 0))]
    assert harness.unsubscribes == harness.subscribes


def test_the_server_decides_whether_to_suspend(harness):
    """An empty queue is not evidence that the stream is empty — items
    arrive asynchronously, so a consumer that checked locally would
    suspend before hearing anything, wake at once because the item had
    been there all along, and repeat forever. The server is asked
    instead, exactly as it is for a result."""
    harness.context.suspense_timeout = 0
    # Only the first item is delivered on subscribe; the second arrives
    # after the server has been asked, which is the ordering that broke a
    # consumer deciding for itself — it would have suspended here.
    harness.serve([[0, "a"]])

    def available(stream_id, sequence):
        harness.context.probes.append((stream_id, sequence))
        subscription_id = harness.subscribes[-1]
        harness.registry._on_items(
            {"subscription_id": subscription_id, "items": [[1, "b"]]}
        )
        harness.registry._on_closed(
            {"subscription_id": subscription_id, "reason": "complete"}
        )
        return True

    harness.context.stream_available = available

    assert list(Stream(PRODUCER_STREAM_ID)) == ["a", "b"]
    assert harness.context.probes == [(PRODUCER_STREAM_ID, 1)]
    assert harness.context.suspended == []


def test_zero_timeout_suspends_once_caught_up(harness):
    """With nothing left, the server says so and the consumer suspends —
    once, gated on the sequence it is waiting for."""
    harness.context.suspense_timeout = 0
    harness.context.available = False
    harness.serve([[0, "a"]])

    iterator = iter(Stream(PRODUCER_STREAM_ID))
    assert next(iterator) == "a"
    with pytest.raises(Suspending):
        next(iterator)

    assert harness.context.probes == [(PRODUCER_STREAM_ID, 1)]
    assert harness.context.suspended == [(None, (PRODUCER_STREAM_ID, 1))]
    assert harness.context.checkpoints == {CURSOR: 1}
