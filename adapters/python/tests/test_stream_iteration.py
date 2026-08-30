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
from types import SimpleNamespace

import pytest
from coflux import protocol, streams
from coflux.models import Stream

PRODUCER_STREAM_ID = "Eproducer_0"


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
        self.unsubscribes = []
        self.acks = []
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
        producer_execution_id,
        index,
        from_sequence,
        prefetch,
        stride=None,
    ):
        self.subscribes.append(subscription_id)
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
    monkeypatch.setattr(
        streams, "get_context", lambda: SimpleNamespace(execution_id="Econsumer")
    )
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
