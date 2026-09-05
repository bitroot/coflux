"""Producer and consumer stream plumbing.

The producer side owns ``StreamDriver``: each execution whose return value
(or submitted arguments) contains generators uses one to run each
generator in a background thread. Both sync (``def`` + ``yield``) and
async (``async def`` + ``yield``) generators are supported; async
generators get a fresh event loop confined to their worker thread.

A stream belongs to the step, not the execution. Registering is a
round-trip: the server allocates the stream's index within the step and
tells the driver whether this registration resumes a stream a suspended
predecessor left paused — in which case items continue the existing
sequence, and consumers see one unbroken stream across the suspension.
That is what makes ``cf.suspend()`` inside a generator body the way to
write a producer that pauses and carries on.

The consumer side owns a module-level ``StreamRegistry``: open consumer
subscriptions are keyed by subscription id. The registry's dispatcher
handlers (``stream_items``/``stream_closed``) route incoming pushes from
the server to the right iterator's queue, which yields as the user
iterates. Readers come in two flavours — a blocking one for ``for`` and
a loop-native one for ``async for`` — sharing their acknowledgement and
release bookkeeping via ``_Subscription``; only the queue they deliver
through differs. On dispatcher EOF every active iterator is woken with a
synthetic abandoned-close so user code doesn't hang forever.

Both sides are thread-safe: the ``Dispatcher`` owns stdin (so subtask
calls from generator bodies don't race), and stdout writes go through
``Protocol._write_lock``.
"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import queue
import threading
import traceback
import weakref
from collections.abc import Iterator
from typing import Any, final

from . import protocol
from .dispatcher import get_dispatcher
from .errors import raise_for_close
from .models import Stream
from .serialization import deserialize_value, serialize_value
from .state import get_context
from .target import Streams, _validate_buffer, _validate_timeout

# --- Producer side ---


_STREAM_OPT_UNSET: Any = object()


def stream(
    generator: Any,
    *,
    buffer: Any = _STREAM_OPT_UNSET,
    timeout: Any = _STREAM_OPT_UNSET,
) -> Any:
    """Register a generator as a Coflux stream and return a handle.

    Use this when a task returns multiple streams or needs to override
    the task-level stream configuration. For the common case where a
    task body is itself a generator, ``@cf.task(streams=cf.Streams(...))``
    handles the registration automatically — you don't need to call
    ``cf.stream`` explicitly.

    Registration happens at call time: the server is asked to register
    the stream (a round-trip, like ``submit``), the driver thread starts,
    and any later serialisation sees a regular ``Stream`` handle. That
    means ``cf.stream`` must be called inside a task or workflow body
    (where an execution context is active); calling it from module scope
    or outside a task raises.

    Streams are matched across a suspension by the order they're
    registered in, so code that registers several streams before
    suspending has to register them in the same order when it resumes —
    the same determinism suspend already requires of the code before the
    suspend point.

    Unspecified options inherit from the enclosing task's
    ``streams=cf.Streams(...)``. Explicit options override per-call.

    Args:
        generator: A sync or async generator. Other iterables aren't
            accepted — wrapping a list in ``cf.stream`` doesn't make
            sense; pass it as a value directly.
        buffer: Backpressure budget. ``0`` (the default if neither
            ``cf.stream(buffer=...)`` nor the task-level default sets
            it) means strict lockstep — the producer emits an item,
            waits for a consumer to finish processing it, then emits
            the next. ``N`` allows the producer to stay up to ``N``
            items ahead of the *slowest* consumer. ``None`` disables
            backpressure entirely.
        timeout: Idle-timeout budget. If the producer doesn't append a
            new item within this window (including when blocked on
            consumer demand), the stream is force-closed with reason
            ``"timeout"``. Accepts a positive number of seconds, a
            ``timedelta``, or ``None`` to disable.

    Returns:
        A ``Stream`` handle referencing the registered stream. It
        serialises as ``{"type": "stream", "id": ...}`` and is iterable
        by downstream tasks.
    """
    if not (inspect.isgenerator(generator) or inspect.isasyncgen(generator)):
        raise TypeError(
            f"cf.stream expects a generator, got {type(generator).__name__}"
        )

    ctx = get_context()
    default = ctx.get_default_streams() or Streams()
    resolved_buffer = (
        _validate_buffer(buffer) if buffer is not _STREAM_OPT_UNSET else default.buffer
    )
    resolved_timeout = (
        _validate_timeout(timeout)
        if timeout is not _STREAM_OPT_UNSET
        else default.timeout
    )
    stream_id = ctx.register_stream(generator, resolved_buffer, resolved_timeout)
    return Stream(stream_id)


class StreamDriver:
    """Manages streams produced by a single execution."""

    def __init__(self, execution_id: str) -> None:
        self._execution_id = execution_id
        # Registration order within this execution. The server matches a
        # resuming execution's k-th registration onto the paused stream a
        # suspended predecessor registered k-th.
        self._next_position = 0
        self._threads: list[threading.Thread] = []
        self._generators: list[Any] = []
        self._lock = threading.Lock()
        # Demand tracking: each registered stream gets a per-index slot in
        # `_demand`. Drivers wait on `_demand_cv` until credit is granted
        # by the server (via stream_demand notifications) or the driver is
        # asked to close. ``None`` means unbounded demand (buffer=None at
        # registration time); the driver never waits.
        self._demand_cv = threading.Condition()
        self._demand: dict[int, int | None] = {}
        # Credits granted for an index we haven't been told about yet. The
        # server grants demand while it handles a registration — before
        # the reply carrying the stream's index reaches us — so the grant
        # can overtake the reply. Held here until ``register`` learns the
        # index, then applied.
        self._pending_demand: dict[int, int] = {}
        self._closing = False
        self._demand_handler_registered = False
        self._force_close_handler_registered = False
        # Indexes of streams the worker (CLI) has force-closed — typically
        # because their idle timeout elapsed. Read by ``_acquire_demand``
        # and by the producer loop so the driver thread exits promptly
        # and skips sending its own stream_close (the server already
        # recorded the closure).
        self._force_closed: dict[int, str] = {}
        # Per-index generator entry, for clean close on force-close.
        self._by_index: dict[int, dict[str, Any]] = {}

    def register(
        self,
        generator: Any,
        buffer: int | None,
        timeout_ms: int | None = None,
    ) -> str:
        """Register a generator and start running it in a worker thread.

        Accepts both sync generators (``def`` + ``yield``) and async
        generators (``async def`` + ``yield``). Each gets its own thread;
        async generators run inside a fresh event loop confined to that
        thread.

        Registration is a request to the server, which replies with the
        stream's id, its index within the step, and the head to sequence
        from. A new stream starts at 0; one resumed after a suspend
        continues from where the suspended execution left it.

        ``buffer`` is the producer-side backpressure budget. ``None``
        means unbounded (no flow control); ``0`` means strict lockstep
        (producer waits for a consumer to ack each item before emitting
        the next); ``N>0`` allows the producer to stay up to N items
        ahead of the slowest consumer.

        ``timeout_ms`` is the idle-timeout budget (milliseconds). The
        worker (CLI) closes the stream with reason "timeout" if no item
        is appended within that window. ``None`` disables the timeout.

        Returns the stream's opaque ``id`` for embedding in the serialized
        value as a stream reference.
        """
        self._ensure_demand_handler_registered()
        self._ensure_force_close_handler_registered()

        with self._lock:
            position = self._next_position
            self._next_position += 1

        request_id = protocol.request_stream_register(
            self._execution_id, position, buffer=buffer, timeout_ms=timeout_ms
        )
        response = get_dispatcher().wait_for_response(request_id)
        if response is None:
            raise RuntimeError("timed out registering stream")
        if response.get("error"):
            error = response["error"]
            raise RuntimeError(f"{error['code']}: {error['message']}")
        registration = response.get("result") or {}
        stream_id: str = registration["id"]
        index: int = registration["index"]
        head: int = registration.get("head", -1)

        with self._demand_cv:
            # Unbounded ⇒ driver never waits. Bounded ⇒ starts from whatever
            # the server has already granted: it issues a credit grant while
            # handling the registration when demand warrants it (a larger
            # buffer to pre-warm, or — for a resumed stream — subscribers
            # already waiting), and that grant may have arrived ahead of
            # the reply.
            pending = self._pending_demand.pop(index, 0)
            self._demand[index] = None if buffer is None else pending

        is_async = inspect.isasyncgen(generator)
        target = self._run_async if is_async else self._run
        # Capture the context of the registering thread (usually the main
        # executor thread) and run the generator body inside it, so any
        # `cf.group` / `cf.suspense` scope active at registration time
        # flows through to `cf.submit_task` and friends called from the
        # generator body. Without this the driver thread sees a fresh
        # context and would lose those settings.
        parent_context = contextvars.copy_context()
        thread = threading.Thread(
            target=lambda: parent_context.run(target, index, generator, head + 1),
            name=f"stream-{self._execution_id}-{index}",
            daemon=False,
        )
        entry = {"generator": generator, "is_async": is_async, "loop": None}
        with self._lock:
            self._generators.append(entry)
            self._threads.append(thread)
            self._by_index[index] = entry
        thread.start()

        return stream_id

    def _ensure_demand_handler_registered(self) -> None:
        if self._demand_handler_registered:
            return
        get_dispatcher().register_notification("stream_demand", self._on_stream_demand)
        self._demand_handler_registered = True

    def _ensure_force_close_handler_registered(self) -> None:
        if self._force_close_handler_registered:
            return
        get_dispatcher().register_notification(
            "stream_force_close", self._on_stream_force_close
        )
        self._force_close_handler_registered = True

    def _on_stream_demand(self, params: dict[str, Any]) -> None:
        """Server granted additional demand for one of our streams.

        The notification carries the delta (``n`` extra credits). We add
        to the per-stream counter and wake any waiter.
        """
        index = params.get("index")
        n = params.get("n", 0)
        if index is None or n <= 0:
            return
        with self._demand_cv:
            if index not in self._demand:
                # Overtook the register reply — see ``_pending_demand``.
                self._pending_demand[index] = self._pending_demand.get(index, 0) + n
                return
            current = self._demand[index]
            if current is None:
                # Unbounded — nothing to account for.
                return
            self._demand[index] = current + n
            self._demand_cv.notify_all()

    def _on_stream_force_close(self, params: dict[str, Any]) -> None:
        """CLI is telling us to stop producing for a specific stream.

        Fires when the worker's stream-timer has elapsed and it has
        already informed the server. We mark the stream force-closed so
        ``_acquire_demand`` returns False and the producer thread exits
        without sending its own ``stream_close`` (that would race the
        closure the server already recorded).

        Also closes the generator so any work it's doing (e.g., a long
        ``next()``) is interrupted at the next yield point.
        """
        index = params.get("index")
        reason = params.get("reason") or "timeout"
        if index is None:
            return
        with self._demand_cv:
            self._force_closed[index] = reason
            self._demand_cv.notify_all()
        # Close the generator off the dispatcher thread to avoid blocking
        # on a long-running next() call there.
        with self._lock:
            entry = self._by_index.get(index)
        if entry is None:
            return
        try:
            if entry["is_async"]:
                loop = entry["loop"]
                if loop is not None and not loop.is_closed():
                    gen = entry["generator"]

                    async def _close(g=gen) -> None:
                        try:
                            await g.aclose()
                        except Exception:  # noqa: BLE001, S110
                            # Best-effort close of a user generator.
                            pass

                    asyncio.run_coroutine_threadsafe(_close(), loop)
            else:
                entry["generator"].close()
        except Exception:  # noqa: BLE001, S110
            # Best-effort close of a user generator.
            pass

    def _acquire_demand(self, index: int) -> bool:
        """Wait for a credit and consume it. Returns False if closed mid-wait."""
        with self._demand_cv:
            while True:
                if self._closing or index in self._force_closed:
                    return False
                current = self._demand.get(index)
                if current is None:
                    # Unbounded stream — never waits.
                    return True
                if current > 0:
                    self._demand[index] = current - 1
                    return True
                self._demand_cv.wait()

    def _is_force_closed(self, index: int) -> bool:
        with self._demand_cv:
            return index in self._force_closed

    def _run(self, index: int, generator: Any, start_sequence: int) -> None:
        """Run one sync generator to exhaustion (or error).

        ``start_sequence`` is where numbering begins: 0 for a new stream,
        or one past the head of a stream this execution is resuming.
        """
        sequence = start_sequence
        try:
            iterator = iter(generator)
            while True:
                # Block until the server grants a credit (or the driver is
                # asked to close). For unbounded streams this returns
                # immediately without consuming any credit.
                if not self._acquire_demand(index):
                    return
                try:
                    item = next(iterator)
                except StopIteration:
                    break
                serialized = serialize_value(item)
                protocol.send_stream_append(
                    self._execution_id,
                    index,
                    sequence,
                    serialized,
                )
                sequence += 1
        except GeneratorExit:
            # Generator explicitly closed (via close_all on error path, or
            # by the force-close handler for a worker-initiated timeout).
            # Skip send_stream_close — the server either records a
            # lifecycle closure on execution-end, or has already recorded
            # the force-close reason (e.g. "timeout").
            return
        except SystemExit:
            # The generator body suspended (``cf.suspend()`` / implicit
            # suspense). The stream stays open — paused — for the
            # execution that resumes the step to continue, so nothing is
            # sent: a close here would end it for every consumer.
            return
        except BaseException as e:  # noqa: BLE001 - we propagate all
            if self._is_force_closed(index):
                # Worker already recorded the close; don't overwrite.
                return
            error_type = f"{type(e).__module__}.{type(e).__qualname__}"
            tb = traceback.format_exc()
            protocol.send_stream_close(
                self._execution_id,
                index,
                error_type=error_type,
                error_message=str(e),
                traceback=tb,
            )
        else:
            if self._is_force_closed(index):
                return
            protocol.send_stream_close(self._execution_id, index)

    def _run_async(self, index: int, generator: Any, start_sequence: int) -> None:
        """Run one async generator in a fresh event loop on this thread.

        The loop handle is recorded so ``close_all`` can schedule aclose()
        from another thread via ``run_coroutine_threadsafe``.
        """
        loop = asyncio.new_event_loop()
        self._record_loop(generator, loop)
        asyncio.set_event_loop(loop)

        async def iterate() -> None:
            sequence = start_sequence
            iterator = generator.__aiter__()
            while True:
                # The demand wait uses a threading.Condition, which would
                # block the event loop. This loop is dedicated to one
                # generator though — nothing else scheduled — so blocking
                # in-thread is harmless and simpler than bridging to an
                # asyncio primitive.
                if not self._acquire_demand(index):
                    return
                try:
                    item = await iterator.__anext__()
                except StopAsyncIteration:
                    break
                serialized = serialize_value(item)
                protocol.send_stream_append(
                    self._execution_id,
                    index,
                    sequence,
                    serialized,
                )
                sequence += 1

        try:
            loop.run_until_complete(iterate())
        except (GeneratorExit, asyncio.CancelledError):
            return
        except SystemExit:
            # Suspended from inside the generator — see ``_run``.
            return
        except BaseException as e:  # noqa: BLE001 - we propagate all
            if self._is_force_closed(index):
                return
            error_type = f"{type(e).__module__}.{type(e).__qualname__}"
            tb = traceback.format_exc()
            protocol.send_stream_close(
                self._execution_id,
                index,
                error_type=error_type,
                error_message=str(e),
                traceback=tb,
            )
        else:
            if self._is_force_closed(index):
                return
            protocol.send_stream_close(self._execution_id, index)
        finally:
            try:
                loop.run_until_complete(generator.aclose())
            except Exception:  # noqa: BLE001, S110
                # Best-effort teardown; the close was already reported.
                pass
            try:
                loop.close()
            except Exception:  # noqa: BLE001, S110
                # Best-effort teardown; the close was already reported.
                pass

    def _record_loop(self, generator: Any, loop: asyncio.AbstractEventLoop) -> None:
        with self._lock:
            for entry in self._generators:
                if entry["generator"] is generator:
                    entry["loop"] = loop
                    return

    def wait_all(self) -> None:
        """Block until every worker thread has finished."""
        with self._lock:
            threads = list(self._threads)
        for t in threads:
            t.join()

    def close_all(self) -> None:
        """Close every registered generator so worker threads exit promptly.

        Used on the error path: when the task body raises, we want in-flight
        streams to stop producing rather than racing the execution_error
        notification. For sync generators, ``generator.close()`` raises
        ``GeneratorExit`` at the current yield point. For async generators,
        we schedule ``aclose()`` onto the generator's own event loop so the
        awaiting coroutine is cancelled cleanly.

        We also flip a closing flag and broadcast on the demand condition
        so drivers parked in ``_acquire_demand`` (blocked for credits that
        will never arrive) wake and exit.
        """
        with self._demand_cv:
            self._closing = True
            self._demand_cv.notify_all()

        with self._lock:
            entries = list(self._generators)
        for entry in entries:
            try:
                if entry["is_async"]:
                    loop = entry["loop"]
                    if loop is not None and not loop.is_closed():
                        gen = entry["generator"]

                        async def _close(g=gen) -> None:
                            try:
                                await g.aclose()
                            except Exception:  # noqa: BLE001, S110
                                # Best-effort close of a user generator.
                                pass

                        asyncio.run_coroutine_threadsafe(_close(), loop)
                else:
                    entry["generator"].close()
            except Exception:  # noqa: BLE001, S110
                # Best-effort close of a user generator.
                pass


# --- Consumer side ---


# Sentinel pushed onto a subscriber's queue to signal close. `reason`
# is the semantic close reason (``"complete"`` / ``"errored"`` /
# ``"cancelled"`` / ``"abandoned"`` / ``"crashed"`` / ``"timeout"`` /
# ``"not_found"``). ``error`` is only populated when ``reason ==
# "errored"`` — it's the producer's actual ``{type, message, frames}``.
class _Closed:
    __slots__ = ("error", "reason")

    def __init__(self, reason: str, error: dict[str, Any] | None) -> None:
        self.reason = reason
        self.error = error


# Delivery window for a consumer subscription: the server won't push
# more than this many items beyond what we've acknowledged. This is what
# bounds the iterator's queue, and it's deliberately independent of the
# producer's ``buffer`` — the item log is durable, so the server can hold
# items back without anything being lost. A producer allowed to run far
# ahead doesn't oblige its consumers to buffer that far ahead in memory.
_PREFETCH = 16

# Report progress once this many items have been retired, rather than one
# message per item. Progress is also flushed whenever we're about to
# block, so batching can't stall a producer that's waiting on us.
_ACK_BATCH = max(1, _PREFETCH // 2)


class _Subscription:
    """Bookkeeping shared by the sync and async subscription readers.

    Everything here is independent of how items reach the caller: the
    acknowledgement accounting that drives the producer's backpressure,
    and the release path that hands the subscription back to the server.
    Only delivery differs between the two readers — a ``queue.Queue`` fed
    from the dispatcher thread for the sync one, an ``asyncio.Queue``
    filled via the consumer's event loop for the async one.
    """

    def __init__(self, subscription_id: int, execution_id: str) -> None:
        self._subscription_id = subscription_id
        self._execution_id = execution_id
        self._done = False
        # Cumulative progress, as reported to the server.
        self._acked_count = 0
        self._acked_sequence = -1
        # Sequence of the item handed to the caller by the previous
        # ``__next__`` and not yet retired. It only counts as processed
        # once the caller comes back for another one.
        self._in_hand: int | None = None
        # Retired items not yet reported.
        self._unreported = 0

    def on_items(self, items: list[list[Any]]) -> None:
        """Called by the registry when the server pushes items for this
        subscription. ``items`` is a list of ``[sequence, value_wire]``.

        Runs on the dispatcher reader thread — keep it cheap. The raw wire
        value is enqueued unmodified; deserialization happens on the
        consumer's side so heavy decode work doesn't stall stdin reads.
        """
        raise NotImplementedError

    def on_closed(self, reason: str, error: dict[str, Any] | None) -> None:
        """Called by the registry when the stream closes."""
        raise NotImplementedError

    def close(self) -> None:
        """Release the subscription. Idempotent.

        Mirrors ``generator.close()``: a consumer that stops iterating
        early — ``break``, an exception in the loop body, or just dropping
        the last reference — has to tell the server. Otherwise the
        subscription lives on until the whole consumer execution
        terminates (``drop_execution_subscriptions``, server-side), which
        is far later.

        That's not merely a leak. An abandoned subscription's
        acknowledged position stops advancing, and the producer's
        ``buffer`` is measured against the *slowest* subscriber, so
        walking away from a bounded stream pins its producer until the
        idle timeout fires — or forever, if no timeout was configured.

        Pending acks are deliberately not flushed first: unsubscribing
        removes this subscriber from the watermark entirely, which
        releases the producer at least as much as any ack would.
        """
        if self._done:
            return
        self._done = True
        self._in_hand = None
        self._unreported = 0
        _stream_registry().drop(self._subscription_id)
        # Skip the unsubscribe roundtrip when the dispatcher is gone —
        # stdout may still be writable but there's no one to receive it,
        # and a closed pipe would raise from send_*.
        if get_dispatcher().is_closed():
            return
        try:
            protocol.send_stream_unsubscribe(self._execution_id, self._subscription_id)
        except Exception:  # noqa: BLE001, S110
            # The pipe may be tearing down underneath us; nothing to do.
            pass

    def __del__(self) -> None:
        # The registry holds iterators weakly, so this is the hook that
        # makes an abandoned `for` loop behave like an abandoned
        # generator. Best-effort: interpreter shutdown may already have
        # torn down the globals `close` reaches for, and `__del__` must
        # never raise.
        try:
            self.close()
        except Exception:  # noqa: BLE001, S110
            # `__del__` must never raise.
            pass

    def _retire_in_hand(self) -> None:
        """Count the previously-yielded item as processed.

        Done on re-entry to ``__next__`` rather than when the item was
        handed over, so the acknowledgement reflects the loop body having
        actually run. That's what makes ``buffer=0`` genuine lockstep
        instead of merely bounding what's been put on the wire.
        """
        if self._in_hand is None:
            return
        self._acked_count += 1
        self._acked_sequence = max(self._acked_sequence, self._in_hand)
        self._in_hand = None
        self._unreported += 1
        if self._unreported >= _ACK_BATCH:
            self._flush_ack()

    def _flush_ack(self) -> None:
        if self._unreported == 0:
            return
        # Skip the roundtrip when the dispatcher is gone — stdout may still
        # be writable but there's no one to receive it. Terminal, so clear
        # the counter: no later flush could succeed either.
        if get_dispatcher().is_closed():
            self._unreported = 0
            return
        try:
            protocol.send_stream_ack(
                self._execution_id,
                self._subscription_id,
                self._acked_count,
                self._acked_sequence,
            )
        except Exception:  # noqa: BLE001
            # Leave `_unreported` set so the next flush retries. The
            # counters are cumulative, so a re-send subsumes this one.
            # Clearing it before the send would strand a lockstep producer
            # waiting on exactly the acknowledgement we just dropped,
            # with nothing left to trigger another flush.
            return
        self._unreported = 0


@final
class _StreamIterator(_Subscription, Iterator[Any]):
    """Drains items for one active subscription, synchronously.

    Delivery is credit-gated: the server sends at most ``_PREFETCH``
    items beyond what we've acknowledged, so the queue is bounded even
    if the consumer is much slower than the producer.
    """

    def __init__(self, subscription_id: int, execution_id: str) -> None:
        super().__init__(subscription_id, execution_id)
        self._queue: queue.Queue[Any] = queue.Queue()

    def on_items(self, items: list[list[Any]]) -> None:
        for sequence, value in items:
            self._queue.put((sequence, value))

    def on_closed(self, reason: str, error: dict[str, Any] | None) -> None:
        self._queue.put(_Closed(reason, error))

    def __iter__(self) -> _StreamIterator:
        return self

    def __enter__(self) -> _StreamIterator:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    def __next__(self) -> Any:
        if self._done:
            raise StopIteration

        self._retire_in_hand()

        try:
            item = self._queue.get_nowait()
        except queue.Empty:
            # About to block. Report progress first — with a small buffer
            # the producer may be waiting on precisely the acknowledgement
            # we're batching, so holding it back would deadlock.
            self._flush_ack()
            item = self._queue.get()

        if isinstance(item, _Closed):
            # Same release path as an early exit — the server has already
            # dropped its side, but the unsubscribe is harmless and keeps
            # the two paths from drifting apart.
            self.close()
            raise_for_close(item.reason, item.error)
            raise StopIteration

        sequence, value = item
        self._in_hand = sequence
        return deserialize_value(value)


@final
class _AsyncStreamIterator(_Subscription):
    """Drains items for one active subscription, asynchronously.

    Same accounting as ``_StreamIterator`` — the difference is where the
    caller waits. Items arrive on the dispatcher's reader thread and are
    handed to the consumer's event loop with ``call_soon_threadsafe``,
    so ``__anext__`` awaits an ``asyncio.Queue`` instead of blocking a
    thread. That keeps the loop free to run other tasks between items,
    and makes cancellation clean: a cancelled ``__anext__`` leaves the
    item sitting in the queue for the next reader rather than consuming
    and dropping it.

    The loop is captured when the iterator is created, which is inside
    the ``async for`` that opened the subscription.
    """

    def __init__(self, subscription_id: int, execution_id: str) -> None:
        super().__init__(subscription_id, execution_id)
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(
                "iterating a stream with `async for` needs a running event loop"
                " — use a plain `for` outside async code"
            ) from None
        self._queue: asyncio.Queue[Any] = asyncio.Queue()

    def _put(self, item: Any) -> None:
        """Hand an item to the consumer's loop.

        Called on the dispatcher thread. The loop may already be closed —
        an execution that finished while items were in flight, or the
        dispatcher-EOF wake-up arriving after the consumer's loop shut
        down — in which case there's no one left to deliver to.
        """
        try:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, item)
        except RuntimeError:
            pass

    def on_items(self, items: list[list[Any]]) -> None:
        for sequence, value in items:
            self._put((sequence, value))

    def on_closed(self, reason: str, error: dict[str, Any] | None) -> None:
        self._put(_Closed(reason, error))

    def __aiter__(self) -> _AsyncStreamIterator:
        return self

    async def aclose(self) -> None:
        """Release the subscription. Idempotent.

        A coroutine to mirror ``agen.aclose()``, though the work itself is
        synchronous — releasing is a single protocol write.
        """
        self.close()

    async def __aenter__(self) -> _AsyncStreamIterator:
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        self.close()

    async def __anext__(self) -> Any:
        if self._done:
            raise StopAsyncIteration

        self._retire_in_hand()

        if self._queue.empty():
            # About to suspend. Report progress first, for the same reason
            # the sync path does: a lockstep producer may be waiting on
            # precisely the acknowledgement we're batching.
            self._flush_ack()
        item = await self._queue.get()

        if isinstance(item, _Closed):
            self.close()
            raise_for_close(item.reason, item.error)
            raise StopAsyncIteration

        sequence, value = item
        self._in_hand = sequence
        return deserialize_value(value)


class StreamRegistry:
    """Per-process registry of open consumer subscriptions."""

    def __init__(self) -> None:
        # Reentrant because finalization can re-enter: the cyclic collector
        # may finalize an abandoned iterator at an arbitrary allocation
        # point, including one inside a method that already holds this
        # lock, and `__del__` -> `close` -> `drop` wants it again. A plain
        # Lock would deadlock that thread against itself.
        self._lock = threading.RLock()
        self._next_id = 0
        # Weak values, so that dropping the last user-held reference to an
        # iterator finalizes it and `_StreamIterator.__del__` can release
        # the subscription — the same way an abandoned generator gets
        # closed. A strong map here would keep every iterator alive for the
        # life of the execution and defeat that entirely.
        #
        # Nothing is lost by holding them weakly: an iterator with no
        # remaining reference is by definition one no consumer can read
        # from again.
        self._iterators: weakref.WeakValueDictionary[int, _Subscription] = (
            weakref.WeakValueDictionary()
        )
        self._installed = False

    def _ensure_installed(self) -> None:
        # Register dispatcher handlers on first use. Deferred so importing
        # this module is free until a task actually iterates a stream.
        # Locked so two consumer threads first-iterating a stream at the
        # same time don't both register handlers — the dispatcher would
        # silently replace the first, but registering `add_close_callback`
        # twice would fire the close-handling twice on EOF.
        with self._lock:
            if self._installed:
                return
            d = get_dispatcher()
            d.register_notification("stream_items", self._on_items)
            d.register_notification("stream_closed", self._on_closed)
            # If stdin goes away before the server sends close messages,
            # blocked iterators would hang on their queues forever. Push
            # a synthetic closed sentinel into each so ``__next__`` raises.
            d.add_close_callback(self._on_dispatcher_closed)
            self._installed = True

    def _on_dispatcher_closed(self) -> None:
        """Wake all active iterators — connection to the server is gone
        so no close message is going to arrive. Treat as ``abandoned``
        (we don't know anything more specific from this side)."""
        with self._lock:
            iterators = list(self._iterators.values())
        for it in iterators:
            it.on_closed("abandoned", None)

    def allocate(
        self,
        execution_id: str,
        factory: type[_Subscription] = _StreamIterator,
    ) -> tuple[int, Any]:
        """Claim a subscription id and reader.

        ``factory`` selects the sync or async reader; both are registered
        here identically, so the dispatcher routes to either without
        caring which kind it is.
        """
        self._ensure_installed()
        with self._lock:
            subscription_id = self._next_id
            self._next_id += 1
            it = factory(subscription_id, execution_id)
            self._iterators[subscription_id] = it
        return subscription_id, it

    def drop(self, subscription_id: int) -> None:
        with self._lock:
            self._iterators.pop(subscription_id, None)

    def _on_items(self, params: dict[str, Any]) -> None:
        subscription_id = params.get("subscription_id")
        items = params.get("items") or []
        with self._lock:
            it = self._iterators.get(subscription_id)
        if it is not None:
            it.on_items(items)

    def _on_closed(self, params: dict[str, Any]) -> None:
        subscription_id = params.get("subscription_id")
        reason = params.get("reason") or "complete"
        error = params.get("error")
        with self._lock:
            it = self._iterators.get(subscription_id)
        if it is not None:
            it.on_closed(reason, error)


_registry_instance: StreamRegistry | None = None


def _stream_registry() -> StreamRegistry:
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = StreamRegistry()
    return _registry_instance


def _open_subscription(
    stream_id: str,
    stride: tuple[int, int | None, int],
    factory: type[_Subscription],
) -> Any:
    """Allocate a subscription and tell the server about it.

    ``stride`` is a ``(start, stop, step)`` tuple — any chain of
    slice/partition/stride calls on the handle collapses to a single
    stride before this point. The wire message is the same whichever
    reader the caller asked for; only local delivery differs. The stream
    id is opaque here — the server resolves it.
    """
    ctx = get_context()
    execution_id = ctx.execution_id
    subscription_id, iterator = _stream_registry().allocate(execution_id, factory)

    start, stop, step = stride
    wire_stride = {"start": start, "stop": stop, "step": step}

    protocol.send_stream_subscribe(
        execution_id,
        subscription_id,
        stream_id,
        0,
        _PREFETCH,
        stride=wire_stride,
    )
    return iterator


def open_subscription(
    stream_id: str,
    stride: tuple[int, int | None, int],
) -> _StreamIterator:
    """Begin iterating a stream. Called by ``Stream.__iter__``.

    Returns an iterator that yields as items arrive, blocking the calling
    thread between them.
    """
    return _open_subscription(stream_id, stride, _StreamIterator)


def open_async_subscription(
    stream_id: str,
    stride: tuple[int, int | None, int],
) -> _AsyncStreamIterator:
    """Begin iterating a stream. Called by ``Stream.__aiter__``.

    Returns an async iterator that yields as items arrive, suspending the
    calling task rather than blocking its thread. Must be called with a
    running event loop — the loop it binds to is the one that will be
    woken when items land.
    """
    return _open_subscription(stream_id, stride, _AsyncStreamIterator)
