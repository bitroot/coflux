"""Producer and consumer stream plumbing.

The producer side owns ``StreamDriver``: each execution whose return value
(or submitted arguments) contains generators uses one to run each
generator in a background thread. Both sync (``def`` + ``yield``) and
async (``async def`` + ``yield``) generators are supported; async
generators get a fresh event loop confined to their worker thread.

The consumer side owns a module-level ``StreamRegistry``: open consumer
subscriptions are keyed by subscription id. The registry's dispatcher
handlers (``stream_items``/``stream_closed``) route incoming pushes from
the server to the right iterator's queue, which yields as the user
iterates. On dispatcher EOF every active iterator is woken with a
synthetic abandoned-close so user code doesn't hang forever.

Both sides are thread-safe: the ``Dispatcher`` owns stdin (so subtask
calls from generator bodies don't race), and stdout writes go through
``Protocol._write_lock``.
"""

from __future__ import annotations

import asyncio
import inspect
import queue
import threading
import traceback
from typing import Any, Iterator

from . import protocol
from .dispatcher import get_dispatcher
from .errors import create_stream_error
from .serialization import deserialize_value, serialize_value
from .state import get_context


# --- Producer side ---


def stream(generator: Any, *, buffer: int | None = 0) -> Any:
    """Register a generator as a Coflux stream and return a handle.

    Use this when a task returns multiple streams or needs a buffer size
    different from the task default. For the common case where a task
    body is itself a generator, ``@cf.task(buffer=N)`` handles the
    registration automatically — you don't need to call ``cf.stream``
    explicitly.

    Registration happens at call time: the driver thread starts, the
    server is told about the stream, and any later serialisation sees a
    regular ``Stream`` handle. That means ``cf.stream`` must be called
    inside a task or workflow body (where an execution context is
    active); calling it from module scope or outside a task raises.

    Args:
        generator: A sync or async generator. Other iterables aren't
            accepted — wrapping a list in ``cf.stream`` doesn't make
            sense; pass it as a value directly.
        buffer: Backpressure budget. ``0`` (the default) means strict
            lockstep — the producer emits an item, waits for a consumer
            to acknowledge it, then emits the next. ``N`` allows the
            producer to stay up to ``N`` items ahead of the fastest
            consumer. ``None`` disables backpressure entirely.

    Returns:
        A ``Stream`` handle referencing the newly registered stream.
        It serialises as ``{"type": "stream", "id": ...}`` and is
        iterable by downstream tasks.
    """
    if not (inspect.isgenerator(generator) or inspect.isasyncgen(generator)):
        raise TypeError(
            f"cf.stream expects a generator, got {type(generator).__name__}"
        )
    if buffer is not None and buffer < 0:
        raise ValueError(f"buffer must be non-negative or None, got {buffer}")
    ctx = get_context()
    stream_id = ctx.register_stream(generator, buffer)
    # Local import to avoid a top-level cycle — models imports nothing
    # from streams but streams already imports from models at top.
    from .models import Stream as StreamHandle

    return StreamHandle(stream_id)


class StreamDriver:
    """Manages streams produced by a single execution."""

    def __init__(self, execution_id: str) -> None:
        self._execution_id = execution_id
        self._next_index = 0
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
        self._closing = False
        self._demand_handler_registered = False

    def register(self, generator: Any, buffer: int | None) -> str:
        """Register a generator and start running it in a worker thread.

        Accepts both sync generators (``def`` + ``yield``) and async
        generators (``async def`` + ``yield``). Each gets its own thread;
        async generators run inside a fresh event loop confined to that
        thread.

        ``buffer`` is the producer-side backpressure budget. ``None``
        means unbounded (no flow control); ``0`` means strict lockstep
        (producer waits for a consumer to ack each item before emitting
        the next); ``N>0`` allows the producer to stay up to N items
        ahead of the fastest consumer.

        Returns the stream's opaque ``id`` (``<execution_id>_<index>``)
        for embedding in the serialized value as a stream reference.
        """
        self._ensure_demand_handler_registered()

        with self._lock:
            index = self._next_index
            self._next_index += 1

        with self._demand_cv:
            # Unbounded ⇒ driver never waits. Bounded ⇒ starts at 0; the
            # server issues a credit grant once demand calculation warrants
            # it (or on first consumer subscribing).
            self._demand[index] = None if buffer is None else 0

        protocol.send_stream_register(self._execution_id, index, buffer=buffer)

        is_async = inspect.isasyncgen(generator)
        target = self._run_async if is_async else self._run
        thread = threading.Thread(
            target=target,
            args=(index, generator),
            name=f"stream-{self._execution_id}-{index}",
            daemon=False,
        )
        entry = {"generator": generator, "is_async": is_async, "loop": None}
        with self._lock:
            self._generators.append(entry)
            self._threads.append(thread)
        thread.start()

        return compose_stream_id(self._execution_id, index)

    def _ensure_demand_handler_registered(self) -> None:
        if self._demand_handler_registered:
            return
        get_dispatcher().register_notification("stream_demand", self._on_stream_demand)
        self._demand_handler_registered = True

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
            current = self._demand.get(index)
            if current is None:
                # Unbounded — nothing to account for.
                return
            self._demand[index] = current + n
            self._demand_cv.notify_all()

    def _acquire_demand(self, index: int) -> bool:
        """Wait for a credit and consume it. Returns False if closed mid-wait."""
        with self._demand_cv:
            while True:
                if self._closing:
                    return False
                current = self._demand.get(index)
                if current is None:
                    # Unbounded stream — never waits.
                    return True
                if current > 0:
                    self._demand[index] = current - 1
                    return True
                self._demand_cv.wait()

    def _run(self, index: int, generator: Any) -> None:
        """Run one sync generator to exhaustion (or error)."""
        sequence = 0
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
            # server-initiated cancel). Skip send_stream_close — the server
            # records a lifecycle closure when the execution terminates and
            # derives the error from the execution's outcome.
            return
        except BaseException as e:  # noqa: BLE001 - we propagate all
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
            protocol.send_stream_close(self._execution_id, index)

    def _run_async(self, index: int, generator: Any) -> None:
        """Run one async generator in a fresh event loop on this thread.

        The loop handle is recorded so ``close_all`` can schedule aclose()
        from another thread via ``run_coroutine_threadsafe``.
        """
        loop = asyncio.new_event_loop()
        self._record_loop(generator, loop)
        asyncio.set_event_loop(loop)

        async def iterate() -> None:
            sequence = 0
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
        except BaseException as e:  # noqa: BLE001 - we propagate all
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
            protocol.send_stream_close(self._execution_id, index)
        finally:
            try:
                loop.run_until_complete(generator.aclose())
            except Exception:
                pass
            try:
                loop.close()
            except Exception:
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
                            except Exception:
                                pass

                        asyncio.run_coroutine_threadsafe(_close(), loop)
                else:
                    entry["generator"].close()
            except Exception:
                pass


# --- Consumer side ---


# Sentinel pushed onto a subscriber's queue to signal close. Carries the
# optional error dict ({"type": str, "message": str} or None).
class _Closed:
    __slots__ = ("error",)

    def __init__(self, error: dict[str, Any] | None) -> None:
        self.error = error


class _StreamIterator(Iterator[Any]):
    """Drains items for one active subscription via a bounded-free queue."""

    def __init__(self, subscription_id: int, execution_id: str) -> None:
        self._subscription_id = subscription_id
        self._execution_id = execution_id
        self._queue: queue.Queue[Any] = queue.Queue()
        self._done = False

    def on_items(self, items: list[list[Any]]) -> None:
        """Called by the registry when the server pushes items for this
        subscription. ``items`` is a list of ``[sequence, value_wire]``.

        Runs on the dispatcher reader thread — keep it cheap. The raw wire
        value goes onto the queue unmodified; deserialization happens in
        ``__next__`` on the consumer's thread so heavy decode work doesn't
        stall stdin reads.
        """
        for _sequence, value in items:
            self._queue.put(value)

    def on_closed(self, error: dict[str, Any] | None) -> None:
        """Called by the registry when the stream closes."""
        self._queue.put(_Closed(error))

    def __iter__(self) -> "_StreamIterator":
        return self

    def __next__(self) -> Any:
        if self._done:
            raise StopIteration
        item = self._queue.get()
        if isinstance(item, _Closed):
            self._done = True
            _stream_registry().drop(self._subscription_id)
            # Skip the unsubscribe roundtrip when the dispatcher is gone —
            # stdout may still be writable but there's no one to receive it,
            # and a closed pipe would raise from send_*.
            if not get_dispatcher().is_closed():
                try:
                    protocol.send_stream_unsubscribe(
                        self._execution_id, self._subscription_id
                    )
                except Exception:
                    pass
            if item.error is not None:
                raise create_stream_error(item.error)
            raise StopIteration
        return deserialize_value(item)


class StreamRegistry:
    """Per-process registry of open consumer subscriptions."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next_id = 0
        self._iterators: dict[int, _StreamIterator] = {}
        self._installed = False

    def _ensure_installed(self) -> None:
        # Register dispatcher handlers on first use. Deferred so importing
        # this module is free until a task actually iterates a stream.
        if self._installed:
            return
        d = get_dispatcher()
        d.register_notification("stream_items", self._on_items)
        d.register_notification("stream_closed", self._on_closed)
        # If stdin goes away before the server sends close messages,
        # blocked iterators would hang on their queues forever. Push a
        # synthetic closed sentinel into each so ``__next__`` raises.
        d.add_close_callback(self._on_dispatcher_closed)
        self._installed = True

    def _on_dispatcher_closed(self) -> None:
        """Wake all active iterators with a connection-closed error."""
        error = {
            "type": "Coflux.ExecutionAbandoned",
            "message": "connection closed",
            "frames": [],
        }
        with self._lock:
            iterators = list(self._iterators.values())
        for it in iterators:
            it.on_closed(error)

    def allocate(self, execution_id: str) -> tuple[int, _StreamIterator]:
        """Claim a subscription id and iterator."""
        self._ensure_installed()
        with self._lock:
            subscription_id = self._next_id
            self._next_id += 1
            it = _StreamIterator(subscription_id, execution_id)
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
        error = params.get("error")
        with self._lock:
            it = self._iterators.get(subscription_id)
        if it is not None:
            it.on_closed(error)


_registry_instance: StreamRegistry | None = None


def _stream_registry() -> StreamRegistry:
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = StreamRegistry()
    return _registry_instance


def compose_stream_id(execution_id: str, index: int) -> str:
    """Build the opaque stream id from its two components.

    Joined with ``_`` because the alternatives are overloaded: ``:`` is
    used inside the execution id, ``#`` is used for attempt numbers, ``/``
    separates module/target. Execution ids use only alphanumerics, so
    ``rpartition('_')`` is unambiguous on the parse side.
    """
    return f"{execution_id}_{index}"


def parse_stream_id(id: str) -> tuple[str, int]:
    """Reverse of ``compose_stream_id``. Raises ValueError on bad input."""
    exec_id, sep, index = id.rpartition("_")
    if not sep or not exec_id:
        raise ValueError(f"invalid stream id: {id!r}")
    return exec_id, int(index)


def open_subscription(
    stream_id: str,
    filters: tuple[dict[str, Any], ...],
) -> Iterator[Any]:
    """Begin iterating a stream. Called by ``Stream.__iter__``.

    Allocates a subscription id, sends the subscribe message, and returns
    an iterator that yields as items arrive.
    """
    ctx = get_context()
    execution_id = ctx.execution_id
    subscription_id, iterator = _stream_registry().allocate(execution_id)

    # Split the opaque id for the wire message, which still takes
    # producer_execution_id + index positionally.
    producer_execution_id, index = parse_stream_id(stream_id)

    filter = _compose_filter(filters)
    protocol.send_stream_subscribe(
        execution_id,
        subscription_id,
        producer_execution_id,
        index,
        0,
        filter,
    )
    return iterator


def _compose_filter(
    filters: tuple[dict[str, Any], ...],
) -> dict[str, Any] | None:
    """Collapse a list of filters for the wire.

    Empty → null. Single → pass through. Many → wrap in {"type": "chain"}.
    """
    if not filters:
        return None
    if len(filters) == 1:
        return filters[0]
    return {"type": "chain", "filters": list(filters)}
