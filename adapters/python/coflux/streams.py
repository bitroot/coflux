"""Producer and consumer stream plumbing.

The producer side owns ``StreamDriver``: each execution whose return value
(or submitted arguments) contains generators uses one to drive each
generator in a background thread.

The consumer side owns a module-level ``StreamRegistry``: open consumer
subscriptions are keyed by subscription id. The registry's dispatcher
handlers (``stream_items``/``stream_closed``) route incoming pushes from
the server to the right iterator's queue, which yields as the user
iterates.

Both sides are thread-safe: the ``Dispatcher`` owns stdin (so subtask
calls from generator bodies don't race), and stdout writes go through
``Protocol._write_lock``.
"""

from __future__ import annotations

import queue
import threading
import traceback
from typing import Any, Iterator

from . import protocol
from .dispatcher import get_dispatcher
from .errors import create_execution_error
from .serialization import deserialize_value, serialize_value
from .state import get_context


# --- Producer side ---


class StreamDriver:
    """Manages streams produced by a single execution."""

    def __init__(self, execution_id: str) -> None:
        self._execution_id = execution_id
        self._next_sequence = 0
        self._threads: list[threading.Thread] = []
        self._lock = threading.Lock()

    def register(self, generator: Any) -> tuple[str, int]:
        """Register a generator, spawn its driver thread.

        Returns ``(execution_id, sequence)`` for embedding in the serialized
        value as a stream reference.
        """
        with self._lock:
            sequence = self._next_sequence
            self._next_sequence += 1

        protocol.send_stream_register(self._execution_id, sequence)

        thread = threading.Thread(
            target=self._drive,
            args=(sequence, generator),
            name=f"stream-{self._execution_id}-{sequence}",
            daemon=False,
        )
        thread.start()
        self._threads.append(thread)

        return self._execution_id, sequence

    def _drive(self, sequence: int, generator: Any) -> None:
        """Pump one generator to exhaustion (or error)."""
        position = 0
        try:
            for item in generator:
                serialized = serialize_value(item)
                protocol.send_stream_append(
                    self._execution_id,
                    sequence,
                    position,
                    serialized,
                )
                position += 1
        except GeneratorExit:
            # Generator explicitly closed (e.g. execution cancelled). The
            # server already knows — no close message needed.
            return
        except BaseException as e:  # noqa: BLE001 - we propagate all
            error_type = f"{type(e).__module__}.{type(e).__qualname__}"
            tb = traceback.format_exc()
            protocol.send_stream_close(
                self._execution_id,
                sequence,
                error_type=error_type,
                error_message=str(e),
                traceback=tb,
            )
        else:
            protocol.send_stream_close(self._execution_id, sequence)

    def wait_all(self) -> None:
        """Block until every driver thread has finished."""
        with self._lock:
            threads = list(self._threads)
        for t in threads:
            t.join()


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
        subscription. ``items`` is a list of ``[position, value_wire]``.
        """
        for _position, value in items:
            # Decode eagerly so iteration cost is paid per-item as it arrives.
            self._queue.put(deserialize_value(value))

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
            protocol.send_stream_unsubscribe(self._execution_id, self._subscription_id)
            if item.error is not None:
                raise create_execution_error(
                    item.error.get("type", ""),
                    item.error.get("message", ""),
                )
            raise StopIteration
        return item


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
        self._installed = True

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


def open_subscription(
    producer_execution_id: str,
    sequence: int,
    filters: tuple[dict[str, Any], ...],
) -> Iterator[Any]:
    """Begin iterating a stream. Called by ``Stream.__iter__``.

    Allocates a subscription id, sends the subscribe message, and returns
    an iterator that yields as items arrive.
    """
    ctx = get_context()
    execution_id = ctx.execution_id
    subscription_id, iterator = _stream_registry().allocate(execution_id)

    filter = _compose_filter(filters)
    protocol.send_stream_subscribe(
        execution_id,
        subscription_id,
        producer_execution_id,
        sequence,
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
