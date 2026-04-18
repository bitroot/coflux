"""Message dispatcher for concurrent stdio access.

The CLI can send messages to the adapter at any time:

  * **Responses** — `{"id": N, "result": ...}` or `{"id": N, "error": ...}`
    replying to an earlier request.
  * **Notifications** — `{"method": "X", "params": {...}}` pushed without a
    prior request (e.g. `stream_produce_until` flow-control signals for
    producers, `stream_items` pushes for consumers).

Multiple threads in the adapter may be waiting on different responses
simultaneously (e.g. each generator driver thread calling a Coflux subtask).
Reading stdin from multiple threads would corrupt the protocol, so one
dedicated reader thread owns stdin and dispatches what it reads:

  * Responses are routed to the specific waiter by request id.
  * Notifications are routed by method name to registered handlers.

Writers are separately protected by ``Protocol._write_lock``.

Lifecycle: ``start()`` spawns the reader thread (daemon — dies with the
process on EOF). ``wait_for_response`` and ``wait_closed`` are the
blocking APIs used by the rest of the adapter.
"""

from __future__ import annotations

import threading
from typing import Any, Callable

from .protocol import Protocol


class Dispatcher:
    """Owns stdin; routes responses by request id, notifications by method."""

    def __init__(self, protocol: Protocol) -> None:
        self._protocol = protocol
        self._lock = threading.Lock()

        # request_id → (Event, [response_msg | None])
        # The list is a one-slot mutable box because closures can't rebind.
        self._waiting: dict[int, tuple[threading.Event, list[Any]]] = {}

        # Responses that arrived before their waiter registered — rare but
        # possible if the writer thread and reader thread interleave.
        self._early_responses: dict[int, dict[str, Any]] = {}

        # method → handler(params). Handlers run on the reader thread; keep
        # them fast and non-blocking (delegate heavy work to queues or
        # other threads).
        self._notification_handlers: dict[str, Callable[[dict[str, Any]], None]] = {}

        # Set when stdin reaches EOF. Wakes all pending waiters.
        self._closed = threading.Event()

        self._thread = threading.Thread(
            target=self._run,
            name="coflux-dispatcher",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def wait_closed(self) -> None:
        """Block until stdin closes (server disconnected / aborted us)."""
        self._closed.wait()

    def register_notification(
        self,
        method: str,
        handler: Callable[[dict[str, Any]], None],
    ) -> None:
        """Register a handler for an incoming notification method.

        Handlers run on the reader thread — they must not block. For any
        real work, enqueue to another thread.
        """
        with self._lock:
            self._notification_handlers[method] = handler

    def unregister_notification(self, method: str) -> None:
        with self._lock:
            self._notification_handlers.pop(method, None)

    def wait_for_response(
        self,
        request_id: int,
        timeout: float | None = None,
    ) -> dict[str, Any] | None:
        """Block until the response for ``request_id`` arrives.

        Returns the raw response dict (``{"id": ..., "result": ...}`` or
        ``{"id": ..., "error": ...}``). Returns ``None`` if the wait times
        out. Raises ``RuntimeError`` if the connection closes before the
        response arrives.
        """
        event = threading.Event()
        slot: list[Any] = [None]

        with self._lock:
            if request_id in self._early_responses:
                return self._early_responses.pop(request_id)
            if self._closed.is_set():
                raise RuntimeError("Connection closed while waiting for response")
            self._waiting[request_id] = (event, slot)

        try:
            ready = event.wait(timeout) if timeout is not None else event.wait()
        finally:
            with self._lock:
                self._waiting.pop(request_id, None)

        if not ready:
            return None
        if slot[0] is None:
            # Woken by EOF rather than a real response.
            raise RuntimeError("Connection closed while waiting for response")
        return slot[0]

    def _run(self) -> None:
        while True:
            msg = self._protocol.receive()
            if msg is None:
                # EOF — wake all waiters with a null slot; they'll raise.
                self._closed.set()
                with self._lock:
                    for event, _slot in self._waiting.values():
                        event.set()
                return

            if "id" in msg:
                request_id = msg["id"]
                with self._lock:
                    entry = self._waiting.get(request_id)
                    if entry is not None:
                        _event, slot = entry
                        slot[0] = msg
                        _event.set()
                    else:
                        # Buffer for a waiter that registers later.
                        self._early_responses[request_id] = msg
            elif "method" in msg:
                with self._lock:
                    handler = self._notification_handlers.get(msg["method"])
                if handler is not None:
                    try:
                        handler(msg.get("params", {}))
                    except Exception:  # noqa: BLE001
                        # Don't let a handler fault kill the dispatcher.
                        # Adapter-side logging hooks into protocol anyway,
                        # but we swallow here rather than taking the loop down.
                        pass
            # Silently ignore malformed messages rather than killing the
            # reader — log once per session if we want to be strict.


# Module-level singleton, mirroring how Protocol is handled.
_dispatcher: Dispatcher | None = None


def get_dispatcher() -> Dispatcher:
    """Return the active dispatcher. Raises if ``start_dispatcher`` hasn't run."""
    if _dispatcher is None:
        raise RuntimeError("Dispatcher hasn't been started")
    return _dispatcher


def start_dispatcher(protocol: Protocol) -> Dispatcher:
    """Create and start the dispatcher. Idempotent."""
    global _dispatcher
    if _dispatcher is None:
        _dispatcher = Dispatcher(protocol)
        _dispatcher.start()
    return _dispatcher
