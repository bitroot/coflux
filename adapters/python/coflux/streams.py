"""Producer-side stream management.

Each execution that returns a value containing generators spins up a
``StreamDriver``. The driver:

    * Assigns monotonic sequence numbers to generators in encounter order.
    * Registers each stream with the server (``stream_register``) at
      encoding time so consumers can subscribe as soon as the result is
      visible.
    * Spawns one background thread per generator — a slow generator
      doesn't block sibling streams.
    * Joins all driver threads before the executor exits, so the process
      stays alive until every stream has drained naturally.

Threading is safe here because the ``Dispatcher`` owns stdin: any
subtask call from a generator body gets its response routed back to the
right driver thread. Writes to stdout go through ``Protocol._write_lock``.
"""

from __future__ import annotations

import threading
import traceback
from typing import Any

from . import protocol
from .serialization import serialize_value


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
