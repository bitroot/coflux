"""Log store abstraction for writing logs to external storage.

This module provides a pluggable interface for storing log messages,
with an HTTP implementation that POSTs logs to the Coflux server's
/logs endpoint.
"""

import abc
import threading
import time
import typing as t
from dataclasses import dataclass
from queue import Empty, Queue

import httpx

from . import config


@dataclass
class LogEntry:
    """A log entry to be written to the log store."""

    run_id: str
    execution_id: str
    workspace_id: str
    timestamp: int
    level: int
    template: str | None
    values: dict[str, t.Any]


def _format_values(values: dict[str, t.Any]) -> dict[str, t.Any]:
    """Format serialized values for HTTP log posting."""
    return {key: _format_value(value) for key, value in values.items()}


def _format_value(value: t.Any) -> dict[str, t.Any]:
    """Format a single serialized value for HTTP log posting."""
    match value:
        case ("raw", data, references):
            return {
                "type": "raw",
                "data": data,
                "references": [_format_reference(r) for r in references],
            }
        case ("blob", key, size, references):
            return {
                "type": "blob",
                "key": key,
                "size": size,
                "references": [_format_reference(r) for r in references],
            }
        case _:
            return {"type": "raw", "data": value, "references": []}


def _format_reference(ref: t.Any) -> dict[str, t.Any]:
    """Format a reference for HTTP log posting, including metadata."""
    match ref:
        case ("execution", execution_id, metadata):
            return {
                "type": "execution",
                "executionId": execution_id,
                "runId": metadata.run_id,
                "stepId": metadata.step_id,
                "attempt": metadata.attempt,
                "module": metadata.module,
                "target": metadata.target,
            }
        case ("asset", asset_id, metadata):
            return {
                "type": "asset",
                "assetId": asset_id,
                "name": metadata.name,
                "totalCount": metadata.total_count,
                "totalSize": metadata.total_size,
            }
        case ("fragment", format_, blob_key, size, metadata):
            return {
                "type": "fragment",
                "format": format_,
                "blobKey": blob_key,
                "size": size,
                "metadata": metadata,
            }
        case _:
            raise ValueError(f"Unknown reference type: {ref}")


class LogStore(abc.ABC):
    """Abstract base class for log stores."""

    @abc.abstractmethod
    def write(
        self,
        run_id: str,
        execution_id: str,
        workspace_id: str,
        timestamp: int,
        level: int,
        template: str | None,
        values: dict[str, t.Any],
    ) -> None:
        """Write a log message to the store."""
        ...

    def __enter__(self) -> "LogStore":
        return self

    def __exit__(self, *_) -> None:
        pass


class HttpLogStore(LogStore):
    """HTTP-based log store that POSTs logs to the Coflux server.

    Buffers logs and sends them in batches for efficiency.
    """

    def __init__(
        self,
        base_url: str,
        *,
        batch_size: int = 100,
        flush_interval: float = 0.5,
        secure: bool = False,
    ):
        """Initialize the HTTP log store.

        Args:
            base_url: The base URL of the Coflux server (e.g., "localhost:7777")
            batch_size: Maximum number of logs to batch before sending
            flush_interval: Maximum time (seconds) between flushes
            secure: Whether to use HTTPS
        """
        protocol = "https" if secure else "http"
        self._url = f"{protocol}://{base_url}/logs"
        self._batch_size = batch_size
        self._flush_interval = flush_interval

        self._queue: Queue[LogEntry] = Queue()
        self._running = False
        self._thread: threading.Thread | None = None

    def write(
        self,
        run_id: str,
        execution_id: str,
        workspace_id: str,
        timestamp: int,
        level: int,
        template: str | None,
        values: dict[str, t.Any],
    ) -> None:
        """Queue a log message for batched sending."""
        entry = LogEntry(run_id, execution_id, workspace_id, timestamp, level, template, values)
        self._queue.put(entry)

    def __enter__(self) -> "HttpLogStore":
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
        self._flush_all()

    def _run(self) -> None:
        """Background thread that periodically flushes the queue."""
        last_flush = time.time()
        buffer: list[LogEntry] = []

        while self._running:
            try:
                entry = self._queue.get(timeout=0.1)
                buffer.append(entry)

                # Flush if buffer is full or interval elapsed
                if len(buffer) >= self._batch_size or (
                    time.time() - last_flush > self._flush_interval and buffer
                ):
                    self._flush(buffer)
                    buffer = []
                    last_flush = time.time()

            except Empty:
                # Check if we should flush on interval
                if buffer and time.time() - last_flush > self._flush_interval:
                    self._flush(buffer)
                    buffer = []
                    last_flush = time.time()

        # Flush remaining on stop
        if buffer:
            self._flush(buffer)

    def _flush_all(self) -> None:
        """Flush all remaining items in the queue."""
        buffer: list[LogEntry] = []
        while True:
            try:
                entry = self._queue.get_nowait()
                buffer.append(entry)
            except Empty:
                break
        if buffer:
            self._flush(buffer)

    def _flush(self, entries: list[LogEntry]) -> None:
        """Send buffered logs to the server."""
        if not entries:
            return

        try:
            payload = {
                "messages": [self._format_entry(e) for e in entries],
            }
            response = httpx.post(self._url, json=payload, timeout=10)
            response.raise_for_status()
        except httpx.HTTPError as e:
            # Log error but don't crash - logs are best-effort
            print(f"Failed to send logs: {e}")

    def _format_entry(self, entry: LogEntry) -> dict[str, t.Any]:
        """Format a log entry for the API."""
        return {
            "runId": entry.run_id,
            "executionId": entry.execution_id,
            "workspaceId": entry.workspace_id,
            "timestamp": entry.timestamp,
            "level": entry.level,
            "template": entry.template,
            "values": _format_values(entry.values),
        }


def create_log_store(
    store_config: config.LogStoreConfig,
    *,
    server_host: str,
    server_secure: bool = False,
) -> LogStore:
    """Create a log store from configuration."""
    match store_config:
        case config.HTTPLogStoreConfig():
            host = store_config.host if store_config.host is not None else server_host
            secure = store_config.secure if store_config.secure is not None else server_secure
            return HttpLogStore(
                host,
                batch_size=store_config.batch_size,
                flush_interval=store_config.flush_interval,
                secure=secure,
            )
        case _:
            raise ValueError(f"Unknown log store config type: {store_config}")
