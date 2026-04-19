"""Execution context for running targets."""

from __future__ import annotations

import contextvars
import datetime as dt
import fnmatch as fnmatch
import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator

from . import protocol
from .dispatcher import get_dispatcher
from .errors import (
    ExecutionAbandoned,
    ExecutionCancelled,
    ExecutionCrashed,
    ExecutionTimeout,
    InputDismissed,
    create_execution_error,
)
from .models import Asset, AssetEntry, AssetMetadata, Execution, Input
from .serialization import deserialize_value, serialize_value
from .streams import StreamDriver


def _handle_key(handle: Any) -> tuple[str, str]:
    """Composite cache key for an Execution/Input handle."""
    if isinstance(handle, Execution):
        return ("execution", handle.id)
    if isinstance(handle, Input):
        return ("input", handle.id)
    raise TypeError(f"Unsupported select handle type: {type(handle).__name__}")


def _unwrap_response(
    response: dict[str, Any],
    parser: Callable[[Any], Any] | None = None,
) -> Any:
    """Convert a select winner response into a return value or raised error.

    If ``parser`` is given, it is applied to the deserialized value before
    returning. Error/cancelled/dismissed statuses raise regardless of
    whether a parser is supplied.
    """
    status = response.get("status")
    if status == "ok":
        value = deserialize_value(response["value"])
        return parser(value) if parser is not None else value
    if status == "error":
        error = response.get("error") or {}
        raise create_execution_error(
            error.get("type", ""),
            error.get("message", ""),
        )
    if status == "cancelled":
        raise ExecutionCancelled()
    if status == "dismissed":
        raise InputDismissed()
    if status == "timeout":
        raise ExecutionTimeout()
    if status == "abandoned":
        raise ExecutionAbandoned()
    if status == "crashed":
        raise ExecutionCrashed()
    raise RuntimeError(f"Unexpected select status: {status}")


# Context variable for group tracking
_group_id: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "_group_id", default=None
)
# Context variable for timeout tracking (not yet enforced)
_timeout: contextvars.ContextVar[float | None] = contextvars.ContextVar(
    "_timeout", default=None
)


class ExecutorContext:
    """Context for an executing target, providing access to CLI services."""

    def __init__(self, execution_id: str, working_dir: Path | None = None):
        self.execution_id = execution_id
        self._groups: list[str | None] = []
        self._working_dir = working_dir or Path.cwd()
        self._defined_metrics: dict[str, dict] = {}
        self._defined_scales: dict[str, dict] = {}
        self._defined_groups: dict[str, dict] = {}
        # Cache of resolved select responses keyed by (type, id). Populated
        # by select() when a handle resolves; consumed by resolve_handle /
        # poll_handle to avoid a round-trip for handles that have already
        # been seen in this context's lifetime.
        self._resolved: dict[tuple[str, str], dict[str, Any]] = {}
        # Owns generator streams for this execution. Generators encountered
        # during serialization (of the return value OR of submit arguments)
        # are registered here and driven in background threads.
        self._stream_driver = StreamDriver(execution_id)

    def register_stream(self, generator: Any, buffer: int | None) -> str:
        """Register a generator with this execution's stream driver and
        return the resulting opaque stream id.

        Called from ``cf.stream(...)``; also from the executor when the
        task body itself is a generator.
        """
        return self._stream_driver.register(generator, buffer)

    def wait_streams(self) -> None:
        """Block until every stream produced by this execution has drained."""
        self._stream_driver.wait_all()

    def close_streams(self) -> None:
        """Close every registered generator so driver threads exit promptly.

        Used on the error path before reporting execution_error.
        """
        self._stream_driver.close_all()

    def submit_execution(
        self,
        module: str,
        target: str,
        arguments: list[dict[str, Any]],
        type: str | None = None,
        wait_for: Any = None,
        group_id: int | None = None,
        cache: dict[str, Any] | None = None,
        defer: dict[str, Any] | None = None,
        memo: bool | list[int] | None = None,
        delay: float | None = None,
        retries: dict[str, Any] | None = None,
        recurrent: bool = False,
        requires: dict[str, list[str]] | None = None,
        timeout: int = 0,
    ) -> dict[str, Any]:
        """Submit a child execution and return its details.

        Returns a dict with 'execution_id', 'module', and 'target' keys.
        """
        # Use current group if not specified
        if group_id is None:
            group_id = _group_id.get()
        request_id = protocol.request_submit_execution(
            self.execution_id,
            module,
            target,
            arguments,
            type=type,
            wait_for=wait_for,
            group_id=group_id,
            cache=cache,
            defer=defer,
            memo=memo,
            delay=delay,
            retries=retries,
            recurrent=recurrent,
            requires=requires,
            timeout=timeout,
        )
        return self._wait_response(request_id)

    def select(
        self,
        handles: list[Any],
        *,
        suspend: bool = True,
        cancel_remaining: bool = False,
        timeout_ms: int | None = None,
    ) -> int | None:
        """Wait for the first of one or more handles to resolve.

        On success, the winner's response is stored in this context's
        resolve cache so subsequent ``.result()`` / ``.poll()`` calls on the
        handle can return without a round-trip.

        Args:
            handles: List of Execution or Input objects.
            suspend: Whether to allow suspension while waiting.
            cancel_remaining: If True, cancel non-winner execution handles.
            timeout_ms: Optional wait timeout. If None, falls back to the
                current ``cf.suspense`` timeout context var.

        Returns:
            The index in ``handles`` of the handle that resolved, or
            ``None`` on timeout.
        """
        if not handles:
            raise ValueError("select requires at least one handle")

        if timeout_ms is None:
            timeout = _timeout.get()
            if timeout is not None:
                timeout_ms = int(timeout * 1000)

        request_id = protocol.request_select(
            self.execution_id,
            [{"type": k, "id": i} for k, i in map(_handle_key, handles)],
            timeout_ms=timeout_ms,
            suspend=suspend,
            cancel_remaining=cancel_remaining,
        )
        response = self._wait_response(request_id)
        if response is None:
            # Server signals a wait timeout (nothing resolved before the
            # timeout expired) by returning a null result.
            return None

        winner = response.get("winner")
        if winner is None:
            raise RuntimeError(f"Unexpected select response: {response}")

        self._resolved[_handle_key(handles[winner])] = response
        return winner

    def resolve_handle(self, handle: Any) -> Any:
        """Block until ``handle`` resolves and return its value (or raise).

        Uses this context's resolve cache if ``cf.select`` has already seen
        the handle; otherwise issues a single-handle ``select`` call. If
        the handle has a parser (e.g. an ``Input[Model]``), it is applied
        to the deserialized value.
        """
        key = _handle_key(handle)
        if key not in self._resolved:
            if self.select([handle]) is None:
                # The wait expired before the handle resolved. Only reachable
                # from inside a `cf.suspense(timeout=...)` scope; otherwise the
                # server either resolves or kills the process.
                raise TimeoutError("timed out waiting for handle to resolve")
        return _unwrap_response(self._resolved[key], handle._parser)

    def poll_handle(
        self,
        handle: Any,
        timeout: float | None,
        default: Any,
    ) -> Any:
        """Non-suspending resolve: returns ``default`` if the handle isn't ready.

        If the handle resolves, applies its parser (if any) to the value.
        ``default`` is returned as-is when the handle isn't ready; no parser
        is applied to it.
        """
        key = _handle_key(handle)
        if key not in self._resolved:
            timeout_ms = int(timeout * 1000) if timeout else 0
            if self.select([handle], suspend=False, timeout_ms=timeout_ms) is None:
                return default
        return _unwrap_response(self._resolved[key], handle._parser)

    def get_asset_entries(self, asset_id: str) -> list[AssetEntry]:
        """Get all entries for an asset by ID."""
        request_id = protocol.request_get_asset(
            self.execution_id,
            asset_id,
        )
        response = self._wait_response(request_id)
        entries = []
        for path, (blob_key, size, metadata) in response.get("entries", {}).items():
            entries.append(AssetEntry(path, blob_key, size, metadata or {}))
        return entries

    def download_blob(self, blob_key: str, target_path: Path) -> None:
        """Download a blob to a local file."""
        request_id = protocol.request_download_blob(
            self.execution_id,
            blob_key,
            str(target_path),
        )
        self._wait_response(request_id)

    def create_asset(
        self,
        entries=None,
        *,
        at: Path | None = None,
        match: str | None = None,
        name: str | None = None,
    ):
        """Create and persist an asset from files or existing asset entries.

        Args:
            entries: What to include. Can be:
                - A single file path (str or Path)
                - A list of file paths
                - An Asset (re-reference all its entries)
                - A dict mapping paths to file paths, Assets, or AssetEntries
                - None (use with `match` to find files by pattern)
            at: Base directory for relative paths and pattern matching.
            match: Glob pattern to match files (e.g., "*.csv", "**/*.json").
            name: Optional name for the asset.

        Returns:
            The created Asset object.
        """
        base_dir = (at or self._working_dir).resolve()
        matcher = fnmatch.fnmatch if match else None
        paths_to_upload: list[tuple[str, Path]] = []
        # Pre-resolved entries referencing existing blobs: {path: (blob_key, size, metadata)}
        resolved_entries: dict[str, tuple[str, int, dict]] = {}

        if isinstance(entries, Asset):
            entries = {e.path: e for e in entries.entries}

        if entries is None and match:
            for file_path in base_dir.rglob("*"):
                if file_path.is_file() and fnmatch.fnmatch(
                    str(file_path.relative_to(base_dir)), match
                ):
                    rel_path = str(file_path.relative_to(base_dir))
                    paths_to_upload.append((rel_path, file_path))
        elif entries is None:
            for file_path in base_dir.rglob("*"):
                if file_path.is_file():
                    rel_path = str(file_path.relative_to(base_dir))
                    if matcher is None or matcher(rel_path, match):
                        paths_to_upload.append((rel_path, file_path))
        elif isinstance(entries, (str, Path)):
            path = Path(entries)
            if not path.is_absolute():
                path = base_dir / path
            if path.is_file():
                rel_path = (
                    str(path.relative_to(base_dir))
                    if base_dir in path.parents or path.parent == base_dir
                    else path.name
                )
                paths_to_upload.append((rel_path, path))
        elif isinstance(entries, list):
            for entry in entries:
                path = Path(entry)
                if not path.is_absolute():
                    path = base_dir / path
                if path.is_file():
                    rel_path = (
                        str(path.relative_to(base_dir))
                        if base_dir in path.parents or path.parent == base_dir
                        else path.name
                    )
                    paths_to_upload.append((rel_path, path))
        elif isinstance(entries, dict):
            if at is not None:
                raise ValueError(
                    "Base directory (`at`) cannot be specified with dictionary of entries"
                )
            for path_str, entry in entries.items():
                if isinstance(entry, (str, Path)):
                    path = Path(entry).resolve()
                    if path.is_file():
                        if matcher is None or matcher(path_str, match):
                            paths_to_upload.append((path_str, path))
                elif isinstance(entry, Asset):
                    for asset_entry in entry.entries:
                        full_path = f"{path_str}/{asset_entry.path}"
                        if matcher is None or matcher(full_path, match):
                            resolved_entries[full_path] = (
                                asset_entry.blob_key,
                                asset_entry.size,
                                asset_entry.metadata,
                            )
                elif isinstance(entry, AssetEntry):
                    if matcher is None or matcher(path_str, match):
                        resolved_entries[path_str] = (
                            entry.blob_key,
                            entry.size,
                            entry.metadata,
                        )
                else:
                    raise ValueError(f"Unhandled entry type ({type(entry)})")
        else:
            raise ValueError(f"Unhandled entries type ({type(entries)})")

        if not paths_to_upload and not resolved_entries:
            raise ValueError("No files found to create asset")

        abs_paths = [str(p) for _, p in paths_to_upload] if paths_to_upload else None
        request_id = protocol.request_persist_asset(
            self.execution_id,
            abs_paths,
            {"name": name} if name else None,
            resolved_entries if resolved_entries else None,
        )
        response = self._wait_response(request_id)
        asset_id = response.get("asset_id", "")
        total_size = sum(p.stat().st_size for _, p in paths_to_upload)
        total_size += sum(size for _, size, _ in resolved_entries.values())
        metadata = AssetMetadata(
            name=name,
            total_count=len(paths_to_upload) + len(resolved_entries),
            total_size=total_size,
        )
        return Asset(asset_id, metadata)

    def cancel(self, handles: list[Any]) -> None:
        """Cancel one or more handles (executions and/or inputs).

        For each execution handle, its result is recorded as ``cancelled``
        and descendant executions are cancelled recursively. For each
        input handle, it transitions to a terminal ``cancelled`` state
        (distinct from ``dismissed``) and any select waiters are notified.

        Handles that are already resolved are silently skipped.
        """
        if not handles:
            return
        request_id = protocol.request_cancel(
            self.execution_id,
            [{"type": k, "id": i} for k, i in map(_handle_key, handles)],
        )
        self._wait_response(request_id)

    def submit_input(
        self,
        template: str,
        placeholders: dict[str, Any] | None = None,
        schema: str | None = None,
        key: str | None = None,
        title: str | None = None,
        actions: tuple[str, str] | None = None,
        initial: Any = None,
        requires: dict[str, list[str]] | None = None,
    ) -> str:
        """Create an input request and return its external ID.

        The server creates or finds the input by key. Use resolve_input
        (via Input.result()) to wait for the response.
        """
        if key is None:
            h = hashlib.sha256()
            h.update(template.encode())
            if placeholders:
                h.update(json.dumps(placeholders, sort_keys=True).encode())
            if schema:
                h.update(schema.encode())
            if title:
                h.update(title.encode())
            if actions:
                h.update(json.dumps(actions).encode())
            key = h.hexdigest()[:16]
        request_id = protocol.submit_input(
            self.execution_id,
            template,
            placeholders=placeholders,
            schema=schema,
            key=key,
            title=title,
            actions=actions,
            initial=initial,
            requires=requires,
        )
        result = self._wait_response(request_id)
        return result["input_id"]

    def log(self, level: int, message: str) -> None:
        """Send a simple log message (used for stdout/stderr capture).

        Level values:
            0 = debug
            1 = stdout
            2 = info
            3 = stderr
            4 = warning
            5 = error
        """
        # Simple message without structured values
        protocol.send_log(self.execution_id, level, template=message)

    def log_message(self, level: int, template: str | None = None, **kwargs) -> None:
        """Send a log message with optional template and structured values.

        Args:
            level: Log level as integer (0=debug, 2=info, 4=warning, 5=error).
            template: Message template with {placeholders} for kwargs.
            **kwargs: Values to serialize and include in the log.
        """
        if not kwargs:
            # No values to serialize, just send template as message
            protocol.send_log(self.execution_id, level, template=template)
            return

        # Serialize each value
        serialized_values: dict[str, Any] = {}
        for key, value in kwargs.items():
            serialized_values[key] = serialize_value(value)

        protocol.send_log(
            self.execution_id,
            level,
            template=template,
            values=serialized_values,
        )

    def log_debug(self, message: str) -> None:
        """Send a debug log message."""
        self.log(0, message)

    def log_info(self, message: str) -> None:
        """Send an info log message."""
        self.log(2, message)

    def log_warning(self, message: str) -> None:
        """Send a warning log message."""
        self.log(4, message)

    def log_error(self, message: str) -> None:
        """Send an error log message."""
        self.log(5, message)

    @contextmanager
    def group(self, name: str | None = None) -> Iterator[None]:
        """Context manager for grouping child executions."""
        group_id = len(self._groups)
        self._groups.append(name)
        protocol.send_register_group(self.execution_id, group_id, name)
        token = _group_id.set(group_id)
        try:
            yield
        finally:
            _group_id.reset(token)

    @contextmanager
    def suspense(self, timeout: float | None = None) -> Iterator[None]:
        """Context manager for setting timeout on result waits."""
        token = _timeout.set(timeout if timeout is not None else 0)
        try:
            yield
        finally:
            _timeout.reset(token)

    def suspend_execution(
        self, delay: float | dt.timedelta | dt.datetime | None = None
    ) -> None:
        """Suspend the current execution, optionally resuming after a delay."""
        execute_after = None
        if isinstance(delay, dt.datetime):
            execute_after = int(delay.timestamp() * 1000)
        elif isinstance(delay, dt.timedelta):
            execute_after = int((dt.datetime.now() + delay).timestamp() * 1000)
        elif isinstance(delay, (int, float)) and delay > 0:
            execute_after = int(
                (dt.datetime.now() + dt.timedelta(seconds=delay)).timestamp() * 1000
            )
        request_id = protocol.request_suspend(self.execution_id, execute_after)
        self._wait_response(request_id)
        # Suspension confirmed. Block until the server aborts this execution.
        get_dispatcher().wait_closed()
        raise SystemExit(0)

    def _parse_response(self, msg: dict) -> Any:
        """Extract the result from a response message, raising on error."""
        if "error" in msg and msg["error"]:
            error = msg["error"]
            raise RuntimeError(f"{error['code']}: {error['message']}")
        return msg.get("result", {})

    def _wait_response(self, request_id: int) -> Any:
        """Wait for a response to a request.

        Delegates to the dispatcher, which owns stdin and routes the matching
        response to this caller. Safe to call from any thread.
        """
        msg = get_dispatcher().wait_for_response(request_id)
        if msg is None:
            raise RuntimeError("Timed out waiting for response")
        return self._parse_response(msg)
