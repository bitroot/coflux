"""Execution context for running targets."""

from __future__ import annotations

import contextvars
import datetime as dt
import json
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from . import protocol

# Transfer threshold - values larger than this are passed via temp files
# rather than inline in the stdio JSON protocol. This is separate from
# the blob store threshold (which is configured in the worker).
TRANSFER_THRESHOLD = 64 * 1024  # 64KB

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
        self._pending_requests: dict[int, Any] = {}
        self._groups: list[str | None] = []
        self._working_dir = working_dir or Path.cwd()

    def _write_temp_file(self, data: bytes) -> str:
        """Write data to a temp file and return the path.

        The caller (worker) is responsible for cleaning up the file after reading.
        """
        fd, path = tempfile.mkstemp(prefix="coflux_log_")
        with open(fd, "wb") as f:
            f.write(data)
        return path

    def _serialize_log_value(self, value: Any) -> tuple[Any, ...]:
        """Serialize a value for logging.

        Returns:
            Tuple of ("raw", data, references) or ("file", path, size, references)

        The executor uses temp files for large data transfer to the worker.
        The worker then decides whether to upload to blob store based on its
        own threshold configuration.
        """
        from .serialization import encode_value

        data, references = encode_value(value, self._write_temp_file)
        encoded = json.dumps(data, separators=(",", ":")).encode()

        if len(encoded) > TRANSFER_THRESHOLD:
            # Write to temp file for large data
            path = self._write_temp_file(encoded)
            return ("file", path, len(encoded), references)
        else:
            return ("raw", data, references)

    def submit_execution(
        self,
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
    ) -> list[Any]:
        """Submit a child execution and return its reference."""
        # Use current group if not specified
        if group_id is None:
            group_id = _group_id.get()
        request_id = protocol.request_submit_execution(
            self.execution_id,
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
        )
        response = self._wait_response(request_id)
        return response["reference"]

    def resolve_reference(self, reference: list[Any]) -> dict[str, Any]:
        """Resolve a reference to get its value."""
        request_id = protocol.request_resolve_reference(
            self.execution_id,
            reference,
        )
        return self._wait_response(request_id)

    def persist_asset(
        self,
        paths: list[str],
        metadata: dict[str, Any] | None = None,
    ) -> list[Any]:
        """Persist files as an asset and return the reference."""
        request_id = protocol.request_persist_asset(
            self.execution_id,
            paths,
            metadata,
        )
        response = self._wait_response(request_id)
        return response["reference"]

    def get_asset(self, reference: list[Any]) -> list[str]:
        """Get paths for an asset."""
        request_id = protocol.request_get_asset(
            self.execution_id,
            reference,
        )
        response = self._wait_response(request_id)
        return response["paths"]

    def get_asset_entries(self, asset_id: str) -> list:
        """Get all entries for an asset by ID."""
        from .models import AssetEntry

        request_id = protocol.request_get_asset(
            self.execution_id,
            ["asset", asset_id],
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
        import fnmatch as fnmatch_module
        from .models import Asset, AssetEntry, AssetMetadata

        base_dir = (at or self._working_dir).resolve()
        matcher = fnmatch_module.fnmatch if match else None
        paths_to_upload: list[tuple[str, Path]] = []
        # Pre-resolved entries referencing existing blobs: {path: (blob_key, size, metadata)}
        resolved_entries: dict[str, tuple[str, int, dict]] = {}

        if isinstance(entries, Asset):
            entries = {e.path: e for e in entries.entries}

        if entries is None and match:
            for file_path in base_dir.rglob("*"):
                if file_path.is_file() and fnmatch_module.fnmatch(
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
                rel_path = str(path.relative_to(base_dir)) if base_dir in path.parents or path.parent == base_dir else path.name
                paths_to_upload.append((rel_path, path))
        elif isinstance(entries, list):
            for entry in entries:
                path = Path(entry)
                if not path.is_absolute():
                    path = base_dir / path
                if path.is_file():
                    rel_path = str(path.relative_to(base_dir)) if base_dir in path.parents or path.parent == base_dir else path.name
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

    def suspend(self) -> None:
        """Suspend the current execution."""
        request_id = protocol.request_suspend(self.execution_id)
        self._wait_response(request_id)

    def cancel_execution(self, target_reference: list[Any]) -> None:
        """Cancel another execution."""
        request_id = protocol.request_cancel_execution(
            self.execution_id,
            target_reference,
        )
        self._wait_response(request_id)

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
        serialized_values: dict[str, list[Any]] = {}
        for key, value in kwargs.items():
            serialized_values[key] = list(self._serialize_log_value(value))

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
        token = _timeout.set(timeout)
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

    def _wait_response(self, request_id: int) -> Any:
        """Wait for a response to a request."""
        while True:
            msg = protocol.receive_message()
            if msg is None:
                raise RuntimeError("Connection closed while waiting for response")

            # Check if this is a response
            if "id" in msg:
                if msg["id"] == request_id:
                    if "error" in msg and msg["error"]:
                        error = msg["error"]
                        raise RuntimeError(f"{error['code']}: {error['message']}")
                    return msg.get("result", {})
                # Store other responses for later
                self._pending_requests[msg["id"]] = msg
            else:
                # Unexpected message during wait
                raise RuntimeError(f"Unexpected message while waiting for response: {msg}")


# Current execution context (thread-local would be needed for concurrency)
_current_context: ExecutorContext | None = None


def get_context() -> ExecutorContext:
    """Get the current execution context."""
    if _current_context is None:
        raise RuntimeError("Not in an execution context")
    return _current_context


def set_context(ctx: ExecutorContext | None) -> None:
    """Set the current execution context."""
    global _current_context
    _current_context = ctx
