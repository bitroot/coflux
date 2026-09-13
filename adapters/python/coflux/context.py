"""Execution context for running targets."""

from __future__ import annotations

import contextvars
import datetime as dt
import fnmatch
import hashlib
import json
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, NoReturn

from . import protocol
from .dispatcher import get_dispatcher
from .errors import (
    ExecutionAbandoned,
    ExecutionCancelled,
    ExecutionCrashed,
    ExecutionTimeout,
    InputDismissed,
    RequestError,
    Suspending,
    create_execution_error,
)
from .models import (
    Asset,
    AssetEntry,
    AssetMetadata,
    CatalogEntry,
    Execution,
    Input,
    validate_catalog_path,
)
from .serialization import deserialize_value, serialize_value
from .streams import StreamDriver
from .target import Streams


class _CatalogPosition:
    """A wait for whatever comes after ``number`` at ``path``.

    A ``CatalogEntry`` as a select handle waits past whatever the execution
    can see; this pins the position explicitly instead, for ``current()``
    on an empty path, which waits on position 0.
    """

    def __init__(self, path: str, number: int):
        self.path = path
        self.number = number


def _handle_key(handle: Any) -> tuple[str, str]:
    """Composite cache key for a select handle."""
    if isinstance(handle, Execution):
        return ("execution", handle.id)
    if isinstance(handle, Input):
        return ("input", handle.id)
    if isinstance(handle, CatalogEntry):
        return ("catalog", handle.path)
    if isinstance(handle, _CatalogPosition):
        return ("catalog", f"{handle.path}@{handle.number}")
    raise TypeError(f"Unsupported select handle type: {type(handle).__name__}")


def _handle_wire(handle: Any) -> dict[str, Any]:
    """A select handle as the CLI expects it."""
    if isinstance(handle, CatalogEntry):
        # No position: the server waits past what this execution can see.
        return {"type": "catalog", "path": handle.path}
    if isinstance(handle, _CatalogPosition):
        return {"type": "catalog", "path": handle.path, "number": handle.number}
    kind, id_ = _handle_key(handle)
    return {"type": kind, "id": id_}


def _cacheable(key: tuple[str, str]) -> bool:
    """Whether a resolved select response can be reused for the handle.

    An execution or input resolves once and for all. A catalog wait
    resolves against what has been published so far, which moves: the
    handle stands for "a version this execution hasn't seen", so every
    wait on it has to ask again.
    """
    return key[0] != "catalog"


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


def _timeout_to_ms(timeout: float | dt.timedelta | None) -> int | None:
    if timeout is None:
        return None
    if isinstance(timeout, dt.timedelta):
        return int(timeout.total_seconds() * 1000)
    return int(timeout * 1000)


# Context variable for group tracking
_group_id: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "_group_id", default=None
)
# Enclosing `cf.suspense` timeout. Read by `select` when deciding how long
# to wait before suspending, and by stream subscriptions, where it also
# switches on cursor tracking so a resumed execution carries on rather than
# re-reading from the start.
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
        # Guards the mutable collections above. Stream driver threads, the
        # main task thread, and any user-spawned threads may call methods
        # on this context concurrently; the lock protects check-then-set
        # patterns (metric definition, group registration, resolve cache)
        # from racing.
        self._lock = threading.Lock()
        # Owns generator streams for this execution. Generators encountered
        # during serialization (of the return value OR of submit arguments)
        # are registered here and driven in background threads.
        self._stream_driver = StreamDriver(execution_id)
        # Default stream config for this execution, populated by the
        # executor from the target's ``@cf.task(streams=...)`` setting.
        # Used by ``cf.stream(...)`` to fill in unspecified options.
        self._default_streams: Streams | None = None
        # Checkpoint state, split by whether it has been materialised yet.
        # ``_checkpoint_wire`` holds what arrived with the execute message,
        # still in protocol form — a checkpoint that's never read is never
        # deserialised. ``_checkpoint_values`` holds materialised reads and
        # anything written by this execution, and always wins. Both are
        # guarded by ``self._lock``.
        self._checkpoint_wire: dict[str, Any] = {}
        self._checkpoint_values: dict[str, Any] = {}
        # Writes held back until the execution is somewhere it could resume
        # from. ``_checkpoint_holds`` counts the non-replayable inputs
        # currently in the body's hands (see ``hold_checkpoints``); while it
        # is non-zero, writes accumulate here instead of going on the wire.
        # A name is either set or reset, never both — the later write
        # replaces the earlier one, so the delta only describes the net
        # effect, exactly as the worker-side throttle coalesces them.
        self._checkpoint_holds = 0
        self._checkpoint_pending_set: dict[str, Any] = {}
        self._checkpoint_pending_reset: set[str] = set()
        # Occurrence counts for auto-named stream cursors, so two loops
        # over an identical view of the same stream get distinct
        # checkpoints. Keyed by the content-addressed base name; the
        # count is deterministic across attempts as long as subscriptions
        # are opened in the same order, which is the determinism suspend
        # already requires.
        self._cursor_occurrences: dict[str, int] = {}

    def set_default_streams(self, streams: Streams | None) -> None:
        """Record the decorator's stream config so ``cf.stream(...)`` can
        inherit from it. Called once by the executor before running the
        target function."""
        self._default_streams = streams

    def get_default_streams(self) -> Streams | None:
        return self._default_streams

    def register_stream(
        self,
        generator: Any,
        buffer: int | None,
        timeout: float | dt.timedelta | None = None,
    ) -> str:
        """Register a generator with this execution's stream driver and
        return the resulting opaque stream id.

        Called from ``cf.stream(...)``; also from the executor when the
        task body itself is a generator.
        """
        timeout_ms = _timeout_to_ms(timeout)
        return self._stream_driver.register(generator, buffer, timeout_ms)

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
        streams: dict[str, Any] | None = None,
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
            streams=streams,
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
        handle can return without a round-trip — unless the winner is a
        catalog entry, whose resolution isn't reusable (``_cacheable``).

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
        winner, _response = self._select(
            handles,
            suspend=suspend,
            cancel_remaining=cancel_remaining,
            timeout_ms=timeout_ms,
        )
        return winner

    def _select(
        self,
        handles: list[Any],
        *,
        suspend: bool,
        cancel_remaining: bool,
        timeout_ms: int | None,
    ) -> tuple[int | None, dict[str, Any] | None]:
        """``select``, also handing back the winner's response.

        A caller waiting on a single handle reads the value from what is
        returned here rather than from the context, so a response that
        isn't cached — a catalog wait's — never sits anywhere another
        thread's wait could pick it up. ``(None, None)`` on timeout.
        """
        if not handles:
            raise ValueError("select requires at least one handle")

        if timeout_ms is None:
            timeout = _timeout.get()
            if timeout is not None:
                timeout_ms = int(timeout * 1000)

        request_id = protocol.request_select(
            self.execution_id,
            [_handle_wire(handle) for handle in handles],
            timeout_ms=timeout_ms,
            suspend=suspend,
            cancel_remaining=cancel_remaining,
        )
        response = self._wait_response(request_id)
        if response is None:
            # Server signals a wait timeout (nothing resolved before the
            # timeout expired) by returning a null result.
            return None, None

        winner = response.get("winner")
        if winner is None:
            raise RuntimeError(f"Unexpected select response: {response}")

        key = _handle_key(handles[winner])
        if _cacheable(key):
            with self._lock:
                self._resolved[key] = response
        return winner, response

    def resolve_handle(self, handle: Any) -> Any:
        """Block until ``handle`` resolves and return its value (or raise).

        Uses this context's resolve cache if ``cf.select`` has already seen
        the handle; otherwise issues a single-handle ``select`` call. If
        the handle has a parser (e.g. an ``Input[Model]``), it is applied
        to the deserialized value.
        """
        key = _handle_key(handle)
        with self._lock:
            cached = self._resolved.get(key)
        if cached is None:
            _winner, cached = self._select(
                [handle], suspend=True, cancel_remaining=False, timeout_ms=None
            )
            if cached is None:
                # The wait expired before the handle resolved. Only reachable
                # from inside a `cf.suspense(timeout=...)` scope; otherwise the
                # server either resolves or kills the process.
                raise TimeoutError("timed out waiting for handle to resolve")
        return _unwrap_response(cached, getattr(handle, "_parser", None))

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
        with self._lock:
            cached = self._resolved.get(key)
        if cached is None:
            timeout_ms = int(timeout * 1000) if timeout else 0
            _winner, cached = self._select(
                [handle], suspend=False, cancel_remaining=False, timeout_ms=timeout_ms
            )
            if cached is None:
                return default
        return _unwrap_response(cached, getattr(handle, "_parser", None))

    # --- Catalog ---

    def catalog_publish(self, path: str, value: Any) -> int:
        """Publish ``value`` at ``path``, serialised the way a result is,
        and return the version's number. An invalid path is refused here,
        the way ``cf.catalog`` refuses one, rather than by the server."""
        path = validate_catalog_path(path)
        request_id = protocol.request_catalog_publish(
            self.execution_id, path, serialize_value(value)
        )
        return self._wait_response(request_id)["number"]

    def catalog_current(self, path: str) -> Any:
        """The value at ``path`` as of the snapshot, waiting for a first
        publish."""
        found = self._catalog_get(path, None)
        if found is not None:
            return deserialize_value(found["value"])
        # Nothing there yet. Wait for anything at the path — position 0 —
        # following the suspense rule like any other wait, then read what
        # landed by number, since it is newer than the snapshot.
        number = self.resolve_handle(_CatalogPosition(path, 0))
        found = self._catalog_get(path, number)
        if found is None:
            raise RuntimeError(f"{path}@{number} landed but could not be read")
        return deserialize_value(found["value"])

    def catalog_next(self, path: str) -> NoReturn:
        """Suspend until ``path`` has a version newer than this execution
        can see.

        The wait travels on the suspend request, the way a stream
        consumer's does, so the server records the gate — at the
        execution's own view of the path, which it knows — as part of the
        same suspension. If a newer version already exists the gate is met
        at once and the successor runs immediately.
        """
        self.suspend_execution(catalog_wait=path)

    def _catalog_get(self, path: str, number: int | None) -> dict[str, Any] | None:
        request_id = protocol.request_catalog_get(self.execution_id, path, number)
        response = self._wait_response(request_id)
        if response is None or response.get("version") is None:
            return None
        return response["version"]

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

    def download_blob(
        self,
        blob_key: str,
        target_path: Path,
        *,
        offset: int | None = None,
        length: int | None = None,
    ) -> None:
        """Download a blob, or a byte range of one, to a local file."""
        request_id = protocol.request_download_blob(
            self.execution_id,
            blob_key,
            str(target_path),
            offset,
            length,
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
                    if path.is_file() and (matcher is None or matcher(path_str, match)):
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
                    raise TypeError(f"Unhandled entry type ({type(entry)})")
        else:
            raise TypeError(f"Unhandled entries type ({type(entries)})")

        if not paths_to_upload and not resolved_entries:
            raise ValueError("No files found to create asset")

        upload_paths = {rel: str(p) for rel, p in paths_to_upload} or None
        request_id = protocol.request_persist_asset(
            self.execution_id,
            upload_paths,
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

        Handles that are already resolved are silently skipped. A catalog
        entry is a select handle but not a cancellable one — nothing is
        pending behind it — so passing one is a ``TypeError``.
        """
        if not handles:
            return
        for handle in handles:
            if isinstance(handle, (CatalogEntry, _CatalogPosition)):
                raise TypeError(
                    f"cannot cancel {handle!r}: a catalog entry has nothing to cancel"
                )
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
        with self._lock:
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

    def set_checkpoints(self, checkpoints: dict[str, Any] | None) -> None:
        """Seed the effective checkpoint state from the execute message."""
        with self._lock:
            self._checkpoint_wire = dict(checkpoints or {})
            self._checkpoint_values = {}

    def checkpoint_get(self, name: str) -> Any:
        """Read a checkpoint value, raising ``KeyError`` if it isn't set.

        Reads are served entirely from local state — the effective checkpoint
        arrives with the execute message, and this execution's own writes are
        applied to it — so this never round-trips to the server.
        """
        with self._lock:
            if name in self._checkpoint_values:
                return self._checkpoint_values[name]
            if name not in self._checkpoint_wire:
                raise KeyError(name)
            wire = self._checkpoint_wire[name]

        # Materialise outside the lock: a blob-backed value is read off disk
        # here, and the lock is shared with the stream driver threads.
        value = deserialize_value(wire)

        with self._lock:
            self._checkpoint_wire.pop(name, None)
            # A write may have landed while this was materialising, and a
            # write always wins over the inherited value.
            if name not in self._checkpoint_values:
                self._checkpoint_values[name] = value
            return self._checkpoint_values[name]

    def checkpoint_has(self, name: str) -> bool:
        with self._lock:
            return name in self._checkpoint_values or name in self._checkpoint_wire

    def checkpoint_set(self, name: str, value: Any) -> None:
        with self._lock:
            self._checkpoint_values[name] = value
            self._checkpoint_wire.pop(name, None)
        # Serialised here rather than at publication time: the value is the
        # one the caller passed, and holding a reference to a mutable object
        # would record whatever it became later instead.
        self._record_checkpoint_delta(set_={name: serialize_value(value)})

    def checkpoint_reset(self, name: str) -> None:
        with self._lock:
            self._checkpoint_values.pop(name, None)
            self._checkpoint_wire.pop(name, None)
        self._record_checkpoint_delta(reset=[name])

    def hold_checkpoints(self) -> None:
        """Note that a non-replayable input is in the body's hands.

        Checkpoint state is one snapshot of a step's progress rather than a
        set of independent cells — the server stores an execution's row-set
        as a complete snapshot and applies each delta in a transaction. What
        decides whether that snapshot is *coherent* is where the deltas get
        cut, and one cut at an arbitrary point describes a state the
        execution was never in.

        That only matters for state derived from something a replay can't
        re-read. A result resolves again; a stream item does not — its
        position lives in a cursor, and if the cursor and whatever the body
        derived from the item reach the server separately, a crash in
        between leaves the successor counting on from a position it never
        actually reached.

        So writes made while an item is in hand are held, and published in
        the same delta as the cursor advance that retires it. The pair moves
        together or not at all, and a replay repeats whole items rather than
        fractions of one.

        Balanced by ``release_checkpoints``. Nested holds — a body iterating
        two streams — publish at the outermost release, the only point at
        which every cursor involved is up to date.
        """
        with self._lock:
            self._checkpoint_holds += 1

    def release_checkpoints(self, *, publish: bool) -> None:
        """Retire a hold taken by ``hold_checkpoints``.

        ``publish`` says whether the item the hold covered was consumed. On
        the way out of a completed iteration it is true, and the held writes
        go out with the cursor advance. Where the item is abandoned instead
        — ``break``, an exception, a dropped iterator — it is false: the
        cursor was never advanced, so the item will be delivered again, and
        anything derived from it must not be recorded or the replay counts
        it twice.

        Discarding drops the whole pending delta, including writes made
        under an enclosing hold. That is not over-eager: an enclosing hold
        means that iteration has not advanced its own cursor either, so
        everything pending derives from an item that is still unconsumed.
        """
        with self._lock:
            if not self._checkpoint_holds:
                return
            self._checkpoint_holds -= 1
            held = self._checkpoint_holds
            if not publish:
                self._checkpoint_pending_set.clear()
                self._checkpoint_pending_reset.clear()
        if publish and not held:
            self._publish_checkpoints()

    def _record_checkpoint_delta(
        self,
        set_: dict[str, Any] | None = None,
        reset: list[str] | None = None,
    ) -> None:
        """Put a write on the wire, or hold it for the next safe point."""
        with self._lock:
            if self._checkpoint_holds:
                for name, value in (set_ or {}).items():
                    self._checkpoint_pending_set[name] = value
                    self._checkpoint_pending_reset.discard(name)
                for name in reset or []:
                    self._checkpoint_pending_reset.add(name)
                    self._checkpoint_pending_set.pop(name, None)
                return
        protocol.send_checkpoint_update(self.execution_id, set_=set_, reset=reset)

    def _publish_checkpoints(self) -> None:
        """Send whatever is being held, as a single delta."""
        with self._lock:
            set_ = self._checkpoint_pending_set
            reset = self._checkpoint_pending_reset
            self._checkpoint_pending_set = {}
            self._checkpoint_pending_reset = set()
        if set_ or reset:
            protocol.send_checkpoint_update(
                self.execution_id,
                set_=set_ or None,
                # Sorted only so the delta is deterministic; the server
                # applies the whole thing at once either way.
                reset=sorted(reset) or None,
            )

    def flush(self) -> None:
        """Block until buffered state has reached the server.

        Publishes anything currently held first. An explicit flush is the
        caller declaring this point consistent, which is what makes it the
        escape hatch for state that has to be durable before a side effect —
        including inside a loop body, where the runtime would otherwise wait
        for the iteration to end. The cursor advance is still to come at
        that point, so a flush there deliberately records derived state
        ahead of the position it came from.
        """
        self._publish_checkpoints()
        request_id = protocol.request_flush(self.execution_id)
        self._wait_response(request_id)

    def suspend_execution(
        self,
        delay: float | dt.timedelta | dt.datetime | None = None,
        stream_wait: tuple[str, int] | None = None,
        catalog_wait: str | None = None,
    ) -> NoReturn:
        """Signal that this execution should suspend.

        Raises rather than performing the handshake here. The server
        records a suspension as a completion, and a completed execution's
        checkpoint writes are rejected — so everything that runs while the
        body unwinds (``finally`` blocks, cancelled tasks, generator
        cleanup) has to happen *before* the request is sent, or its state
        is silently dropped. ``finish_suspension`` completes it once the
        body is done.
        """
        execute_after = None
        if isinstance(delay, dt.datetime):
            execute_after = int(delay.timestamp() * 1000)
        elif isinstance(delay, dt.timedelta):
            execute_after = int(
                (dt.datetime.now(dt.timezone.utc) + delay).timestamp() * 1000
            )
        elif isinstance(delay, (int, float)) and delay > 0:
            execute_after = int(
                (
                    dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=delay)
                ).timestamp()
                * 1000
            )
        raise Suspending(execute_after, stream_wait, catalog_wait)

    def finish_suspension(
        self,
        execute_after: int | None,
        stream_wait: tuple[str, int] | None = None,
        catalog_wait: str | None = None,
    ) -> None:
        """Complete a suspension once the body has unwound. Never returns.

        Stops any in-flight stream producers and joins their driver threads
        first, so their cleanup runs while the execution is still live.

        Winding the generators down does *not* close their streams: the
        driver skips ``send_stream_close`` on ``GeneratorExit``, and the
        server leaves a suspended execution's streams paused rather than
        closing them, so the execution that resumes the step continues
        them.

        ``stream_wait`` gates the successor on a stream reaching a
        sequence, for a consumer that suspended partway through iterating;
        ``catalog_wait`` gates it on a catalog path having a version newer
        than this execution could see, for a ``next()``.
        """
        try:
            self.close_streams()
            self.wait_streams()
        except Exception:  # noqa: BLE001, S110
            # Best-effort teardown — the suspension below is what matters.
            pass
        request_id = protocol.request_suspend(
            self.execution_id, execute_after, stream_wait, catalog_wait
        )
        self._wait_response(request_id)
        # Suspension confirmed. Block until the server aborts this execution.
        get_dispatcher().wait_closed()
        raise SystemExit(0)

    def take_stream_suspension(
        self,
    ) -> tuple[int | None, tuple[str, int] | None, str | None] | None:
        """Claim a suspension requested from inside a generator body.

        Returns ``(execute_after, stream_wait, catalog_wait)`` — any of
        which may itself be ``None`` — or ``None`` when no generator asked
        to suspend. The executor checks this after its streams have drained.
        """
        return self._stream_driver.take_suspension()

    def stream_available(self, stream_id: str, sequence: int) -> bool:
        """Whether ``stream_id`` has reached ``sequence``, or has closed.

        The consumer can't answer this itself. Its queue is fed
        asynchronously, so an empty one means "nothing has arrived yet",
        not "the stream has nothing" — checking locally right after
        subscribing always finds it empty, whatever the stream holds.

        A poll, never a suspension: the server reports what it knows and
        the decision of what to do about it stays here, so a suspension
        still unwinds the body before the handshake.
        """
        request_id = protocol.request_select(
            self.execution_id,
            [{"type": "stream", "id": stream_id, "sequence": sequence}],
            timeout_ms=0,
            suspend=False,
        )
        return self._wait_response(request_id) is not None

    @property
    def suspense_timeout(self) -> float | None:
        """The enclosing ``cf.suspense`` timeout, or ``None`` outside one."""
        return _timeout.get()

    def next_cursor_occurrence(self, name: str) -> int:
        """Count of prior subscriptions in this execution sharing ``name``."""
        with self._lock:
            occurrence = self._cursor_occurrences.get(name, 0)
            self._cursor_occurrences[name] = occurrence + 1
            return occurrence

    def _parse_response(self, msg: dict) -> Any:
        """Extract the result from a response message, raising on error."""
        if msg.get("error"):
            error = msg["error"]
            raise RequestError(error.get("code", ""), error.get("message", ""))
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
