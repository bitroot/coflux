"""JSON Lines protocol for stdio communication with the CLI."""

from __future__ import annotations

import json
import sys
import threading
from typing import Any

from ._version import __version__


class Protocol:
    """Handles JSON Lines communication over stdio."""

    def __init__(self) -> None:
        self._next_id = 0
        # Save references to original stdio streams so we can use them
        # even when stdout/stderr are redirected for output capture
        self._stdout = sys.stdout
        self._stdin = sys.stdin
        # Multiple threads (main + stream drivers + dispatcher-invoked
        # handlers) can emit messages concurrently; serialize writes so JSON
        # lines don't interleave.
        self._write_lock = threading.Lock()

    def send_message(self, method: str, params: dict[str, Any] | None = None) -> None:
        """Send a notification message (no response expected)."""
        msg: dict[str, Any] = {"method": method}
        if params:
            msg["params"] = params
        self._write(msg)

    def send_request(self, method: str, params: dict[str, Any]) -> int:
        """Send a request and return the request ID."""
        self._next_id += 1
        req = {
            "id": self._next_id,
            "method": method,
            "params": params,
        }
        self._write(req)
        return self._next_id

    def send_response(
        self,
        request_id: int,
        result: Any = None,
        error: dict[str, str] | None = None,
    ) -> None:
        """Send a response to a request."""
        resp: dict[str, Any] = {"id": request_id}
        if error:
            resp["error"] = error
        else:
            resp["result"] = result
        self._write(resp)

    def receive(self) -> dict[str, Any] | None:
        """Receive the next message from stdin. Returns None on EOF."""
        line = self._stdin.readline()
        if not line:
            return None
        return json.loads(line)

    def _write(self, obj: dict[str, Any]) -> None:
        """Write a JSON object as a line to stdout."""
        line = json.dumps(obj, separators=(",", ":"))
        with self._write_lock:
            self._stdout.write(line + "\n")
            self._stdout.flush()


# Global protocol instance for convenience
_protocol: Protocol | None = None


def get_protocol() -> Protocol:
    """Get the global protocol instance."""
    global _protocol
    if _protocol is None:
        _protocol = Protocol()
    return _protocol


def _derive_api_version(version: str) -> str:
    """Derive the API version from a full semantic version string.

    Pre-1.0: "0.{minor}" (e.g., "0.8.1" -> "0.8")
    Post-1.0: "{major}" (e.g., "1.2.3" -> "1")
    """
    if version == "dev" or not version:
        return "dev"
    parts = version.split(".")
    if len(parts) >= 2:
        major = int(parts[0])
        minor = int(parts[1])
        if major == 0:
            return f"0.{minor}"
        return str(major)
    return version


def send_ready() -> None:
    """Send ready notification with protocol version to indicate executor is ready."""
    get_protocol().send_message("ready", {"version": _derive_api_version(__version__)})


def send_execution_result(
    execution_id: str,
    result: dict[str, Any] | None = None,
) -> None:
    """Send execution result notification."""
    params: dict[str, Any] = {"execution_id": execution_id}
    if result:
        params["result"] = result
    get_protocol().send_message("execution_result", params)


def send_execution_error(
    execution_id: str,
    error_type: str,
    message: str,
    traceback: str = "",
    retryable: bool | None = None,
) -> None:
    """Send execution error notification."""
    error: dict[str, Any] = {
        "type": error_type,
        "message": message,
        "traceback": traceback,
    }
    if retryable is not None:
        error["retryable"] = retryable
    get_protocol().send_message(
        "execution_error",
        {
            "execution_id": execution_id,
            "error": error,
        },
    )


def send_log(
    execution_id: str,
    level: int,
    template: str | None = None,
    values: dict[str, Any] | None = None,
) -> None:
    """Send a log message with optional structured values.

    Level values (matching server convention):
        0 = debug
        1 = stdout (captured print statements)
        2 = info
        3 = stderr (captured print to stderr)
        4 = warning
        5 = error

    Args:
        execution_id: The execution this log belongs to
        level: Log level as integer
        template: Optional message template with {placeholders}
        values: Optional dict of serialized values (each is a Value dict with type/format/value/references)
    """
    params: dict[str, Any] = {
        "execution_id": execution_id,
        "level": level,
    }
    if template is not None:
        params["template"] = template
    if values is not None:
        params["values"] = values
    get_protocol().send_message("log", params)


def request_submit_execution(
    execution_id: str,
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
) -> int:
    """Request to submit a child execution."""
    params: dict[str, Any] = {
        "execution_id": execution_id,
        "module": module,
        "target": target,
        "arguments": arguments,
    }
    if type is not None:
        params["type"] = type
    if wait_for is not None:
        params["wait_for"] = wait_for
    if group_id is not None:
        params["group_id"] = group_id
    if cache is not None:
        params["cache"] = cache
    if defer is not None:
        params["defer"] = defer
    if memo is not None:
        params["memo"] = memo
    if delay is not None:
        params["delay"] = delay
    if retries is not None:
        params["retries"] = retries
    if recurrent:
        params["recurrent"] = recurrent
    if requires:
        params["requires"] = requires
    if timeout:
        params["timeout"] = timeout
    return get_protocol().send_request("submit_execution", params)


def request_select(
    execution_id: str,
    handles: list[dict[str, str]],
    timeout_ms: int | None = None,
    suspend: bool = True,
    cancel_remaining: bool = False,
) -> int:
    """Request to wait for the first of one or more handles to resolve.

    Args:
        execution_id: The calling execution.
        handles: List of handle dicts: ``{"type": "execution"|"input", "id": "..."}``.
        timeout_ms: Maximum wait time in ms. None means no timeout.
        suspend: If True, the server may suspend the caller while waiting.
        cancel_remaining: If True, cancel non-winner execution handles atomically
            once a handle resolves.
    """
    params: dict[str, Any] = {
        "execution_id": execution_id,
        "handles": handles,
        "suspend": suspend,
    }
    if timeout_ms is not None:
        params["timeout_ms"] = timeout_ms
    if cancel_remaining:
        params["cancel_remaining"] = cancel_remaining
    return get_protocol().send_request("select", params)


def request_persist_asset(
    execution_id: str,
    paths: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
    entries: dict[str, tuple[str, int, dict[str, Any]]] | None = None,
) -> int:
    """Request to persist an asset.

    Args:
        execution_id: The execution this asset belongs to.
        paths: Local file paths to upload and include.
        metadata: Asset-level metadata (e.g. name).
        entries: Pre-resolved entries referencing existing blobs.
            Each value is (blob_key, size, entry_metadata).
    """
    params: dict[str, Any] = {
        "execution_id": execution_id,
    }
    if paths:
        params["paths"] = paths
    if metadata:
        params["metadata"] = metadata
    if entries:
        params["entries"] = {
            path: [blob_key, size, entry_metadata]
            for path, (blob_key, size, entry_metadata) in entries.items()
        }
    return get_protocol().send_request("persist_asset", params)


def request_get_asset(
    execution_id: str,
    asset_id: str,
) -> int:
    """Request to get an asset."""
    return get_protocol().send_request(
        "get_asset",
        {
            "execution_id": execution_id,
            "asset_id": asset_id,
        },
    )


def request_download_blob(
    execution_id: str,
    blob_key: str,
    target_path: str,
) -> int:
    """Request to download a blob to a local file."""
    return get_protocol().send_request(
        "download_blob",
        {
            "execution_id": execution_id,
            "blob_key": blob_key,
            "target_path": target_path,
        },
    )


def request_upload_blob(
    execution_id: str,
    source_path: str,
) -> int:
    """Request to upload a blob from a local file.

    Returns request ID. Response will contain {"blob_key": "..."}.
    """
    return get_protocol().send_request(
        "upload_blob",
        {
            "execution_id": execution_id,
            "source_path": source_path,
        },
    )


def request_suspend(execution_id: str, execute_after: int | None = None) -> int:
    """Request to suspend execution."""
    params: dict[str, Any] = {"execution_id": execution_id}
    if execute_after is not None:
        params["execute_after"] = execute_after
    return get_protocol().send_request("suspend", params)


def submit_input(
    execution_id: str,
    template: str,
    placeholders: dict[str, dict[str, Any]] | None = None,
    schema: str | None = None,
    key: str | None = None,
    title: str | None = None,
    actions: tuple[str, str] | None = None,
    initial: Any = None,
    requires: dict[str, list[str]] | None = None,
) -> int:
    """Submit an input request, returning a request ID.

    The server creates or finds the input and returns its external ID.

    Args:
        execution_id: The execution creating the input.
        template: Markdown prompt template with {placeholders}.
        placeholders: Serialized values for template placeholders.
        schema: JSON Schema string for validating input.
        key: Memoization key for reusing inputs.
        title: Short title for the input (shown in UI).
        actions: Tuple of (respond_label, dismiss_label) for button text.
        initial: Plain JSON initial values for pre-populating the form.
        requires: Tag set for routing inputs to specific users.
    """
    params: dict[str, Any] = {
        "execution_id": execution_id,
        "template": template,
    }
    if placeholders:
        params["placeholders"] = placeholders
    if schema is not None:
        params["schema"] = schema
    if key is not None:
        params["key"] = key
    if title is not None:
        params["title"] = title
    if actions is not None:
        params["actions"] = list(actions)
    if initial is not None:
        params["initial"] = initial
    if requires is not None:
        params["requires"] = requires
    return get_protocol().send_request("submit_input", params)


def request_cancel(
    execution_id: str,
    handles: list[dict[str, str]],
) -> int:
    """Request to cancel one or more handles (executions and/or inputs)."""
    return get_protocol().send_request(
        "cancel",
        {
            "execution_id": execution_id,
            "handles": handles,
        },
    )


def send_register_group(
    execution_id: str,
    group_id: int,
    name: str | None = None,
) -> None:
    """Register a group for organizing child executions."""
    get_protocol().send_message(
        "register_group",
        {
            "execution_id": execution_id,
            "group_id": group_id,
            "name": name,
        },
    )


def send_define_metric(
    execution_id: str,
    key: str,
    definition: dict[str, Any],
) -> None:
    """Send a metric definition to the server.

    Args:
        execution_id: The execution this metric belongs to.
        key: Metric key name.
        definition: Definition dict with group/scale/units/progress/lower/upper.
    """
    get_protocol().send_message(
        "define_metric",
        {
            "execution_id": execution_id,
            "key": key,
            "definition": definition,
        },
    )


def send_metric(
    execution_id: str,
    key: str,
    value: float,
    at: float | None = None,
) -> None:
    """Send a metric data point.

    Args:
        execution_id: The execution this metric belongs to.
        key: Metric key name.
        value: Metric value (float).
        at: Optional x-axis value. If None, the Go worker uses time-since-execution-start.
    """
    params: dict[str, Any] = {
        "execution_id": execution_id,
        "key": key,
        "value": value,
    }
    if at is not None:
        params["at"] = at
    get_protocol().send_message("metric", params)


def send_stream_register(execution_id: str, index: int) -> None:
    """Register a stream owned by this execution.

    ``index`` is worker-assigned and monotonic per execution (0, 1, 2, ...);
    it identifies the stream within its producer execution.
    """
    get_protocol().send_message(
        "stream_register",
        {"execution_id": execution_id, "index": index},
    )


def send_stream_append(
    execution_id: str,
    index: int,
    sequence: int,
    value: dict[str, Any],
) -> None:
    """Append an item to a stream.

    ``sequence`` is monotonic per stream (0, 1, 2, ...); it identifies the
    item within its stream. Value uses the same Value shape as execution
    results (type + format + value/path + refs).
    """
    get_protocol().send_message(
        "stream_append",
        {
            "execution_id": execution_id,
            "index": index,
            "sequence": sequence,
            "value": value,
        },
    )


def send_stream_close(
    execution_id: str,
    index: int,
    error_type: str | None = None,
    error_message: str = "",
    traceback: str = "",
) -> None:
    """Close a stream.

    With no error args, signals a clean close (generator exhausted). With
    error args set, signals that the generator raised — consumers will see
    the exception on their next iteration.
    """
    params: dict[str, Any] = {
        "execution_id": execution_id,
        "index": index,
    }
    if error_type is not None:
        params["error"] = {
            "type": error_type,
            "message": error_message,
            "traceback": traceback,
        }
    get_protocol().send_message("stream_close", params)


def send_stream_subscribe(
    execution_id: str,
    subscription_id: int,
    producer_execution_id: str,
    index: int,
    from_sequence: int,
    filter: dict[str, Any] | None = None,
) -> None:
    """Open a consumer subscription to a stream owned by another execution.

    ``execution_id`` is the consumer's own execution — the server uses it to
    track who's subscribed and where to push items.
    """
    params: dict[str, Any] = {
        "execution_id": execution_id,
        "subscription_id": subscription_id,
        "producer_execution_id": producer_execution_id,
        "index": index,
        "from_sequence": from_sequence,
    }
    if filter is not None:
        params["filter"] = filter
    get_protocol().send_message("stream_subscribe", params)


def send_stream_unsubscribe(execution_id: str, subscription_id: int) -> None:
    """Drop a consumer subscription."""
    get_protocol().send_message(
        "stream_unsubscribe",
        {"execution_id": execution_id, "subscription_id": subscription_id},
    )


def receive_message() -> dict[str, Any] | None:
    """Receive the next message from the CLI."""
    return get_protocol().receive()
