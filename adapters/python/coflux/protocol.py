"""JSON Lines protocol for stdio communication with the CLI."""

from __future__ import annotations

import json
import sys
from typing import Any


class Protocol:
    """Handles JSON Lines communication over stdio."""

    def __init__(self) -> None:
        self._next_id = 0
        # Save references to original stdio streams so we can use them
        # even when stdout/stderr are redirected for output capture
        self._stdout = sys.stdout
        self._stdin = sys.stdin

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


def send_ready() -> None:
    """Send ready notification to indicate executor is ready for work."""
    get_protocol().send_message("ready")


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
) -> None:
    """Send execution error notification."""
    get_protocol().send_message(
        "execution_error",
        {
            "execution_id": execution_id,
            "error": {
                "type": error_type,
                "message": message,
                "traceback": traceback,
            },
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
        values: Optional dict of serialized values (each is ["raw", data, refs] or ["blob", key, size, refs])
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
) -> int:
    """Request to submit a child execution."""
    params: dict[str, Any] = {
        "execution_id": execution_id,
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
    return get_protocol().send_request("submit_execution", params)


def request_resolve_reference(
    execution_id: str,
    reference: list[Any],
) -> int:
    """Request to resolve a reference (get result of another execution)."""
    return get_protocol().send_request(
        "resolve_reference",
        {
            "execution_id": execution_id,
            "reference": reference,
        },
    )


def request_persist_asset(
    execution_id: str,
    paths: list[str],
    metadata: dict[str, Any] | None = None,
) -> int:
    """Request to persist an asset."""
    params: dict[str, Any] = {
        "execution_id": execution_id,
        "paths": paths,
    }
    if metadata:
        params["metadata"] = metadata
    return get_protocol().send_request("persist_asset", params)


def request_get_asset(
    execution_id: str,
    reference: list[Any],
) -> int:
    """Request to get an asset."""
    return get_protocol().send_request(
        "get_asset",
        {
            "execution_id": execution_id,
            "reference": reference,
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


def request_cancel_execution(
    execution_id: str,
    target_reference: list[Any],
) -> int:
    """Request to cancel another execution."""
    return get_protocol().send_request(
        "cancel_execution",
        {
            "execution_id": execution_id,
            "target_reference": target_reference,
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


def receive_message() -> dict[str, Any] | None:
    """Receive the next message from the CLI."""
    return get_protocol().receive()
