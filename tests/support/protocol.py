import json


def encode_message(msg: dict) -> bytes:
    return json.dumps(msg, separators=(",", ":")).encode() + b"\n"


def decode_message(line: bytes) -> dict:
    return json.loads(line)


def ready_message() -> dict:
    return {"method": "ready"}


def execution_result(execution_id, value=None, format="json"):
    result = None
    if value is not None:
        result = {"type": "inline", "format": format, "value": value}
    return {
        "method": "execution_result",
        "params": {"execution_id": execution_id, "result": result},
    }


def execution_error(execution_id, error_type, message, traceback=""):
    error = {"type": error_type, "message": message}
    if traceback:
        error["traceback"] = traceback
    return {
        "method": "execution_error",
        "params": {"execution_id": execution_id, "error": error},
    }


def submit_execution_request(
    request_id,
    execution_id,
    target,
    arguments,
    *,
    type=None,
    cache=None,
    memo=None,
    group_id=None,
    delay=None,
    retries=None,
    requires=None,
    defer_config=None,
    recurrent=False,
    wait_for=None,
):
    params = {
        "execution_id": execution_id,
        "target": target,
        "arguments": arguments,
    }
    if type is not None:
        params["type"] = type
    if cache is not None:
        params["cache"] = cache
    if memo is not None:
        params["memo"] = memo
    if group_id is not None:
        params["group_id"] = group_id
    if delay is not None:
        params["delay"] = delay
    if retries is not None:
        params["retries"] = retries
    if requires is not None:
        params["requires"] = requires
    if defer_config is not None:
        params["defer"] = defer_config
    if recurrent:
        params["recurrent"] = True
    if wait_for is not None:
        params["wait_for"] = wait_for
    return {"id": request_id, "method": "submit_execution", "params": params}


def resolve_reference_request(request_id, execution_id, reference):
    return {
        "id": request_id,
        "method": "resolve_reference",
        "params": {"execution_id": execution_id, "reference": reference},
    }


_LEVEL_MAP = {
    "debug": 0,
    "stdout": 1,
    "info": 2,
    "stderr": 3,
    "warning": 4,
    "error": 5,
}


def log_message(execution_id, level, message, values=None):
    """Send a log message.

    Level can be a string (debug, info, warning, error) or an integer
    matching the adapter protocol (0=debug, 1=stdout, 2=info, 3=stderr,
    4=warning, 5=error).

    values is an optional dict mapping placeholder keys to wire-format
    values: {"key": ["raw", data, refs]} where refs is a list of
    reference arrays processed by the worker's processLogValue.
    """
    if isinstance(level, str):
        level = _LEVEL_MAP[level]
    params = {
        "execution_id": execution_id,
        "level": level,
        "template": message,
    }
    if values is not None:
        params["values"] = values
    return {
        "method": "log",
        "params": params,
    }


def json_args(*values):
    """Build an arguments list from plain values."""
    return [{"type": "inline", "format": "json", "value": v} for v in values]


def cancel_execution_request(request_id, execution_id, target_reference):
    return {
        "id": request_id,
        "method": "cancel_execution",
        "params": {
            "execution_id": execution_id,
            "target_reference": target_reference,
        },
    }


def suspend_request(request_id, execution_id, execute_after=None):
    params = {"execution_id": execution_id}
    if execute_after is not None:
        params["execute_after"] = execute_after
    return {"id": request_id, "method": "suspend", "params": params}


def persist_asset_request(request_id, execution_id, paths, metadata=None):
    params = {
        "execution_id": execution_id,
        "paths": paths,
    }
    if metadata is not None:
        params["metadata"] = metadata
    return {"id": request_id, "method": "persist_asset", "params": params}


def get_asset_request(request_id, execution_id, reference):
    return {
        "id": request_id,
        "method": "get_asset",
        "params": {
            "execution_id": execution_id,
            "reference": reference,
        },
    }


def register_group_notification(execution_id, group_id, name=None):
    params = {"execution_id": execution_id, "group_id": group_id}
    if name is not None:
        params["name"] = name
    return {"method": "register_group", "params": params}
