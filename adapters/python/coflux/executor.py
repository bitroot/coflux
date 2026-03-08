"""Executor process that runs targets and communicates via stdio."""

from __future__ import annotations

import importlib
import os
import sys
import traceback
from pathlib import Path
from typing import Any

from . import protocol
from .context import ExecutorContext
from .state import set_context
from .output import capture_output
from .serialization import deserialize_value, serialize_value

_COFLUX_PKG_DIR = os.path.dirname(os.path.abspath(__file__))


def _format_filtered_traceback(exc: Exception) -> str:
    """Format a traceback string with Coflux-internal frames removed."""
    frames = traceback.extract_tb(exc.__traceback__)
    filtered = [
        f for f in frames
        if not os.path.abspath(f.filename).startswith(_COFLUX_PKG_DIR)
    ]
    if not filtered:
        filtered = list(frames)
    lines = ["Traceback (most recent call last):\n"]
    lines.extend(traceback.format_list(filtered))
    lines.extend(traceback.format_exception_only(type(exc), exc))
    return "".join(lines)


def execute_target(execution_id: str, module_name: str, target_name: str, arguments: list[dict[str, Any]], working_dir: str | None = None) -> None:
    """Execute a target with the given arguments."""
    original_dir = os.getcwd()
    try:
        if working_dir:
            os.chdir(working_dir)

        # Import module and get target
        module = importlib.import_module(module_name)
        target_obj = getattr(module, target_name, None)
        if target_obj is None:
            raise ValueError(f"Target not found: {module_name}/{target_name}")

        # Deserialize arguments
        deserialized_args = [deserialize_value(arg) for arg in arguments]

        # Set up execution context
        set_context(ExecutorContext(execution_id, working_dir=Path(working_dir) if working_dir else None))

        # Call the target function with output capture
        # Note: We call the underlying function, not the Target wrapper
        fn = target_obj.fn if hasattr(target_obj, "fn") else target_obj
        with capture_output(execution_id):
            result = fn(*deserialized_args)

        # Serialize and send result
        result_value = serialize_value(result)
        protocol.send_execution_result(execution_id, result_value)

    except Exception as e:
        # Send error with real exception type and filtered traceback
        error_type = f"{type(e).__module__}.{type(e).__qualname__}"
        tb = _format_filtered_traceback(e)
        protocol.send_execution_error(
            execution_id,
            error_type=error_type,
            message=str(e),
            traceback=tb,
        )
    finally:
        os.chdir(original_dir)
        set_context(None)


def run_executor() -> int:
    """Run the executor loop.

    Returns:
        Exit code (0 for clean shutdown).
    """
    # Signal ready
    protocol.send_ready()

    msg = protocol.receive_message()
    if msg is None:
        # EOF - clean shutdown (e.g., pool is shutting down)
        return 0

    method = msg.get("method")
    if method == "execute":
        params = msg.get("params", {})
        execute_target(
            execution_id=params["execution_id"],
            module_name=params["module"],
            target_name=params["target"],
            arguments=params.get("arguments", []),
            working_dir=params.get("working_dir"),
        )
        return 0

    print(f"Unknown method: {method}", file=sys.stderr)
    return 1
