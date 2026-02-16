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
from .serialization import deserialize_argument, serialize_result


def execute_target(execution_id: str, target: str, arguments: list[dict[str, Any]], working_dir: str | None = None) -> None:
    """Execute a target with the given arguments."""
    original_dir = os.getcwd()
    try:
        if working_dir:
            os.chdir(working_dir)

        # Parse target name (module.name)
        parts = target.rsplit(".", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid target name: {target}")
        module_name, target_name = parts

        # Import module and get target
        module = importlib.import_module(module_name)
        target_obj = getattr(module, target_name, None)
        if target_obj is None:
            raise ValueError(f"Target not found: {target}")

        # Deserialize arguments
        deserialized_args = [deserialize_argument(arg) for arg in arguments]

        # Set up execution context
        set_context(ExecutorContext(execution_id, working_dir=Path(working_dir) if working_dir else None))

        # Call the target function with output capture
        # Note: We call the underlying function, not the Target wrapper
        fn = target_obj.fn if hasattr(target_obj, "fn") else target_obj
        with capture_output(execution_id):
            result = fn(*deserialized_args)

        # Serialize and send result
        result_value = serialize_result(result)
        protocol.send_execution_result(execution_id, result_value)

    except Exception as e:
        # Send error
        tb = traceback.format_exc()
        protocol.send_execution_error(
            execution_id,
            error_type="exception",
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

    while True:
        msg = protocol.receive_message()
        if msg is None:
            # EOF - clean shutdown
            return 0

        method = msg.get("method")
        if method == "execute":
            params = msg.get("params", {})
            execute_target(
                execution_id=params["execution_id"],
                target=params["target"],
                arguments=params.get("arguments", []),
                working_dir=params.get("working_dir"),
            )
        else:
            print(f"Unknown method: {method}", file=sys.stderr)

    return 0
