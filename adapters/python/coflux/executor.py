"""Executor process that runs targets and communicates via stdio."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import os
import sys
import traceback
from pathlib import Path
from typing import Any, get_type_hints

from . import protocol
from .context import ExecutorContext
from .dispatcher import start_dispatcher
from .state import set_context
from .output import capture_output
from .models import Input
from .serialization import deserialize_value, serialize_value

_COFLUX_PKG_DIR = os.path.dirname(os.path.abspath(__file__))


def _format_filtered_traceback(exc: Exception) -> str:
    """Format a traceback string with Coflux-internal frames removed."""
    frames = traceback.extract_tb(exc.__traceback__)
    filtered = [
        f for f in frames if not os.path.abspath(f.filename).startswith(_COFLUX_PKG_DIR)
    ]
    if not filtered:
        filtered = list(frames)
    lines = ["Traceback (most recent call last):\n"]
    lines.extend(traceback.format_list(filtered))
    lines.extend(traceback.format_exception_only(type(exc), exc))
    return "".join(lines)


def _apply_type_hints(fn: Any, args: list[Any]) -> list[Any]:
    """Upgrade deserialized args using the function's type hints.

    Plain ``Input`` objects lose their type parameter during serialization.
    If the function annotates a parameter as ``Input[Model]``, reconstruct
    the parameterized Input so that ``result()`` returns a validated model.
    """
    try:
        hints = get_type_hints(fn)
    except Exception:
        return args
    params = list(inspect.signature(fn).parameters.keys())
    for i, (arg, name) in enumerate(zip(args, params)):
        if not isinstance(arg, Input) or arg._type_arg is not None:
            continue
        hint = hints.get(name)
        if hint is None:
            continue
        # Input.__class_getitem__ creates a subclass with _type_arg set,
        # so Input[Model] is a subclass of Input (not a typing alias).
        if isinstance(hint, type) and issubclass(hint, Input):
            type_arg = getattr(hint, "_type_arg", None)
            if type_arg is not None:
                args[i] = Input[type_arg](arg.id)
    return args


def execute_target(
    execution_id: str,
    module_name: str,
    target_name: str,
    arguments: list[dict[str, Any]],
    working_dir: str | None = None,
) -> None:
    """Execute a target with the given arguments."""
    original_dir = os.getcwd()
    # Start the stdin dispatcher. From here on, all incoming messages flow
    # through it — individual threads block on the dispatcher rather than
    # racing on stdin directly.
    start_dispatcher(protocol.get_protocol())
    ctx: ExecutorContext | None = None
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

        # Call the target function with output capture
        # Note: We call the underlying function, not the Target wrapper
        fn = target_obj.fn if hasattr(target_obj, "fn") else target_obj

        # Upgrade Input objects using type hints (restores lost type params)
        deserialized_args = _apply_type_hints(fn, deserialized_args)

        # Set up execution context
        ctx = ExecutorContext(
            execution_id, working_dir=Path(working_dir) if working_dir else None
        )
        set_context(ctx)

        with capture_output(execution_id):
            if inspect.iscoroutinefunction(fn):
                # Native async def targets: run the coroutine to completion.
                # Each execution is its own OS process with nothing else
                # scheduled, so a fresh event loop per call is fine.
                result = asyncio.run(fn(*deserialized_args))
            else:
                result = fn(*deserialized_args)

        # Serialize result. Generators anywhere in the return value (or that
        # were passed to submitted child executions as args) have already
        # been registered with the context's stream driver.
        result_value = serialize_value(result, on_generator=ctx.register_stream)
        protocol.send_execution_result(execution_id, result_value)

        # Hold the process open until every stream has drained. Thread
        # safety: stdin access goes through the dispatcher (so subtask
        # calls from generator bodies don't race), and stdout writes are
        # serialised by Protocol._write_lock.
        ctx.wait_streams()

    except Exception as e:
        # Evaluate retry 'when' callback if present
        # None = no callback configured, True/False = callback result
        # If the callback raises, report that exception instead (makes bugs visible)
        retryable = None
        if hasattr(target_obj, "definition"):
            retries = target_obj.definition.retries
            if retries and retries.when is not None:
                try:
                    retryable = bool(retries.when(e))
                except Exception as callback_exc:
                    e = callback_exc

        # Stop any in-flight stream producers and wait for their driver
        # threads to exit before reporting the execution error. The server's
        # close_open_streams will then synthesise a Coflux.ExecutionErrored
        # close for any streams still open when the error is recorded.
        if ctx is not None:
            try:
                ctx.close_streams()
                ctx.wait_streams()
            except Exception:
                pass

        error_type = f"{type(e).__module__}.{type(e).__qualname__}"
        tb = _format_filtered_traceback(e)
        protocol.send_execution_error(
            execution_id,
            error_type=error_type,
            message=str(e),
            traceback=tb,
            retryable=retryable,
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
