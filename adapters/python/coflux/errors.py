"""Exception types for Coflux execution errors."""

from __future__ import annotations

import importlib


class ExecutionError(Exception):
    """Raised when a child execution failed.

    When the original exception type can be resolved, dynamically-created
    subclasses inherit from both ExecutionError and the original type,
    so callers can catch either::

        except ZeroDivisionError:  # catches the specific type
        except ExecutionError:      # catches any execution failure
    """

    def __init__(self, message: str, error_type: str = "", error_message: str = ""):
        self.error_type = error_type
        self.error_message = error_message
        super().__init__(message)


class ExecutionTerminated(Exception):
    """Base class for orchestration-initiated terminations.

    Distinct from ExecutionError, which wraps a user exception raised by
    the dependency's own code. Subclasses here represent reasons the
    server decided the dependency would not complete normally (cancelled,
    timed out, abandoned, crashed).

    Catch ExecutionTerminated to handle any of these uniformly::

        except ExecutionTerminated:
            # any server-initiated termination
            ...

    Or catch specific subclasses to distinguish causes.
    """


class ExecutionCancelled(ExecutionTerminated):
    """Raised when a child execution was cancelled."""

    def __init__(self, message: str = "execution was cancelled"):
        super().__init__(message)


class ExecutionTimeout(ExecutionTerminated):
    """Raised when a child execution timed out."""

    def __init__(self, message: str = "execution timed out"):
        super().__init__(message)


class ExecutionAbandoned(ExecutionTerminated):
    """Raised when a child execution was abandoned.

    The server gave up on the execution — typically because the worker
    session expired (heartbeat missed) or the session was removed.
    """

    def __init__(self, message: str = "execution was abandoned"):
        super().__init__(message)


class ExecutionCrashed(ExecutionTerminated):
    """Raised when a child execution's worker terminated without reporting.

    The worker process ended (sent notify_terminated) but never reported
    a result or error — typically indicates a process crash or shutdown
    that didn't give the task code a chance to report.
    """

    def __init__(self, message: str = "worker terminated without reporting a result"):
        super().__init__(message)


class InputDismissed(Exception):
    """Raised when an input request was dismissed."""

    def __init__(self, message: str = "input was dismissed"):
        super().__init__(message)


def _resolve_exception_class(qualified_name: str) -> type | None:
    """Attempt to resolve a fully qualified exception type name to a class.

    Tries progressively shorter module paths to handle nested classes
    (e.g., ``myapp.MyClass.InnerError`` -> import ``myapp``, getattr
    ``MyClass``, getattr ``InnerError``).
    """
    if not qualified_name or "." not in qualified_name:
        return None

    parts = qualified_name.split(".")
    for i in range(len(parts) - 1, 0, -1):
        module_name = ".".join(parts[:i])
        try:
            module = importlib.import_module(module_name)
        except (ImportError, ModuleNotFoundError):
            continue
        obj = module
        try:
            for attr in parts[i:]:
                obj = getattr(obj, attr)
        except AttributeError:
            continue
        if isinstance(obj, type) and issubclass(obj, Exception):
            return obj
    return None


_error_class_cache: dict[str, type] = {}


def create_execution_error(error_type: str, error_message: str) -> ExecutionError:
    """Create an ExecutionError, trying to also subclass the original exception type.

    Falls back to plain ExecutionError if the original type cannot be resolved
    or its constructor is incompatible.
    """
    original_class = _resolve_exception_class(error_type)

    if original_class is None or original_class is Exception:
        return ExecutionError(
            error_message,
            error_type=error_type,
            error_message=error_message,
        )

    if error_type not in _error_class_cache:
        short_name = error_type.rsplit(".", 1)[-1]
        try:
            dynamic_cls = type(
                short_name,
                (ExecutionError, original_class),
                {},
            )
            dynamic_cls.__module__ = original_class.__module__
            dynamic_cls.__qualname__ = original_class.__qualname__
            _error_class_cache[error_type] = dynamic_cls
        except TypeError:
            return ExecutionError(
                error_message,
                error_type=error_type,
                error_message=error_message,
            )

    cls = _error_class_cache[error_type]
    try:
        return cls(
            error_message,
            error_type=error_type,
            error_message=error_message,
        )
    except Exception:
        return ExecutionError(
            error_message,
            error_type=error_type,
            error_message=error_message,
        )
