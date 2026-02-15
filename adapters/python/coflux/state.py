"""Execution context state management.

This module holds the current execution context reference, extracted from
context.py to break circular import dependencies. It has no internal imports.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .context import ExecutorContext

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
