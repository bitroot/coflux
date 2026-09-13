"""Coflux Python adapter for the Go CLI.

This package provides the bridge between the Go CLI and Python code,
handling discovery of @task/@workflow decorated functions and execution
of targets.
"""

from __future__ import annotations

import datetime as dt
import typing as t
from pathlib import Path

from ._version import __version__
from .checkpoint import Checkpoint, flush
from .decorators import stub, task, workflow
from .errors import (
    ExecutionAbandoned,
    ExecutionCancelled,
    ExecutionCrashed,
    ExecutionError,
    ExecutionTerminated,
    ExecutionTimeout,
    InputDismissed,
    RequestError,
    StreamSuperseded,
)
from .metric import Metric, MetricGroup, MetricScale, progress
from .models import (
    Asset,
    AssetEntry,
    AssetMetadata,
    AsyncStreamIterator,
    CatalogEntry,
    Execution,
    Input,
    Stream,
    StreamIterator,
)
from .prompt import Prompt
from .state import get_context
from .streams import stream
from .target import Cache, Defer, Retries, Streams

# Grouped by category rather than sorted alphabetically.
__all__ = [  # noqa: RUF022
    # Version
    "__version__",
    # Decorators
    "task",
    "workflow",
    "stub",
    # Classes
    "Execution",
    "ExecutionError",
    "ExecutionTerminated",
    "ExecutionCancelled",
    "ExecutionTimeout",
    "ExecutionAbandoned",
    "ExecutionCrashed",
    "StreamSuperseded",
    "InputDismissed",
    "RequestError",
    "Input",
    "Metric",
    "MetricGroup",
    "MetricScale",
    "Prompt",
    "Cache",
    "Checkpoint",
    "Defer",
    "Retries",
    "Streams",
    "Asset",
    "AssetEntry",
    "AssetMetadata",
    "CatalogEntry",
    "Stream",
    "StreamIterator",
    "AsyncStreamIterator",
    # Producer-side stream helper
    "stream",
    # Context functions
    "group",
    "suspense",
    "suspend",
    "select",
    "cancel",
    "log_debug",
    "log_info",
    "log_warning",
    "log_error",
    "progress",
    "asset",
    "catalog",
    "publish",
    "flush",
]


# User-facing context functions


def group(name: str | None = None):
    """Context manager for grouping child executions.

    All child executions submitted within this context will be grouped
    together in the UI.

    Example:
        with group("data processing"):
            process_chunk.submit(chunk1)
            process_chunk.submit(chunk2)
    """
    return get_context().group(name)


def suspense(timeout: float | None = None):
    """Context manager for setting timeout on result waits.

    When waiting for execution results within this context, the wait
    will timeout after the specified number of seconds.

    Example:
        with suspense(30):
            result = slow_task.submit().result()  # Will timeout after 30s
    """
    return get_context().suspense(timeout)


_H = t.TypeVar("_H", bound="Execution[t.Any] | Input[t.Any] | CatalogEntry")


def select(
    handles: t.Sequence[_H],
    *,
    cancel_remaining: bool = False,
) -> tuple[_H, list[_H]]:
    """Wait for the first of one or more handles to resolve.

    Args:
        handles: Sequence of Execution, Input and/or CatalogEntry objects.
            Must be non-empty. A CatalogEntry resolves when its path has a
            version this execution hasn't seen — the first, on an empty
            path — and the thing to do when it wins is call ``next()`` on
            it.
        cancel_remaining: If True, cancel non-winner execution handles
            atomically once a handle resolves. Input handles are left
            pending; a catalog handle has nothing to cancel.

    Returns:
        Tuple of ``(winner, remaining)`` where ``winner`` is the first handle
        to resolve — call ``.result()`` on an execution or input to get its
        value or raise its error, or ``next()`` on a catalog entry to re-run
        on the version it saw — and ``remaining`` is the list of handles
        that did not win, in input order.

    Example:
        winner, remaining = cf.select([a.submit(), b.submit(), c.submit()])
        value = winner.result()

    Timeouts are taken from an enclosing ``cf.suspense(timeout=...)`` scope.
    """
    if not handles:
        raise ValueError("select requires at least one handle")

    winner_idx = get_context().select(list(handles), cancel_remaining=cancel_remaining)
    if winner_idx is None:
        raise TimeoutError("timed out waiting for any handle to resolve")

    winner = handles[winner_idx]
    remaining = [h for i, h in enumerate(handles) if i != winner_idx]
    return winner, remaining


def cancel(handles: t.Sequence[Execution[t.Any] | Input[t.Any]]) -> None:
    """Cancel one or more handles (executions and/or inputs) atomically.

    Executions are cancelled recursively (descendants too). Inputs
    transition to a terminal ``cancelled`` state, distinct from
    ``dismissed``. Handles that are already resolved are silently skipped.
    """
    get_context().cancel(handles)


def suspend(delay: float | dt.timedelta | dt.datetime | None = None) -> None:
    """Suspend the current execution.

    The execution will be paused and can be resumed later, optionally
    after the specified delay.

    Args:
        delay: When to resume. Can be:
            - None: Resume immediately when resources available
            - float: Number of seconds to wait
            - timedelta: Duration to wait
            - datetime: Specific time to resume
    """
    get_context().suspend_execution(delay)


def log_debug(template: str | None = None, **kwargs) -> None:
    """Log a debug message.

    Args:
        template: Message template with {placeholders} for kwargs.
        **kwargs: Values to substitute into the template.

    Examples:
        log_debug("Processing item {id}", id=123)
        log_debug(status="complete", count=5)
    """
    get_context().log_message(0, template, **kwargs)


def log_info(template: str | None = None, **kwargs) -> None:
    """Log an info message.

    Args:
        template: Message template with {placeholders} for kwargs.
        **kwargs: Values to substitute into the template.

    Examples:
        log_info("User {name} logged in", name="Alice")
        log_info(event="login", user_id=42)
    """
    get_context().log_message(2, template, **kwargs)


def log_warning(template: str | None = None, **kwargs) -> None:
    """Log a warning message.

    Args:
        template: Message template with {placeholders} for kwargs.
        **kwargs: Values to substitute into the template.

    Examples:
        log_warning("Rate limit approaching: {current}/{max}", current=90, max=100)
    """
    get_context().log_message(4, template, **kwargs)


def log_error(template: str | None = None, **kwargs) -> None:
    """Log an error message.

    Args:
        template: Message template with {placeholders} for kwargs.
        **kwargs: Values to substitute into the template.

    Examples:
        log_error("Failed to process {item}: {error}", item="order-123", error=str(e))
    """
    get_context().log_message(5, template, **kwargs)


def asset(
    entries: (
        str
        | Path
        | list[str | Path]
        | Asset
        | dict[str, str | Path | Asset | AssetEntry]
        | None
    ) = None,
    *,
    at: Path | None = None,
    match: str | None = None,
    name: str | None = None,
) -> Asset:
    """Create and persist an asset from files or existing asset entries.

    Assets are collections of files that can be passed between executions
    and persisted for later retrieval.

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

    Examples:
        # Single file
        asset("output.csv")

        # Multiple files
        asset(["data.csv", "report.pdf"])

        # Pattern matching
        asset(match="*.json", at=Path("./output"))

        # Compose from existing asset entries
        asset({f"{i}.jpg": e.result()["photo.jpg"] for i, e in enumerate(photos)})
    """
    return get_context().create_asset(entries, at=at, match=match, name=name)


def catalog(path: str) -> CatalogEntry:
    """A handle to a path in the catalog, which holds versioned values.

    Nothing round-trips until the handle is used:

        entry = cf.catalog("models/churn")
        entry.current()          # the value, as of this execution's snapshot
        entry.next()             # suspend until there's a newer one; never returns

    Publishing is ``cf.publish(path, value)``. See ``CatalogEntry``.
    """
    return CatalogEntry(path)


def publish(path: str, value: t.Any) -> int:
    """Publish ``value`` at a catalog path and return the version's number.
    An invalid path is a ``ValueError``, as it is for ``cf.catalog``.

    ``value`` is anything that can be passed to a task: an asset, a data
    structure holding assets, a reference to something external, a plain
    number. Facts about a publish — a metric, what it was built from — go
    in the value too, alongside the thing itself. Publishing what is
    already the visible head — the same value — writes nothing and returns
    the existing version's number, which is what makes a publish safe to
    re-run.

    The catalog pins the value, not what the value points at: a handle (an
    execution, an input) resolves to whatever it resolves to when read, and
    a locator for external data is only as stable as that data.
    """
    return get_context().catalog_publish(path, value)
