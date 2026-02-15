"""Coflux Python adapter for the Go CLI.

This package provides the bridge between the Go CLI and Python code,
handling discovery of @task/@workflow decorated functions and execution
of targets.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from .context import get_context
from .decorators import task, workflow, stub, Target, Execution, Retries
from .models import Asset, AssetEntry, AssetMetadata, ExecutionMetadata

__version__ = "0.1.0"

__all__ = [
    # Decorators
    "task",
    "workflow",
    "stub",
    # Classes
    "Target",
    "Execution",
    "Retries",
    "Asset",
    "AssetEntry",
    "AssetMetadata",
    "ExecutionMetadata",
    # Context functions
    "group",
    "suspense",
    "suspend",
    "log_debug",
    "log_info",
    "log_warning",
    "log_error",
    "asset",
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
