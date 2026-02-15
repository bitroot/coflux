"""Models for the Coflux Python SDK."""

from __future__ import annotations

import fnmatch
import functools
import typing as t
from pathlib import Path
from .state import get_context


T = t.TypeVar("T")


class AssetEntry(t.NamedTuple):
    """An entry within an asset (a single file)."""

    path: str
    blob_key: str
    size: int
    metadata: dict[str, t.Any]

    def restore(self, *, at: Path | str | None = None) -> Path:
        """Download and restore this entry to the filesystem.

        Args:
            at: Base directory to restore to. Defaults to current directory.

        Returns:
            Path to the restored file.
        """
        ctx = get_context()
        base_path = Path(at).resolve() if at else Path.cwd()
        target = base_path / self.path
        target.parent.mkdir(parents=True, exist_ok=True)
        ctx.download_blob(self.blob_key, target)
        return target


class AssetMetadata(t.NamedTuple):
    """Metadata for an asset reference."""

    name: str | None = None
    total_count: int | None = None
    total_size: int | None = None


class Asset:
    """A collection of files persisted as an asset."""

    def __init__(
        self,
        id: str,
        metadata: AssetMetadata | None = None,
    ):
        self._id = id
        self._metadata = metadata or AssetMetadata()

    @property
    def id(self) -> str:
        return self._id

    @property
    def metadata(self) -> AssetMetadata:
        return self._metadata

    @functools.cached_property
    def entries(self) -> list[AssetEntry]:
        """Get all entries in this asset."""
        ctx = get_context()
        return ctx.get_asset_entries(self._id)

    def __getitem__(self, path: str) -> AssetEntry:
        """Get an entry by path."""
        entry = next((e for e in self.entries if e.path == path), None)
        if not entry:
            raise KeyError(path)
        return entry

    def __contains__(self, path: str) -> bool:
        """Check if an entry exists at the given path."""
        return any(e.path == path for e in self.entries)

    def restore(
        self,
        *,
        match: str | None = None,
        at: Path | str | None = None,
    ) -> dict[str, Path]:
        """Restore all entries (or matching entries) to the filesystem.

        Args:
            match: Glob pattern to filter entries.
            at: Base directory to restore to. Defaults to current directory.

        Returns:
            Mapping of entry paths to restored file paths.
        """
        entries = self.entries
        if match:
            entries = [e for e in entries if fnmatch.fnmatch(e.path, match)]
        return {e.path: e.restore(at=at) for e in entries}


class Execution(t.Generic[T]):
    """A handle to a submitted execution that can be awaited for its result."""

    def __init__(self, execution_id: str, module: str, target: str):
        self._execution_id = execution_id
        self._module = module
        self._target = target

    @property
    def id(self) -> str:
        return self._execution_id

    @property
    def module(self) -> str:
        return self._module

    @property
    def target(self) -> str:
        return self._target

    def result(self) -> T:
        """Wait for and return the execution result."""
        ctx = get_context()
        return ctx.resolve_execution(self._execution_id)

    def cancel(self) -> None:
        """Cancel this execution."""
        ctx = get_context()
        ctx.cancel_execution(self._execution_id)
