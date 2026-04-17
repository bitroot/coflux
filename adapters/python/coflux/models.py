"""Models for the Coflux Python SDK."""

from __future__ import annotations

import fnmatch
import functools
import typing as t
from pathlib import Path
from .state import get_context


T = t.TypeVar("T")
D = t.TypeVar("D")

_MISSING = object()


class ModelSchema(t.Protocol):
    """Protocol for schema classes that can validate JSON data.

    Compatible with Pydantic BaseModel and any class providing
    model_json_schema() and model_validate() classmethods.
    """

    @classmethod
    def model_json_schema(cls) -> dict[str, t.Any]: ...

    @classmethod
    def model_validate(cls, obj: t.Any) -> t.Any: ...


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


class Input(t.Generic[T]):
    """A handle to a requested input, identified by its external ID.

    Can be passed between executions (only the ID is needed).

    When parameterised with a model class (e.g. ``Input[MyModel]``), the
    type argument is preserved at runtime so that ``result()`` and ``poll()``
    can automatically validate the response using ``model_validate``.
    """

    _type_arg: type | None = None

    def __class_getitem__(cls, item: type) -> type:
        # Create a subclass that remembers the type argument at runtime.
        # This means Input[MyModel](id).result() can call MyModel.model_validate().
        name = getattr(item, "__name__", str(item))
        return type(f"Input[{name}]", (cls,), {"_type_arg": item})

    def __init__(self, input_id: str):
        self._input_id = input_id

    @property
    def id(self) -> str:
        return self._input_id

    def result(self) -> T:
        """Wait for and return the input response.

        Records a dependency and blocks until the response is available,
        suspending the execution if necessary. If this Input was
        parameterised with a model class, the response is validated
        and returned as an instance of that model.
        """
        ctx = get_context()
        value = ctx.resolve_input(self._input_id)
        if self._type_arg is not None and hasattr(self._type_arg, "model_validate"):
            return self._type_arg.model_validate(value)
        return value

    @t.overload
    def poll(self, timeout: float | None = None) -> T | None: ...

    @t.overload
    def poll(self, timeout: float | None = None, *, default: D) -> T | D: ...

    def poll(self, timeout: float | None = None, default: t.Any = None) -> t.Any:
        """Poll for the input response without suspending.

        Returns the response if available, or ``default`` if not ready yet.
        """
        ctx = get_context()
        timeout_ms = int(timeout * 1000) if timeout else None
        value = ctx.poll_input(
            self._input_id,
            timeout_ms,
            default=_MISSING,
        )
        if value is _MISSING:
            return default
        if self._type_arg is not None and hasattr(self._type_arg, "model_validate"):
            return self._type_arg.model_validate(value)
        return value


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

    @t.overload
    def poll(self, timeout: float | None = None) -> T | None: ...

    @t.overload
    def poll(self, timeout: float | None = None, *, default: D) -> T | D: ...

    def poll(self, timeout: float | None = None, default: t.Any = None) -> t.Any:
        """Poll for the execution result without suspending.

        Returns the result if available, or ``default`` if not ready yet.
        """
        ctx = get_context()
        timeout_ms = int(timeout * 1000) if timeout else None
        return ctx.poll_execution(self._execution_id, timeout_ms, default=default)

    def cancel(self) -> None:
        """Cancel this execution."""
        ctx = get_context()
        ctx.cancel_execution(self._execution_id)
