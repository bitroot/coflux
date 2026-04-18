"""Models for the Coflux Python SDK."""

from __future__ import annotations

import fnmatch
import functools
import typing as t
from pathlib import Path

from .state import get_context


T = t.TypeVar("T")
D = t.TypeVar("D")


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


class _Handle(t.Generic[T]):
    """Base for handles that resolve via ``cf.select``.

    Resolved responses are cached on the ``ExecutorContext`` (keyed by
    handle type + id), not on the handle itself — handles stay pure
    reference objects so they're safe to serialize and pass between
    executions.

    Subclasses may set ``_parser`` (typically via ``__class_getitem__``)
    to transform resolved values into typed objects (e.g. a Pydantic
    model instance). When unset, ``.result()`` / ``.poll()`` return the
    raw value. The parser is applied inside the context's resolve path.
    """

    _parser: t.ClassVar[t.Callable[[t.Any], t.Any] | None] = None

    def result(self) -> T:
        """Wait for and return the resolved value (or raise on error)."""
        return get_context().resolve_handle(self)

    @t.overload
    def poll(self, timeout: float | None = None) -> T | None: ...

    @t.overload
    def poll(self, timeout: float | None = None, *, default: D) -> T | D: ...

    def poll(self, timeout: float | None = None, default: t.Any = None) -> t.Any:
        """Return the resolved value if ready, else ``default``."""
        return get_context().poll_handle(self, timeout, default)

    def cancel(self) -> None:
        """Cancel this handle.

        For an ``Execution``, records its result as ``cancelled`` and
        recursively cancels descendants. For an ``Input``, transitions it
        to a terminal ``cancelled`` state (distinct from ``dismissed``).
        No-op if the handle is already resolved.
        """
        get_context().cancel([self])


class Input(_Handle[T]):
    """A handle to a requested input, identified by its external ID.

    Can be passed between executions (only the ID is needed).

    When parameterised with a model class (e.g. ``Input[MyModel]``), the
    model's ``model_validate`` is captured as the handle's parser so that
    ``result()`` / ``poll()`` return an instance of the model.
    """

    _type_arg: type | None = None

    def __class_getitem__(cls, item: type) -> type:
        # Create a subclass that remembers the type argument and (if the
        # type exposes ``model_validate``) captures it as the parser.
        name = getattr(item, "__name__", str(item))
        attrs: dict[str, t.Any] = {"_type_arg": item}
        parser = getattr(item, "model_validate", None)
        if parser is not None:
            attrs["_parser"] = parser
        return type(f"Input[{name}]", (cls,), attrs)

    def __init__(self, input_id: str):
        self._input_id = input_id

    @property
    def id(self) -> str:
        return self._input_id


class Execution(_Handle[T]):
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


class Stream(t.Iterable[T]):
    """A handle to a stream produced by another execution.

    Iterating a ``Stream`` opens a subscription with the server; items arrive
    pushed over the WebSocket and yield from the iterator. Each ``__iter__``
    starts a fresh subscription from position 0, so a stream can be iterated
    multiple times and each iteration sees the whole sequence.

    ``partition`` and ``slice`` return new ``Stream`` views with an additional
    filter; no server round-trip happens until iteration begins.
    """

    def __init__(
        self,
        producer_execution_id: str,
        sequence: int,
        filters: tuple[dict[str, t.Any], ...] = (),
    ):
        self._producer_execution_id = producer_execution_id
        self._sequence = sequence
        self._filters = filters

    @property
    def producer_execution_id(self) -> str:
        return self._producer_execution_id

    @property
    def sequence(self) -> int:
        return self._sequence

    def partition(self, n: int, i: int) -> "Stream[T]":
        """Return a view of this stream where only positions ``p`` with
        ``p % n == i`` are delivered. Round-robin partitioning for parallel
        consumers.
        """
        if n < 1 or i < 0 or i >= n:
            raise ValueError(f"invalid partition args: n={n}, i={i}")
        return Stream(
            self._producer_execution_id,
            self._sequence,
            self._filters + ({"type": "partition", "n": n, "i": i},),
        )

    def slice(self, start: int, stop: int | None = None) -> "Stream[T]":
        """Return a view of this stream restricted to positions ``[start, stop)``.

        ``stop=None`` means unbounded. Equivalent to ``itertools.islice`` on
        the source stream's positions.
        """
        if start < 0 or (stop is not None and stop < start):
            raise ValueError(f"invalid slice args: start={start}, stop={stop}")
        return Stream(
            self._producer_execution_id,
            self._sequence,
            self._filters + ({"type": "slice", "start": start, "stop": stop},),
        )

    def __iter__(self) -> t.Iterator[T]:
        # Deferred import to avoid a cycle (streams.py imports serialization
        # which imports models for Execution/Input/Asset).
        from .streams import open_subscription

        return open_subscription(
            self._producer_execution_id,
            self._sequence,
            self._filters,
        )
