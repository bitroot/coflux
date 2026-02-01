import functools
import typing as t
from pathlib import Path

from . import types, utils

T = t.TypeVar("T")


class Parameter(t.NamedTuple):
    name: str
    annotation: str | None
    default: str | None


class Cache(t.NamedTuple):
    params: list[int] | t.Literal[True]
    max_age: int | None
    namespace: str | None
    version: str | None


class Defer(t.NamedTuple):
    params: list[int] | t.Literal[True]


class Retries(t.NamedTuple):
    limit: int | None = None  # 0 = no retries, None = unlimited
    delay_min: float = 1      # seconds
    delay_max: float = 60     # seconds


class Target(t.NamedTuple):
    type: types.TargetType
    parameters: list[Parameter]
    wait_for: set[int]
    cache: Cache | None
    defer: Defer | None
    delay: float
    retries: Retries | None
    recurrent: bool
    memo: list[int] | bool
    requires: types.Requires | None
    instruction: str | None
    is_stub: bool


# TODO: better way to avoid circular dependency?
def _get_channel():
    from . import execution

    return execution.get_channel()


class ExecutionMetadata(t.NamedTuple):
    """Metadata for an execution reference, used in log serialization."""

    run_id: str | None = None
    step_id: str | None = None
    attempt: int | None = None
    module: str | None = None
    target: str | None = None


class Execution[T]:
    """Reference to a step execution."""

    def __init__(
        self,
        id: str,
        metadata: ExecutionMetadata | None = None,
    ):
        self._id = id
        self._metadata = metadata or ExecutionMetadata()

    @property
    def id(self) -> str:
        return self._id

    @property
    def metadata(self) -> ExecutionMetadata:
        return self._metadata

    def result(self) -> T:
        return _get_channel().resolve_reference(self._id)

    def cancel(self) -> None:
        _get_channel().cancel_execution(self._id)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Execution):
            return self._id == other._id
        return False

    def __hash__(self) -> int:
        return hash(self._id)


# TODO: (make non-tuple?)
class AssetEntry(t.NamedTuple):
    path: str
    blob_key: str
    size: int
    metadata: dict[str, t.Any]

    def restore(self, *, at: Path | str | None = None) -> Path:
        base_path = Path(at).resolve() if at else Path.cwd()
        target = base_path.joinpath(self.path)
        target.parent.mkdir(parents=True, exist_ok=True)
        _get_channel().download_blob(self.blob_key, target)
        return target


class AssetMetadata(t.NamedTuple):
    """Metadata for an asset reference."""

    name: str | None = None
    total_count: int | None = None
    total_size: int | None = None


class Asset:
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
        return _get_channel().resolve_asset(self._id)

    def __getitem__(self, path: str) -> AssetEntry:
        entry = next((e for e in self.entries if e.path == path), None)
        if not entry:
            raise KeyError(path)
        return entry

    def __contains__(self, path: str) -> bool:
        return any(e.path == path for e in self.entries)

    def restore(
        self,
        *,
        match: str | None = None,
        at: Path | str | None = None,
    ) -> dict[str, Path]:
        # TODO: parallelise
        matcher = utils.GlobMatcher(match) if match else None
        return {
            e.path: e.restore(at=at)
            for e in self.entries
            if matcher is None or matcher.match(e.path)
        }
