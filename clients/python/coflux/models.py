import typing as t
from pathlib import Path
import functools

T = t.TypeVar("T")


TargetType = t.Literal["workflow", "task", "sensor"]


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
    limit: int
    delay_min: int
    delay_max: int


Requires = dict[str, list[str]]


class Target(t.NamedTuple):
    type: TargetType
    parameters: list[Parameter]
    wait_for: set[int]
    cache: Cache | None
    defer: Defer | None
    delay: float
    retries: Retries | None
    memo: list[int] | bool
    requires: Requires | None
    instruction: str | None
    is_stub: bool


Metadata = dict[str, t.Any]

Reference = (
    tuple[t.Literal["execution"], int]
    | tuple[t.Literal["asset"], int]
    | tuple[t.Literal["fragment"], str, str, int, dict[str, t.Any]]
)

Value = t.Union[
    tuple[t.Literal["raw"], t.Any, list[Reference]],
    tuple[t.Literal["blob"], str, int, list[Reference]],
]

Result = t.Union[
    tuple[t.Literal["value"], Value],
    tuple[t.Literal["error"], str, str],
    tuple[t.Literal["abandoned"]],
    tuple[t.Literal["cancelled"]],
]


class Execution(t.Generic[T]):
    def __init__(
        self,
        resolve_fn: t.Callable[[], T],
        cancel_fn: t.Callable[[], None],
        id: int | None,
    ):
        self._resolve_fn = resolve_fn
        self._cancel_fn = cancel_fn
        self._id = id

    @property
    def id(self) -> int | None:
        return self._id

    def result(self) -> T:
        return self._resolve_fn()

    def cancel(self) -> None:
        self._cancel_fn()


class Asset:
    def __init__(
        self,
        resolve_fn: t.Callable[[], dict[str, str]],
        download_fn: t.Callable[[str, Path], None],
        id: int,
    ):
        self._resolve_fn = resolve_fn
        self._download_fn = download_fn
        self._id = id

    @property
    def id(self) -> int:
        return self._id

    @functools.cached_property
    def _entries(self):
        return self._resolve_fn()

    def restore(self, *, match: str = "*", at: Path | str | None = None) -> dict[str, Path]:
        if at:
            if isinstance(at, str):
                at = Path(at)
            at = at.resolve()
        base_path = at or Path.cwd()
        result = {}
        for path_str, blob_key in self._entries.items():
            # TODO: filter using `match`
            target = base_path.joinpath(path_str)
            target.parent.mkdir(parents=True, exist_ok=True)
            self._download_fn(blob_key, target)
            result[path_str] = target
        return result
