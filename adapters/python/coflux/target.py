"""Target class, configuration objects, and definition types for Coflux tasks and workflows."""

from __future__ import annotations

import dataclasses
import datetime as dt
import functools
import inspect
import json
import re
import typing as t

from .models import Execution
from .serialization import serialize_value
from .state import get_context

P = t.ParamSpec("P")
T = t.TypeVar("T")

# Type definitions
TargetType = t.Literal["workflow", "task"]
Requires = dict[str, list[str]]


@dataclasses.dataclass(frozen=True)
class Cache:
    max_age: float | dt.timedelta | None = None
    _: dataclasses.KW_ONLY
    params: t.Iterable[str] | str | None = None
    namespace: str | None = None
    version: str | None = None


@dataclasses.dataclass(frozen=True)
class Defer:
    _: dataclasses.KW_ONLY
    params: t.Iterable[str] | str | None = None


@dataclasses.dataclass(frozen=True)
class Retries:
    limit: int | None = None
    _: dataclasses.KW_ONLY
    backoff: tuple[float | dt.timedelta, float | dt.timedelta] = (1, 60)
    when: (
        type[BaseException]
        | tuple[type[BaseException], ...]
        | t.Callable[[BaseException], bool]
        | None
    ) = None


class Parameter(t.NamedTuple):
    name: str
    annotation: str | None
    default: str | None


class TargetDefinition(t.NamedTuple):
    type: TargetType
    parameters: list[Parameter]
    wait_for: set[int]
    cache: Cache | None
    defer: Defer | None
    delay: float | dt.timedelta
    retries: Retries | None
    recurrent: bool
    memo: list[int] | bool
    requires: Requires | None
    timeout: float | dt.timedelta | None
    instruction: str | None
    is_stub: bool


def _json_dumps(obj: t.Any) -> str:
    return json.dumps(obj, separators=(",", ":"))


def _build_parameter(parameter: inspect.Parameter) -> Parameter:
    return Parameter(
        parameter.name,
        (
            str(parameter.annotation)
            if parameter.annotation != inspect.Parameter.empty
            else None
        ),
        (
            _json_dumps(parameter.default)
            if parameter.default != inspect.Parameter.empty
            else None
        ),
    )


def _get_param_indexes(
    parameters: list[Parameter],
    names: t.Iterable[str] | str,
) -> list[int]:
    if isinstance(names, str):
        names = re.split(r",\s*", names)
    indexes = []
    parameter_names = [p.name for p in parameters]
    for name in names:
        if name not in parameter_names:
            raise Exception(f"Unrecognised parameter in wait ({name})")
        indexes.append(parameter_names.index(name))
    return indexes


def _parse_wait(
    wait: bool | t.Iterable[str] | str, parameters: list[Parameter]
) -> set[int]:
    if wait is True:
        return set(range(len(parameters)))
    if wait is False:
        return set()
    return set(_get_param_indexes(parameters, wait))


def _expand_cache(cache: bool | float | dt.timedelta | Cache) -> Cache | None:
    if not cache:
        return None
    if isinstance(cache, Cache):
        return cache
    # bool, float, or timedelta — treat as max_age shorthand
    return Cache(max_age=None if isinstance(cache, bool) else cache)


def _normalize_retry_when(
    when: type[BaseException]
    | tuple[type[BaseException], ...]
    | t.Callable[[BaseException], bool]
    | None,
) -> t.Callable[[BaseException], bool] | None:
    if when is None:
        return None
    if isinstance(when, type) and issubclass(when, BaseException):
        return lambda e, _cls=when: isinstance(e, _cls)
    if isinstance(when, tuple) and all(
        isinstance(c, type) and issubclass(c, BaseException) for c in when
    ):
        return lambda e, _classes=when: isinstance(e, _classes)
    if callable(when):
        return when
    raise TypeError(
        f"Invalid 'when' argument: expected exception type(s) or callable, got {type(when).__name__}"
    )


def _expand_retries(retries: int | bool | Retries) -> Retries | None:
    if retries is False or retries == 0:
        return None
    if retries is True:
        return Retries()
    if isinstance(retries, int):
        return Retries(limit=retries, backoff=(0, 0))
    if isinstance(retries, Retries):
        if retries.limit == 0:
            return None
        return Retries(
            limit=retries.limit,
            backoff=retries.backoff,
            when=_normalize_retry_when(retries.when),
        )


def _expand_defer(defer: bool | Defer) -> Defer | None:
    if not defer:
        return None
    if isinstance(defer, Defer):
        return defer
    return Defer()


def _parse_memo(
    memo: bool | t.Iterable[str] | str, parameters: list[Parameter]
) -> list[int] | bool:
    if isinstance(memo, bool):
        return memo
    return _get_param_indexes(parameters, memo)


def _parse_require(value: str | bool | list[str]):
    if isinstance(value, bool):
        return ["true"] if value else ["false"]
    elif isinstance(value, str):
        return [value]
    else:
        return value


def _parse_requires(
    requires: dict[str, str | bool | list[str]] | None,
) -> Requires | None:
    return {k: _parse_require(v) for k, v in requires.items()} if requires else None


def _build_definition(
    type: TargetType,
    fn: t.Callable,
    wait: bool | t.Iterable[str] | str,
    cache: bool | float | dt.timedelta | Cache,
    retries: int | bool | Retries,
    recurrent: bool,
    defer: bool | Defer,
    delay: float | dt.timedelta,
    memo: bool | t.Iterable[str] | str,
    requires: dict[str, str | bool | list[str]] | None,
    timeout: float | dt.timedelta | None,
    is_stub: bool,
) -> TargetDefinition:
    parameters = inspect.signature(fn).parameters.values()
    for p in parameters:
        if p.kind != inspect.Parameter.POSITIONAL_OR_KEYWORD:
            raise Exception(f"Unsupported parameter type ({p.kind})")
    parameters_ = [_build_parameter(p) for p in parameters]
    return TargetDefinition(
        type,
        parameters_,
        _parse_wait(wait, parameters_),
        _expand_cache(cache),
        _expand_defer(defer),
        delay,
        _expand_retries(retries),
        recurrent,
        _parse_memo(memo, parameters_),
        _parse_requires(requires),
        timeout,
        inspect.getdoc(fn),
        is_stub,
    )


# --- Serialization to wire format ---


def _to_ms(value: float | dt.timedelta) -> int:
    if isinstance(value, dt.timedelta):
        return int(value.total_seconds() * 1000)
    return int(value * 1000)


def _param_indexes(
    params: t.Iterable[str] | str | None,
    parameters: list[Parameter],
) -> list[int] | t.Literal[True]:
    if params is None:
        return True
    return _get_param_indexes(parameters, params)


def serialize_cache(cache: Cache, parameters: list[Parameter]) -> dict:
    result: dict[str, t.Any] = {
        "params": _param_indexes(cache.params, parameters),
    }
    if cache.max_age is not None:
        result["max_age_ms"] = _to_ms(cache.max_age)
    if cache.namespace:
        result["namespace"] = cache.namespace
    if cache.version:
        result["version"] = cache.version
    return result


def serialize_defer(defer: Defer, parameters: list[Parameter]) -> dict:
    return {"params": _param_indexes(defer.params, parameters)}


def serialize_retries(retries: Retries) -> dict:
    result: dict[str, t.Any] = {}
    if retries.limit is not None:
        result["limit"] = retries.limit
    backoff_min_ms = _to_ms(retries.backoff[0])
    backoff_max_ms = _to_ms(retries.backoff[1])
    if backoff_min_ms:
        result["backoff_min_ms"] = backoff_min_ms
    if backoff_max_ms:
        result["backoff_max_ms"] = backoff_max_ms
    return result


class Target(t.Generic[P, T]):
    """Wrapper for a decorated task or workflow function."""

    def __init__(
        self,
        fn: t.Callable[P, T],
        type: TargetType,
        *,
        module: str | None = None,
        name: str | None = None,
        wait: bool | t.Iterable[str] | str = False,
        cache: bool | float | dt.timedelta | Cache = False,
        retries: int | bool | Retries = 0,
        recurrent: bool = False,
        defer: bool | Defer = False,
        delay: float | dt.timedelta = 0,
        memo: bool | t.Iterable[str] | str = False,
        requires: dict[str, str | bool | list[str]] | None = None,
        timeout: float | dt.timedelta | None = None,
        is_stub: bool = False,
    ):
        self._fn = fn
        self._name = name or fn.__name__
        self._module = module or fn.__module__
        self._definition = _build_definition(
            type,
            fn,
            wait,
            cache,
            retries,
            recurrent,
            defer,
            delay,
            memo,
            requires,
            timeout,
            is_stub,
        )
        functools.update_wrapper(self, fn)

    @property
    def name(self) -> str:
        return self._name

    @property
    def module(self) -> str:
        return self._module

    @property
    def definition(self) -> TargetDefinition:
        return self._definition

    @property
    def fn(self) -> t.Callable[P, T]:
        return self._fn

    def submit(self, *args: P.args, **kwargs: P.kwargs) -> Execution[T]:
        """Submit this target for execution and return a handle."""
        if kwargs:
            raise ValueError("Keyword arguments not yet supported")

        ctx = get_context()

        # Serialize arguments
        serialized_args = [serialize_value(arg) for arg in args]

        # Use only the declared wait_for from the decorator
        wait_for_val = (
            sorted(self._definition.wait_for) if self._definition.wait_for else None
        )

        # Serialize config objects to wire format
        parameters = self._definition.parameters

        cache_dict = (
            serialize_cache(self._definition.cache, parameters)
            if self._definition.cache
            else None
        )
        defer_dict = (
            serialize_defer(self._definition.defer, parameters)
            if self._definition.defer
            else None
        )
        retries_dict = (
            serialize_retries(self._definition.retries)
            if self._definition.retries
            else None
        )

        # Get memo value (bool or list of indices)
        memo_val = self._definition.memo if self._definition.memo else None

        # Submit via context with all target definition fields
        result = ctx.submit_execution(
            self._module,
            self._name,
            serialized_args,
            type=self._definition.type,
            wait_for=wait_for_val,
            cache=cache_dict,
            defer=defer_dict,
            memo=memo_val,
            delay=_to_ms(self._definition.delay) if self._definition.delay else None,
            retries=retries_dict,
            recurrent=self._definition.recurrent,
            requires=self._definition.requires,
            timeout=_to_ms(self._definition.timeout) if self._definition.timeout else None,
        )
        return Execution(result["execution_id"], result["module"], result["target"])

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """Call the target - submits for execution and waits for the result."""
        return self.submit(*args, **kwargs).result()
