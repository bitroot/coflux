"""
Decorators for defining Coflux tasks and workflows.

This is a standalone copy of the decorators from the main coflux package,
used by the adapter for discovery without requiring the full package.
"""

from __future__ import annotations

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


class Parameter(t.NamedTuple):
    name: str
    annotation: str | None
    default: str | None


class Cache(t.NamedTuple):
    max_age: float | dt.timedelta | None = None  # seconds
    params: t.Iterable[str] | str | None = None
    namespace: str | None = None
    version: str | None = None


class Defer(t.NamedTuple):
    params: t.Iterable[str] | str | None = None


class Retries(t.NamedTuple):
    limit: int | None = None  # 0 = no retries, None = unlimited
    backoff: tuple[float, float] = (1, 60)  # (min, max) seconds
    when: type[BaseException] | tuple[type[BaseException], ...] | t.Callable[[BaseException], bool] | None = None


class TargetDefinition(t.NamedTuple):
    type: TargetType
    parameters: list[Parameter]
    wait_for: set[int]
    cache: Cache | None
    defer: Defer | None
    delay: float
    retries: Retries | None
    recurrent: bool
    memo: list[int] | bool
    requires: Requires | None
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


def _parse_wait(
    wait: bool | t.Iterable[str] | str, parameters: list[Parameter]
) -> set[int]:
    if wait is True:
        return set(range(len(parameters)))
    if wait is False:
        return set()
    return set(_get_param_indexes(parameters, wait))


def _parse_max_age(max_age: float | dt.timedelta | None) -> int | None:
    if max_age is None:
        return None
    if isinstance(max_age, dt.timedelta):
        return int(max_age.total_seconds() * 1000)
    return int(max_age * 1000)


def _parse_cache(
    cache: bool | float | dt.timedelta | Cache,
    parameters: list[Parameter],
) -> Cache | None:
    if not cache:
        return None
    match cache:
        case Cache(max_age, params, namespace, version):
            return Cache(
                _parse_max_age(max_age),
                True if params is None else _get_param_indexes(parameters, params),
                namespace,
                version,
            )
        case _:
            # bool or float or timedelta — treat as max_age shorthand
            return Cache(
                _parse_max_age(None if isinstance(cache, bool) else cache),
                True,
                None,
                None,
            )


def _normalize_retry_when(
    when: type[BaseException] | tuple[type[BaseException], ...] | t.Callable[[BaseException], bool] | None,
) -> t.Callable[[BaseException], bool] | None:
    if when is None:
        return None
    if isinstance(when, type) and issubclass(when, BaseException):
        return lambda e, _cls=when: isinstance(e, _cls)
    if isinstance(when, tuple) and all(isinstance(c, type) and issubclass(c, BaseException) for c in when):
        return lambda e, _classes=when: isinstance(e, _classes)
    if callable(when):
        return when
    raise TypeError(f"Invalid 'when' argument: expected exception type(s) or callable, got {type(when).__name__}")


def _parse_retries(
    retries: int | bool | Retries,
) -> Retries | None:
    match retries:
        case False | 0:
            return None
        case True:
            # Unlimited with sensible defaults (1s-60s backoff)
            return Retries(None, (1000, 60000))
        case int(limit):
            return Retries(limit, (0, 0))
        case Retries(limit, (backoff_min, backoff_max), when):
            if limit == 0:
                return None
            return Retries(
                limit,
                (int(backoff_min * 1000), int(backoff_max * 1000)),
                _normalize_retry_when(when),
            )


def _parse_defer(
    defer: bool | Defer,
    parameters: list[Parameter],
) -> Defer | None:
    if not defer:
        return None
    match defer:
        case Defer(params):
            return Defer(
                True if params is None else _get_param_indexes(parameters, params),
            )
        case _:
            # bool shorthand
            return Defer(True)


def _parse_delay(delay: float | dt.timedelta) -> float:
    if isinstance(delay, dt.timedelta):
        return delay.total_seconds()
    return delay


def _parse_memo(
    memo: bool | t.Iterable[str] | str, parameters: list[Parameter]
) -> list[int] | bool:
    if isinstance(memo, bool):
        return memo
    return _get_param_indexes(parameters, memo)


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
        _parse_cache(cache, parameters_),
        _parse_defer(defer, parameters_),
        _parse_delay(delay),
        _parse_retries(retries),
        recurrent,
        _parse_memo(memo, parameters_),
        _parse_requires(requires),
        inspect.getdoc(fn),
        is_stub,
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
        wait_for_val = sorted(self._definition.wait_for) if self._definition.wait_for else None


        # Build cache dict if present
        cache_dict = None
        if self._definition.cache:
            cache_dict = {
                "params": self._definition.cache.params,
                "max_age_ms": self._definition.cache.max_age,
                "namespace": self._definition.cache.namespace,
                "version": self._definition.cache.version,
            }

        # Build defer dict if present
        defer_dict = None
        if self._definition.defer:
            defer_dict = {"params": self._definition.defer.params}

        # Build retries dict if present
        retries_dict = None
        if self._definition.retries:
            retries_dict = {
                "limit": self._definition.retries.limit,
                "backoff_min_ms": self._definition.retries.backoff[0] or None,
                "backoff_max_ms": self._definition.retries.backoff[1] or None,
            }

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
            delay=self._definition.delay if self._definition.delay else None,
            retries=retries_dict,
            recurrent=self._definition.recurrent,
            requires=self._definition.requires,
        )
        return Execution(result["execution_id"], result["module"], result["target"])

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """Call the target - submits for execution and waits for the result."""
        return self.submit(*args, **kwargs).result()


def task(
    *,
    name: str | None = None,
    wait: bool | t.Iterable[str] | str = False,
    cache: bool | float | dt.timedelta | Cache = False,
    retries: int | bool | Retries = 0,
    recurrent: bool = False,
    defer: bool | Defer = False,
    delay: float | dt.timedelta = 0,
    memo: bool | t.Iterable[str] = False,
    requires: dict[str, str | bool | list[str]] | None = None,
) -> t.Callable[[t.Callable[P, T]], Target[P, T]]:
    """Decorator for defining a task."""
    def decorator(fn: t.Callable[P, T]) -> Target[P, T]:
        return Target(
            fn,
            "task",
            name=name,
            wait=wait,
            cache=cache,
            retries=retries,
            recurrent=recurrent,
            defer=defer,
            delay=delay,
            memo=memo,
            requires=requires,
        )

    return decorator


def workflow(
    *,
    name: str | None = None,
    wait: bool | t.Iterable[str] | str = False,
    cache: bool | float | dt.timedelta | Cache = False,
    retries: int | bool | Retries = 0,
    recurrent: bool = False,
    defer: bool | Defer = False,
    delay: float | dt.timedelta = 0,
    requires: dict[str, str | bool | list[str]] | None = None,
) -> t.Callable[[t.Callable[P, T]], Target[P, T]]:
    """Decorator for defining a workflow."""
    def decorator(fn: t.Callable[P, T]) -> Target[P, T]:
        return Target(
            fn,
            "workflow",
            name=name,
            wait=wait,
            cache=cache,
            retries=retries,
            recurrent=recurrent,
            defer=defer,
            delay=delay,
            requires=requires,
        )

    return decorator


def stub(
    module: str,
    *,
    name: str | None = None,
    type: t.Literal["workflow", "task"] = "task",
    wait: bool | t.Iterable[str] | str = False,
    cache: bool | float | dt.timedelta | Cache = False,
    retries: int | bool | Retries = 0,
    recurrent: bool = False,
    defer: bool | Defer = False,
    delay: float | dt.timedelta = 0,
    memo: bool | t.Iterable[str] = False,
) -> t.Callable[[t.Callable[P, T]], Target[P, T]]:
    """Decorator for defining a stub (external reference)."""
    def decorator(fn: t.Callable[P, T]) -> Target[P, T]:
        return Target(
            fn,
            type,
            module=module,
            name=name,
            wait=wait,
            cache=cache,
            retries=retries,
            recurrent=recurrent,
            defer=defer,
            delay=delay,
            memo=memo,
            is_stub=True,
        )

    return decorator
