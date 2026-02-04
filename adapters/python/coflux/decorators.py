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

T = t.TypeVar("T")
P = t.ParamSpec("P")

# Type definitions
TargetType = t.Literal["workflow", "task"]
Requires = dict[str, list[str]]


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


def _parse_cache(
    cache: bool | float | dt.timedelta,
    cache_params: t.Iterable[str] | str | None,
    cache_namespace: str | None,
    cache_version: str | None,
    parameters: list[Parameter],
) -> Cache | None:
    if not cache:
        return None
    return Cache(
        (
            True
            if cache_params is None
            else _get_param_indexes(parameters, cache_params)
        ),
        (
            int(cache * 1000)
            if isinstance(cache, (int, float)) and not isinstance(cache, bool)
            else (
                int(cache.total_seconds() * 1000)
                if isinstance(cache, dt.timedelta)
                else None
            )
        ),
        cache_namespace,
        cache_version,
    )


def _parse_retries(
    retries: int | bool | Retries,
) -> Retries | None:
    match retries:
        case False | 0:
            return None
        case True:
            # Unlimited with sensible defaults (1s-60s backoff)
            return Retries(None, 1000, 60000)
        case int(limit):
            return Retries(limit, 0, 0)
        case Retries(limit, delay_min, delay_max):
            if limit == 0:
                return None
            return Retries(
                limit,
                int(delay_min * 1000),
                int(delay_max * 1000),
            )


def _parse_defer(
    defer: bool,
    defer_params: t.Iterable[str] | str | None,
    parameters: list[Parameter],
) -> Defer | None:
    if not defer:
        return None
    return Defer(
        (True if defer_params is None else _get_param_indexes(parameters, defer_params))
    )


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
    cache: bool | float | dt.timedelta,
    cache_params: t.Iterable[str] | str | None,
    cache_namespace: str | None,
    cache_version: str | None,
    retries: int | bool | Retries,
    recurrent: bool,
    defer: bool,
    defer_params: t.Iterable[str] | str | None,
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
        _parse_cache(cache, cache_params, cache_namespace, cache_version, parameters_),
        _parse_defer(defer, defer_params, parameters_),
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


class Execution(t.Generic[T]):
    """A handle to a submitted execution that can be awaited for its result."""

    def __init__(self, reference: list[t.Any], target: "Target | None" = None):
        self._reference = reference
        self._target = target

    def __reduce__(self):
        """Support pickling by only serializing the reference (not the target)."""
        return (Execution, (self._reference,))

    @property
    def reference(self) -> list[t.Any]:
        return self._reference

    @property
    def id(self) -> str:
        """Get the execution ID from the reference."""
        # Reference format: ["execution", id, ...]
        if len(self._reference) >= 2:
            return str(self._reference[1])
        return ""

    @property
    def metadata(self):
        """Get execution metadata from the reference."""
        from .models import ExecutionMetadata
        # Reference format: ["execution", id, run_id, step_id, attempt, module, target]
        if len(self._reference) >= 7:
            return ExecutionMetadata(
                run_id=self._reference[2],
                step_id=self._reference[3],
                attempt=self._reference[4],
                module=self._reference[5],
                target=self._reference[6],
            )
        return None

    def result(self) -> T:
        """Wait for and return the execution result."""
        from .context import get_context
        ctx = get_context()
        value = ctx.resolve_reference(self._reference)
        # Deserialize the result
        from .serialization import deserialize_argument
        return deserialize_argument(value)

    def cancel(self) -> None:
        """Cancel this execution."""
        from .context import get_context
        ctx = get_context()
        ctx.cancel_execution(self._reference)


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
        cache: bool | float | dt.timedelta = False,
        cache_params: t.Iterable[str] | str | None = None,
        cache_namespace: str | None = None,
        cache_version: str | None = None,
        retries: int | bool | Retries = 0,
        recurrent: bool = False,
        defer: bool = False,
        defer_params: t.Iterable[str] | str | None = None,
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
            cache_params,
            cache_namespace,
            cache_version,
            retries,
            recurrent,
            defer,
            defer_params,
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
        from .context import get_context
        from .serialization import serialize_result

        if kwargs:
            raise ValueError("Keyword arguments not yet supported")

        ctx = get_context()

        # Serialize arguments
        serialized_args = []
        for arg in args:
            result = serialize_result(arg)
            if result is None:
                serialized_args.append({"type": "inline", "format": "json", "value": None})
            else:
                serialized_args.append(result)

        # Build full target name
        full_target = f"{self._module}.{self._name}"

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
                "delay_min_ms": int(self._definition.retries.delay_min * 1000) if self._definition.retries.delay_min else None,
                "delay_max_ms": int(self._definition.retries.delay_max * 1000) if self._definition.retries.delay_max else None,
            }

        # Get memo value (bool or list of indices)
        memo_val = self._definition.memo if self._definition.memo else None

        # Submit via context with all target definition fields
        reference = ctx.submit_execution(
            full_target,
            serialized_args,
            cache=cache_dict,
            defer=defer_dict,
            memo=memo_val,
            delay=self._definition.delay if self._definition.delay else None,
            retries=retries_dict,
            recurrent=self._definition.recurrent,
            requires=self._definition.requires,
        )
        return Execution(reference, self)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """Call the target - submits if in execution context, else runs directly."""
        from .context import get_context
        try:
            # If we're in an execution context, submit and wait for result
            get_context()
            return self.submit(*args, **kwargs).result()
        except RuntimeError:
            # Not in execution context, run directly
            return self._fn(*args, **kwargs)


def task(
    *,
    name: str | None = None,
    wait: bool | t.Iterable[str] | str = False,
    cache: bool | float | dt.timedelta = False,
    cache_params: t.Iterable[str] | str | None = None,
    cache_namespace: str | None = None,
    cache_version: str | None = None,
    retries: int | bool | Retries = 0,
    recurrent: bool = False,
    defer: bool = False,
    defer_params: t.Iterable[str] | str | None = None,
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
            cache_params=cache_params,
            cache_namespace=cache_namespace,
            cache_version=cache_version,
            retries=retries,
            recurrent=recurrent,
            defer=defer,
            defer_params=defer_params,
            delay=delay,
            memo=memo,
            requires=requires,
        )

    return decorator


def workflow(
    *,
    name: str | None = None,
    wait: bool | t.Iterable[str] | str = False,
    cache: bool | float | dt.timedelta = False,
    cache_params: t.Iterable[str] | str | None = None,
    cache_namespace: str | None = None,
    cache_version: str | None = None,
    retries: int | bool | Retries = 0,
    recurrent: bool = False,
    defer: bool = False,
    defer_params: t.Iterable[str] | str | None = None,
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
            cache_params=cache_params,
            cache_namespace=cache_namespace,
            cache_version=cache_version,
            retries=retries,
            recurrent=recurrent,
            defer=defer,
            defer_params=defer_params,
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
    cache: bool | float | dt.timedelta = False,
    cache_params: t.Iterable[str] | str | None = None,
    cache_namespace: str | None = None,
    cache_version: str | None = None,
    retries: int | bool | Retries = 0,
    recurrent: bool = False,
    defer: bool = False,
    defer_params: t.Iterable[str] | str | None = None,
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
            cache_params=cache_params,
            cache_namespace=cache_namespace,
            cache_version=cache_version,
            retries=retries,
            recurrent=recurrent,
            defer=defer,
            defer_params=defer_params,
            delay=delay,
            memo=memo,
            is_stub=True,
        )

    return decorator
