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


@dataclasses.dataclass(frozen=True)
class Streams:
    """Default stream configuration for a task or workflow.

    Applies to:
      * Streams created explicitly with ``cf.stream(...)`` — each option
        (``buffer``, ``timeout``) can be overridden per-call.
      * Generator-bodied tasks, where the task itself produces the stream.

    ``buffer`` is the producer-side backpressure budget. ``0`` (the
    default) means strict lockstep: the producer emits an item, waits
    for a consumer to ack, then emits the next. ``N`` allows the
    producer to run up to ``N`` items ahead of the fastest consumer.
    ``None`` disables backpressure entirely.

    ``timeout`` is the idle-timeout budget — if the producer hasn't
    appended a new item within this window (including when blocked
    waiting for consumer demand), the stream is force-closed with
    reason ``"timeout"``. Enforced at the worker level. ``None``
    disables the timeout.
    """

    _: dataclasses.KW_ONLY
    buffer: int | None = 0
    timeout: float | dt.timedelta | None = None


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
    timeout: float | dt.timedelta
    instruction: str | None
    is_stub: bool
    # Default stream configuration. Used for generator-bodied tasks
    # (where the task itself produces a stream) and as the default for
    # ``cf.stream(...)`` calls within the task body. Individual
    # ``cf.stream`` kwargs override these per-call. ``None`` means the
    # task never deals with streams — validated at decoration time.
    streams: Streams | None


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


_STREAMS_UNSET = object()


def _validate_buffer(buffer: t.Any) -> int | None:
    if buffer is None:
        return None
    if not isinstance(buffer, int) or isinstance(buffer, bool) or buffer < 0:
        raise ValueError(
            f"buffer must be a non-negative integer or None, got {buffer!r}"
        )
    return buffer


def _validate_timeout(
    timeout: t.Any,
) -> float | dt.timedelta | None:
    if timeout is None:
        return None
    if isinstance(timeout, dt.timedelta):
        if timeout.total_seconds() <= 0:
            raise ValueError(f"timeout must be positive, got {timeout!r}")
        return timeout
    if isinstance(timeout, (int, float)) and not isinstance(timeout, bool):
        if timeout <= 0:
            raise ValueError(f"timeout must be positive, got {timeout!r}")
        return timeout
    raise TypeError(
        f"timeout must be a positive number, timedelta, or None, got {timeout!r}"
    )


def _resolve_streams(
    streams: t.Any,
    fn: t.Callable,
) -> Streams | None:
    """Validate the decorator's ``streams=`` and return the resolved value.

    A non-generator task gets ``None`` (no stream config makes sense).
    A generator-bodied task with no explicit ``streams=`` gets a default
    ``Streams()`` (buffer=0 strict lockstep, no timeout). Passing
    ``streams=`` on a non-generator task raises.
    """
    is_generator = inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn)
    if streams is _STREAMS_UNSET:
        return Streams() if is_generator else None
    if not is_generator:
        raise TypeError(
            f"@cf.task/@cf.workflow(streams=...) only applies to generator functions "
            f"(def + yield or async def + yield); {fn.__name__} is not."
        )
    if streams is None:
        return None
    if not isinstance(streams, Streams):
        raise TypeError(
            f"streams= must be a cf.Streams instance or None, got {type(streams).__name__}"
        )
    # Re-validate the options (defensive — Streams itself is a plain dataclass).
    return Streams(
        buffer=_validate_buffer(streams.buffer),
        timeout=_validate_timeout(streams.timeout),
    )


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
    timeout: float | dt.timedelta,
    is_stub: bool,
    streams: t.Any = _STREAMS_UNSET,
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
        _resolve_streams(streams, fn),
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


def serialize_streams(streams: Streams) -> dict | None:
    """Serialise a Streams dataclass to the wire format used in the
    manifest and in submit_execution requests. Returns ``None`` if
    neither option is set (so the key is omitted from the wire)."""
    result: dict[str, t.Any] = {}
    if streams.buffer is not None:
        result["buffer"] = streams.buffer
    if streams.timeout is not None:
        result["timeout_ms"] = _to_ms(streams.timeout)
    return result if result else None


class Target(t.Generic[P, T]):
    """Wrapper for a decorated task or workflow function.

    The fluent ``with_*`` methods return a new ``Target`` with per-call-site
    overrides applied, leaving the original decorator-bound target unchanged.
    Only the original is registered during discovery; fluent copies are
    call-site wrappers.

    Examples:
        # Override a single option at the call site
        my_task.with_retries(3).submit(x)

        # Disable a decorator-level option
        my_task.with_cache(False).submit(x)

        # Stash a preconfigured variant and reuse it
        cached = my_task.with_cache(60)
        cached.submit(a)
        cached.submit(b)

        # Chain multiple overrides
        my_task.with_cache(60).with_timeout(30).submit(x)
    """

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
        timeout: float | dt.timedelta = 0,
        is_stub: bool = False,
        streams: t.Any = _STREAMS_UNSET,
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
            streams,
        )
        functools.update_wrapper(self, fn)

    def _copy(self, **definition_overrides: t.Any) -> Target[P, T]:
        """Return a new Target with ``_definition`` fields overridden."""
        new = Target.__new__(type(self))
        new._fn = self._fn
        new._name = self._name
        new._module = self._module
        new._definition = self._definition._replace(**definition_overrides)
        functools.update_wrapper(new, self._fn)
        return new

    def with_cache(self, cache: bool | float | dt.timedelta | Cache) -> Target[P, T]:
        """Return a new Target with caching config overridden for this call site.

        Pass ``False`` to disable caching that was set on the decorator.
        """
        return self._copy(cache=_expand_cache(cache))

    def with_retries(self, retries: int | bool | Retries) -> Target[P, T]:
        """Return a new Target with retries config overridden for this call site.

        Pass ``0`` or ``False`` to disable retries.
        """
        return self._copy(retries=_expand_retries(retries))

    def with_defer(self, defer: bool | Defer) -> Target[P, T]:
        """Return a new Target with defer config overridden for this call site."""
        return self._copy(defer=_expand_defer(defer))

    def with_memo(self, memo: bool | t.Iterable[str] | str) -> Target[P, T]:
        """Return a new Target with memoisation config overridden for this call site."""
        return self._copy(memo=_parse_memo(memo, self._definition.parameters))

    def with_delay(self, delay: float | dt.timedelta) -> Target[P, T]:
        """Return a new Target with submission delay overridden for this call site."""
        return self._copy(delay=delay)

    def with_timeout(self, timeout: float | dt.timedelta) -> Target[P, T]:
        """Return a new Target with execution timeout overridden for this call site."""
        return self._copy(timeout=timeout)

    def with_requires(
        self, requires: dict[str, str | bool | list[str]] | None
    ) -> Target[P, T]:
        """Return a new Target with routing tags overridden for this call site."""
        return self._copy(requires=_parse_requires(requires))

    def with_streams(self, streams: Streams | None) -> Target[P, T]:
        """Return a new Target with stream config overridden for this call site.

        Only meaningful for targets that produce streams (generator
        functions, or bodies that call ``cf.stream(...)``). The new
        config becomes the default for ``cf.stream(...)`` inside the
        task; per-call ``cf.stream(buffer=..., timeout=...)`` overrides
        still win.
        """
        if self._definition.streams is None:
            raise TypeError(
                f"with_streams is only applicable to stream-producing targets; "
                f"{self._name} was declared without a streams config."
            )
        if streams is not None and not isinstance(streams, Streams):
            raise TypeError(
                f"with_streams expects a cf.Streams instance or None, got "
                f"{type(streams).__name__}"
            )
        resolved = (
            None
            if streams is None
            else Streams(
                buffer=_validate_buffer(streams.buffer),
                timeout=_validate_timeout(streams.timeout),
            )
        )
        return self._copy(streams=resolved)

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

        # Serialize arguments. Streams passed as args must already have
        # been registered via cf.stream(...) — the caller becomes the
        # producer, the callee gets a Stream handle. Bare generators
        # raise; the user should wrap them explicitly.
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

        streams_dict = (
            serialize_streams(self._definition.streams)
            if self._definition.streams
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
            timeout=_to_ms(self._definition.timeout) if self._definition.timeout else 0,
            streams=streams_dict,
        )
        return Execution(result["execution_id"], result["module"], result["target"])

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """Call the target - submits for execution and waits for the result."""
        return self.submit(*args, **kwargs).result()
