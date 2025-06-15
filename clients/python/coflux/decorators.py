import datetime as dt
import functools
import inspect
import json
import re
import typing as t

from . import execution, models, types

TARGET_KEY = "_coflux_target"

T = t.TypeVar("T")
P = t.ParamSpec("P")


def _json_dumps(obj: t.Any) -> str:
    return json.dumps(obj, separators=(",", ":"))


def _build_parameter(parameter: inspect.Parameter) -> models.Parameter:
    return models.Parameter(
        parameter.name,
        # TODO: better serialisation?
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
    wait: bool | t.Iterable[str] | str, parameters: list[models.Parameter]
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
    parameters: list[models.Parameter],
) -> models.Cache | None:
    if not cache:
        return None
    return models.Cache(
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
    retries: int | tuple[int, int] | tuple[int, int, int],
) -> models.Retries | None:
    # TODO: parse string (e.g., '1h')
    match retries:
        case 0:
            return None
        case int(limit):
            return models.Retries(limit, 0, 0)
        case (limit, delay):
            return models.Retries(limit, delay, delay)
        case (limit, delay_min, delay_max):
            return models.Retries(limit, delay_min, delay_max)
        case other:
            raise ValueError(other)


def _parse_defer(
    defer: bool,
    defer_params: t.Iterable[str] | str | None,
    parameters: list[models.Parameter],
) -> models.Defer | None:
    if not defer:
        return None
    return models.Defer(
        (True if defer_params is None else _get_param_indexes(parameters, defer_params))
    )


def _parse_delay(delay: float | dt.timedelta) -> float:
    if isinstance(delay, dt.timedelta):
        return delay.total_seconds()
    return delay


def _parse_memo(
    memo: bool | t.Iterable[str] | str, parameters: list[models.Parameter]
) -> list[int] | bool:
    if isinstance(memo, bool):
        return memo
    return _get_param_indexes(parameters, memo)


def _build_definition(
    type: types.TargetType,
    fn: t.Callable,
    wait: bool | t.Iterable[str] | str,
    cache: bool | float | dt.timedelta,
    cache_params: t.Iterable[str] | str | None,
    cache_namespace: str | None,
    cache_version: str | None,
    retries: int | tuple[int, int] | tuple[int, int, int],
    defer: bool,
    defer_params: t.Iterable[str] | str | None,
    delay: float | dt.timedelta,
    memo: bool | t.Iterable[str] | str,
    requires: dict[str, str | bool | list[str]] | None,
    is_stub: bool,
) -> models.Target:
    parameters = inspect.signature(fn).parameters.values()
    for p in parameters:
        if p.kind != inspect.Parameter.POSITIONAL_OR_KEYWORD:
            raise Exception(f"Unsupported parameter type ({p.kind})")
    parameters_ = [_build_parameter(p) for p in parameters]
    return models.Target(
        type,
        parameters_,
        _parse_wait(wait, parameters_),
        _parse_cache(cache, cache_params, cache_namespace, cache_version, parameters_),
        _parse_defer(defer, defer_params, parameters_),
        _parse_delay(delay),
        _parse_retries(retries),
        _parse_memo(memo, parameters_),
        _parse_requires(requires),
        inspect.getdoc(fn),
        is_stub,
    )


def _build_arguments(
    callable: t.Callable[P, T], *args: P.args, **kwargs: P.kwargs
) -> tuple[object, ...]:
    sig = inspect.signature(callable)
    bound = sig.bind_partial(*args, **kwargs)
    bound.apply_defaults()
    result: list[object] = []
    for param in sig.parameters.values():
        if param.kind is inspect.Parameter.VAR_POSITIONAL:
            result.extend(bound.arguments.get(param.name, ()))
        elif param.kind is not inspect.Parameter.VAR_KEYWORD:
            result.append(bound.arguments[param.name])
    return tuple(result)


class Target(t.Generic[P, T]):
    def __init__(
        self,
        fn: t.Callable[P, T],
        type: types.TargetType,
        *,
        module: str | None = None,
        name: str | None = None,
        wait: bool | t.Iterable[str] | str = False,
        cache: bool | float | dt.timedelta = False,
        cache_params: t.Iterable[str] | str | None = None,
        cache_namespace: str | None = None,
        cache_version: str | None = None,
        retries: int | tuple[int, int] | tuple[int, int, int] = 0,
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
    def definition(self) -> models.Target:
        return self._definition

    @property
    def fn(self) -> t.Callable[P, T]:
        return self._fn

    def submit(self, *args: P.args, **kwargs: P.kwargs) -> models.Execution[T]:
        arguments = _build_arguments(self._fn, *args, **kwargs)
        try:
            return execution.get_channel().submit_execution(
                self._definition.type,
                self._module,
                self._name,
                arguments,
                wait_for=self._definition.wait_for,
                cache=self._definition.cache,
                retries=self._definition.retries,
                defer=self._definition.defer,
                delay=self._definition.delay,
                memo=self._definition.memo,
                requires=self._definition.requires,
            )
        except execution.NotInContextException:
            # TODO: run in thread?
            # TODO: use 'arguments'?
            # result = self._fn(*args, **kwargs)
            # return (
            #     models.Execution(None)
            #     if not isinstance(result, models.Execution)
            #     else result
            # )
            raise

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        return self.submit(*args, **kwargs).result()


def _get_param_indexes(
    parameters: list[models.Parameter],
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
) -> types.Requires | None:
    return {k: _parse_require(v) for k, v in requires.items()} if requires else None


def task(
    *,
    name: str | None = None,
    wait: bool | t.Iterable[str] | str = False,
    cache: bool | float | dt.timedelta = False,
    cache_params: t.Iterable[str] | str | None = None,
    cache_namespace: str | None = None,
    cache_version: str | None = None,
    retries: int | tuple[int, int] | tuple[int, int, int] = 0,
    defer: bool = False,
    defer_params: t.Iterable[str] | str | None = None,
    delay: float | dt.timedelta = 0,
    memo: bool | t.Iterable[str] = False,
    requires: dict[str, str | bool | list[str]] | None = None,
) -> t.Callable[[t.Callable[P, T]], Target[P, T]]:
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
    retries: int | tuple[int, int] | tuple[int, int, int] = 0,
    defer: bool = False,
    defer_params: t.Iterable[str] | str | None = None,
    delay: float | dt.timedelta = 0,
    requires: dict[str, str | bool | list[str]] | None = None,
) -> t.Callable[[t.Callable[P, T]], Target[P, T]]:
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
    retries: int | tuple[int, int] | tuple[int, int, int] = 0,
    defer: bool = False,
    defer_params: t.Iterable[str] | str | None = None,
    delay: float | dt.timedelta = 0,
    memo: bool | t.Iterable[str] = False,
) -> t.Callable[[t.Callable[P, T]], Target[P, T]]:
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
            defer=defer,
            defer_params=defer_params,
            delay=delay,
            memo=memo,
            is_stub=True,
        )

    return decorator


def sensor(
    *,
    name=None,
    requires: dict[str, str | bool | list[str]] | None = None,
) -> t.Callable[[t.Callable[P, None]], Target[P, None]]:
    def decorator(fn: t.Callable[P, None]) -> Target[P, None]:
        return Target(
            fn,
            "sensor",
            name=name,
            requires=requires,
        )

    return decorator
