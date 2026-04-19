"""Decorators for defining Coflux tasks and workflows."""

from __future__ import annotations

import datetime as dt
import typing as t

from .target import _BUFFER_UNSET, Cache, Defer, Retries, Target

if t.TYPE_CHECKING:
    from .models import Stream

P = t.ParamSpec("P")
T = t.TypeVar("T")


class _TargetDecorator(t.Protocol):
    """Decorator protocol that unwraps ``Coroutine`` and collapses generator
    return types to ``Stream``.

    Overloading on ``__call__`` (rather than on the factory function) lets
    the type checker pick the right overload based on the decorated
    function's return type — which is visible at application time, but not
    at the factory call.

    Overload resolution order matters: generator functions match first (so
    ``-> Iterator[T]`` gives a ``Target[P, Stream[T]]``), then async
    coroutines (unwrapped), then the general case.
    """

    @t.overload
    def __call__(self, fn: t.Callable[P, t.Iterator[T]]) -> Target[P, "Stream[T]"]: ...

    @t.overload
    def __call__(
        self, fn: t.Callable[P, t.Coroutine[t.Any, t.Any, T]]
    ) -> Target[P, T]: ...

    @t.overload
    def __call__(self, fn: t.Callable[P, T]) -> Target[P, T]: ...


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
    timeout: float | dt.timedelta = 0,
    buffer: int | None = _BUFFER_UNSET,  # type: ignore[assignment]
) -> _TargetDecorator:
    """Decorator for defining a task.

    For ``async def`` functions, the coroutine is run to completion by
    the executor; the task's return type is the coroutine's resolved
    value (not the coroutine itself).

    ``buffer`` only applies to generator-bodied tasks. ``0`` (default)
    gives strict lockstep: the producer emits an item, waits for a
    consumer to ack, then emits the next. ``N`` lets the producer stay
    up to N items ahead of the fastest consumer. ``None`` disables
    backpressure entirely. Passing ``buffer`` on a non-generator task
    raises ``TypeError`` at decoration time.
    """

    def decorator(fn):
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
            timeout=timeout,
            buffer=buffer,
        )

    return decorator  # type: ignore[return-value]


def workflow(
    *,
    name: str | None = None,
    wait: bool | t.Iterable[str] | str = False,
    cache: bool | float | dt.timedelta | Cache = False,
    retries: int | bool | Retries = 0,
    recurrent: bool = False,
    defer: bool | Defer = False,
    delay: float | dt.timedelta = 0,
    memo: bool = False,
    requires: dict[str, str | bool | list[str]] | None = None,
    timeout: float | dt.timedelta = 0,
    buffer: int | None = _BUFFER_UNSET,  # type: ignore[assignment]
) -> _TargetDecorator:
    """Decorator for defining a workflow.

    For ``async def`` functions, the coroutine is run to completion by
    the executor; the workflow's return type is the coroutine's resolved
    value (not the coroutine itself).

    See ``@cf.task`` for ``buffer=`` semantics.
    """

    def decorator(fn):
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
            memo=memo,
            requires=requires,
            timeout=timeout,
            buffer=buffer,
        )

    return decorator  # type: ignore[return-value]


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
) -> _TargetDecorator:
    """Decorator for defining a stub (external reference)."""

    def decorator(fn):
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

    return decorator  # type: ignore[return-value]
