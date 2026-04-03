"""Decorators for defining Coflux tasks and workflows."""

from __future__ import annotations

import datetime as dt
import typing as t

from .target import Cache, Defer, Retries, Target

P = t.ParamSpec("P")
T = t.TypeVar("T")


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
    timeout: float | dt.timedelta | None = None,
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
            timeout=timeout,
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
    timeout: float | dt.timedelta | None = None,
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
            timeout=timeout,
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
