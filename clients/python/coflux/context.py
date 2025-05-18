import typing as t
import datetime as dt
from pathlib import Path

from . import execution, models


class NotInContextException(Exception):
    pass


def _get_channel() -> execution.Channel:
    channel = execution.get_channel()
    if channel is None:
        raise NotInContextException("Not running in execution context")
    return channel


def submit(
    type: models.TargetType,
    module: str,
    target: str,
    arguments: tuple[t.Any, ...],
    *,
    wait_for: set[int] | None = None,
    cache: models.Cache | None = None,
    retries: models.Retries | None = None,
    defer: models.Defer | None = None,
    execute_after: dt.datetime | None = None,
    delay: float | dt.timedelta = 0,
    memo: list[int] | bool = False,
    requires: models.Requires | None = None,
) -> models.Execution[t.Any]:
    return _get_channel().submit_execution(
        type,
        module,
        target,
        arguments,
        wait_for=(wait_for or set()),
        cache=cache,
        retries=retries,
        defer=defer,
        execute_after=execute_after,
        delay=delay,
        memo=memo,
        requires=requires,
    )


def group(name: str | None = None):
    return _get_channel().group(name)


def suspense(timeout: float | None):
    return _get_channel().suspense(timeout)


def suspend(delay: float | dt.datetime | None = None):
    return _get_channel().suspend(delay)


def persist_asset(
    path: Path | str | None = None, *, match: str | None = None
) -> models.Asset:
    return _get_channel().persist_asset(path, match=match)


def checkpoint(*arguments: t.Any) -> None:
    return _get_channel().record_checkpoint(arguments)


def log_debug(template: str | None = None, **kwargs) -> None:
    _get_channel().log_message(0, template, **kwargs)


def log_info(template: str | None = None, **kwargs) -> None:
    _get_channel().log_message(2, template, **kwargs)


def log_warning(template: str | None = None, **kwargs) -> None:
    _get_channel().log_message(4, template, **kwargs)


def log_error(template: str | None = None, **kwargs) -> None:
    _get_channel().log_message(5, template, **kwargs)
