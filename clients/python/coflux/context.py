import datetime as dt
import typing as t
from pathlib import Path

from . import execution, models, types


def submit(
    type: types.TargetType,
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
    requires: types.Requires | None = None,
) -> models.Execution[t.Any]:
    return execution.get_channel().submit_execution(
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
    return execution.get_channel().group(name)


def suspense(timeout: float | None = 0):
    return execution.get_channel().suspense(timeout)


def suspend(delay: float | dt.datetime | None = None):
    return execution.get_channel().suspend(delay)


def asset(
    entries: str
    | Path
    | list[str | Path]
    | models.Asset
    | dict[str, str | Path | models.Asset | models.AssetEntry]
    | None = None,
    *,
    at: Path | None = None,
    match: str | None = None,
    name: str | None = None,
) -> models.Asset:
    return execution.get_channel().create_asset(entries, at=at, match=match, name=name)


def checkpoint(*arguments: t.Any) -> None:
    return execution.get_channel().record_checkpoint(arguments)


def log_debug(template: str | None = None, **kwargs) -> None:
    execution.get_channel().log_message(0, template, **kwargs)


def log_info(template: str | None = None, **kwargs) -> None:
    execution.get_channel().log_message(2, template, **kwargs)


def log_warning(template: str | None = None, **kwargs) -> None:
    execution.get_channel().log_message(4, template, **kwargs)


def log_error(template: str | None = None, **kwargs) -> None:
    execution.get_channel().log_message(5, template, **kwargs)
