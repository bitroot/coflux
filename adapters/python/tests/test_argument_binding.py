"""Defaults are filled in at the call site, not left to the callee.

Cache, memo and defer keys are derived from the arguments a call was
submitted with, so ``expensive(5)`` and ``expensive(5, 1)`` — the same call,
one of them spelling out the default — have to reach the server as the same
list or they key differently and never share a cached result.

Binding against the signature rather than reconstructing defaults from the
manifest also keeps the real objects: a tuple default stays a tuple, where a
JSON round-trip would have made it a list.
"""

from __future__ import annotations

import pytest

import coflux as cf


def bind(target, *args):
    return target._bind_arguments(args)


def test_omitted_defaults_are_filled_in():
    @cf.task()
    def expensive(x: int, y: int = 1, z: int = 2) -> int:
        return x + y + z

    assert bind(expensive, 5) == (5, 1, 2)


def test_omitted_and_explicit_defaults_agree():
    @cf.task()
    def expensive(x: int, y: int = 1, z: int = 2) -> int:
        return x + y + z

    assert bind(expensive, 5) == bind(expensive, 5, 1, 2)
    assert bind(expensive, 5, 1) == bind(expensive, 5, 1, 2)


def test_overridden_values_are_kept():
    @cf.task()
    def expensive(x: int, y: int = 1, z: int = 2) -> int:
        return x + y + z

    assert bind(expensive, 5, 9) == (5, 9, 2)
    assert bind(expensive, 5) != bind(expensive, 5, 9)


def test_no_defaults_is_unchanged():
    @cf.task()
    def add(x: int, y: int) -> int:
        return x + y

    assert bind(add, 1, 2) == (1, 2)


def test_no_parameters_is_unchanged():
    @cf.task()
    def tick() -> None:
        return None

    assert bind(tick) == ()


def test_default_objects_survive_intact():
    # Reconstructing this from the manifest's JSON would yield a list.
    @cf.task()
    def batched(x: int, shape: tuple = (1, 2)) -> int:
        return x

    (_, shape) = bind(batched, 5)
    assert shape == (1, 2)
    assert isinstance(shape, tuple)


def test_too_many_arguments_is_rejected_at_submit():
    @cf.task()
    def add(x: int, y: int) -> int:
        return x + y

    with pytest.raises(TypeError, match="too many positional arguments"):
        bind(add, 1, 2, 3)


def test_missing_required_argument_is_rejected_at_submit():
    @cf.task()
    def add(x: int, y: int) -> int:
        return x + y

    with pytest.raises(TypeError, match="missing a required argument"):
        bind(add, 1)
