"""Declaring a concurrency limit, and what reaches the wire.

The limit is enforced by the scheduler, so what's tested here is only the
declaration: shorthand expansion, validation at decoration time, mapping
parameter names to the indexes the server keys on, and the serialised shape.
"""

from __future__ import annotations

import pytest

import coflux as cf
from coflux.target import serialize_concurrency


def definition(target):
    return target.definition.concurrency


def serialized(target):
    concurrency = target.definition.concurrency
    assert concurrency is not None
    return serialize_concurrency(concurrency, target.definition.parameters)


def test_no_limit_by_default():
    @cf.task()
    def example(x: int) -> int:
        return x

    assert definition(example) is None


def test_integer_shorthand_expands():
    @cf.task(concurrency=1)
    def example(x: int) -> int:
        return x

    assert definition(example) == cf.Concurrency(1)


def test_zero_disables():
    @cf.task(concurrency=0)
    def example(x: int) -> int:
        return x

    assert definition(example) is None


def test_concurrency_object_is_kept():
    @cf.task(concurrency=cf.Concurrency(4, namespace="openai"))
    def example(x: int) -> int:
        return x

    assert definition(example) == cf.Concurrency(4, namespace="openai")


@pytest.mark.parametrize("limit", [-1, 0.5, "1", True])
def test_invalid_limit_is_rejected(limit):
    with pytest.raises(ValueError):

        @cf.task(concurrency=limit)
        def example(x: int) -> int:
            return x


def test_explicit_zero_limit_is_rejected():
    with pytest.raises(ValueError):

        @cf.task(concurrency=cf.Concurrency(0))
        def example(x: int) -> int:
            return x


def test_serialises_limit_only():
    @cf.task(concurrency=2)
    def example(x: int, y: int) -> int:
        return x + y

    assert serialized(example) == {"limit": 2}


def test_serialises_params_true():
    @cf.task(concurrency=cf.Concurrency(1, params=True))
    def example(x: int, y: int) -> int:
        return x + y

    assert serialized(example) == {"limit": 1, "params": True}


def test_serialises_named_params_as_indexes():
    @cf.task(concurrency=cf.Concurrency(1, params=["account_id"]))
    def example(x: int, account_id: str) -> str:
        return account_id

    assert serialized(example) == {"limit": 1, "params": [1]}


def test_accepts_comma_separated_params():
    @cf.task(concurrency=cf.Concurrency(1, params="x, account_id"))
    def example(x: int, account_id: str) -> str:
        return account_id

    assert serialized(example) == {"limit": 1, "params": [0, 1]}


def test_unrecognised_param_is_rejected():
    # Names are resolved to indexes when the definition is serialised (as
    # for cache and defer), so the failure surfaces at discovery/submit.
    @cf.task(concurrency=cf.Concurrency(1, params=["nope"]))
    def example(x: int) -> int:
        return x

    with pytest.raises(ValueError):
        serialized(example)


def test_serialises_namespace():
    @cf.task(concurrency=cf.Concurrency(4, namespace="openai"))
    def example(x: int) -> int:
        return x

    assert serialized(example) == {"limit": 4, "namespace": "openai"}


def test_with_concurrency_overrides():
    @cf.task(concurrency=1)
    def example(x: int) -> int:
        return x

    overridden = example.with_concurrency(cf.Concurrency(3, params=["x"]))
    assert serialized(overridden) == {"limit": 3, "params": [0]}
    # The decorator-bound target is left alone
    assert definition(example) == cf.Concurrency(1)


def test_with_concurrency_disables():
    @cf.task(concurrency=1)
    def example(x: int) -> int:
        return x

    assert definition(example.with_concurrency(0)) is None


def test_workflows_and_stubs_accept_it():
    @cf.workflow(concurrency=1)
    def flow(x: int) -> int:
        return x

    @cf.stub("other", concurrency=cf.Concurrency(2, params=True))
    def remote(x: int) -> int: ...

    assert definition(flow) == cf.Concurrency(1)
    assert serialized(remote) == {"limit": 2, "params": True}
