"""Declaring a concurrency limit on a group, and what reaches the wire.

The limit is enforced by the scheduler; what's tested here is the argument
validation on ``cf.group`` and the shape of the ``register_group`` message.
"""

from __future__ import annotations

import pytest

from coflux import protocol
from coflux.context import ExecutorContext


class _Wire:
    def __init__(self):
        self.messages = []

    def send_message(self, method, params=None):
        self.messages.append((method, params))


@pytest.fixture
def wire(monkeypatch):
    w = _Wire()
    monkeypatch.setattr(protocol, "get_protocol", lambda: w)
    return w


@pytest.fixture
def ctx():
    return ExecutorContext("Eparent")


def test_no_limit_omits_the_field(ctx, wire):
    with ctx.group("batch"):
        pass

    assert wire.messages == [
        (
            "register_group",
            {"execution_id": "Eparent", "group_id": 0, "name": "batch"},
        )
    ]


def test_limit_is_sent_with_the_registration(ctx, wire):
    with ctx.group(concurrency=2):
        pass

    assert wire.messages == [
        (
            "register_group",
            {
                "execution_id": "Eparent",
                "group_id": 0,
                "name": None,
                "concurrency": 2,
            },
        )
    ]


@pytest.mark.parametrize("limit", [-1, True, "2", 1.5])
def test_invalid_limit_is_rejected(ctx, wire, limit):
    with pytest.raises(ValueError), ctx.group("batch", concurrency=limit):
        pass

    # Rejected before anything is registered.
    assert wire.messages == []
