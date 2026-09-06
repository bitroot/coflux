"""The reserved checkpoint namespace.

Names starting with ``_`` belong to the adapter — currently the cursors
behind stream suspension, which are named after the stream and view being
read. Reserving the whole prefix rather than individual names means later
internal state doesn't need another round of this.

The check lives in ``Checkpoint`` rather than in the context's
``checkpoint_get``/``checkpoint_set``/``checkpoint_reset``, because those
are the path the adapter's own cursors go through — validating there would
reject the very thing the prefix exists for.
"""

from __future__ import annotations

import pytest

from coflux.checkpoint import RESERVED_PREFIX, Checkpoint


def test_ordinary_names_are_accepted():
    assert Checkpoint("cursor").name == "cursor"
    assert Checkpoint("total", default=0.0).default == 0.0
    # Only the *leading* character is reserved.
    assert Checkpoint("last_seen").name == "last_seen"


@pytest.mark.parametrize("name", ["_cursor", "_", "_cursor/Erun:2_0/0,None,1"])
def test_reserved_names_are_rejected(name):
    with pytest.raises(ValueError, match="reserved"):
        Checkpoint(name)


def test_the_prefix_is_what_the_adapter_uses():
    """The cursor names the stream subscription generates have to be
    exactly what this rejects, or the two could drift apart."""
    from coflux.streams import _CURSOR_PREFIX

    assert _CURSOR_PREFIX.startswith(RESERVED_PREFIX)


class _FakeContext:
    """Just enough context for get/set — the storage semantics are the
    server's business and are covered by the e2e suite."""

    def __init__(self, values=None):
        self.values = dict(values or {})

    def checkpoint_get(self, name):
        if name not in self.values:
            raise KeyError(name)
        return self.values[name]

    def checkpoint_set(self, name, value):
        self.values[name] = value


@pytest.fixture
def context(monkeypatch):
    ctx = _FakeContext()
    monkeypatch.setattr("coflux.checkpoint.get_context", lambda: ctx)
    return ctx


def test_update_applies_to_the_current_value(context):
    context.values["count"] = 4
    count = Checkpoint("count", default=0)

    assert count.update(lambda n: n + 1) == 5
    assert context.values["count"] == 5


def test_update_starts_from_the_default_when_unset(context):
    """`fn` sees what `get()` would have returned, so an unset checkpoint
    doesn't need special-casing at the call site."""
    count = Checkpoint("count", default=10)

    assert count.update(lambda n: n + 1) == 11
    assert context.values["count"] == 11
