"""What a ``Checkpoint`` handle itself decides.

The storage semantics — what survives an attempt, when a delta is cut —
are the server's and the context's, and are covered by the e2e suite and
by ``test_checkpoint_publication``. This is the reserved namespace, the
read-modify-write of ``update``, and what a typed handle adds on either
side.

Names starting with ``_`` belong to the adapter — currently the cursors
behind stream suspension, which are named after the stream and view being
read. Reserving the whole prefix rather than individual names means later
internal state doesn't need another round of this. The check lives in
``Checkpoint`` rather than in the context's ``checkpoint_get``/
``checkpoint_set``/``checkpoint_reset``, because those are the path the
adapter's own cursors go through — validating there would reject the very
thing the prefix exists for.
"""

from __future__ import annotations

import pytest

from coflux import validation as validation_module
from coflux.checkpoint import RESERVED_PREFIX, Checkpoint
from coflux.models import Asset


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


# --- typed checkpoints -------------------------------------------------------


def _pydantic():
    return pytest.importorskip("pydantic")


def _cursor_model(pydantic, **extra_fields):
    fields = {"__annotations__": {"since": int, **extra_fields}}
    return type("Cursor", (pydantic.BaseModel,), fields)


def test_a_typed_write_validates_the_value_and_stores_plain_data(context):
    """A model goes in as its fields, so what an attempt reads back doesn't
    depend on the class the attempt that wrote it had."""
    pydantic = _pydantic()
    Cursor = _cursor_model(pydantic)
    cursor = Checkpoint[Cursor]("cursor", default=Cursor(since=0))

    cursor.set(Cursor(since=5))
    assert context.values["cursor"] == {"since": 5}
    # A dict is accepted where the model accepts it, coerced as it would be.
    cursor.set({"since": "6"})
    assert context.values["cursor"] == {"since": 6}
    # And refused before anything is written where it doesn't.
    with pytest.raises(pydantic.ValidationError):
        cursor.set({"nope": 1})
    assert context.values["cursor"] == {"since": 6}


def test_a_typed_read_validates_with_the_declared_type(context):
    """What's stored was written by an earlier attempt, possibly by older
    code, so it's checked against the type this one declares — and a value
    that fits comes back as an instance."""
    pydantic = _pydantic()
    Cursor = _cursor_model(pydantic)
    Stricter = _cursor_model(pydantic, page=int)
    context.values["cursor"] = {"since": 5}

    value = Checkpoint[Cursor]("cursor", default=Cursor(since=0)).get()
    assert isinstance(value, Cursor)
    assert value.since == 5

    stricter = Checkpoint[Stricter]("cursor", default=Stricter(since=0, page=0))
    with pytest.raises(pydantic.ValidationError, match="page"):
        stricter.get()


def test_any_type_pydantic_can_validate_is_validated(context):
    """A model is one case: a plain annotation is checked the same way,
    and a class pydantic doesn't know — an asset — by instance."""
    pydantic = _pydantic()

    seen = Checkpoint[set[str]]("seen", default=set())
    seen.set(["a", "b"])
    assert context.values["seen"] == {"a", "b"}
    assert seen.get() == {"a", "b"}
    with pytest.raises(pydantic.ValidationError):
        seen.set(4)

    weights = Checkpoint[Asset | None]("weights")
    weights.set(Asset("A1"))
    assert context.values["weights"].id == "A1"
    with pytest.raises(pydantic.ValidationError):
        weights.set("A1")


def test_update_round_trips_through_the_type(context):
    """``update`` is a read then a write, so both ends are validated, and
    what it returns is the value it stored."""
    pydantic = _pydantic()
    Cursor = _cursor_model(pydantic)
    cursor = Checkpoint[Cursor]("cursor", default=Cursor(since=0))

    assert cursor.update(lambda c: Cursor(since=c.since + 1)).since == 1
    assert context.values["cursor"] == {"since": 1}
    assert cursor.update(lambda c: Cursor(since=c.since + 1)).since == 2


def test_the_default_is_validated_where_it_is_declared():
    """It's part of what makes ``get()`` a ``T``, and is checked here
    rather than at the read it would be returned from."""
    pydantic = _pydantic()
    assert Checkpoint[int]("count", default=0).default == 0
    assert Checkpoint[int]("count", default="4").default == 4
    with pytest.raises(pydantic.ValidationError):
        Checkpoint[int]("count", default="x")


def test_a_type_declared_without_a_default_has_to_admit_none():
    """Nothing has to be stored for a read to return the default, so a
    checkpoint with no default reads as ``None`` to begin with."""
    _pydantic()
    with pytest.raises(ValueError, match="no default"):
        Checkpoint[int]("count")
    assert Checkpoint[int | None]("count").default is None
    assert Checkpoint[int]("count", default=0).default == 0


def test_an_untyped_checkpoint_stores_the_value_as_it_is(context):
    """Nothing is declared, so there's nothing to validate against: a
    model stays a model, as it did before types were checked at all."""
    pydantic = _pydantic()
    Cursor = _cursor_model(pydantic)
    cursor = Checkpoint("cursor")

    assert cursor.default is None
    cursor.set(Cursor(since=5))
    assert isinstance(context.values["cursor"], Cursor)
    assert cursor.get().since == 5


def test_without_pydantic_the_type_only_informs_type_checkers(context, monkeypatch):
    """With nothing to validate with, the value is written and read as it
    is, and a type that doesn't admit ``None`` is no longer a declaration
    that has anything to say."""
    monkeypatch.setattr(validation_module, "pydantic", None)
    count = Checkpoint[int]("count")
    assert count._adapter is None
    assert count.default is None
    count.set("4")
    assert context.values["count"] == "4"
    assert count.get() == "4"


def test_a_typed_handle_says_so():
    _pydantic()
    assert repr(Checkpoint[int]("count", default=0)) == "Checkpoint[int]('count')"
    assert repr(Checkpoint[dict[str, int] | None]("t")) == (
        "Checkpoint[dict[str, int] | None]('t')"
    )
    assert repr(Checkpoint("count")) == "Checkpoint('count')"
