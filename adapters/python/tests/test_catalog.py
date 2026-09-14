"""The catalog API on the adapter side.

The server owns the semantics — snapshots, numbering, visibility — and the
e2e suite covers those. This checks what the adapter itself decides: how a
handle is declared and bound, that it is a handle and not a value, how the
select handles are shaped, that ``next()`` is a suspension carrying the
path, how ``current()`` and ``publish()`` turn the server's answers into
values and numbers, and what a typed handle adds on either side.
"""

from __future__ import annotations

import typing as t

import pytest

from coflux import catalog as catalog_module
from coflux import context as context_module
from coflux import protocol
from coflux.catalog import Catalog
from coflux.context import ExecutorContext, _CatalogPosition, _handle_key, _handle_wire
from coflux.errors import Suspending
from coflux.models import Asset, Execution
from coflux.serialization import serialize_value

# --- declaring and binding ---------------------------------------------------


def test_a_handle_validates_the_path_up_front():
    """The server's rule, applied at declaration, so a typo fails here
    rather than as a wait that never ends."""
    for bad in ["", "/abs", "a//b", "a/../b", "a@1", "a:b", "a b", 1]:
        with pytest.raises(ValueError):
            Catalog(bad)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="too long"):
        Catalog("a" * 513)
    assert Catalog("a-b_c.d/e").path == "a-b_c.d/e"


def test_a_template_is_validated_around_its_placeholders():
    """The literal parts have to be valid before anything is bound, and a
    placeholder is a plain name — nothing is formatted into it."""
    assert Catalog("models/{name}").template == "models/{name}"
    assert Catalog("{a}/{b}").at(a="x", b="y").path == "x/y"
    for bad in [
        "models/{name}/",
        "models//{name}",
        "{}",
        "{0}",
        "{name!r}",
        "{name:>3}",
        "{a b}",
        "models/{name",
    ]:
        with pytest.raises(ValueError):
            Catalog(bad)


def test_binding_fills_placeholders_and_keeps_the_type():
    models = Catalog("models/{name}")
    churn = models.at(name="churn")
    assert churn.path == "models/churn"
    assert type(churn) is type(models)
    assert repr(churn) == "Catalog('models/churn')"
    # The declaration is untouched.
    assert models.template == "models/{name}"
    # Placeholders left out stay unbound for a later at().
    both = Catalog("a/{x}/{y}")
    assert both.at(x=1).template == "a/1/{y}"
    assert both.at(x=1).at(y="z").path == "a/1/z"
    # A value can span segments, as long as the result is a path.
    assert Catalog("d/{name}").at(name="raw/customers").path == "d/raw/customers"


def test_binding_refuses_what_would_not_be_a_path():
    models = Catalog("models/{name}")
    with pytest.raises(ValueError, match="unknown placeholder"):
        models.at(nope="x")
    for bad in ["", "a b", "..", "{x}"]:
        with pytest.raises(ValueError):
            models.at(name=bad)


def test_an_unbound_handle_refuses_to_be_used(wire, monkeypatch):
    """Nothing goes out for a path that isn't one yet."""
    fake = wire()
    _install_context(monkeypatch, ExecutorContext("E1"))
    models = Catalog("models/{name}")
    for use in [
        models.current,
        models.next,
        lambda: models.publish(1),
        lambda: _handle_wire(models),
    ]:
        with pytest.raises(ValueError, match="unbound placeholders"):
            use()
    assert fake.requests == []


def test_handles_compare_by_template_and_type():
    assert Catalog("a") == Catalog("a")
    assert Catalog("a") != Catalog("b")
    assert Catalog("a/{x}") == Catalog("a/{x}")
    assert Catalog[int]("a") != Catalog("a")
    assert len({Catalog("a"), Catalog("a")}) == 1
    assert repr(Catalog("a/b")) == "Catalog('a/b')"
    assert repr(Catalog[int]("a/b")) == "Catalog[int]('a/b')"
    assert repr(Catalog[dict[int, str]]("a")) == "Catalog[dict[int, str]]('a')"
    assert repr(Catalog[t.Any]("a")) == "Catalog[Any]('a')"


def test_a_handle_is_local_not_a_value():
    """Pass the path, or the value the handle holds — never the handle."""
    with pytest.raises(TypeError, match="pass 'models/{name}'"):
        serialize_value({"e": Catalog("models/{name}")})


def test_request_error_is_part_of_the_public_api():
    import coflux as cf

    error = cf.RequestError("invisible", "server error: invisible")
    assert error.code == "invisible"
    assert isinstance(error, RuntimeError)


# --- select handles ----------------------------------------------------------


def test_a_handle_is_a_catalog_select_handle_without_a_position():
    """The server fills the position in from the execution's own view."""
    handle = Catalog("p/{q}").at(q="q")
    assert _handle_key(handle) == ("catalog", "p/q")
    assert _handle_wire(handle) == {"type": "catalog", "path": "p/q"}
    # An execution handle is unchanged.
    assert _handle_wire(Execution("E1", "m", "t")) == {"type": "execution", "id": "E1"}


def test_a_position_pins_the_number_to_wait_past():
    position = _CatalogPosition("p/q", 0)
    assert _handle_key(position) == ("catalog", "p/q@0")
    assert _handle_wire(position) == {"type": "catalog", "path": "p/q", "number": 0}


def test_a_handle_cannot_be_cancelled(wire):
    """A select handle, but not a cancellable one: nothing is pending
    behind a path. Refused locally, before anything goes out."""
    fake = wire()
    with pytest.raises(TypeError, match="cannot cancel Catalog"):
        ExecutorContext("E1").cancel([Execution("E2", "m", "t"), Catalog("p/q")])
    assert fake.requests == []


# --- the wire ------------------------------------------------------------------


class _FakeWire:
    """Scripted replies for the requests the catalog methods make."""

    def __init__(self, get_replies=(), select_replies=(), publish_reply=None):
        self.get_replies = list(get_replies)
        self.select_replies = list(select_replies)
        self.publish_reply = publish_reply
        self.requests = []
        self._next_id = 0

    def _send(self, method, params):
        self._next_id += 1
        self.requests.append((method, params))
        return self._next_id

    def wait_for_response(self, request_id):
        method, _params = self.requests[request_id - 1]
        if method == "catalog_get":
            reply = self.get_replies.pop(0)
        elif method == "select":
            reply = self.select_replies.pop(0)
        elif method == "catalog_publish":
            reply = self.publish_reply
        else:  # pragma: no cover
            raise AssertionError(method)
        return {"id": request_id, **reply}


def _found(number, value):
    return {"result": {"version": {"number": number, "value": serialize_value(value)}}}


_NOTHING = {"result": {"version": None}}


def _select_ok(number):
    return {"result": {"winner": 0, "status": "ok", "value": serialize_value(number)}}


def _fields(encoded):
    """The items of a dict in the encoded JSON value format, as a dict."""
    assert encoded["type"] == "dict"
    items = encoded["items"]
    return dict(zip(items[::2], items[1::2]))


@pytest.fixture
def wire(monkeypatch):
    def install(get_replies=(), select_replies=(), publish_reply=None):
        fake = _FakeWire(get_replies, select_replies, publish_reply)
        monkeypatch.setattr(protocol, "get_protocol", lambda: _Proto(fake))
        monkeypatch.setattr(context_module, "get_dispatcher", lambda: fake)
        return fake

    return install


class _Proto:
    def __init__(self, fake):
        self._fake = fake

    def send_request(self, method, params):
        return self._fake._send(method, params)


def _install_context(monkeypatch, ctx):
    from coflux import state

    monkeypatch.setattr(state, "get_context", lambda: ctx)
    monkeypatch.setattr("coflux.catalog.get_context", lambda: ctx)
    monkeypatch.setattr("coflux.get_context", lambda: ctx)


# --- reads ---------------------------------------------------------------------


def test_current_returns_the_value_without_waiting(wire, monkeypatch):
    fake = wire([_found(3, {"threshold": 0.7, "model": Asset("A1")})])
    _install_context(monkeypatch, ExecutorContext("E1"))
    value = Catalog("configs/{name}").at(name="training").current()
    assert value["threshold"] == 0.7
    assert value["model"].id == "A1"
    assert fake.requests == [
        ("catalog_get", {"execution_id": "E1", "path": "configs/training"})
    ]


def test_current_on_an_empty_path_waits_on_position_zero_then_reads_by_number(wire):
    """What lands is newer than the snapshot, so it's read by number."""
    fake = wire([_NOTHING, _found(1, "first")], [_select_ok(1)])
    assert ExecutorContext("E1").catalog_current("models/churn") == "first"
    assert [m for m, _ in fake.requests] == ["catalog_get", "select", "catalog_get"]
    assert fake.requests[1][1]["handles"] == [
        {"type": "catalog", "path": "models/churn", "number": 0}
    ]
    assert fake.requests[2][1]["number"] == 1


def test_concurrent_waits_on_empty_paths_each_read_their_own_version(monkeypatch):
    """Two threads waiting on different empty paths each read back the
    version that woke their own select. The response travels back to the
    waiter that asked rather than being parked on the shared context, so
    one thread's wait can't hand its answer to another's."""
    import threading

    selected_a = threading.Event()
    release_a = threading.Event()

    class _Wire:
        def __init__(self):
            self.requests = {}
            self._next_id = 0
            self._lock = threading.Lock()

        def _send(self, method, params):
            with self._lock:
                self._next_id += 1
                self.requests[self._next_id] = (method, params)
                return self._next_id

        def wait_for_response(self, request_id):
            method, params = self.requests[request_id]
            if method == "catalog_get":
                number = params.get("number")
                if number is None:
                    reply = _NOTHING
                else:
                    reply = _found(number, f"{params['path']}@{number}")
            elif params["handles"][0]["path"] == "a":
                # Hold the first wait open until the other has been answered.
                selected_a.set()
                assert release_a.wait(5)
                reply = _select_ok(1)
            else:
                reply = _select_ok(2)
            return {"id": request_id, **reply}

    fake = _Wire()
    monkeypatch.setattr(protocol, "get_protocol", lambda: _Proto(fake))
    monkeypatch.setattr(context_module, "get_dispatcher", lambda: fake)
    ctx = ExecutorContext("E1")

    results = {}
    waiter = threading.Thread(target=lambda: results.update(a=ctx.catalog_current("a")))
    waiter.start()
    assert selected_a.wait(5)
    results["b"] = ctx.catalog_current("b")
    release_a.set()
    waiter.join(5)
    assert results == {"a": "a@1", "b": "b@2"}
    # Neither wait left anything behind to be reused.
    assert ctx._resolved == {}


# --- publish -------------------------------------------------------------------


def test_publish_sends_the_serialised_value_and_returns_the_number(wire, monkeypatch):
    """What a handle publishes is serialised like a result — an asset
    becomes a reference — and the reply is the version's number."""
    fake = wire(publish_reply={"result": {"number": 7}})
    _install_context(monkeypatch, ExecutorContext("E1"))
    churn = Catalog("models/{name}").at(name="churn")
    assert churn.publish({"weights": Asset("A1"), "auc": 0.9}) == 7
    method, params = fake.requests[0]
    assert method == "catalog_publish"
    assert params["path"] == "models/churn"
    assert params["value"]["type"] == "inline"
    assert params["value"]["references"] == [["asset", "A1", None, None, None]]


# --- typed handles -------------------------------------------------------------


def _pydantic():
    return pytest.importorskip("pydantic")


def _churn_model(pydantic, **extra_fields):
    fields = {
        "__annotations__": {"weights": Asset, "auc": float, **extra_fields},
        "model_config": pydantic.ConfigDict(arbitrary_types_allowed=True),
    }
    return type("Churn", (pydantic.BaseModel,), fields)


def test_a_typed_publish_stores_the_models_fields_as_plain_data(wire, monkeypatch):
    """What goes to the server is a dict, encoded like any other — the
    asset inside it becomes a reference — so the catalog holds fields
    rather than an instance of this particular class."""
    pydantic = _pydantic()
    Churn = _churn_model(pydantic)
    fake = wire(publish_reply={"result": {"number": 1}})
    _install_context(monkeypatch, ExecutorContext("E1"))
    churn = Catalog[Churn]("models/{name}").at(name="churn")
    assert churn.publish(Churn(weights=Asset("A1"), auc=0.9)) == 1
    value = fake.requests[0][1]["value"]
    assert _fields(value["value"]) == {
        "weights": {"type": "ref", "index": 0},
        "auc": 0.9,
    }
    assert value["references"] == [["asset", "A1", None, None, None]]


def test_a_typed_publish_validates_a_dict_first(wire, monkeypatch):
    """A dict is accepted where the model accepts it, and refused — before
    anything goes out — where it doesn't."""
    pydantic = _pydantic()
    Churn = _churn_model(pydantic)
    fake = wire(publish_reply={"result": {"number": 1}})
    _install_context(monkeypatch, ExecutorContext("E1"))
    churn = Catalog[Churn]("models/churn")
    assert churn.publish({"weights": Asset("A1"), "auc": "0.9"}) == 1  # type: ignore[arg-type]
    assert _fields(fake.requests[0][1]["value"]["value"])["auc"] == 0.9
    with pytest.raises(pydantic.ValidationError):
        churn.publish({"auc": 0.9})  # type: ignore[arg-type]
    assert len(fake.requests) == 1


def test_a_typed_read_validates_with_the_readers_model(wire, monkeypatch):
    """The stored fields are validated with the model this handle
    declares, whatever the publisher used, so a version that doesn't fit
    is an error at the read — and one that does comes back an instance."""
    pydantic = _pydantic()
    Churn = _churn_model(pydantic)
    Stricter = _churn_model(pydantic, threshold=float)
    stored = _found(3, {"weights": Asset("A1"), "auc": 0.9})
    _install_context(monkeypatch, ExecutorContext("E1"))

    wire([stored])
    value = Catalog[Churn]("models/churn").current()
    assert isinstance(value, Churn)
    assert value.weights.id == "A1"
    assert value.auc == 0.9

    wire([stored])
    with pytest.raises(pydantic.ValidationError, match="threshold"):
        Catalog[Stricter]("models/churn").current()


def test_any_type_pydantic_can_validate_is_validated(wire, monkeypatch):
    """A model is one case: a plain annotation is checked the same way on
    both sides, coercing where the type would, and a class pydantic
    doesn't know — an asset — is checked by instance, with no config."""
    pydantic = _pydantic()
    _install_context(monkeypatch, ExecutorContext("E1"))

    counts = Catalog[dict[int, str]]("counts/x")
    fake = wire(publish_reply={"result": {"number": 4}})
    assert counts.publish({"1": "a"}) == 4  # type: ignore[arg-type]
    assert _fields(fake.requests[0][1]["value"]["value"]) == {1: "a"}
    with pytest.raises(pydantic.ValidationError):
        counts.publish({"x": "a"})  # type: ignore[arg-type]
    wire([_found(4, {1: "a"})])
    assert counts.current() == {1: "a"}
    wire([_found(5, {1: 2})])
    with pytest.raises(pydantic.ValidationError):
        counts.current()

    weights = Catalog[list[Asset]]("weights")
    wire([_found(1, [Asset("A1")])])
    assert weights.current()[0].id == "A1"
    wire([_found(2, ["A1"])])
    with pytest.raises(pydantic.ValidationError):
        weights.current()


def test_any_passes_a_value_through_untouched(wire, monkeypatch):
    """``Any`` validates everything and dumps an unknown object as itself,
    so an asset under it still becomes a reference."""
    _pydantic()
    _install_context(monkeypatch, ExecutorContext("E1"))
    fake = wire(publish_reply={"result": {"number": 1}})
    assert Catalog[t.Any]("x").publish({"weights": Asset("A1")}) == 1
    assert fake.requests[0][1]["value"]["references"] == [
        ["asset", "A1", None, None, None]
    ]


def test_without_pydantic_the_type_only_informs_type_checkers(wire, monkeypatch):
    """With nothing to validate with, the value is published and read as
    it is."""
    monkeypatch.setattr(catalog_module, "pydantic", None)
    _install_context(monkeypatch, ExecutorContext("E1"))
    counts = Catalog[dict[int, str]]("counts/x")
    assert counts._adapter is None
    wire([_found(3, {"1": "a"})])
    assert counts.current() == {"1": "a"}
    fake = wire(publish_reply={"result": {"number": 4}})
    assert counts.publish({"1": "a"}) == 4  # type: ignore[arg-type]
    assert _fields(fake.requests[0][1]["value"]["value"]) == {"1": "a"}


# --- next ----------------------------------------------------------------------


def test_next_suspends_with_the_path(monkeypatch):
    """``next()`` never returns: it raises the suspension signal carrying
    the path, and the server gates the successor at the execution's own
    view of it. No request goes out before the body has unwound."""
    ctx = ExecutorContext("E1")
    _install_context(monkeypatch, ctx)
    with pytest.raises(Suspending) as raised:
        Catalog("models/{name}").at(name="churn").next()
    assert raised.value.catalog_wait == "models/churn"
    assert raised.value.execute_after is None
    assert raised.value.stream_wait is None


def test_the_suspend_request_carries_the_catalog_wait(monkeypatch):
    sent = []

    class _Proto:
        def send_request(self, method, params):
            sent.append((method, params))
            return 1

    monkeypatch.setattr(protocol, "get_protocol", lambda: _Proto())
    from coflux.protocol import request_suspend

    request_suspend("E1", None, None, "models/churn")
    assert sent == [
        ("suspend", {"execution_id": "E1", "catalog_wait": {"path": "models/churn"}})
    ]
