"""The catalog API on the adapter side.

The server owns the semantics — snapshots, numbering, visibility — and the
e2e suite covers those. This checks what the adapter itself decides: that
an entry is a handle and not a value, how the select handles are shaped,
that ``next()`` is a suspension carrying the path, and how ``current()``
and ``cf.publish()`` turn the server's answers into values and numbers.
"""

from __future__ import annotations

import pytest

from coflux import context as context_module
from coflux import protocol
from coflux.context import ExecutorContext, _CatalogPosition, _handle_key, _handle_wire
from coflux.errors import Suspending
from coflux.models import Asset, CatalogEntry, Execution
from coflux.serialization import serialize_value

# --- the entry ---------------------------------------------------------------


def test_an_entry_is_a_local_handle_not_a_value():
    """Pass the path, or the value the entry holds — never the entry."""
    with pytest.raises(TypeError, match="pass 'models/churn'"):
        serialize_value({"e": CatalogEntry("models/churn")})


def test_entries_compare_by_path():
    assert CatalogEntry("a") == CatalogEntry("a")
    assert CatalogEntry("a") != CatalogEntry("b")
    assert len({CatalogEntry("a"), CatalogEntry("a")}) == 1
    assert repr(CatalogEntry("a/b")) == "CatalogEntry('a/b')"


def test_entry_validates_the_path_up_front():
    """The server's rule, applied at construction, so a typo fails here
    rather than as a wait that never ends."""
    for bad in ["", "/abs", "a//b", "a/../b", "a@1", "a:b", "a b", 1]:
        with pytest.raises(ValueError):
            CatalogEntry(bad)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="too long"):
        CatalogEntry("a" * 513)
    assert CatalogEntry("a-b_c.d/e").path == "a-b_c.d/e"


def test_publish_validates_the_path_up_front(wire, monkeypatch):
    """The same rule as ``cf.catalog``, before anything goes out."""
    import coflux as cf

    fake = wire()
    _install_context(monkeypatch, ExecutorContext("E1"))
    with pytest.raises(ValueError, match="invalid catalog path"):
        cf.publish("bad path", 1)
    assert fake.requests == []


def test_request_error_is_part_of_the_public_api():
    import coflux as cf

    error = cf.RequestError("invisible", "server error: invisible")
    assert error.code == "invisible"
    assert isinstance(error, RuntimeError)


# --- select handles ----------------------------------------------------------


def test_an_entry_is_a_catalog_select_handle_without_a_position():
    """The server fills the position in from the execution's own view."""
    entry = CatalogEntry("p/q")
    assert _handle_key(entry) == ("catalog", "p/q")
    assert _handle_wire(entry) == {"type": "catalog", "path": "p/q"}
    # An execution handle is unchanged.
    assert _handle_wire(Execution("E1", "m", "t")) == {"type": "execution", "id": "E1"}


def test_a_position_pins_the_number_to_wait_past():
    position = _CatalogPosition("p/q", 0)
    assert _handle_key(position) == ("catalog", "p/q@0")
    assert _handle_wire(position) == {"type": "catalog", "path": "p/q", "number": 0}


def test_an_entry_cannot_be_cancelled(wire):
    """A select handle, but not a cancellable one: nothing is pending
    behind a path. Refused locally, before anything goes out."""
    fake = wire()
    with pytest.raises(TypeError, match="cannot cancel CatalogEntry"):
        ExecutorContext("E1").cancel([Execution("E2", "m", "t"), CatalogEntry("p/q")])
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


# --- reads ---------------------------------------------------------------------


def test_current_returns_the_value_without_waiting(wire):
    fake = wire([_found(3, {"threshold": 0.7, "model": Asset("A1")})])
    value = ExecutorContext("E1").catalog_current("configs/training")
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


def _install_context(monkeypatch, ctx):
    from coflux import state

    monkeypatch.setattr(state, "get_context", lambda: ctx)
    monkeypatch.setattr("coflux.models.get_context", lambda: ctx)
    monkeypatch.setattr("coflux.get_context", lambda: ctx)


def test_publish_sends_the_serialised_value_and_returns_the_number(wire, monkeypatch):
    """``cf.publish`` is a top-level verb, not a method on the entry. What
    it publishes is serialised like a result — an asset becomes a
    reference — and the reply is the version's number."""
    import coflux as cf

    fake = wire(publish_reply={"result": {"number": 7}})
    _install_context(monkeypatch, ExecutorContext("E1"))
    number = cf.publish("models/churn", {"weights": Asset("A1"), "auc": 0.9})
    assert number == 7
    assert not hasattr(CatalogEntry("models/churn"), "publish")
    method, params = fake.requests[0]
    assert method == "catalog_publish"
    assert params["path"] == "models/churn"
    assert params["value"]["type"] == "inline"
    assert params["value"]["references"] == [["asset", "A1", None, None, None]]


# --- next ----------------------------------------------------------------------


def test_next_suspends_with_the_path(monkeypatch):
    """``next()`` never returns: it raises the suspension signal carrying
    the path, and the server gates the successor at the execution's own
    view of it. No request goes out before the body has unwound."""
    ctx = ExecutorContext("E1")
    _install_context(monkeypatch, ctx)
    with pytest.raises(Suspending) as raised:
        CatalogEntry("models/churn").next()
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
