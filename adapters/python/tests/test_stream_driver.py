"""Producer-side driver tests: registration is a request the server answers
with the stream's identity and resume point, and a suspend from inside a
generator leaves the stream open.

The driver talks to the server through ``protocol`` and waits on the
``dispatcher``; both are faked here, so no CLI or server is involved.
"""

from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from coflux import protocol, streams
from coflux.streams import StreamDriver


class _FakeDispatcher:
    """Answers stream_register requests with a canned reply and grants
    unlimited demand, so the driver never waits."""

    def __init__(self, reply):
        self.reply = reply
        self.handlers = {}
        self.closed = False

    def register_notification(self, method, handler):
        self.handlers[method] = handler

    def wait_for_response(self, request_id, timeout=None):
        return {"id": request_id, "result": dict(self.reply)}

    def is_closed(self):
        return self.closed


class _Harness:
    def __init__(self, reply):
        self.dispatcher = _FakeDispatcher(reply)
        self.registers = []
        self.appends = []
        self.closes = []
        self.done = threading.Event()

    def request_stream_register(
        self, execution_id, position, buffer=None, timeout_ms=None
    ):
        self.registers.append((execution_id, position, buffer, timeout_ms))
        return len(self.registers)

    def send_stream_append(self, execution_id, index, sequence, value):
        self.appends.append((index, sequence, value))

    def send_stream_close(self, execution_id, index, **error):
        self.closes.append((index, error))


@pytest.fixture
def harness(monkeypatch):
    def make(reply):
        h = _Harness(reply)
        monkeypatch.setattr(streams, "get_dispatcher", lambda: h.dispatcher)
        monkeypatch.setattr(
            protocol, "request_stream_register", h.request_stream_register
        )
        monkeypatch.setattr(protocol, "send_stream_append", h.send_stream_append)
        monkeypatch.setattr(protocol, "send_stream_close", h.send_stream_close)
        monkeypatch.setattr(streams, "serialize_value", lambda value: value)
        return h

    return make


def test_new_stream_sequences_from_zero(harness):
    h = harness({"id": "run1:2_0", "index": 0, "head": -1})
    driver = StreamDriver("run1:2:1")

    def gen():
        yield "a"
        yield "b"

    # Unbounded, so the driver doesn't wait for demand grants.
    stream_id = driver.register(gen(), buffer=None)
    driver.wait_all()

    assert stream_id == "run1:2_0"
    assert h.registers == [("run1:2:1", 0, None, None)]
    assert h.appends == [(0, 0, "a"), (0, 1, "b")]
    assert h.closes == [(0, {})]


def test_resumed_stream_continues_from_the_head(harness):
    # The server says this registration resumes a paused stream whose
    # last item was sequence 4, under the step's index 3.
    h = harness({"id": "run1:2_3", "index": 3, "head": 4})
    driver = StreamDriver("run1:2:2")

    def gen():
        yield "e"
        yield "f"

    stream_id = driver.register(gen(), buffer=None)
    driver.wait_all()

    assert stream_id == "run1:2_3"
    assert h.appends == [(3, 5, "e"), (3, 6, "f")]
    assert h.closes == [(3, {})]


def test_positions_count_registrations_within_the_execution(harness):
    h = harness({"id": "run1:2_7", "index": 7, "head": -1})
    driver = StreamDriver("run1:2:1")

    def gen():
        yield from ()

    driver.register(gen(), buffer=None)
    driver.register(gen(), buffer=None)
    driver.wait_all()

    assert [position for _, position, _, _ in h.registers] == [0, 1]


def test_suspend_inside_the_generator_sends_no_close(harness):
    h = harness({"id": "run1:2_0", "index": 0, "head": -1})
    driver = StreamDriver("run1:2:1")

    def gen():
        yield "a"
        # ``cf.suspend()`` ends the calling thread with SystemExit once the
        # server has confirmed the suspension. The stream must be left
        # paused for the resumed execution, not closed.
        raise SystemExit(0)

    driver.register(gen(), buffer=None)
    driver.wait_all()

    assert h.appends == [(0, 0, "a")]
    assert h.closes == []


def test_generator_error_closes_with_the_error(harness):
    h = harness({"id": "run1:2_0", "index": 0, "head": -1})
    driver = StreamDriver("run1:2:1")

    def gen():
        yield "a"
        raise ValueError("boom")

    driver.register(gen(), buffer=None)
    driver.wait_all()

    assert h.appends == [(0, 0, "a")]
    assert len(h.closes) == 1
    index, error = h.closes[0]
    assert index == 0
    assert error["error_type"] == "builtins.ValueError"
    assert error["error_message"] == "boom"


def test_registration_error_is_raised_to_the_caller(harness, monkeypatch):
    h = harness({})
    monkeypatch.setattr(
        h.dispatcher,
        "wait_for_response",
        lambda request_id, timeout=None: {
            "id": request_id,
            "error": {"code": "execution_completed", "message": "finalised"},
        },
    )
    driver = StreamDriver("run1:2:1")

    def gen():
        yield "a"

    with pytest.raises(RuntimeError, match="execution_completed"):
        driver.register(gen(), buffer=None)
    assert h.appends == []


def test_recurrent_generator_target_is_rejected():
    import coflux as cf

    with pytest.raises(TypeError, match="recurrent=True"):

        @cf.task(recurrent=True)
        def ticker():
            yield 1

    # The non-recurrent form, and recurrence on a plain body, are fine.
    @cf.task()
    def stream_task():
        yield 1

    @cf.task(recurrent=True)
    def plain_task():
        return None

    _ = SimpleNamespace(stream_task=stream_task, plain_task=plain_task)


def test_demand_granted_before_the_register_reply_is_not_lost(harness):
    """The server grants demand while handling a registration, so the
    grant can reach the adapter before the reply that names the stream's
    index. That's the normal case for a resumed stream (its subscribers
    are already waiting) and for a pre-warming buffer. The credits must
    be held and applied, not dropped as belonging to an unknown stream.
    """
    h = harness({"id": "run1:2_0", "index": 0, "head": 3})
    dispatcher = h.dispatcher
    original = dispatcher.wait_for_response

    def grant_then_reply(request_id, timeout=None):
        # One credit for the item, one for the ``next()`` that ends the
        # generator — both delivered ahead of the reply.
        dispatcher.handlers["stream_demand"]({"index": 0, "n": 2})
        return original(request_id, timeout)

    dispatcher.wait_for_response = grant_then_reply
    driver = StreamDriver("run1:2:2")

    def gen():
        yield "e"

    # Bounded (lockstep), so the driver only proceeds on granted credit.
    driver.register(gen(), buffer=0)
    driver.wait_all()

    assert h.appends == [(0, 4, "e")]
    assert h.closes == [(0, {})]
