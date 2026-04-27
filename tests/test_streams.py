"""Integration tests for the streaming protocol.

These tests drive the mock adapter directly — they send/receive the wire
messages the real Python adapter would use, so they exercise the full
server + CLI relay + subscription/push pipeline.

Two common patterns:

  * Producer-only: a single execution registers a stream, appends items,
    and closes. Verification is done by a subsequent consumer subscription
    (also driven by the same test).

  * Producer + consumer interleaved: both are driven from the same test,
    taking turns over different connections.
"""

import time

import pytest

from support.manifest import task, workflow
from support.protocol import (
    execution_handle,
    execution_result,
    json_args,
    partition_stride,
    slice_stride,
    stride,
)


def _run_and_handle_stream(ctx, targets, produce_fn):
    """Submit a no-arg workflow and hand the executor connection to `produce_fn`.

    ``produce_fn(conn, execution_id)`` does whatever stream work the test
    needs (register / append / close) and then sends an execution_result.
    Returns the run_id so tests can assert on topic state if desired.
    """
    resp = ctx.submit("test", targets[0]["name"])
    ex = ctx.executor.next_execute()
    produce_fn(ex.conn, ex.execution_id)
    return resp["runId"], ex.execution_id


def test_producer_writes_and_consumer_reads_backlog(worker):
    """Producer registers, appends 3 items, closes. Then a consumer in a
    separate execution subscribes and drains the backlog plus close.
    """
    targets = [
        workflow("test", "producer"),
        workflow("test", "consumer"),
    ]

    with worker(targets) as ctx:
        # Producer
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)
        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 0, "a")
        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 1, "b")
        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 2, "c")
        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.complete(prod_ex.execution_id, value=42)

        # Producer's run must finish before the consumer can subscribe —
        # otherwise the execution_id isn't known to the consumer's workflow.
        ctx.result(prod_resp["runId"])

        # Consumer in a separate workflow subscribes to the producer's stream.
        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
        )
        items, closed = cons_ex.conn.drain_stream(subscription_id=1)
        cons_ex.conn.complete(cons_ex.execution_id)

        assert [item[0] for item in items] == [0, 1, 2]
        assert [item[1]["value"] for item in items] == ["a", "b", "c"]
        assert closed.get("error") is None


def test_backlog_replay_after_producer_terminated_with_buffer(worker):
    """Regression: subscribing to a closed bounded-buffer stream whose
    producer execution has terminated must replay the backlog and close.

    Previously, ``ensure_stream_producer`` rebuilt the producer's
    in-memory state with ``session_id=nil`` for the vanished execution.
    After ``push_backlog_items`` advanced the consumer's cursor past
    ``demand_granted``, ``refresh_stream_demand`` would compute a
    positive delta and call ``send_session(state, nil, ...)``, crashing
    the project GenServer with ``KeyError :connection`` and leaving the
    consumer hung. The unbounded (``buffer=null``) path didn't trigger
    it because ``ensure_stream_producer`` no-ops in that case.
    """
    targets = [
        workflow("test", "producer"),
        workflow("test", "consumer"),
    ]

    with worker(targets) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0, buffer=0)
        # First subscriber to a buffer=0 stream needs a credit before the
        # producer can append; the demand grant arrives once the consumer
        # subscribes. To keep the test focused on the post-termination
        # replay, append items here without waiting on demand — the test
        # adapter doesn't enforce credit accounting on appends.
        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 0, "a")
        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 1, "b")
        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 2, "c")
        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.complete(prod_ex.execution_id, value=42)

        # Producer execution must be fully gone — its session entry for
        # this execution removed — before the consumer subscribes.
        ctx.result(prod_resp["runId"])

        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
        )
        items, closed = cons_ex.conn.drain_stream(subscription_id=1)
        cons_ex.conn.complete(cons_ex.execution_id)

        assert [item[0] for item in items] == [0, 1, 2]
        assert [item[1]["value"] for item in items] == ["a", "b", "c"]
        assert closed.get("error") is None


def test_consumer_sees_live_push(worker):
    """Consumer subscribes *before* the producer appends. Items arrive live."""
    targets = [
        workflow("test", "producer"),
        workflow("test", "consumer"),
    ]

    with worker(targets, concurrency=2) as ctx:
        # Producer registers but doesn't append yet.
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)

        # Consumer subscribes now — stream is open with no items yet.
        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
        )

        # Now producer appends + closes.
        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 0, 10)
        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 1, 20)
        prod_ex.conn.stream_close(prod_ex.execution_id, 0)

        items, closed = cons_ex.conn.drain_stream(subscription_id=1)

        prod_ex.conn.complete(prod_ex.execution_id)
        cons_ex.conn.complete(cons_ex.execution_id)

        assert [item[1]["value"] for item in items] == [10, 20]
        assert closed.get("error") is None


def test_slice_filter_restricts_items(worker):
    """Slice filter ``[1, 3)`` delivers only positions 1 and 2."""
    targets = [workflow("test", "producer"), workflow("test", "consumer")]

    with worker(targets) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)
        for i in range(5):
            prod_ex.conn.stream_append(prod_ex.execution_id, 0, i, i * 10)
        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.complete(prod_ex.execution_id)
        ctx.result(prod_resp["runId"])

        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
            stride=slice_stride(1, 3),
        )
        items, _ = cons_ex.conn.drain_stream(subscription_id=1)
        cons_ex.conn.complete(cons_ex.execution_id)

        assert [item[0] for item in items] == [1, 2]
        assert [item[1]["value"] for item in items] == [10, 20]


def test_partition_filter_round_robin(worker):
    """Partition filter ``(n=3, i=1)`` delivers positions 1, 4, 7."""
    targets = [workflow("test", "producer"), workflow("test", "consumer")]

    with worker(targets) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)
        for i in range(9):
            prod_ex.conn.stream_append(prod_ex.execution_id, 0, i, i)
        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.complete(prod_ex.execution_id)
        ctx.result(prod_resp["runId"])

        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
            stride=partition_stride(n=3, i=1),
        )
        items, _ = cons_ex.conn.drain_stream(subscription_id=1)
        cons_ex.conn.complete(cons_ex.execution_id)

        assert [item[0] for item in items] == [1, 4, 7]


def test_producer_error_closes_with_error_info(worker):
    """Generator raises mid-stream: subscriber sees items-so-far then an
    errored closure carrying {type, message}.
    """
    targets = [workflow("test", "producer"), workflow("test", "consumer")]

    with worker(targets) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)
        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 0, "ok")
        prod_ex.conn.stream_close(
            prod_ex.execution_id,
            0,
            error={"type": "ValueError", "message": "boom", "traceback": ""},
        )
        prod_ex.conn.complete(prod_ex.execution_id)
        ctx.result(prod_resp["runId"])

        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
        )
        items, closed = cons_ex.conn.drain_stream(subscription_id=1)
        cons_ex.conn.complete(cons_ex.execution_id)

        assert [item[1]["value"] for item in items] == ["ok"]
        err = closed.get("error")
        assert err is not None
        assert err["type"] == "ValueError"
        assert err["message"] == "boom"


def test_subscribe_to_unknown_producer_closes_immediately(worker):
    """Subscribing to a stream that doesn't exist yields an immediate error
    closure (not an indefinite wait).
    """
    targets = [workflow("test", "consumer")]

    with worker(targets) as ctx:
        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id="00000000:0:0",
            index=0,
        )
        _items, closed = cons_ex.conn.drain_stream(subscription_id=1)
        cons_ex.conn.complete(cons_ex.execution_id)

        assert closed.get("reason") == "producer_not_found"
        assert closed.get("error") is None


def test_topic_exposes_stream_state(worker):
    """Studio topic gets `streams` per execution: opened, closed, error."""
    targets = [workflow("test", "producer")]

    with worker(targets) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)
        prod_ex.conn.stream_register(prod_ex.execution_id, 1)
        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.stream_close(
            prod_ex.execution_id,
            1,
            error={"type": "RuntimeError", "message": "bad", "traceback": ""},
        )
        prod_ex.conn.complete(prod_ex.execution_id)
        ctx.result(prod_resp["runId"])

        snapshot = ctx.inspect(prod_resp["runId"])
        # The run snapshot has a `steps → {run:step → {executions → {attempt → {...}}}}` shape.
        step = next(iter(snapshot["steps"].values()))
        execution = next(iter(step["executions"].values()))
        streams = execution["streams"]

        assert "0" in streams and "1" in streams
        assert streams["0"]["openedAt"] is not None
        assert streams["0"]["closedAt"] is not None
        assert streams["0"]["reason"] == "complete"
        assert streams["0"]["error"] is None
        assert streams["1"]["closedAt"] is not None
        assert streams["1"]["reason"] == "errored"
        assert streams["1"]["error"]["type"] == "RuntimeError"
        assert streams["1"]["error"]["message"] == "bad"
        assert isinstance(streams["1"]["error"]["frames"], list)


def test_cancellation_closes_streams_with_cancelled_reason(worker):
    """Cancel an execution mid-stream: the subscriber receives a closure
    carrying reason="cancelled" — no fabricated exception type, the
    adapter maps the reason to its own idiom.
    """
    targets = [workflow("test", "producer"), workflow("test", "consumer")]

    with worker(targets, concurrency=2) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)
        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 0, "before")

        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
        )

        ctx.cancel(prod_ex.execution_id)

        items, closed = cons_ex.conn.drain_stream(subscription_id=1)
        cons_ex.conn.complete(cons_ex.execution_id)

        assert [item[1]["value"] for item in items] == ["before"]
        assert closed.get("reason") == "cancelled"
        assert closed.get("error") is None


def test_multiple_subscribers_get_independent_delivery(worker):
    """Two consumers subscribe to the same stream — each gets the full
    sequence independently.
    """
    targets = [workflow("test", "producer"), workflow("test", "consumer")]

    with worker(targets, concurrency=3) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)
        for i in range(3):
            prod_ex.conn.stream_append(prod_ex.execution_id, 0, i, i)
        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.complete(prod_ex.execution_id)
        ctx.result(prod_resp["runId"])

        # Each consumer picks its own subscription id locally; they only
        # need to be unique within each consumer execution. Use different
        # values here so we'd also catch any stale cross-consumer routing.
        a_resp = ctx.submit("test", "consumer")
        a_ex = ctx.executor.next_execute()
        a_ex.conn.stream_subscribe(
            a_ex.execution_id,
            subscription_id=7,
            producer_execution_id=prod_ex.execution_id,
            index=0,
        )

        b_resp = ctx.submit("test", "consumer")
        b_ex = ctx.executor.next_execute()
        b_ex.conn.stream_subscribe(
            b_ex.execution_id,
            subscription_id=42,
            producer_execution_id=prod_ex.execution_id,
            index=0,
        )

        a_items, _ = a_ex.conn.drain_stream(subscription_id=7)
        b_items, _ = b_ex.conn.drain_stream(subscription_id=42)
        a_ex.conn.complete(a_ex.execution_id)
        b_ex.conn.complete(b_ex.execution_id)

        assert [item[1]["value"] for item in a_items] == [0, 1, 2]
        assert [item[1]["value"] for item in b_items] == [0, 1, 2]


def test_subscription_ids_can_collide_across_consumers(worker):
    """Two different consumer executions can each allocate the same
    subscription id locally — the server scopes its routing map by
    consumer_execution_id, so items for each consumer's subscription
    reach the right executor.
    """
    targets = [workflow("test", "producer"), workflow("test", "consumer")]

    with worker(targets, concurrency=3) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)
        for i in range(3):
            prod_ex.conn.stream_append(prod_ex.execution_id, 0, i, f"v{i}")
        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.complete(prod_ex.execution_id)
        ctx.result(prod_resp["runId"])

        # Both consumers use subscription_id=1 — they must not collide.
        a_resp = ctx.submit("test", "consumer")
        a_ex = ctx.executor.next_execute()
        a_ex.conn.stream_subscribe(
            a_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
        )

        b_resp = ctx.submit("test", "consumer")
        b_ex = ctx.executor.next_execute()
        b_ex.conn.stream_subscribe(
            b_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
        )

        a_items, a_closed = a_ex.conn.drain_stream(subscription_id=1)
        b_items, b_closed = b_ex.conn.drain_stream(subscription_id=1)
        a_ex.conn.complete(a_ex.execution_id)
        b_ex.conn.complete(b_ex.execution_id)

        assert [item[1]["value"] for item in a_items] == ["v0", "v1", "v2"]
        assert [item[1]["value"] for item in b_items] == ["v0", "v1", "v2"]
        assert a_closed.get("error") is None
        assert b_closed.get("error") is None


def test_consumer_termination_drops_subscription(worker):
    """When a consumer's notify_terminated arrives, the server must drop
    its stream subscriptions so subsequent producer appends don't try to
    route to a gone consumer.
    """
    targets = [workflow("test", "producer"), workflow("test", "consumer")]

    with worker(targets, concurrency=2) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)

        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
        )

        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 0, "before")
        first = cons_ex.conn.recv_push("stream_items", subscription_id=1, timeout=3)
        assert first["items"][0][1]["value"] == "before"

        # Consumer finishes without explicit unsubscribe — notify_terminated
        # from the session should drop the subscription on the server side.
        cons_ex.conn.complete(cons_ex.execution_id)
        ctx.result(cons_resp["runId"])

        # Producer keeps appending; these should not cause the server to
        # error trying to route to the dead consumer. The producer finishes
        # cleanly — the assertion is that the server doesn't crash.
        for i in range(1, 5):
            prod_ex.conn.stream_append(prod_ex.execution_id, 0, i, i)
        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.complete(prod_ex.execution_id)
        ctx.result(prod_resp["runId"])


def test_slice_with_stop_closes_early(worker):
    """slice(0, 2) on a stream that has more items should close the
    subscriber as soon as position 2 is reached, not wait for the full
    stream to drain. The early-close path is the `filter_exhausted?` branch
    in push_stream_item.
    """
    targets = [workflow("test", "producer"), workflow("test", "consumer")]

    with worker(targets, concurrency=2) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)

        # Subscriber gets first 2 items then close.
        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
            stride=slice_stride(0, 2),
        )

        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 0, "a")
        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 1, "b")

        items, closed = cons_ex.conn.drain_stream(subscription_id=1, timeout=5)

        # items 2+ should NOT reach the subscriber — its slice is satisfied.
        # Finish the producer so its run wraps up cleanly.
        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 2, "c")
        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.complete(prod_ex.execution_id)
        cons_ex.conn.complete(cons_ex.execution_id)

        assert [item[1]["value"] for item in items] == ["a", "b"]
        assert closed.get("error") is None


def test_unsubscribe_prevents_receiving_full_stream(worker):
    """Consumer unsubscribes partway through and doesn't receive every item.

    Ordering note: with the producer and consumer on separate sessions,
    a few items appended immediately after unsubscribe can still reach the
    consumer if they're in flight when the server processes unsubscribe.
    The meaningful check is that the consumer stops seeing items before
    the full stream is delivered — not that unsubscribe is synchronous.
    """
    targets = [workflow("test", "producer"), workflow("test", "consumer")]

    with worker(targets, concurrency=2) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)

        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
        )

        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 0, 0)
        first = cons_ex.conn.recv_push("stream_items", subscription_id=1, timeout=3)
        assert first["items"][0][1]["value"] == 0
        cons_ex.conn.stream_unsubscribe(cons_ex.execution_id, subscription_id=1)

        # Producer keeps appending after unsubscribe.
        for i in range(1, 10):
            prod_ex.conn.stream_append(prod_ex.execution_id, 0, i, i)
        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.complete(prod_ex.execution_id)

        # Collect anything still in flight. It might include the next
        # one or two items (racing with unsubscribe) but MUST NOT include
        # the tail — some positions drop out between unsubscribe and close.
        received_sequences = [0]
        try:
            while True:
                msg = cons_ex.conn.recv_push(
                    "stream_items", subscription_id=1, timeout=0.5
                )
                for item in msg["items"]:
                    received_sequences.append(item[0])
        except TimeoutError:
            pass

        cons_ex.conn.complete(cons_ex.execution_id)

        # The consumer should have received strictly fewer than all 10 items.
        assert len(received_sequences) < 10, (
            f"unsubscribe should stop further delivery; got {received_sequences}"
        )


def test_close_while_subscribed_delivers_closure(worker):
    """Close the stream after a subscriber is already connected — the
    closure gets pushed to the live subscriber (not just stored).
    """
    targets = [workflow("test", "producer"), workflow("test", "consumer")]

    with worker(targets, concurrency=2) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)

        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
        )

        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 0, "only")
        prod_ex.conn.stream_close(prod_ex.execution_id, 0)

        items, closed = cons_ex.conn.drain_stream(subscription_id=1)
        prod_ex.conn.complete(prod_ex.execution_id)
        cons_ex.conn.complete(cons_ex.execution_id)

        assert [item[1]["value"] for item in items] == ["only"]
        assert closed.get("error") is None


def test_lifecycle_close_on_completion_delivers_to_subscriber(worker):
    """Producer registers a stream but never explicitly closes it.
    When the execution completes, close_open_streams backstops — subscriber
    gets a clean close.
    """
    targets = [workflow("test", "producer"), workflow("test", "consumer")]

    with worker(targets, concurrency=2) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)
        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 0, 1)

        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
        )

        # Producer completes *without* closing the stream.
        prod_ex.conn.complete(prod_ex.execution_id)

        items, closed = cons_ex.conn.drain_stream(subscription_id=1)
        cons_ex.conn.complete(cons_ex.execution_id)

        assert [item[1]["value"] for item in items] == [1]
        assert closed.get("error") is None  # clean close — execution completed normally


def test_stride_combines_slice_and_partition(worker):
    """The client composes ``slice(0, 6)`` then ``partition(2, 0)`` into
    a single stride ``[0:6:2]``, which selects positions 0, 2, 4."""
    targets = [workflow("test", "producer"), workflow("test", "consumer")]

    with worker(targets) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)
        for i in range(10):
            prod_ex.conn.stream_append(prod_ex.execution_id, 0, i, i)
        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.complete(prod_ex.execution_id)
        ctx.result(prod_resp["runId"])

        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
            stride=stride(start=0, stop=6, step=2),
        )
        items, _ = cons_ex.conn.drain_stream(subscription_id=1)
        cons_ex.conn.complete(cons_ex.execution_id)

        assert [item[0] for item in items] == [0, 2, 4]


# --- Backpressure -------------------------------------------------------


def test_backpressure_no_buffer_no_initial_demand(worker):
    """Registering with buffer=0 and no subscribers: the server sends no
    demand grants — the producer stays paused until a consumer attaches.
    """
    targets = [workflow("test", "producer")]

    with worker(targets) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0, buffer=0)

        # Producer shouldn't get any demand grants yet.
        with pytest.raises(TimeoutError):
            prod_ex.conn.recv_push("stream_demand", timeout=0.5)

        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.complete(prod_ex.execution_id)
        ctx.result(prod_resp["runId"])


def test_backpressure_prewarms_up_to_buffer(worker):
    """buffer=N without any subscribers: producer is granted N credits
    up front so it can run ahead and pre-warm.
    """
    targets = [workflow("test", "producer")]

    with worker(targets) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0, buffer=5)

        params = prod_ex.conn.recv_push("stream_demand", timeout=2)
        assert params["index"] == 0
        assert params["n"] == 5

        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.complete(prod_ex.execution_id)
        ctx.result(prod_resp["runId"])


def test_backpressure_subscribe_unblocks_producer(worker):
    """buffer=0 + consumer subscribes → server grants 1 credit. Producer
    emits. Consumer reads → cursor advances → server grants 1 more.
    """
    targets = [workflow("test", "producer"), workflow("test", "consumer")]

    with worker(targets, concurrency=2) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0, buffer=0)

        # No consumer yet → no demand.
        with pytest.raises(TimeoutError):
            prod_ex.conn.recv_push("stream_demand", timeout=0.3)

        # Attach consumer. First grant arrives.
        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
        )
        first = prod_ex.conn.recv_push("stream_demand", timeout=2)
        assert first["n"] == 1

        # Producer emits item 0 (it's the adapter's responsibility to
        # decrement credits; we just emulate that here by appending).
        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 0, "hi")

        # Consumer receives item → cursor advances → server grants again.
        cons_ex.conn.recv_push("stream_items", subscription_id=1, timeout=2)
        second = prod_ex.conn.recv_push("stream_demand", timeout=2)
        assert second["n"] == 1

        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.complete(prod_ex.execution_id)
        cons_ex.conn.complete(cons_ex.execution_id)
        ctx.result(prod_resp["runId"])


def test_workflow_produces_stream_while_awaiting_consumer(worker):
    """Regression: a workflow that produces a stream *and* synchronously
    awaits a consumer of that stream must not deadlock.

    Reproduces the ``inline_producer`` pattern from
    ``examples/python/examples/streams.py``: the workflow registers an
    inline stream, submits a consumer task with the stream as its
    argument, then calls ``select`` (blocking) to wait for the
    consumer's result. The producer driver keeps emitting items on the
    same session while the select is in flight.

    Before the fix, the CLI's per-executor message loop handled
    ``select`` synchronously: while waiting on the server's response,
    the loop couldn't read the adapter's subsequent ``stream_append``
    notifications from stdout. The appends stayed in the readLoop's
    channel, the consumer never received any items, and the select
    never resolved — a full deadlock.
    """
    targets = [
        workflow("test", "producer_workflow"),
        task("test", "consumer"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "producer_workflow")
        wf = ctx.executor.next_execute()

        # Workflow registers an inline stream and submits the consumer
        # task with that stream handle as its argument.
        wf.conn.stream_register(wf.execution_id, 0, buffer=0)
        stream_arg = {
            "type": "inline",
            "format": "json",
            "value": {"type": "stream", "id": f"{wf.execution_id}_0"},
            "references": [],
        }
        consumer_id = wf.conn.submit_task(
            wf.execution_id, "test", "consumer", [stream_arg]
        )

        # Send the select ourselves so we can interleave stream_appends
        # behind it without racing a background thread. The real adapter
        # does exactly this: its main thread blocks in _wait_response
        # while the stream driver thread keeps calling send_stream_append
        # on the same stdout.
        select_id = wf.conn._next_request_id
        wf.conn._next_request_id += 1
        wf.conn.send(
            {
                "id": select_id,
                "method": "select",
                "params": {
                    "execution_id": wf.execution_id,
                    "handles": [execution_handle(consumer_id)],
                    "suspend": False,
                },
            }
        )

        # Consumer picks up and subscribes before any items are emitted.
        cons = ctx.executor.next_execute()
        cons.conn.stream_subscribe(
            cons.execution_id,
            subscription_id=1,
            producer_execution_id=wf.execution_id,
            index=0,
        )

        # Emit items on the workflow's connection while its select is
        # pending. Each of these requires the CLI to read and forward
        # the notification *while* the select request is still in
        # flight — the exact path that used to deadlock.
        for i in range(3):
            wf.conn.stream_append(wf.execution_id, 0, i, i * 10)
        wf.conn.stream_close(wf.execution_id, 0)

        # Consumer drains — reaching the close means every append made
        # it through the CLI while select was holding the loop.
        items, closed = cons.conn.drain_stream(subscription_id=1, timeout=5)
        cons.conn.complete(cons.execution_id, value=len(items))

        # Read directly from the socket (not via _buffer, which would
        # re-pop anything we stored) until the select response arrives.
        # If the bug is present this never happens and _recv_raw times
        # out.
        deadline = time.time() + 5
        while True:
            remaining = max(0.01, deadline - time.time())
            msg = wf.conn._recv_raw(remaining)
            if msg.get("id") == select_id:
                assert "error" not in msg, f"select errored: {msg['error']}"
                break

        wf.conn.complete(wf.execution_id, value=len(items))
        ctx.result(resp["runId"])

        assert [item[1]["value"] for item in items] == [0, 10, 20]
        assert closed.get("error") is None
        assert closed.get("reason") != "timeout"


def test_backpressure_unbounded_sends_no_demand(worker):
    """Registering without a buffer (wire buffer=null) opts out of
    backpressure — the server never sends demand grants for this stream.
    """
    targets = [workflow("test", "producer"), workflow("test", "consumer")]

    with worker(targets, concurrency=2) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)  # buffer omitted

        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
        )

        # Even with a consumer attached, no demand grant should fire.
        with pytest.raises(TimeoutError):
            prod_ex.conn.recv_push("stream_demand", timeout=0.5)

        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.complete(prod_ex.execution_id)
        cons_ex.conn.complete(cons_ex.execution_id)
        ctx.result(prod_resp["runId"])


# --- Idle timeout -------------------------------------------------------


def test_timeout_fires_when_producer_idle(worker):
    """A stream registered with ``timeout_ms`` is force-closed by the
    worker after that many milliseconds without an append. The
    adapter receives a ``stream_force_close`` push and any consumer
    sees a ``stream_closed`` push with reason=``"timeout"``.
    """
    targets = [workflow("test", "producer"), workflow("test", "consumer")]

    with worker(targets, concurrency=2) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(
            prod_ex.execution_id, 0, buffer=None, timeout_ms=150
        )

        # Consumer subscribes so it'll see the timeout close.
        cons_resp = ctx.submit("test", "consumer")
        cons_ex = ctx.executor.next_execute()
        cons_ex.conn.stream_subscribe(
            cons_ex.execution_id,
            subscription_id=1,
            producer_execution_id=prod_ex.execution_id,
            index=0,
        )

        # Producer is idle. Within a second (well past 150ms), the CLI
        # should push force-close to the producer and the consumer
        # should see the stream closed with reason="timeout".
        force = prod_ex.conn.recv_push("stream_force_close", timeout=2)
        assert force["index"] == 0
        assert force["reason"] == "timeout"

        closed = cons_ex.conn.recv_push("stream_closed", subscription_id=1, timeout=2)
        assert closed["reason"] == "timeout"
        assert closed.get("error") is None

        # Producer should skip its own stream_close now (server already
        # recorded it); we simulate the real adapter by just completing.
        prod_ex.conn.complete(prod_ex.execution_id)
        cons_ex.conn.complete(cons_ex.execution_id)
        ctx.result(prod_resp["runId"])


def test_timeout_resets_on_append(worker):
    """Each append resets the idle deadline — a producer that emits
    items at a steady pace faster than the timeout does not fire.
    """
    targets = [workflow("test", "producer")]

    with worker(targets) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(
            prod_ex.execution_id, 0, buffer=None, timeout_ms=250
        )

        # Emit 3 items with 100ms gaps — each append resets the
        # deadline, so the total 300ms elapsed doesn't trigger a fire.
        import time as _t

        for seq in range(3):
            _t.sleep(0.1)
            prod_ex.conn.stream_append(prod_ex.execution_id, 0, seq, f"v{seq}")

        # No force-close should have been pushed.
        with pytest.raises(TimeoutError):
            prod_ex.conn.recv_push("stream_force_close", timeout=0.1)

        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.complete(prod_ex.execution_id)
        ctx.result(prod_resp["runId"])


def test_manifest_streams_propagates_to_execute(worker):
    """Workflow registered with ``streams`` in the manifest: when
    submitted (mimicking Studio/CLI), the execute message delivered to
    the worker carries the same ``streams`` config. This is the full
    propagation path — adapter manifest → server → execute dispatch.
    """
    targets = [
        workflow("test", "producer", streams={"buffer": 5, "timeout_ms": 250}),
    ]

    with worker(targets) as ctx:
        ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()

        assert prod_ex.streams is not None
        assert prod_ex.streams.get("buffer") == 5
        assert prod_ex.streams.get("timeout_ms") == 250

        prod_ex.conn.complete(prod_ex.execution_id)


def test_timeout_visible_in_topic(worker):
    """Studio's run topic surfaces ``timeoutMs`` on the stream state,
    and a timeout closure shows ``reason: "timeout"``.
    """
    targets = [workflow("test", "producer")]

    with worker(targets) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(
            prod_ex.execution_id, 0, buffer=None, timeout_ms=120
        )

        # Wait for the timeout to fire.
        force = prod_ex.conn.recv_push("stream_force_close", timeout=2)
        assert force["reason"] == "timeout"

        prod_ex.conn.complete(prod_ex.execution_id)
        ctx.result(prod_resp["runId"])

        snapshot = ctx.inspect(prod_resp["runId"])
        step = next(iter(snapshot["steps"].values()))
        execution = next(iter(step["executions"].values()))
        stream = execution["streams"]["0"]
        assert stream["timeoutMs"] == 120
        assert stream["reason"] == "timeout"
        assert stream["error"] is None


# --- Stream outcomes promoted to execution completion --------------------


def test_clean_stream_keeps_completion_succeeded(worker):
    """Sanity: a value-result + cleanly-closed streams still completes as
    `:succeeded`. (Guards against the new dispatch in
    `finalize_success_completion` regressing the happy path.)
    """
    targets = [workflow("test", "producer")]

    with worker(targets) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)
        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 0, "ok")
        prod_ex.conn.stream_close(prod_ex.execution_id, 0)
        prod_ex.conn.complete(prod_ex.execution_id, value=1)
        ctx.result(prod_resp["runId"])

        snapshot = ctx.inspect(prod_resp["runId"])
        execution = next(iter(snapshot["steps"][f"{prod_resp['runId']}:1"]["executions"].values()))
        assert execution["completion"]["kind"] == "succeeded"


def test_stream_error_promotes_completion_to_stream_errored(worker):
    """When a stream owned by an execution closes with an error but the
    function body returned a value, the execution's completion is
    promoted to `:stream_errored` (so the result is not cacheable and
    the step's retry policy applies).
    """
    targets = [workflow("test", "producer")]

    with worker(targets) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(prod_ex.execution_id, 0)
        prod_ex.conn.stream_append(prod_ex.execution_id, 0, 0, "ok")
        prod_ex.conn.stream_close(
            prod_ex.execution_id,
            0,
            error={"type": "ValueError", "message": "boom", "traceback": ""},
        )
        prod_ex.conn.complete(prod_ex.execution_id, value=1)
        ctx.result(prod_resp["runId"])

        snapshot = ctx.inspect(prod_resp["runId"])
        execution = next(iter(snapshot["steps"][f"{prod_resp['runId']}:1"]["executions"].values()))
        assert execution["completion"]["kind"] == "stream_errored"


def test_stream_timeout_promotes_completion_to_stream_timeout(worker):
    """When a stream owned by an execution closes via idle timeout but the
    function body returned a value, the completion is promoted to
    `:stream_timeout` (logically a success, but ineligible for cache).
    """
    targets = [workflow("test", "producer")]

    with worker(targets) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        prod_ex.conn.stream_register(
            prod_ex.execution_id, 0, buffer=None, timeout_ms=100
        )

        # Wait for the timeout to fire on the producer side.
        force = prod_ex.conn.recv_push("stream_force_close", timeout=2)
        assert force["reason"] == "timeout"

        prod_ex.conn.complete(prod_ex.execution_id, value=1)
        ctx.result(prod_resp["runId"])

        snapshot = ctx.inspect(prod_resp["runId"])
        execution = next(iter(snapshot["steps"][f"{prod_resp['runId']}:1"]["executions"].values()))
        assert execution["completion"]["kind"] == "stream_timeout"


def test_stream_error_outranks_timeout(worker):
    """If one stream errored and another timed out on the same execution,
    `:stream_errored` takes precedence — error is the stronger signal.
    """
    targets = [workflow("test", "producer")]

    with worker(targets) as ctx:
        prod_resp = ctx.submit("test", "producer")
        prod_ex = ctx.executor.next_execute()
        # Stream 0: will time out (no appends).
        prod_ex.conn.stream_register(
            prod_ex.execution_id, 0, buffer=None, timeout_ms=100
        )
        # Stream 1: will be closed with an error.
        prod_ex.conn.stream_register(prod_ex.execution_id, 1)

        # Drain the timeout signal so we're past it.
        force = prod_ex.conn.recv_push("stream_force_close", timeout=2)
        assert force["reason"] == "timeout"

        prod_ex.conn.stream_close(
            prod_ex.execution_id,
            1,
            error={"type": "RuntimeError", "message": "bad", "traceback": ""},
        )
        prod_ex.conn.complete(prod_ex.execution_id, value=1)
        ctx.result(prod_resp["runId"])

        snapshot = ctx.inspect(prod_resp["runId"])
        execution = next(iter(snapshot["steps"][f"{prod_resp['runId']}:1"]["executions"].values()))
        assert execution["completion"]["kind"] == "stream_errored"


def _wait_for_completion(ctx, run_id, step_num, timeout=5):
    """Poll the run topic until the given step's first execution has a
    completion row recorded, then return the completion kind. Used by
    cache-lookup tests that need to be sure the prior execution has
    transitioned past `:draining` (otherwise the in-flight cache lookup
    matches the value-result before the new completion-kind dispatch
    runs)."""
    import time as _t

    deadline = _t.time() + timeout
    while _t.time() < deadline:
        snapshot = ctx.inspect(run_id)
        executions = snapshot["steps"][f"{run_id}:{step_num}"]["executions"]
        execution = next(iter(executions.values()))
        completion = execution.get("completion")
        if completion is not None:
            return completion["kind"]
        _t.sleep(0.05)
    raise AssertionError(f"step {run_id}:{step_num} not completed within {timeout}s")


def test_stream_errored_execution_not_used_as_cache_hit(worker):
    """A `:stream_errored` execution is not eligible for cache lookup — a
    second submission with the same args re-executes the task.
    """
    targets = [
        workflow("test", "main"),
        task("test", "produce", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]
        wf = ctx.executor.next_execute()

        ref1 = wf.conn.submit_task(
            wf.execution_id,
            "test", "produce",
            json_args(1),
            cache={"params": True},
        )

        # First execution: returns a value, but its stream closes with an error.
        prod = ctx.executor.next_execute()
        prod.conn.stream_register(prod.execution_id, 0)
        prod.conn.stream_close(
            prod.execution_id,
            0,
            error={"type": "ValueError", "message": "oops", "traceback": ""},
        )
        prod.conn.complete(prod.execution_id, value="v")
        assert wf.conn.resolve(wf.execution_id, ref1)["value"] == "v"

        # Wait for the completion to be promoted before the second submit —
        # otherwise the in-flight (`:draining`) cache lookup matches the
        # value-result first and our promotion logic never gets to run.
        assert _wait_for_completion(ctx, run_id, 2) == "stream_errored"

        # Second submission with same args: should NOT cache-hit — the
        # previous execution's completion is :stream_errored.
        wf.conn.submit_task(
            wf.execution_id,
            "test", "produce",
            json_args(1),
            cache={"params": True},
        )
        # A cache hit would skip the execute; we expect a fresh dispatch.
        prod2 = ctx.executor.next_execute(timeout=3)
        prod2.conn.complete(prod2.execution_id, value="v2")

        wf.conn.complete(wf.execution_id)


def test_stream_timeout_execution_not_used_as_cache_hit(worker):
    """A `:stream_timeout` execution (stream timed out) is not eligible
    for cache lookup — a second submission with the same args re-executes.
    """
    targets = [
        workflow("test", "main"),
        task("test", "produce", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]
        wf = ctx.executor.next_execute()

        ref1 = wf.conn.submit_task(
            wf.execution_id,
            "test", "produce",
            json_args(1),
            cache={"params": True},
        )

        prod = ctx.executor.next_execute()
        prod.conn.stream_register(
            prod.execution_id, 0, buffer=None, timeout_ms=100
        )
        force = prod.conn.recv_push("stream_force_close", timeout=2)
        assert force["reason"] == "timeout"
        prod.conn.complete(prod.execution_id, value="v")
        assert wf.conn.resolve(wf.execution_id, ref1)["value"] == "v"

        assert _wait_for_completion(ctx, run_id, 2) == "stream_timeout"

        # Second submission with same args: previous completion is
        # :stream_timeout, not cacheable — expect a fresh execution.
        wf.conn.submit_task(
            wf.execution_id,
            "test", "produce",
            json_args(1),
            cache={"params": True},
        )
        prod2 = ctx.executor.next_execute(timeout=3)
        prod2.conn.complete(prod2.execution_id, value="v2")

        wf.conn.complete(wf.execution_id)


def test_stream_errored_execution_not_used_as_memo_hit(worker):
    """A `:stream_errored` execution is not eligible for memo lookup
    either — a second memoised call within the same run re-executes
    once the first attempt's completion records the stream error.
    """
    targets = [
        workflow("test", "main"),
        task("test", "produce", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]
        wf = ctx.executor.next_execute()

        ref1 = wf.conn.submit_task(
            wf.execution_id,
            "test", "produce",
            json_args(1),
            memo=True,
        )

        prod = ctx.executor.next_execute()
        prod.conn.stream_register(prod.execution_id, 0)
        prod.conn.stream_close(
            prod.execution_id,
            0,
            error={"type": "ValueError", "message": "oops", "traceback": ""},
        )
        prod.conn.complete(prod.execution_id, value="v")
        assert wf.conn.resolve(wf.execution_id, ref1)["value"] == "v"

        # Wait for the completion so the memo lookup sees :stream_errored,
        # not the in-flight :draining state (which would still match).
        assert _wait_for_completion(ctx, run_id, 2) == "stream_errored"

        # Same memo key, but the prior attempt is :stream_errored — expect
        # a fresh execution rather than a memo hit on the broken stream.
        wf.conn.submit_task(
            wf.execution_id,
            "test", "produce",
            json_args(1),
            memo=True,
        )
        prod2 = ctx.executor.next_execute(timeout=3)
        prod2.conn.complete(prod2.execution_id, value="v2")

        wf.conn.complete(wf.execution_id)


def test_stream_timeout_execution_not_used_as_memo_hit(worker):
    """A `:stream_timeout` execution is not eligible for memo lookup —
    a second memoised call within the same run re-executes.
    """
    targets = [
        workflow("test", "main"),
        task("test", "produce", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]
        wf = ctx.executor.next_execute()

        ref1 = wf.conn.submit_task(
            wf.execution_id,
            "test", "produce",
            json_args(1),
            memo=True,
        )

        prod = ctx.executor.next_execute()
        prod.conn.stream_register(
            prod.execution_id, 0, buffer=None, timeout_ms=100
        )
        force = prod.conn.recv_push("stream_force_close", timeout=2)
        assert force["reason"] == "timeout"
        prod.conn.complete(prod.execution_id, value="v")
        assert wf.conn.resolve(wf.execution_id, ref1)["value"] == "v"

        assert _wait_for_completion(ctx, run_id, 2) == "stream_timeout"

        wf.conn.submit_task(
            wf.execution_id,
            "test", "produce",
            json_args(1),
            memo=True,
        )
        prod2 = ctx.executor.next_execute(timeout=3)
        prod2.conn.complete(prod2.execution_id, value="v2")

        wf.conn.complete(wf.execution_id)
