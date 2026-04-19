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

from support.manifest import workflow
from support.protocol import (
    execution_result,
    json_args,
    partition_filter,
    slice_filter,
    chain_filter,
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
            filter=slice_filter(1, 3),
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
            filter=partition_filter(n=3, i=1),
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

        err = closed.get("error")
        assert err is not None
        assert err["type"] == "Coflux.StreamNotFound"


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
        assert streams["1"]["error"] == {"type": "RuntimeError", "message": "bad"}


def test_cancellation_closes_streams_with_cancelled_error(worker):
    """Cancel an execution mid-stream: the subscriber receives a closure
    carrying the ExecutionCancelled error synthesised by close_open_streams.
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
        err = closed.get("error")
        assert err is not None
        assert err["type"] == "Coflux.ExecutionCancelled"


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
            filter=slice_filter(0, 2),
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


def test_filter_chain_combines_slice_and_partition(worker):
    """``chain(slice(0, 6), partition(2, 0))`` → positions 0, 2, 4."""
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
            filter=chain_filter(slice_filter(0, 6), partition_filter(n=2, i=0)),
        )
        items, _ = cons_ex.conn.drain_stream(subscription_id=1)
        cons_ex.conn.complete(cons_ex.execution_id)

        assert [item[0] for item in items] == [0, 2, 4]
