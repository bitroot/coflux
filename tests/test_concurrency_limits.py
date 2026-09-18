"""Tests for concurrency limits: the scheduler holding an execution back until
a permit is free.

A gate is asserted by *not* receiving an execution: ``next_execute`` raising
``TimeoutError`` means the server chose not to assign it. The worker's own
concurrency is always set well above the limit under test, so it's the gate
doing the holding and not a shortage of slots.
"""

import time

import pytest
from support import cli
from support.helpers import managed_worker, poll_result
from support.manifest import task, workflow
from support.protocol import json_args


def assert_nothing_dispatched(ctx, timeout=1):
    with pytest.raises(TimeoutError):
        ctx.executor.next_execute(timeout=timeout)


def test_limit_one_serialises(worker):
    """Two executions of a limit-1 task run one after the other."""
    targets = [
        workflow("test", "main"),
        task("test", "compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=4) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        ref1 = ex0.conn.submit_task(
            ex0.execution_id,
            "test",
            "compute",
            json_args(1),
            concurrency_config={"limit": 1},
        )
        ref2 = ex0.conn.submit_task(
            ex0.execution_id,
            "test",
            "compute",
            json_args(2),
            concurrency_config={"limit": 1},
        )

        # One is admitted; the other waits, despite three free worker slots.
        ex1 = ctx.executor.next_execute(timeout=5)
        assert_nothing_dispatched(ctx)

        ex1.conn.complete(ex1.execution_id, value="first")

        ex2 = ctx.executor.next_execute(timeout=5)
        assert ex2.execution_id != ex1.execution_id
        ex2.conn.complete(ex2.execution_id, value="second")

        assert {
            ex0.conn.resolve(ex0.execution_id, ref1)["value"],
            ex0.conn.resolve(ex0.execution_id, ref2)["value"],
        } == {"first", "second"}

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_limit_two_admits_two(worker):
    """A limit above one admits that many at once, and no more."""
    targets = [
        workflow("test", "main"),
        task("test", "compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=6) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        for x in (1, 2, 3):
            ex0.conn.submit_task(
                ex0.execution_id,
                "test",
                "compute",
                json_args(x),
                concurrency_config={"limit": 2},
            )

        first = ctx.executor.next_execute(timeout=5)
        second = ctx.executor.next_execute(timeout=5)
        assert_nothing_dispatched(ctx)

        first.conn.complete(first.execution_id, value="a")

        third = ctx.executor.next_execute(timeout=5)
        assert third.execution_id not in (first.execution_id, second.execution_id)

        second.conn.complete(second.execution_id, value="b")
        third.conn.complete(third.execution_id, value="c")

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_params_key_isolates_values(worker):
    """Keying on an argument gives each value its own allowance."""
    targets = [
        workflow("test", "main"),
        task("test", "compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=6) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        config = {"limit": 1, "params": [0]}
        ex0.conn.submit_task(
            ex0.execution_id, "test", "compute", json_args(1), concurrency_config=config
        )
        ex0.conn.submit_task(
            ex0.execution_id, "test", "compute", json_args(2), concurrency_config=config
        )

        # Different arguments, different keys — both run.
        first = ctx.executor.next_execute(timeout=5)
        second = ctx.executor.next_execute(timeout=5)
        assert {first.arguments[0]["value"], second.arguments[0]["value"]} == {1, 2}

        # A repeat of an argument already running is held back.
        ex0.conn.submit_task(
            ex0.execution_id, "test", "compute", json_args(1), concurrency_config=config
        )
        assert_nothing_dispatched(ctx)

        for ex in (first, second):
            ex.conn.complete(ex.execution_id, value="done")

        third = ctx.executor.next_execute(timeout=5)
        assert third.arguments[0]["value"] == 1
        third.conn.complete(third.execution_id, value="done")

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_params_true_uses_all_arguments(worker):
    """params=True keys on the whole argument tuple."""
    targets = [
        workflow("test", "main"),
        task("test", "compute", parameters=["x", "y"]),
    ]

    with worker(targets, concurrency=6) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        config = {"limit": 1, "params": True}
        ex0.conn.submit_task(
            ex0.execution_id,
            "test",
            "compute",
            json_args(1, "a"),
            concurrency_config=config,
        )
        ex0.conn.submit_task(
            ex0.execution_id,
            "test",
            "compute",
            json_args(1, "b"),
            concurrency_config=config,
        )

        # Differing in the second argument is enough to be a different key.
        first = ctx.executor.next_execute(timeout=5)
        second = ctx.executor.next_execute(timeout=5)

        ex0.conn.submit_task(
            ex0.execution_id,
            "test",
            "compute",
            json_args(1, "a"),
            concurrency_config=config,
        )
        assert_nothing_dispatched(ctx)

        for ex in (first, second):
            ex.conn.complete(ex.execution_id, value="done")

        third = ctx.executor.next_execute(timeout=5)
        third.conn.complete(third.execution_id, value="done")

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_namespace_shares_pool_across_targets(worker):
    """Two targets naming the same namespace draw on one allowance."""
    targets = [
        workflow("test", "main"),
        task("test", "alpha"),
        task("test", "beta"),
    ]

    with worker(targets, concurrency=6) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        config = {"limit": 1, "namespace": "shared"}
        ex0.conn.submit_task(
            ex0.execution_id, "test", "alpha", [], concurrency_config=config
        )
        ex0.conn.submit_task(
            ex0.execution_id, "test", "beta", [], concurrency_config=config
        )

        first = ctx.executor.next_execute(timeout=5)
        assert_nothing_dispatched(ctx)

        first.conn.complete(first.execution_id, value="done")

        second = ctx.executor.next_execute(timeout=5)
        assert {first.target, second.target} == {"alpha", "beta"}
        second.conn.complete(second.execution_id, value="done")

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_released_on_error(worker):
    """A failed attempt releases its permit."""
    targets = [
        workflow("test", "main"),
        task("test", "compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=4) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        config = {"limit": 1}
        refs = [
            ex0.conn.submit_task(
                ex0.execution_id,
                "test",
                "compute",
                json_args(x),
                concurrency_config=config,
            )
            for x in (1, 2)
        ]

        first = ctx.executor.next_execute(timeout=5)
        assert_nothing_dispatched(ctx)

        first.conn.fail(first.execution_id, "RuntimeError", "boom")

        second = ctx.executor.next_execute(timeout=5)
        second.conn.complete(second.execution_id, value="after failure")

        # Whichever was admitted first is the one that errored; the point is
        # that the other one ran at all.
        resolved = [ex0.conn.resolve(ex0.execution_id, ref) for ref in refs]
        assert sorted(
            r.get("status", "ok") if isinstance(r, dict) else "ok" for r in resolved
        ) == ["error", "ok"]

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_released_on_cancel(worker):
    """Cancelling the holder lets the next execution through."""
    targets = [
        workflow("test", "main"),
        task("test", "compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=4) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        config = {"limit": 1}
        ex0.conn.submit_task(
            ex0.execution_id, "test", "compute", json_args(1), concurrency_config=config
        )
        ex0.conn.submit_task(
            ex0.execution_id, "test", "compute", json_args(2), concurrency_config=config
        )

        first = ctx.executor.next_execute(timeout=5)
        assert_nothing_dispatched(ctx)

        ctx.cancel(first.execution_id)

        second = ctx.executor.next_execute(timeout=5)
        assert second.execution_id != first.execution_id
        second.conn.complete(second.execution_id, value="after cancel")

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_released_on_suspend_and_reacquired(worker):
    """Suspending releases the permit; the resumed successor re-acquires it."""
    targets = [
        workflow("test", "main"),
        task("test", "compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=4) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        config = {"limit": 1}
        ex0.conn.submit_task(
            ex0.execution_id, "test", "compute", json_args(1), concurrency_config=config
        )
        ex0.conn.submit_task(
            ex0.execution_id, "test", "compute", json_args(2), concurrency_config=config
        )

        holder = ctx.executor.next_execute(timeout=5)
        assert_nothing_dispatched(ctx)

        # Suspending isn't running, so it mustn't keep holding the limit.
        holder.conn.suspend(holder.execution_id)

        waiter = ctx.executor.next_execute(timeout=5)
        assert waiter.execution_id != holder.execution_id

        # ...but the suspended step's successor now has to wait its turn.
        assert_nothing_dispatched(ctx)

        waiter.conn.complete(waiter.execution_id, value="waiter")

        resumed = ctx.executor.next_execute(timeout=5)
        resumed.conn.complete(resumed.execution_id, value="resumed")

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_scope_is_per_workspace(worker):
    """A limit is counted per workspace: a holder in a base workspace doesn't
    gate a submission in a derived one, and a holder in the derived workspace
    doesn't gate the base."""
    targets = [workflow("test", "limited", concurrency={"limit": 1})]

    with worker(targets, concurrency=4, workspace="base") as ctx_base:
        cli.workspaces_create(
            "derived", base="base", host=ctx_base.host, workspace="derived"
        )

        with worker(targets, concurrency=4, workspace="derived") as ctx_derived:
            base_resp = ctx_base.submit("test", "limited")
            base_holder = ctx_base.executor.next_execute(timeout=5)

            # The derived workspace isn't held up by the base's holder.
            derived_resp = ctx_derived.submit("test", "limited")
            derived_holder = ctx_derived.executor.next_execute(timeout=5)

            base_holder.conn.complete(base_holder.execution_id, value="base")
            assert ctx_base.result(base_resp["runId"])["value"]["data"] == "base"

            # And the base isn't held up by the derived workspace's holder.
            base_resp2 = ctx_base.submit("test", "limited")
            base_holder2 = ctx_base.executor.next_execute(timeout=5)
            base_holder2.conn.complete(base_holder2.execution_id, value="base2")
            assert ctx_base.result(base_resp2["runId"])["value"]["data"] == "base2"

            derived_holder.conn.complete(derived_holder.execution_id, value="derived")
            assert (
                ctx_derived.result(derived_resp["runId"])["value"]["data"] == "derived"
            )


def test_workflow_manifest_round_trip(worker):
    """A limit declared in the manifest survives registration and is enforced."""
    targets = [
        workflow("test", "limited", parameters=["x"], concurrency={"limit": 1}),
    ]

    with worker(targets, concurrency=4) as ctx:
        manifests = ctx.inspect_manifests()
        assert manifests["test"]["limited"]["concurrency"] == {
            "limit": 1,
            "params": False,
            "namespace": None,
        }

        first_resp = ctx.submit("test", "limited", "1")
        second_resp = ctx.submit("test", "limited", "2")

        first = ctx.executor.next_execute(timeout=5)
        assert_nothing_dispatched(ctx)

        first.conn.complete(first.execution_id, value="first")

        second = ctx.executor.next_execute(timeout=5)
        second.conn.complete(second.execution_id, value="second")

        assert {
            ctx.result(first_resp["runId"])["value"]["data"],
            ctx.result(second_resp["runId"])["value"]["data"],
        } == {"first", "second"}


def test_queue_topic_reports_gate(worker):
    """The queue names the gate, and the executions holding it."""
    targets = [
        workflow("test", "main"),
        task("test", "compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=4) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        config = {"limit": 1}
        ex0.conn.submit_task(
            ex0.execution_id, "test", "compute", json_args(1), concurrency_config=config
        )
        ex0.conn.submit_task(
            ex0.execution_id, "test", "compute", json_args(2), concurrency_config=config
        )

        holder = ctx.executor.next_execute(timeout=5)
        assert_nothing_dispatched(ctx)

        queue = ctx.queue()
        gates = {
            execution_id: [
                d for d in entry["dependencies"] if d["type"] == "concurrency"
            ]
            for execution_id, entry in queue.items()
        }

        gated = {eid: gs[0] for eid, gs in gates.items() if gs}
        assert len(gated) == 1, gated
        (gate,) = gated.values()
        assert gate["limit"] == 1
        assert gate["holders"] == [holder.execution_id]
        assert gate["key"]

        # The holder itself isn't gated.
        assert not gates.get(holder.execution_id)

        holder.conn.complete(holder.execution_id, value="first")

        waiter = ctx.executor.next_execute(timeout=5)
        assert not [
            d
            for d in ctx.queue()[waiter.execution_id]["dependencies"]
            if d["type"] == "concurrency"
        ]

        waiter.conn.complete(waiter.execution_id, value="second")
        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_ledger_rebuilt_on_server_restart(isolated_server, tmp_path):
    """A permit held across a restart is still held afterwards.

    Held permits aren't persisted as such — they're derived from the
    executions that have an assignment and no completion — so a restart has to
    reconstruct them rather than start from empty.
    """
    server, host, _project_id = isolated_server
    targets = [workflow("test", "limited", parameters=["x"], concurrency={"limit": 1})]

    with managed_worker(targets, host, tmp_path, concurrency=4) as executor:
        holder_resp = cli.submit("test/limited", "1", host=host)
        holder = executor.next_execute(timeout=10)

        waiter_resp = cli.submit("test/limited", "2", host=host)
        with pytest.raises(TimeoutError):
            executor.next_execute(timeout=1)

        server.stop()
        time.sleep(0.5)
        server.start()

        # The worker reconnects and reports the holder as still executing; the
        # waiter must stay gated rather than being let through by a ledger
        # that forgot the holder.
        with pytest.raises(TimeoutError):
            executor.next_execute(timeout=3)

        holder.conn.complete(holder.execution_id, value="first")
        assert poll_result(holder_resp["runId"], host, timeout=20)["value"]["data"] == (
            "first"
        )

        waiter = executor.next_execute(timeout=10)
        waiter.conn.complete(waiter.execution_id, value="second")
        assert poll_result(waiter_resp["runId"], host, timeout=20)["value"]["data"] == (
            "second"
        )


def test_cache_hit_takes_no_permit(worker):
    """A cached step resolves server-side without contending for the limit."""
    targets = [
        workflow("test", "main"),
        task("test", "compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=4) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        cache = {"params": True, "max_age": None, "namespace": None, "version": None}
        config = {"limit": 1}

        # Populate the cache, and let the execution go.
        warm = ex0.conn.submit_task(
            ex0.execution_id,
            "test",
            "compute",
            json_args(1),
            cache=cache,
            concurrency_config=config,
        )
        ex1 = ctx.executor.next_execute(timeout=5)
        ex1.conn.complete(ex1.execution_id, value="cached")
        assert ex0.conn.resolve(ex0.execution_id, warm)["value"] == "cached"

        # Occupy the only permit with an unrelated argument...
        ex0.conn.submit_task(
            ex0.execution_id, "test", "compute", json_args(2), concurrency_config=config
        )
        holder = ctx.executor.next_execute(timeout=5)

        # ...and the cache hit still resolves, since it never reaches a worker.
        hit = ex0.conn.submit_task(
            ex0.execution_id,
            "test",
            "compute",
            json_args(1),
            cache=cache,
            concurrency_config=config,
        )
        assert ex0.conn.resolve(ex0.execution_id, hit)["value"] == "cached"

        holder.conn.complete(holder.execution_id, value="held")
        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_no_limit_by_default(worker):
    """Without a limit, executions of one target run side by side."""
    targets = [
        workflow("test", "main"),
        task("test", "compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=4) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        for x in (1, 2, 3):
            ex0.conn.submit_task(ex0.execution_id, "test", "compute", json_args(x))

        executions = [ctx.executor.next_execute(timeout=5) for _ in range(3)]
        for ex in executions:
            ex.conn.complete(ex.execution_id, value="done")

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"
