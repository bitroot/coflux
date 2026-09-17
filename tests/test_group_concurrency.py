"""Tests for group concurrency limits: ``cf.group(concurrency=n)`` capping how
many of the children submitted inside one group run at once.

Same conventions as ``test_concurrency_limits.py``: a gate is asserted by *not*
receiving an execution, and the worker's own concurrency is always well above
the limit under test, so it's the gate doing the holding.

The limit is registered with the group (``register_group``) and then carried
by each child submitted with that ``group_id``.
"""

import time

import pytest
from support import cli
from support.helpers import managed_worker, poll_result
from support.manifest import task, workflow
from support.protocol import json_args, register_group_notification


def assert_nothing_dispatched(ctx, timeout=1):
    with pytest.raises(TimeoutError):
        ctx.executor.next_execute(timeout=timeout)


def register_group(ex, group_id=0, concurrency=None, name=None):
    ex.conn.send(
        register_group_notification(
            ex.execution_id, group_id=group_id, name=name, concurrency=concurrency
        )
    )


def test_group_limit_admits_n(worker):
    """A limit of two admits two children at once, and the third only once
    one of them has finished."""
    targets = [
        workflow("test", "main"),
        task("test", "item", parameters=["x"]),
    ]

    with worker(targets, concurrency=6) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        register_group(ex0, concurrency=2)
        for x in (1, 2, 3):
            ex0.conn.submit_task(
                ex0.execution_id, "test", "item", json_args(x), group_id=0
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


def test_limit_is_per_parent_execution(worker):
    """Two parents each with a limit-1 group don't share an allowance."""
    targets = [
        workflow("test", "main"),
        task("test", "item", parameters=["x"]),
    ]

    with worker(targets, concurrency=6) as ctx:
        resp_a = ctx.submit("test", "main")
        resp_b = ctx.submit("test", "main")
        parent_a = ctx.executor.next_execute()
        parent_b = ctx.executor.next_execute()

        for parent in (parent_a, parent_b):
            register_group(parent, concurrency=1)
            parent.conn.submit_task(
                parent.execution_id, "test", "item", json_args(1), group_id=0
            )

        # One child per group, so both run at once.
        children = [ctx.executor.next_execute(timeout=5) for _ in range(2)]
        for child in children:
            child.conn.complete(child.execution_id, value="ok")

        parent_a.conn.complete(parent_a.execution_id, value="a")
        parent_b.conn.complete(parent_b.execution_id, value="b")
        assert ctx.result(resp_a["runId"])["value"]["data"] == "a"
        assert ctx.result(resp_b["runId"])["value"]["data"] == "b"


def test_ungrouped_siblings_not_counted(worker):
    """A sibling submitted outside the group neither counts against it nor
    is held by it."""
    targets = [
        workflow("test", "main"),
        task("test", "item", parameters=["x"]),
    ]

    with worker(targets, concurrency=6) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        register_group(ex0, concurrency=1)
        ex0.conn.submit_task(ex0.execution_id, "test", "item", json_args(1), group_id=0)
        ex0.conn.submit_task(ex0.execution_id, "test", "item", json_args(2))

        # The grouped child takes the group's one place; the ungrouped one
        # runs alongside it.
        first = ctx.executor.next_execute(timeout=5)
        second = ctx.executor.next_execute(timeout=5)
        assert {first.arguments[0]["value"], second.arguments[0]["value"]} == {1, 2}

        # ...and a second grouped child is what gets held.
        ex0.conn.submit_task(ex0.execution_id, "test", "item", json_args(3), group_id=0)
        assert_nothing_dispatched(ctx)

        for ex in (first, second):
            ex.conn.complete(ex.execution_id, value="ok")

        third = ctx.executor.next_execute(timeout=5)
        assert third.arguments[0]["value"] == 3
        third.conn.complete(third.execution_id, value="ok")

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_grandchildren_not_counted(worker):
    """What a child submits belongs to the child, not to its parent's group."""
    targets = [
        workflow("test", "main"),
        task("test", "item", parameters=["x"]),
        task("test", "leaf"),
    ]

    with worker(targets, concurrency=6) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        register_group(ex0, concurrency=1)
        ex0.conn.submit_task(ex0.execution_id, "test", "item", json_args(1), group_id=0)
        ex0.conn.submit_task(ex0.execution_id, "test", "item", json_args(2), group_id=0)

        child = ctx.executor.next_execute(timeout=5)
        assert_nothing_dispatched(ctx)

        # The running child fans out; the group's one place is still its own.
        ref = child.conn.submit_task(child.execution_id, "test", "leaf", [])
        grandchild = ctx.executor.next_execute(timeout=5)
        assert grandchild.target == "leaf"
        grandchild.conn.complete(grandchild.execution_id, value="leaf")
        assert child.conn.resolve(child.execution_id, ref)["value"] == "leaf"

        child.conn.complete(child.execution_id, value="first")

        sibling = ctx.executor.next_execute(timeout=5)
        assert sibling.arguments[0]["value"] == 2
        sibling.conn.complete(sibling.execution_id, value="second")

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_composes_with_task_limit(worker):
    """A child under both a group limit and a task limit needs room in
    both, and waiting on one reserves nothing in the other."""
    targets = [
        workflow("test", "main"),
        task("test", "limited", parameters=["x"]),
        task("test", "other"),
    ]

    with worker(targets, concurrency=6) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        register_group(ex0, concurrency=2)
        config = {"limit": 1}
        for x in (1, 2):
            ex0.conn.submit_task(
                ex0.execution_id,
                "test",
                "limited",
                json_args(x),
                group_id=0,
                concurrency_config=config,
            )
        ex0.conn.submit_task(ex0.execution_id, "test", "other", [], group_id=0)

        # The first `limited` takes the task's one permit. The second waits
        # on it although the group has room — and that room goes to `other`.
        first = ctx.executor.next_execute(timeout=5)
        second = ctx.executor.next_execute(timeout=5)
        assert {first.target, second.target} == {"limited", "other"}
        assert_nothing_dispatched(ctx)

        other = first if first.target == "other" else second
        limited = second if other is first else first

        # Freeing the group changes nothing for the task-gated child...
        other.conn.complete(other.execution_id, value="other")
        assert_nothing_dispatched(ctx)

        # ...freeing the task permit does.
        limited.conn.complete(limited.execution_id, value="first")
        third = ctx.executor.next_execute(timeout=5)
        assert third.target == "limited"
        assert third.arguments[0]["value"] == 2
        third.conn.complete(third.execution_id, value="second")

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_released_on_error_retry_reacquires(worker):
    """A failed attempt releases its place; its retry has to take it again."""
    targets = [
        workflow("test", "main"),
        task("test", "item", parameters=["x"]),
    ]

    with worker(targets, concurrency=6) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        register_group(ex0, concurrency=1)
        ex0.conn.submit_task(
            ex0.execution_id,
            "test",
            "item",
            json_args(1),
            group_id=0,
            retries={"limit": 1, "backoff_min_ms": 0, "backoff_max_ms": 0},
        )
        ex0.conn.submit_task(ex0.execution_id, "test", "item", json_args(2), group_id=0)

        first = ctx.executor.next_execute(timeout=5)
        assert first.arguments[0]["value"] == 1
        assert_nothing_dispatched(ctx)

        first.conn.fail(first.execution_id, "RuntimeError", "boom")

        # The sibling was queued first, so it gets the freed place; the
        # retry is next in line behind it.
        sibling = ctx.executor.next_execute(timeout=5)
        assert sibling.arguments[0]["value"] == 2
        assert_nothing_dispatched(ctx)

        sibling.conn.complete(sibling.execution_id, value="second")

        retry = ctx.executor.next_execute(timeout=5)
        assert retry.arguments[0]["value"] == 1
        retry.conn.complete(retry.execution_id, value="retried")

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_released_on_cancel(worker):
    """Cancelling the child holding the place lets the next one through."""
    targets = [
        workflow("test", "main"),
        task("test", "item", parameters=["x"]),
    ]

    with worker(targets, concurrency=6) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        register_group(ex0, concurrency=1)
        ex0.conn.submit_task(ex0.execution_id, "test", "item", json_args(1), group_id=0)
        ex0.conn.submit_task(ex0.execution_id, "test", "item", json_args(2), group_id=0)

        first = ctx.executor.next_execute(timeout=5)
        assert_nothing_dispatched(ctx)

        ctx.cancel(first.execution_id)

        second = ctx.executor.next_execute(timeout=5)
        assert second.execution_id != first.execution_id
        second.conn.complete(second.execution_id, value="after cancel")

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_persists_after_parent_completes(worker):
    """The limit is on the children, not the parent: it's still enforced
    once the parent has returned."""
    targets = [
        workflow("test", "main"),
        task("test", "item", parameters=["x"]),
    ]

    with worker(targets, concurrency=6) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        register_group(ex0, concurrency=1)
        ex0.conn.submit_task(ex0.execution_id, "test", "item", json_args(1), group_id=0)
        ex0.conn.submit_task(ex0.execution_id, "test", "item", json_args(2), group_id=0)

        first = ctx.executor.next_execute(timeout=5)
        assert_nothing_dispatched(ctx)

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"

        # Parent gone; sibling still held.
        assert_nothing_dispatched(ctx)

        first.conn.complete(first.execution_id, value="first")

        second = ctx.executor.next_execute(timeout=5)
        assert second.arguments[0]["value"] == 2
        second.conn.complete(second.execution_id, value="second")


def test_child_workflow_gated_at_spawned_run(worker):
    """A workflow submitted in the group is a child like any other: the run
    it spawns is held at its first step."""
    targets = [
        workflow("test", "main"),
        workflow("test", "child", parameters=["x"]),
    ]

    with worker(targets, concurrency=6) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        register_group(ex0, concurrency=1)
        refs = [
            ex0.conn.submit_workflow(
                ex0.execution_id, "test", "child", json_args(x), group_id=0
            )
            for x in (1, 2)
        ]

        first = ctx.executor.next_execute(timeout=5)
        assert first.target == "child"
        assert first.arguments[0]["value"] == 1
        assert_nothing_dispatched(ctx)

        first.conn.complete(first.execution_id, value="first")

        second = ctx.executor.next_execute(timeout=5)
        assert second.arguments[0]["value"] == 2
        second.conn.complete(second.execution_id, value="second")

        assert [ex0.conn.resolve(ex0.execution_id, ref)["value"] for ref in refs] == [
            "first",
            "second",
        ]

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_queue_topic_reports_group_gate(worker):
    """The queue names the group, its parent, and the children holding it."""
    targets = [
        workflow("test", "main"),
        task("test", "item", parameters=["x"]),
    ]

    with worker(targets, concurrency=6) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        register_group(ex0, concurrency=1)
        ex0.conn.submit_task(ex0.execution_id, "test", "item", json_args(1), group_id=0)
        ex0.conn.submit_task(ex0.execution_id, "test", "item", json_args(2), group_id=0)

        holder = ctx.executor.next_execute(timeout=5)
        assert_nothing_dispatched(ctx)

        queue = ctx.queue()
        gates = {
            execution_id: [d for d in entry["dependencies"] if d["type"] == "group"]
            for execution_id, entry in queue.items()
        }

        gated = {eid: gs[0] for eid, gs in gates.items() if gs}
        assert len(gated) == 1, gated
        (gate,) = gated.values()
        assert gate["limit"] == 1
        assert gate["parent"] == ex0.execution_id
        assert gate["group"] == 0
        assert gate["key"] == f"{ex0.execution_id}/0"
        assert gate["holders"] == [holder.execution_id]

        # The holder itself isn't gated.
        assert not gates.get(holder.execution_id)

        holder.conn.complete(holder.execution_id, value="first")

        waiter = ctx.executor.next_execute(timeout=5)
        assert not [
            d
            for d in ctx.queue()[waiter.execution_id]["dependencies"]
            if d["type"] == "group"
        ]

        waiter.conn.complete(waiter.execution_id, value="second")
        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_ledger_rebuilt_on_server_restart(isolated_server, tmp_path):
    """A group place held across a restart is still held afterwards. Held
    permits are derived from assignments without completions, and the
    group key is stored on the step, so nothing is lost with the process."""
    server, host, _project_id = isolated_server
    targets = [
        workflow("test", "main"),
        task("test", "item", parameters=["x"]),
    ]

    with managed_worker(targets, host, tmp_path, concurrency=6) as executor:
        resp = cli.submit("test/main", host=host)
        ex0 = executor.next_execute(timeout=10)

        register_group(ex0, concurrency=1)
        ex0.conn.submit_task(ex0.execution_id, "test", "item", json_args(1), group_id=0)
        ex0.conn.submit_task(ex0.execution_id, "test", "item", json_args(2), group_id=0)

        holder = executor.next_execute(timeout=10)
        with pytest.raises(TimeoutError):
            executor.next_execute(timeout=1)

        server.stop()
        time.sleep(0.5)
        server.start()

        # The worker reconnects and reports the holder as still executing;
        # the waiter must stay gated.
        with pytest.raises(TimeoutError):
            executor.next_execute(timeout=3)

        holder.conn.complete(holder.execution_id, value="first")

        waiter = executor.next_execute(timeout=10)
        assert waiter.arguments[0]["value"] == 2
        waiter.conn.complete(waiter.execution_id, value="second")

        ex0.conn.complete(ex0.execution_id, value="done")
        assert poll_result(resp["runId"], host, timeout=20)["value"]["data"] == "done"


def test_memo_hit_not_counted(worker):
    """A memoised child that some other caller started is linked into the
    group for display, but was never spawned by it, so it takes no place."""
    targets = [
        workflow("test", "main"),
        task("test", "item", parameters=["x"]),
    ]

    with worker(targets, concurrency=6) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        # Started outside the group, and kept running.
        original = ex0.conn.submit_task(
            ex0.execution_id, "test", "item", json_args(1), memo=True
        )
        running = ctx.executor.next_execute(timeout=5)

        register_group(ex0, concurrency=1)

        # Re-submitted inside the group: a memo hit onto the running one.
        hit = ex0.conn.submit_task(
            ex0.execution_id, "test", "item", json_args(1), group_id=0, memo=True
        )
        assert hit == original

        # The group's one place is still free for a child it actually spawns.
        ex0.conn.submit_task(ex0.execution_id, "test", "item", json_args(2), group_id=0)
        spawned = ctx.executor.next_execute(timeout=5)
        assert spawned.arguments[0]["value"] == 2

        spawned.conn.complete(spawned.execution_id, value="second")
        running.conn.complete(running.execution_id, value="first")
        assert ex0.conn.resolve(ex0.execution_id, hit)["value"] == "first"

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"
