"""Tests for caching and memo."""

import support.cli as cli
from support.manifest import task, workflow
from support.protocol import json_args


def test_cache_hit(worker):
    """Same task submitted twice with same args - second uses cached result."""
    targets = [
        workflow("test", "main"),
        task("test", "expensive", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        # First submit - cache miss, task executes
        ref1 = ex0.conn.submit_task(
            ex0.execution_id,
            "test", "expensive",
            json_args(42),
            cache={"params": True},
        )

        ex1 = ctx.executor.next_execute()
        ex1.conn.complete(ex1.execution_id, value=84)

        assert ex0.conn.resolve(ex0.execution_id, ref1)["value"] == 84

        # Second submit - same target + args, should be cached
        ref2 = ex0.conn.submit_task(
            ex0.execution_id,
            "test", "expensive",
            json_args(42),
            cache={"params": True},
        )

        # Should resolve immediately from cache (no second execute)
        assert ex0.conn.resolve(ex0.execution_id, ref2)["value"] == 84

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_cache_miss_different_args(worker):
    """Same task with different arguments does not use cache."""
    targets = [
        workflow("test", "main"),
        task("test", "compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        # First submit with cache
        ref1 = ex0.conn.submit_task(
            ex0.execution_id,
            "test", "compute",
            json_args(1),
            cache={"params": True},
        )
        ex_t1 = ctx.executor.next_execute()
        ex_t1.conn.complete(ex_t1.execution_id, value=10)
        assert ex0.conn.resolve(ex0.execution_id, ref1)["value"] == 10

        # Second submit with different args - should execute (cache miss)
        ref2 = ex0.conn.submit_task(
            ex0.execution_id,
            "test", "compute",
            json_args(2),
            cache={"params": True},
        )
        ex_t2 = ctx.executor.next_execute()
        ex_t2.conn.complete(ex_t2.execution_id, value=20)
        assert ex0.conn.resolve(ex0.execution_id, ref2)["value"] == 20

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_memo_within_run(worker):
    """Same task submitted twice in one run with memo - second reuses first."""
    targets = [
        workflow("test", "main"),
        task("test", "compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        # First submit with memo
        ref1 = ex0.conn.submit_task(
            ex0.execution_id,
            "test", "compute",
            json_args(10),
            memo=True,
        )

        ex1 = ctx.executor.next_execute()
        ex1.conn.complete(ex1.execution_id, value=99)

        assert ex0.conn.resolve(ex0.execution_id, ref1)["value"] == 99

        # Second submit with same args and memo - should reuse
        ref2 = ex0.conn.submit_task(
            ex0.execution_id,
            "test", "compute",
            json_args(10),
            memo=True,
        )

        # Should resolve from memo hit (no second execute)
        assert ex0.conn.resolve(ex0.execution_id, ref2)["value"] == 99

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_cache_from_base_workspace(worker):
    """Task cached in base workspace produces cache hit in derived workspace."""
    targets = [
        workflow("test", "main"),
        task("test", "expensive", parameters=["x"]),
    ]

    # Step 1: Run in "base" workspace, execute the cached task
    with worker(targets, workspace="base", concurrency=2) as ctx_base:
        resp = ctx_base.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx_base.executor.next_execute()
        ref = ex0.conn.submit_task(
            ex0.execution_id,
            "test", "expensive",
            json_args(42),
            cache={"params": True},
        )

        ex1 = ctx_base.executor.next_execute()
        ex1.conn.complete(ex1.execution_id, value=84)
        assert ex0.conn.resolve(ex0.execution_id, ref)["value"] == 84

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx_base.result(run_id)["value"]["data"] == "done"

        saved_host = ctx_base.host

    # Step 2: Create "derived" workspace inheriting from "base"
    cli.workspaces_create("derived", base="base", host=saved_host, workspace="derived")

    # Step 3: Run in "derived" workspace - child task should hit cache
    with worker(
        targets,
        workspace="derived",
        concurrency=2,
    ) as ctx_derived:
        resp = ctx_derived.submit("test", "main")

        ex0 = ctx_derived.executor.next_execute()
        ref = ex0.conn.submit_task(
            ex0.execution_id,
            "test", "expensive",
            json_args(42),
            cache={"params": True},
        )

        # Should resolve from base workspace cache (no new execution needed)
        assert ex0.conn.resolve(ex0.execution_id, ref)["value"] == 84

        ex0.conn.complete(ex0.execution_id, value="cached")
        assert ctx_derived.result(resp["runId"])["value"]["data"] == "cached"


def test_cache_not_shared_across_unrelated_workspaces(worker):
    """Task cached in one workspace does not produce cache hit in a sibling."""
    targets = [
        workflow("test", "main"),
        task("test", "expensive", parameters=["x"]),
    ]

    # Step 1: Run in "ws_a" workspace, execute the cached task
    with worker(targets, workspace="ws_a", concurrency=2) as ctx_a:
        resp = ctx_a.submit("test", "main")

        ex0 = ctx_a.executor.next_execute()
        ref = ex0.conn.submit_task(
            ex0.execution_id,
            "test", "expensive",
            json_args(42),
            cache={"params": True},
        )

        ex1 = ctx_a.executor.next_execute()
        ex1.conn.complete(ex1.execution_id, value=84)
        assert ex0.conn.resolve(ex0.execution_id, ref)["value"] == 84

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx_a.result(resp["runId"])["value"]["data"] == "done"

        saved_host = ctx_a.host

    # Step 2: Create "ws_b" (sibling, NOT derived from ws_a)
    cli.workspaces_create("ws_b", host=saved_host, workspace="ws_b")

    # Step 3: Run in "ws_b" - should NOT hit ws_a's cache (must execute again)
    with worker(targets, workspace="ws_b", concurrency=2) as ctx_b:
        resp = ctx_b.submit("test", "main")

        ex0 = ctx_b.executor.next_execute()
        ref = ex0.conn.submit_task(
            ex0.execution_id,
            "test", "expensive",
            json_args(42),
            cache={"params": True},
        )

        # Should require a new execution (cache miss in unrelated workspace)
        ex1 = ctx_b.executor.next_execute()
        ex1.conn.complete(ex1.execution_id, value=999)
        assert ex0.conn.resolve(ex0.execution_id, ref)["value"] == 999

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx_b.result(resp["runId"])["value"]["data"] == "done"


def test_run_level_memo_inherited_by_child_tasks(worker):
    """Workflow with memo=True causes child tasks to be memoised automatically."""
    targets = [
        workflow("test", "main", memo=True),
        task("test", "compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        # First submit - task has no memo of its own, inherits from run
        ref1 = ex0.conn.submit_task(
            ex0.execution_id,
            "test", "compute",
            json_args(10),
        )

        ex1 = ctx.executor.next_execute()
        ex1.conn.complete(ex1.execution_id, value=99)

        assert ex0.conn.resolve(ex0.execution_id, ref1)["value"] == 99

        # Second submit with same args - should reuse (memo inherited from run)
        ref2 = ex0.conn.submit_task(
            ex0.execution_id,
            "test", "compute",
            json_args(10),
        )

        # Should resolve from memo hit (no second execute)
        assert ex0.conn.resolve(ex0.execution_id, ref2)["value"] == 99

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"
