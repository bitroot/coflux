"""Tests for caching and memo."""

import support.cli as cli
from support.manifest import task, workflow
from support.protocol import json_args


def test_cache_hit(worker):
    """Same task submitted twice with same args - second uses cached result."""
    targets = [
        workflow("test.main"),
        task("test.expensive", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.main")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]
        conn1 = ctx.executor.connections[1]

        wf_eid, _, _ = conn0.recv_execute()

        # First submit - cache miss, task executes
        ref1 = conn0.submit_task(
            wf_eid,
            "test.expensive",
            json_args(42),
            cache={"params": True},
        )

        task_eid, _, _ = conn1.recv_execute()
        conn1.complete(task_eid, value=84)

        assert conn0.resolve(wf_eid, ref1)["value"] == 84

        # Second submit - same target + args, should be cached
        ref2 = conn0.submit_task(
            wf_eid,
            "test.expensive",
            json_args(42),
            cache={"params": True},
        )

        # Should resolve immediately from cache (no second execute on conn1)
        assert conn0.resolve(wf_eid, ref2)["value"] == 84

        conn0.complete(wf_eid, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_cache_miss_different_args(worker):
    """Same task with different arguments does not use cache."""
    targets = [
        workflow("test.main"),
        task("test.compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.main")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]
        conn1 = ctx.executor.connections[1]

        wf_eid, _, _ = conn0.recv_execute()

        # First submit with cache
        ref1 = conn0.submit_task(
            wf_eid,
            "test.compute",
            json_args(1),
            cache={"params": True},
        )
        task_eid1, _, _ = conn1.recv_execute()
        conn1.complete(task_eid1, value=10)
        assert conn0.resolve(wf_eid, ref1)["value"] == 10

        # Second submit with different args - should execute (cache miss)
        ref2 = conn0.submit_task(
            wf_eid,
            "test.compute",
            json_args(2),
            cache={"params": True},
        )
        task_eid2, _, _ = conn1.recv_execute()
        conn1.complete(task_eid2, value=20)
        assert conn0.resolve(wf_eid, ref2)["value"] == 20

        conn0.complete(wf_eid, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_memo_within_run(worker):
    """Same task submitted twice in one run with memo - second reuses first."""
    targets = [
        workflow("test.main"),
        task("test.compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.main")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]
        conn1 = ctx.executor.connections[1]

        wf_eid, _, _ = conn0.recv_execute()

        # First submit with memo
        ref1 = conn0.submit_task(
            wf_eid,
            "test.compute",
            json_args(10),
            memo=True,
        )

        task_eid, _, _ = conn1.recv_execute()
        conn1.complete(task_eid, value=99)

        assert conn0.resolve(wf_eid, ref1)["value"] == 99

        # Second submit with same args and memo - should reuse
        ref2 = conn0.submit_task(
            wf_eid,
            "test.compute",
            json_args(10),
            memo=True,
        )

        # Should resolve from memo hit (no second execute)
        assert conn0.resolve(wf_eid, ref2)["value"] == 99

        conn0.complete(wf_eid, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_cache_from_base_workspace(worker):
    """Task cached in base workspace produces cache hit in derived workspace."""
    targets = [
        workflow("test.main"),
        task("test.expensive", parameters=["x"]),
    ]

    # Step 1: Run in "base" workspace, execute the cached task
    with worker(targets, workspace="base", concurrency=2) as ctx_base:
        resp = ctx_base.submit("test.main")
        run_id = resp["runId"]
        conn0 = ctx_base.executor.connections[0]
        conn1 = ctx_base.executor.connections[1]

        wf_eid, _, _ = conn0.recv_execute()
        ref = conn0.submit_task(
            wf_eid,
            "test.expensive",
            json_args(42),
            cache={"params": True},
        )

        task_eid, _, _ = conn1.recv_execute()
        conn1.complete(task_eid, value=84)
        assert conn0.resolve(wf_eid, ref)["value"] == 84

        conn0.complete(wf_eid, value="done")
        assert ctx_base.result(run_id)["value"]["data"] == "done"

        # Save env for creating derived workspace
        saved_env = ctx_base.env.copy()

    # Step 2: Create "derived" workspace inheriting from "base"
    saved_env["COFLUX_WORKSPACE"] = "derived"
    cli.workspaces_create("derived", base="base", env=saved_env)

    # Step 3: Run in "derived" workspace - child task should hit cache
    with worker(
        targets,
        workspace="derived",
        concurrency=2,
        create_workspace=False,
    ) as ctx_derived:
        resp = ctx_derived.submit("test.main")
        conn0 = ctx_derived.executor.connections[0]

        wf_eid, _, _ = conn0.recv_execute()
        ref = conn0.submit_task(
            wf_eid,
            "test.expensive",
            json_args(42),
            cache={"params": True},
        )

        # Should resolve from base workspace cache (no new execution needed)
        assert conn0.resolve(wf_eid, ref)["value"] == 84

        conn0.complete(wf_eid, value="cached")
        assert ctx_derived.result(resp["runId"])["value"]["data"] == "cached"


def test_cache_not_shared_across_unrelated_workspaces(worker):
    """Task cached in one workspace does not produce cache hit in a sibling."""
    targets = [
        workflow("test.main"),
        task("test.expensive", parameters=["x"]),
    ]

    # Step 1: Run in "ws_a" workspace, execute the cached task
    with worker(targets, workspace="ws_a", concurrency=2) as ctx_a:
        resp = ctx_a.submit("test.main")
        conn0 = ctx_a.executor.connections[0]
        conn1 = ctx_a.executor.connections[1]

        wf_eid, _, _ = conn0.recv_execute()
        ref = conn0.submit_task(
            wf_eid,
            "test.expensive",
            json_args(42),
            cache={"params": True},
        )

        task_eid, _, _ = conn1.recv_execute()
        conn1.complete(task_eid, value=84)
        assert conn0.resolve(wf_eid, ref)["value"] == 84

        conn0.complete(wf_eid, value="done")
        assert ctx_a.result(resp["runId"])["value"]["data"] == "done"

        saved_env = ctx_a.env.copy()

    # Step 2: Create "ws_b" (sibling, NOT derived from ws_a)
    saved_env["COFLUX_WORKSPACE"] = "ws_b"
    cli.workspaces_create("ws_b", env=saved_env)

    # Step 3: Run in "ws_b" - should NOT hit ws_a's cache (must execute again)
    with worker(
        targets, workspace="ws_b", concurrency=2, create_workspace=False
    ) as ctx_b:
        resp = ctx_b.submit("test.main")
        conn0 = ctx_b.executor.connections[0]
        conn1 = ctx_b.executor.connections[1]

        wf_eid, _, _ = conn0.recv_execute()
        ref = conn0.submit_task(
            wf_eid,
            "test.expensive",
            json_args(42),
            cache={"params": True},
        )

        # Should require a new execution (cache miss in unrelated workspace)
        task_eid, _, _ = conn1.recv_execute()
        conn1.complete(task_eid, value=999)
        assert conn0.resolve(wf_eid, ref)["value"] == 999

        conn0.complete(wf_eid, value="done")
        assert ctx_b.result(resp["runId"])["value"]["data"] == "done"
