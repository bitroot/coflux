"""Tests for dispatch timing and ordering: delay, wait_for, defer, suspend, cancel."""

import time

import pytest

from support.manifest import task, workflow
from support.protocol import json_args


def test_workflow_delay_from_manifest(worker):
    """Workflow with delay in manifest starts after the configured delay."""
    targets = [workflow("test.delayed", delay=500)]

    with worker(targets) as ctx:
        start = time.time()
        resp = ctx.submit("test.delayed")
        conn = ctx.executor.connections[0]

        eid, _, _ = conn.recv_execute(timeout=5)
        elapsed = time.time() - start

        conn.complete(eid, value="delayed")
        assert ctx.result(resp["runId"])["value"]["data"] == "delayed"
        assert elapsed >= 0.4, f"expected >=0.4s delay, got {elapsed:.2f}s"


def test_delayed_execution(worker):
    """Child task submitted with delay starts after the configured delay."""
    targets = [
        workflow("test.main"),
        task("test.slow"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.main")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]
        conn1 = ctx.executor.connections[1]

        wf_eid, _, _ = conn0.recv_execute()

        # Submit a child task with 500ms delay
        start = time.time()
        ref = conn0.submit_task(wf_eid, "test.slow", [], delay=500)

        # Wait for the delayed task to be assigned
        task_eid, _, _ = conn1.recv_execute(timeout=5)
        elapsed = time.time() - start
        conn1.complete(task_eid, value="delayed")

        assert conn0.resolve(wf_eid, ref)["value"] == "delayed"

        conn0.complete(wf_eid, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"
        # Task should have been delayed by ~500ms
        assert elapsed >= 0.4, f"expected >=0.4s delay, got {elapsed:.2f}s"


def test_wait_for(worker):
    """Task with wait_for is not dispatched until referenced execution completes."""
    targets = [
        workflow("test.main"),
        task("test.producer"),
        task("test.consumer", parameters=["data"], wait_for=[0]),
    ]

    with worker(targets, concurrency=3) as ctx:
        resp = ctx.submit("test.main")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]
        conn1 = ctx.executor.connections[1]
        conn2 = ctx.executor.connections[2]

        wf_eid, _, _ = conn0.recv_execute()

        # Submit producer (no wait_for)
        ref_a = conn0.submit_task(wf_eid, "test.producer", [])

        # Submit consumer with wait_for=[0] and execution reference in arg
        consumer_args = [
            {
                "type": "inline",
                "format": "json",
                "value": None,
                "references": [["execution", ref_a]],
            }
        ]
        ref_b = conn0.submit_task(wf_eid, "test.consumer", consumer_args, wait_for=[0])

        # Producer should be dispatched to conn1
        prod_eid, target, _ = conn1.recv_execute()
        assert target == "test.producer"

        # Consumer should NOT be dispatched yet (producer not complete)
        with pytest.raises(TimeoutError):
            conn2.recv_execute(timeout=1)

        # Complete producer
        conn1.complete(prod_eid, value=42)

        # Now consumer should be dispatched to conn2
        cons_eid, target, args = conn2.recv_execute(timeout=5)
        assert target == "test.consumer"

        conn2.complete(cons_eid, value="done")

        # Resolve both references
        conn0.resolve(wf_eid, ref_a)
        conn0.resolve(wf_eid, ref_b)

        conn0.complete(wf_eid, value="all done")
        assert ctx.result(run_id)["value"]["data"] == "all done"


def test_wait_for_multiple_dependencies(worker):
    """Task with wait_for on two args waits for both referenced executions."""
    targets = [
        workflow("test.main"),
        task("test.producer"),
        task("test.consumer", parameters=["a", "b"], wait_for=[0, 1]),
    ]

    with worker(targets, concurrency=4) as ctx:
        resp = ctx.submit("test.main")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]
        conn1 = ctx.executor.connections[1]
        conn2 = ctx.executor.connections[2]
        conn3 = ctx.executor.connections[3]

        wf_eid, _, _ = conn0.recv_execute()

        # Submit two producers
        ref_a = conn0.submit_task(wf_eid, "test.producer", [])
        ref_b = conn0.submit_task(wf_eid, "test.producer", [])

        # Submit consumer with both execution references
        consumer_args = [
            {
                "type": "inline",
                "format": "json",
                "value": None,
                "references": [["execution", ref_a]],
            },
            {
                "type": "inline",
                "format": "json",
                "value": None,
                "references": [["execution", ref_b]],
            },
        ]
        ref_c = conn0.submit_task(
            wf_eid, "test.consumer", consumer_args, wait_for=[0, 1]
        )

        # Both producers should be dispatched to conn1 and conn2
        prod_eid1, _, _ = conn1.recv_execute()
        prod_eid2, _, _ = conn2.recv_execute()

        # Complete only the first producer
        conn1.complete(prod_eid1, value=1)

        # Consumer should NOT be dispatched yet (second producer still running)
        with pytest.raises(TimeoutError):
            conn3.recv_execute(timeout=1)

        # Complete the second producer
        conn2.complete(prod_eid2, value=2)

        # Now consumer should be dispatched to conn3
        cons_eid, target, _ = conn3.recv_execute(timeout=5)
        assert target == "test.consumer"
        conn3.complete(cons_eid, value="done")

        conn0.resolve(wf_eid, ref_a)
        conn0.resolve(wf_eid, ref_b)
        conn0.resolve(wf_eid, ref_c)

        conn0.complete(wf_eid, value="all done")
        assert ctx.result(run_id)["value"]["data"] == "all done"


def test_defer_deduplicates(worker):
    """Two child tasks with same defer key - only one executes, both resolve."""
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

        # Submit two tasks with same args and defer config.
        # The delay prevents either from being assigned before both are
        # submitted, so the server sees both unassigned in the same tick
        # and defers one to the other.
        ref1 = conn0.submit_task(
            wf_eid,
            "test.compute",
            json_args(42),
            defer_config={"params": True},
            delay=1000,
        )
        ref2 = conn0.submit_task(
            wf_eid,
            "test.compute",
            json_args(42),
            defer_config={"params": True},
            delay=1000,
        )

        # Only one task should actually execute
        task_eid, _, _ = conn1.recv_execute(timeout=5)
        conn1.complete(task_eid, value=99)

        # Both references should resolve to the same value
        assert conn0.resolve(wf_eid, ref1)["value"] == 99
        assert conn0.resolve(wf_eid, ref2)["value"] == 99

        # No second execution should arrive (it was deferred)
        with pytest.raises(TimeoutError):
            conn1.recv_execute(timeout=1)

        conn0.complete(wf_eid, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_suspend_and_resume(worker):
    """Executor suspends, server creates retry, second attempt succeeds."""
    targets = [workflow("test.suspendable")]

    with worker(targets) as ctx:
        resp = ctx.submit("test.suspendable")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]

        # First execute
        eid1, _, _ = conn0.recv_execute()

        # Suspend (no execute_after = immediate retry)
        conn0.suspend(eid1)

        # After suspending, the executor must still complete the execution
        # (the pool loop waits for execution_result/error before freeing the slot)
        conn0.complete(eid1)

        # Second execute (retry after suspend)
        eid2, _, _ = conn0.recv_execute()
        assert eid2 != eid1

        conn0.complete(eid2, value="resumed")
        assert ctx.result(run_id)["value"]["data"] == "resumed"


def test_suspend_with_execute_after(worker):
    """Suspend with execute_after delays re-dispatch until the specified time."""
    targets = [workflow("test.suspendable")]

    with worker(targets) as ctx:
        resp = ctx.submit("test.suspendable")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]

        eid1, _, _ = conn0.recv_execute()

        # Suspend with execute_after 500ms in the future
        execute_after = int(time.time() * 1000) + 500
        suspend_time = time.time()
        conn0.suspend(eid1, execute_after=execute_after)
        conn0.complete(eid1)

        # Re-dispatch should not arrive immediately
        with pytest.raises(TimeoutError):
            conn0.recv_execute(timeout=0.2)

        # But should arrive after the delay
        eid2, _, _ = conn0.recv_execute(timeout=5)
        elapsed = time.time() - suspend_time
        assert eid2 != eid1
        assert elapsed >= 0.4, f"expected >=0.4s delay, got {elapsed:.2f}s"

        conn0.complete(eid2, value="resumed later")
        assert ctx.result(run_id)["value"]["data"] == "resumed later"


def test_suspend_twice(worker):
    """Execution can suspend, resume, then suspend and resume again."""
    targets = [workflow("test.suspendable")]

    with worker(targets) as ctx:
        resp = ctx.submit("test.suspendable")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]

        # First attempt: suspend immediately
        eid1, _, _ = conn0.recv_execute()
        conn0.suspend(eid1)
        conn0.complete(eid1)

        # Second attempt: suspend again
        eid2, _, _ = conn0.recv_execute()
        assert eid2 != eid1
        conn0.suspend(eid2)
        conn0.complete(eid2)

        # Third attempt: complete normally
        eid3, _, _ = conn0.recv_execute()
        assert eid3 != eid2
        conn0.complete(eid3, value="third time")

        assert ctx.result(run_id)["value"]["data"] == "third time"


def test_cancel_child_execution(worker):
    """Workflow cancels a child task it submitted."""
    targets = [
        workflow("test.main"),
        task("test.slow"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.main")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]
        conn1 = ctx.executor.connections[1]

        wf_eid, _, _ = conn0.recv_execute()

        ref = conn0.submit_task(wf_eid, "test.slow", [])

        # Wait for task to be assigned
        conn1.recv_execute()

        # Cancel the task
        conn0.cancel(wf_eid, ref)

        # Resolve the reference - should get an error (cancelled)
        conn0.resolve(wf_eid, ref)

        # Workflow completes with its own result
        conn0.complete(wf_eid, value="handled")
        assert ctx.result(run_id)["value"]["data"] == "handled"


def test_requires_matching(worker):
    """Task with requires is only dispatched to a worker with matching provides.

    Two workers connect: one without the tag (concurrency=2) and one with the
    tag (concurrency=1). The workflow may land on either worker, but the child
    task with requires must go to the tagged worker and must NOT be dispatched
    to the untagged worker's spare slot.
    """
    targets = [
        workflow("test.main"),
        task("test.compute", requires={"gpu": ["cuda-12"]}),
    ]

    # Worker A: no provides, concurrency=2
    with worker(targets, concurrency=2) as ctx_a:
        # Worker B: provides gpu=cuda-12, concurrency=1
        with worker(
            targets,
            concurrency=1,
            provides={"gpu": ["cuda-12"]},
            create_workspace=False,
        ) as ctx_b:
            resp = ctx_a.submit("test.main")
            run_id = resp["runId"]

            # The workflow has no requires, so it could land on either worker.
            # Try worker A first (more likely with 2 slots), fall back to B.
            conn_a0 = ctx_a.executor.connections[0]
            conn_b0 = ctx_b.executor.connections[0]
            wf_conn = None
            wf_eid = None
            wf_on_a = False
            try:
                wf_eid, target, _ = conn_a0.recv_execute(timeout=5)
                assert target == "test.main"
                wf_conn = conn_a0
                wf_on_a = True
            except TimeoutError:
                wf_eid, target, _ = conn_b0.recv_execute(timeout=5)
                assert target == "test.main"
                wf_conn = conn_b0

            # Submit child task with requires
            ref = wf_conn.submit_task(
                wf_eid,
                "test.compute",
                [],
                requires={"gpu": ["cuda-12"]},
            )

            # Worker B (tagged) should receive the task
            if not wf_on_a:
                # Workflow landed on B's only slot; task must wait.
                # Complete the workflow first so B's slot frees up.
                wf_conn.complete(wf_eid, value="done")
                task_eid, target, _ = conn_b0.recv_execute(timeout=5)
                assert target == "test.compute"
                conn_b0.complete(task_eid, value="computed")
            else:
                # Workflow is on worker A -- B's slot is free
                task_eid, target, _ = conn_b0.recv_execute(timeout=5)
                assert target == "test.compute"

                # Worker A's other slot should NOT receive the requires task
                conn_a1 = ctx_a.executor.connections[1]
                with pytest.raises(TimeoutError):
                    conn_a1.recv_execute(timeout=1)

                conn_b0.complete(task_eid, value="computed")
                assert wf_conn.resolve(wf_eid, ref)["value"] == "computed"
                wf_conn.complete(wf_eid, value="done")

            assert ctx_a.result(run_id)["value"]["data"] == "done"


def test_cancel_execution_externally(worker):
    """Cancel a pending execution via CLI before it is dispatched."""
    targets = [workflow("test.delayed", delay=10000)]

    with worker(targets) as ctx:
        resp = ctx.submit("test.delayed")
        run_id = resp["runId"]
        execution_id = resp["executionId"]

        # Cancel before it's dispatched (it has a 10s delay)
        ctx.cancel(execution_id)

        # Run result should show as cancelled
        result = ctx.result(run_id)
        assert result["type"] == "cancelled"

        # Verify the execute message never arrives
        conn0 = ctx.executor.connections[0]
        with pytest.raises(TimeoutError):
            conn0.recv_execute(timeout=1)
