"""Tests for dispatch timing and ordering: delay, wait_for, defer, suspend, cancel."""

import time

import pytest

from support.manifest import task, workflow
from support.protocol import json_args


def test_workflow_delay_from_manifest(worker):
    """Workflow with delay in manifest starts after the configured delay."""
    targets = [workflow("test", "delayed", delay=500)]

    with worker(targets) as ctx:
        start = time.time()
        resp = ctx.submit("test", "delayed")

        ex = ctx.executor.next_execute(timeout=5)

        elapsed = time.time() - start

        ex.conn.complete(ex.execution_id, value="delayed")
        assert ctx.result(resp["runId"])["value"]["data"] == "delayed"
        assert elapsed >= 0.4, f"expected >=0.4s delay, got {elapsed:.2f}s"


def test_delayed_execution(worker):
    """Child task submitted with delay starts after the configured delay."""
    targets = [
        workflow("test", "main"),
        task("test", "slow"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        # Submit a child task with 500ms delay
        start = time.time()
        ref = ex0.conn.submit_task(ex0.execution_id, "test", "slow", [], delay=500)

        # Wait for the delayed task to be assigned
        ex1 = ctx.executor.next_execute(timeout=5)
        elapsed = time.time() - start
        ex1.conn.complete(ex1.execution_id, value="delayed")

        assert ex0.conn.resolve(ex0.execution_id, ref)["value"] == "delayed"

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"
        # Task should have been delayed by ~500ms
        assert elapsed >= 0.4, f"expected >=0.4s delay, got {elapsed:.2f}s"


def test_wait_for(worker):
    """Task with wait_for is not dispatched until referenced execution completes."""
    targets = [
        workflow("test", "main"),
        task("test", "producer"),
        task("test", "consumer", parameters=["data"], wait_for=[0]),
    ]

    with worker(targets, concurrency=3) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        # Submit producer (no wait_for)
        ref_a = ex0.conn.submit_task(ex0.execution_id, "test", "producer", [])

        # Submit consumer with wait_for=[0] and execution reference in arg
        consumer_args = [
            {
                "type": "inline",
                "format": "json",
                "value": None,
                "references": [["execution", ref_a]],
            }
        ]
        ref_b = ex0.conn.submit_task(ex0.execution_id, "test", "consumer", consumer_args, wait_for=[0])

        # Producer should be dispatched
        ex1 = ctx.executor.next_execute()
        assert ex1.target == "producer"

        # Consumer should NOT be dispatched yet (producer not complete)
        with pytest.raises(TimeoutError):
            ctx.executor.next_execute(timeout=1)

        # Complete producer
        ex1.conn.complete(ex1.execution_id, value=42)

        # Now consumer should be dispatched
        ex2 = ctx.executor.next_execute(timeout=5)
        assert ex2.target == "consumer"

        ex2.conn.complete(ex2.execution_id, value="done")

        # Resolve both references
        ex0.conn.resolve(ex0.execution_id, ref_a)
        ex0.conn.resolve(ex0.execution_id, ref_b)

        ex0.conn.complete(ex0.execution_id, value="all done")
        assert ctx.result(run_id)["value"]["data"] == "all done"


def test_wait_for_multiple_dependencies(worker):
    """Task with wait_for on two args waits for both referenced executions."""
    targets = [
        workflow("test", "main"),
        task("test", "producer"),
        task("test", "consumer", parameters=["a", "b"], wait_for=[0, 1]),
    ]

    with worker(targets, concurrency=4) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        # Submit two producers
        ref_a = ex0.conn.submit_task(ex0.execution_id, "test", "producer", [])
        ref_b = ex0.conn.submit_task(ex0.execution_id, "test", "producer", [])

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
        ref_c = ex0.conn.submit_task(
            ex0.execution_id, "test", "consumer", consumer_args, wait_for=[0, 1]
        )

        # Both producers should be dispatched
        ex1 = ctx.executor.next_execute()
        ex2 = ctx.executor.next_execute()

        # Complete only the first producer
        ex1.conn.complete(ex1.execution_id, value=1)

        # Consumer should NOT be dispatched yet (second producer still running)
        with pytest.raises(TimeoutError):
            ctx.executor.next_execute(timeout=1)

        # Complete the second producer
        ex2.conn.complete(ex2.execution_id, value=2)

        # Now consumer should be dispatched
        ex3 = ctx.executor.next_execute(timeout=5)
        assert ex3.target == "consumer"
        ex3.conn.complete(ex3.execution_id, value="done")

        ex0.conn.resolve(ex0.execution_id, ref_a)
        ex0.conn.resolve(ex0.execution_id, ref_b)
        ex0.conn.resolve(ex0.execution_id, ref_c)

        ex0.conn.complete(ex0.execution_id, value="all done")
        assert ctx.result(run_id)["value"]["data"] == "all done"


def test_defer_deduplicates(worker):
    """Two child tasks with same defer key - only one executes, both resolve."""
    targets = [
        workflow("test", "main"),
        task("test", "compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        # Submit two tasks with same args and defer config.
        # The delay prevents either from being assigned before both are
        # submitted, so the server sees both unassigned in the same tick
        # and defers one to the other.
        ref1 = ex0.conn.submit_task(
            ex0.execution_id,
            "test", "compute",
            json_args(42),
            defer_config={"params": True},
            delay=1000,
        )
        ref2 = ex0.conn.submit_task(
            ex0.execution_id,
            "test", "compute",
            json_args(42),
            defer_config={"params": True},
            delay=1000,
        )

        # Only one task should actually execute
        ex1 = ctx.executor.next_execute(timeout=5)
        ex1.conn.complete(ex1.execution_id, value=99)

        # Both references should resolve to the same value
        assert ex0.conn.resolve(ex0.execution_id, ref1)["value"] == 99
        assert ex0.conn.resolve(ex0.execution_id, ref2)["value"] == 99

        # No second execution should arrive (it was deferred)
        with pytest.raises(TimeoutError):
            ctx.executor.next_execute(timeout=1)

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_suspend_and_resume(worker):
    """Executor suspends, server creates retry, second attempt succeeds."""
    targets = [workflow("test", "suspendable")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "suspendable")
        run_id = resp["runId"]

        # First execute
        ex0 = ctx.executor.next_execute()

        # Suspend (no execute_after = immediate retry)
        ex0.conn.suspend(ex0.execution_id)

        # After suspending, the executor must still complete the execution
        # (the pool loop waits for execution_result/error before freeing the slot)
        ex0.conn.complete(ex0.execution_id)

        # Second execute (retry after suspend) on fresh connection
        ex1 = ctx.executor.next_execute()
        assert ex1.execution_id != ex0.execution_id

        ex1.conn.complete(ex1.execution_id, value="resumed")
        assert ctx.result(run_id)["value"]["data"] == "resumed"


def test_suspend_with_execute_after(worker):
    """Suspend with execute_after delays re-dispatch until the specified time."""
    targets = [workflow("test", "suspendable")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "suspendable")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        # Suspend with execute_after 500ms in the future
        execute_after = int(time.time() * 1000) + 500
        suspend_time = time.time()
        ex0.conn.suspend(ex0.execution_id, execute_after=execute_after)
        ex0.conn.complete(ex0.execution_id)

        # Re-dispatch should not arrive immediately
        with pytest.raises(TimeoutError):
            ctx.executor.next_execute(timeout=0.2)

        # But should arrive after the delay
        ex1 = ctx.executor.next_execute(timeout=5)
        elapsed = time.time() - suspend_time
        assert ex1.execution_id != ex0.execution_id
        assert elapsed >= 0.4, f"expected >=0.4s delay, got {elapsed:.2f}s"

        ex1.conn.complete(ex1.execution_id, value="resumed later")
        assert ctx.result(run_id)["value"]["data"] == "resumed later"


def test_suspend_twice(worker):
    """Execution can suspend, resume, then suspend and resume again."""
    targets = [workflow("test", "suspendable")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "suspendable")
        run_id = resp["runId"]

        # First attempt: suspend immediately
        ex0 = ctx.executor.next_execute()
        ex0.conn.suspend(ex0.execution_id)
        ex0.conn.complete(ex0.execution_id)

        # Second attempt: suspend again
        ex1 = ctx.executor.next_execute()
        assert ex1.execution_id != ex0.execution_id
        ex1.conn.suspend(ex1.execution_id)
        ex1.conn.complete(ex1.execution_id)

        # Third attempt: complete normally
        ex2 = ctx.executor.next_execute()
        assert ex2.execution_id != ex1.execution_id
        ex2.conn.complete(ex2.execution_id, value="third time")

        assert ctx.result(run_id)["value"]["data"] == "third time"


def test_cancel_child_execution(worker):
    """Workflow cancels a child task it submitted."""
    targets = [
        workflow("test", "main"),
        task("test", "slow"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        ref = ex0.conn.submit_task(ex0.execution_id, "test", "slow", [])

        # Wait for task to be assigned
        ctx.executor.next_execute()

        # Cancel the task
        ex0.conn.cancel(ex0.execution_id, ref)

        # Resolve the reference - should get an error (cancelled)
        ex0.conn.resolve(ex0.execution_id, ref)

        # Workflow completes with its own result
        ex0.conn.complete(ex0.execution_id, value="handled")
        assert ctx.result(run_id)["value"]["data"] == "handled"


def test_requires_matching(worker):
    """Task with requires is only dispatched to a worker with matching provides.

    Two workers connect: one without the tag (concurrency=2) and one with the
    tag (concurrency=1). The workflow may land on either worker, but the child
    task with requires must go to the tagged worker and must NOT be dispatched
    to the untagged worker's spare slot.
    """
    targets = [
        workflow("test", "main"),
        task("test", "compute", requires={"gpu": ["cuda-12"]}),
    ]

    # Worker A: no provides, concurrency=2
    with worker(targets, concurrency=2) as ctx_a:
        # Worker B: provides gpu=cuda-12, concurrency=1
        with worker(
            targets,
            concurrency=1,
            provides={"gpu": ["cuda-12"]},
        ) as ctx_b:
            resp = ctx_a.submit("test", "main")
            run_id = resp["runId"]

            # The workflow has no requires, so it could land on either worker.
            # Try worker A first (more likely with 2 slots), fall back to B.
            try:
                ex = ctx_a.executor.next_execute(timeout=5)
                assert ex.target == "main"
                wf_on_a = True
            except TimeoutError:
                ex = ctx_b.executor.next_execute(timeout=5)
                assert ex.target == "main"
                wf_on_a = False

            # Submit child task with requires
            ref = ex.conn.submit_task(
                ex.execution_id,
                "test", "compute",
                [],
                requires={"gpu": ["cuda-12"]},
            )

            # Worker B (tagged) should receive the task
            if not wf_on_a:
                # Workflow landed on B's only slot; task must wait.
                # Complete the workflow first so B's slot frees up.
                ex.conn.complete(ex.execution_id, value="done")
                ex_b = ctx_b.executor.next_execute(timeout=5)
                assert ex_b.target == "compute"
                ex_b.conn.complete(ex_b.execution_id, value="computed")
            else:
                # Workflow is on worker A -- B's slot is free
                ex_b = ctx_b.executor.next_execute(timeout=5)
                assert ex_b.target == "compute"

                ex_b.conn.complete(ex_b.execution_id, value="computed")
                assert ex.conn.resolve(ex.execution_id, ref)["value"] == "computed"
                ex.conn.complete(ex.execution_id, value="done")

            assert ctx_a.result(run_id)["value"]["data"] == "done"


def test_cancel_execution_externally(worker):
    """Cancel a pending execution via CLI before it is dispatched."""
    targets = [workflow("test", "delayed", delay=10000)]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "delayed")
        run_id = resp["runId"]
        execution_id = resp["executionId"]

        # Cancel before it's dispatched (it has a 10s delay)
        ctx.cancel(execution_id)

        # Run result should show as cancelled
        result = ctx.result(run_id)
        assert result["type"] == "cancelled"

        # Verify the execute message never arrives
        with pytest.raises(TimeoutError):
            ctx.executor.next_execute(timeout=1)


def test_cancel_across_spawn(worker):
    """Cancelling a workflow also cancels a child workflow it spawned."""
    targets = [
        workflow("test", "outer"),
        workflow("test", "inner"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "outer")
        run_id = resp["runId"]

        # Outer workflow starts
        ex0 = ctx.executor.next_execute()

        # Outer spawns inner workflow
        ex0.conn.submit_workflow(ex0.execution_id, "test", "inner", [])

        # Inner workflow starts
        ctx.executor.next_execute()

        # Cancel the outer execution externally
        ctx.cancel(resp["executionId"])

        # Run result should be cancelled
        result = ctx.result(run_id)
        assert result["type"] == "cancelled"


def test_cancel_across_multiple_spawns(worker):
    """Cancelling a workflow propagates through multiple levels of spawned workflows."""
    targets = [
        workflow("test", "top"),
        workflow("test", "middle"),
        task("test", "leaf"),
    ]

    with worker(targets, concurrency=3) as ctx:
        resp = ctx.submit("test", "top")
        run_id = resp["runId"]

        # Top workflow starts
        ex0 = ctx.executor.next_execute()

        # Top spawns middle workflow
        ref_mid = ex0.conn.submit_workflow(ex0.execution_id, "test", "middle", [])

        # Middle workflow starts
        ex1 = ctx.executor.next_execute()
        assert ex1.target == "middle"

        # Middle spawns leaf task
        ex1.conn.submit_task(ex1.execution_id, "test", "leaf", [])

        # Leaf task starts
        ex2 = ctx.executor.next_execute()
        assert ex2.target == "leaf"

        # Cancel the top execution externally
        ctx.cancel(resp["executionId"])

        # Run result should be cancelled
        result = ctx.result(run_id)
        assert result["type"] == "cancelled"
