"""Tests for recurrent execution behavior."""

import pytest

from support.manifest import task, workflow


def test_recurrent_execution(worker):
    """Recurrent child task auto-re-executes after each successful completion."""
    targets = [
        workflow("test", "main"),
        task("test", "ticker"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        # Submit a recurrent child task
        ex0.conn.submit_task(ex0.execution_id, "test", "ticker", [], recurrent=True)

        # Three executions arrive automatically, each on a fresh connection.
        # Recurrent tasks must return None to trigger the next iteration.
        prev_eid = None
        for i in range(3):
            ex = ctx.executor.next_execute()
            if prev_eid is not None:
                assert ex.execution_id != prev_eid
            ex.conn.complete(ex.execution_id, value=None)
            prev_eid = ex.execution_id

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_recurrent_stops_on_return_value(worker):
    """Recurrent child task does NOT re-execute after returning a non-null value."""
    targets = [
        workflow("test", "main"),
        task("test", "ticker"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")

        ex0 = ctx.executor.next_execute()

        ex0.conn.submit_task(ex0.execution_id, "test", "ticker", [], recurrent=True)

        # First execution returns None -> triggers re-execution
        ex1 = ctx.executor.next_execute()
        ex1.conn.complete(ex1.execution_id, value=None)

        # Second execution returns a non-null value -> stops recurrence
        ex2 = ctx.executor.next_execute()
        ex2.conn.complete(ex2.execution_id, value="final")

        # No third execution should arrive
        with pytest.raises(TimeoutError):
            ctx.executor.next_execute(timeout=2)

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"


def test_recurrent_stops_on_error(worker):
    """Recurrent child task does NOT re-execute after an error."""
    targets = [
        workflow("test", "main"),
        task("test", "ticker"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")

        ex0 = ctx.executor.next_execute()

        ex0.conn.submit_task(ex0.execution_id, "test", "ticker", [], recurrent=True)

        # First execution returns None -> triggers re-execution
        ex1 = ctx.executor.next_execute()
        ex1.conn.complete(ex1.execution_id, value=None)

        # Second execution arrives (recurrence), fail it
        ex2 = ctx.executor.next_execute()
        ex2.conn.fail(ex2.execution_id, "RuntimeError", "crash")

        # No third execution should arrive (recurrent stops on error)
        with pytest.raises(TimeoutError):
            ctx.executor.next_execute(timeout=2)

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"
