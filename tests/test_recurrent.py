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

        conn0, wf_eid, _, _, _ = ctx.executor.next_execute()

        # Submit a recurrent child task
        conn0.submit_task(wf_eid, "test", "ticker", [], recurrent=True)

        # Three executions arrive automatically, each on a fresh connection
        prev_eid = None
        for i in range(3):
            conn, eid, _, _, _ = ctx.executor.next_execute()
            if prev_eid is not None:
                assert eid != prev_eid
            conn.complete(eid, value=f"tick {i + 1}")
            prev_eid = eid

        conn0.complete(wf_eid, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_recurrent_stops_on_error(worker):
    """Recurrent child task does NOT re-execute after an error."""
    targets = [
        workflow("test", "main"),
        task("test", "ticker"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")

        conn0, wf_eid, _, _, _ = ctx.executor.next_execute()

        conn0.submit_task(wf_eid, "test", "ticker", [], recurrent=True)

        # First execution succeeds -> triggers re-execution
        conn1, eid1, _, _, _ = ctx.executor.next_execute()
        conn1.complete(eid1, value="tick 1")

        # Second execution arrives (recurrence), fail it
        conn2, eid2, _, _, _ = ctx.executor.next_execute()
        conn2.fail(eid2, "RuntimeError", "crash")

        # No third execution should arrive (recurrent stops on error)
        with pytest.raises(TimeoutError):
            ctx.executor.next_execute(timeout=2)

        conn0.complete(wf_eid, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"
