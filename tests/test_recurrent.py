"""Tests for recurrent execution behavior."""

import pytest

from support.manifest import task, workflow


def test_recurrent_execution(worker):
    """Recurrent child task auto-re-executes after each successful completion."""
    targets = [
        workflow("test.main"),
        task("test.ticker"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.main")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]
        conn1 = ctx.executor.connections[1]

        wf_eid, _, _ = conn0.recv_execute()

        # Submit a recurrent child task
        conn0.submit_task(wf_eid, "test.ticker", [], recurrent=True)

        # First execution arrives on conn1
        tick_eid, _, _ = conn1.recv_execute()

        # Three executions arrive automatically on the same connection
        prev_eid = None
        for i in range(3):
            if i == 0:
                eid = tick_eid
            else:
                eid, _, _ = conn1.recv_execute()
            if prev_eid is not None:
                assert eid != prev_eid
            conn1.complete(eid, value=f"tick {i + 1}")
            prev_eid = eid

        conn0.complete(wf_eid, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_recurrent_stops_on_error(worker):
    """Recurrent child task does NOT re-execute after an error."""
    targets = [
        workflow("test.main"),
        task("test.ticker"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.main")
        conn0 = ctx.executor.connections[0]
        conn1 = ctx.executor.connections[1]

        wf_eid, _, _ = conn0.recv_execute()

        conn0.submit_task(wf_eid, "test.ticker", [], recurrent=True)

        # First execution succeeds -> triggers re-execution
        eid1, _, _ = conn1.recv_execute()
        conn1.complete(eid1, value="tick 1")

        # Second execution arrives (recurrence), fail it
        eid2, _, _ = conn1.recv_execute()
        conn1.fail(eid2, "RuntimeError", "crash")

        # No third execution should arrive (recurrent stops on error)
        with pytest.raises(TimeoutError):
            conn1.recv_execute(timeout=2)

        conn0.complete(wf_eid, value="done")
        assert ctx.result(resp["runId"])["value"]["data"] == "done"
