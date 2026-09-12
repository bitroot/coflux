"""A suspended select wakes on whichever handle resolves first."""

import time

from support import protocol
from support.manifest import task, workflow


def test_suspended_select_wakes_on_any_handle(worker):
    """Select is first-wins, and that holds across a suspension: the
    successor is gated on the group as a whole, not on every member."""
    targets = [workflow("test", "main"), task("test", "child")]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        ex = ctx.executor.next_execute()

        input_id = ex.conn.submit_input(ex.execution_id, "Or answer this")
        child = ex.conn.submit_task(ex.execution_id, "test", "child", [])

        msg = protocol.select_request(
            None,
            ex.execution_id,
            [protocol.input_handle(input_id), protocol.execution_handle(child)],
            timeout_ms=0,
            suspend=True,
        )
        msg["id"] = 999
        ex.conn.send(msg)
        time.sleep(0.5)

        child_ex = ctx.executor.next_execute()
        assert child_ex.target == "child"
        child_ex.conn.complete(child_ex.execution_id, value="child done")

        # The input is still unanswered, but the child finishing is enough.
        resumed = ctx.executor.next_execute()
        assert resumed.execution_id == f"{resp['stepId']}:2"
        result = resumed.conn.select(
            resumed.execution_id,
            [protocol.input_handle(input_id), protocol.execution_handle(child)],
            timeout_ms=0,
            suspend=False,
        )
        assert result["winner"] == 1
        assert result["value"]["value"] == "child done"
        resumed.conn.complete(resumed.execution_id, value="resumed")
        assert ctx.result(resp["runId"])["value"]["data"] == "resumed"
