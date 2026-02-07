"""Tests for retry behavior."""

import time

from support.manifest import task, workflow


def test_retry_on_error(worker):
    """First attempt fails, retry succeeds. Run result is the success value."""
    targets = [
        workflow(
            "test.flaky", retries={"limit": 1, "delay_min_ms": 0, "delay_max_ms": 0}
        )
    ]

    with worker(targets) as ctx:
        resp = ctx.submit("test.flaky")
        conn = ctx.executor.connections[0]

        # First attempt: fail
        eid1, _, _ = conn.recv_execute()
        conn.fail(eid1, "RuntimeError", "transient failure")

        # Retry: succeed
        eid2, _, _ = conn.recv_execute()
        assert eid2 != eid1
        conn.complete(eid2, value="recovered")

        result = ctx.result(resp["runId"])
        assert result["type"] == "value"
        assert result["value"]["data"] == "recovered"


def test_retry_limit_exhausted(worker):
    """All retry attempts fail. After the limit, run result is the final error."""
    targets = [
        workflow(
            "test.doomed", retries={"limit": 1, "delay_min_ms": 0, "delay_max_ms": 0}
        )
    ]

    with worker(targets) as ctx:
        resp = ctx.submit("test.doomed")
        conn = ctx.executor.connections[0]

        # Handle all attempts (fail every time) until retries are exhausted
        attempts = 0
        while True:
            try:
                eid, _, _ = conn.recv_execute(timeout=3)
                conn.fail(eid, "RuntimeError", "permanent failure")
                attempts += 1
            except TimeoutError:
                break

        assert attempts >= 2  # original + at least 1 retry
        result = ctx.result(resp["runId"])
        assert result["type"] == "error"
        assert result["error"]["type"] == "RuntimeError"


def test_retry_with_delay(worker):
    """Retry respects delay_min_ms timing between attempts."""
    targets = [
        workflow("test.main"),
        task("test.flaky_task"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.main")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]
        conn1 = ctx.executor.connections[1]

        wf_eid, _, _ = conn0.recv_execute()

        ref = conn0.submit_task(
            wf_eid,
            "test.flaky_task",
            [],
            retries={"limit": 1, "delay_min_ms": 500, "delay_max_ms": 500},
        )

        # First attempt: fail
        task_eid1, _, _ = conn1.recv_execute()
        fail_time = time.time()
        conn1.fail(task_eid1, "RuntimeError", "transient")

        # Retry should be delayed by ~500ms (same connection handles the retry)
        task_eid2, _, _ = conn1.recv_execute(timeout=5)
        elapsed = time.time() - fail_time

        conn1.complete(task_eid2, value="recovered")
        assert conn0.resolve(wf_eid, ref)["value"] == "recovered"

        conn0.complete(wf_eid, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"
        assert elapsed >= 0.4, f"expected >=0.4s retry delay, got {elapsed:.2f}s"
