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

        # First attempt: fail
        conn0, eid1, _, _ = ctx.executor.next_execute()
        conn0.fail(eid1, "RuntimeError", "transient failure")

        # Retry: succeed (fresh connection)
        conn1, eid2, _, _ = ctx.executor.next_execute()
        assert eid2 != eid1
        conn1.complete(eid2, value="recovered")

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

        # Fail all attempts until retries are exhausted
        attempts = 0
        while True:
            try:
                conn, eid, _, _ = ctx.executor.next_execute(timeout=3)
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

        conn0, wf_eid, _, _ = ctx.executor.next_execute()

        ref = conn0.submit_task(
            wf_eid,
            "test.flaky_task",
            [],
            retries={"limit": 1, "delay_min_ms": 500, "delay_max_ms": 500},
        )

        # First attempt: fail
        conn_t1, task_eid1, _, _ = ctx.executor.next_execute()
        fail_time = time.time()
        conn_t1.fail(task_eid1, "RuntimeError", "transient")

        # Retry should be delayed by ~500ms (fresh connection)
        conn_t2, task_eid2, _, _ = ctx.executor.next_execute(timeout=5)
        elapsed = time.time() - fail_time

        conn_t2.complete(task_eid2, value="recovered")
        assert conn0.resolve(wf_eid, ref)["value"] == "recovered"

        conn0.complete(wf_eid, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"
        assert elapsed >= 0.4, f"expected >=0.4s retry delay, got {elapsed:.2f}s"
