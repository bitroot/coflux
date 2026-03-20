"""Tests for retry behavior."""

import time

from support.manifest import task, workflow


def test_retry_on_error(worker):
    """First attempt fails, retry succeeds. Run result is the success value."""
    targets = [
        workflow(
            "test", "flaky", retries={"limit": 1, "delay_min_ms": 0, "delay_max_ms": 0}
        )
    ]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "flaky")

        # First attempt: fail
        ex0 = ctx.executor.next_execute()
        ex0.conn.fail(ex0.execution_id, "RuntimeError", "transient failure")

        # Retry: succeed (fresh connection)
        ex1 = ctx.executor.next_execute()
        assert ex1.execution_id != ex0.execution_id
        ex1.conn.complete(ex1.execution_id, value="recovered")

        result = ctx.result(resp["runId"])
        assert result["type"] == "value"
        assert result["value"]["data"] == "recovered"


def test_retry_limit_exhausted(worker):
    """All retry attempts fail. After the limit, run result is the final error."""
    targets = [
        workflow(
            "test", "doomed", retries={"limit": 1, "delay_min_ms": 0, "delay_max_ms": 0}
        )
    ]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "doomed")

        # Fail all attempts until retries are exhausted
        attempts = 0
        while True:
            try:
                ex = ctx.executor.next_execute(timeout=3)
                ex.conn.fail(ex.execution_id, "RuntimeError", "permanent failure")
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
        workflow("test", "main"),
        task("test", "flaky_task"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        ref = ex0.conn.submit_task(
            ex0.execution_id,
            "test", "flaky_task",
            [],
            retries={"limit": 1, "delay_min_ms": 500, "delay_max_ms": 500},
        )

        # First attempt: fail
        ex_t1 = ctx.executor.next_execute()
        fail_time = time.time()
        ex_t1.conn.fail(ex_t1.execution_id, "RuntimeError", "transient")

        # Retry should be delayed by ~500ms (fresh connection)
        ex_t2 = ctx.executor.next_execute(timeout=5)
        elapsed = time.time() - fail_time

        ex_t2.conn.complete(ex_t2.execution_id, value="recovered")
        assert ex0.conn.resolve(ex0.execution_id, ref)["value"] == "recovered"

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"
        assert elapsed >= 0.4, f"expected >=0.4s retry delay, got {elapsed:.2f}s"


def test_retry_not_retryable(worker):
    """When retryable=False, no retry is attempted despite retry limit."""
    targets = [
        workflow(
            "test", "non_retryable", retries={"limit": 3, "delay_min_ms": 0, "delay_max_ms": 0}
        )
    ]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "non_retryable")

        ex0 = ctx.executor.next_execute()
        ex0.conn.fail(ex0.execution_id, "ValueError", "bad input", retryable=False)

        # No retry should happen — result should be the error directly
        result = ctx.result(resp["runId"])
        assert result["type"] == "error"
        assert result["error"]["type"] == "ValueError"


def test_retry_retryable_true_still_retries(worker):
    """When retryable=True (default), retries work normally."""
    targets = [
        workflow(
            "test", "retryable", retries={"limit": 1, "delay_min_ms": 0, "delay_max_ms": 0}
        )
    ]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "retryable")

        # First attempt: fail with retryable=True (default)
        ex0 = ctx.executor.next_execute()
        ex0.conn.fail(ex0.execution_id, "RuntimeError", "transient", retryable=True)

        # Retry should happen
        ex1 = ctx.executor.next_execute()
        assert ex1.execution_id != ex0.execution_id
        ex1.conn.complete(ex1.execution_id, value="ok")

        result = ctx.result(resp["runId"])
        assert result["type"] == "value"
        assert result["value"]["data"] == "ok"
