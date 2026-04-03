"""Tests for execution timeout enforcement."""

import pytest

from support.manifest import task, workflow


def test_timeout_basic(worker):
    """Task with timeout in manifest is killed and result is 'timeout'."""
    targets = [workflow("test", "slow", timeout=1500)]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "slow")
        run_id = resp["runId"]

        # Receive the execution but don't complete it
        ctx.executor.next_execute()

        # The CLI should kill the executor after ~1.5s and report timeout
        result = ctx.result(run_id, timeout=10)
        assert result["type"] == "timeout"


def test_timeout_with_retry(worker):
    """Timed-out execution is retried when retries are configured."""
    targets = [
        workflow(
            "test", "flaky",
            timeout=1500,
            retries={"limit": 1, "backoff_min_ms": 0, "backoff_max_ms": 0},
        ),
    ]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "flaky")
        run_id = resp["runId"]

        # First attempt: receive but don't complete — will timeout
        ctx.executor.next_execute()

        # Second attempt (retry): complete successfully
        ex1 = ctx.executor.next_execute(timeout=10)
        ex1.conn.complete(ex1.execution_id, value="recovered")

        result = ctx.result(run_id)
        assert result["type"] == "value"
        assert result["value"]["data"] == "recovered"


def test_timeout_cancels_descendants(worker):
    """When a workflow times out, its child tasks are cancelled."""
    targets = [
        workflow("test", "parent", timeout=2000),
        task("test", "child"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "parent")
        run_id = resp["runId"]

        # Parent workflow starts
        ex0 = ctx.executor.next_execute()

        # Parent submits a child task
        ex0.conn.submit_task(ex0.execution_id, "test", "child", [])

        # Child task starts
        ctx.executor.next_execute()

        # Don't complete either — parent will timeout after ~2s
        # which should also cancel the child
        result = ctx.result(run_id, timeout=10)
        assert result["type"] == "timeout"
