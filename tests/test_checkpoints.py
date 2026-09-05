"""Tests for checkpoints — step-scoped durable state."""

import json

import pytest
from support import cli
from support.helpers import api_post, managed_worker, poll_result
from support.manifest import workflow


def values(ex):
    """Plain {name: value} from an execute message's checkpoint payload.

    Blob-backed values arrive as a local file path (the CLI downloads them,
    exactly as it does for arguments), so those are read back off disk.

    A stored null has no "value" key at all — it's omitted on the wire, the
    same as a null argument — so presence of the name is what distinguishes it
    from a reset.
    """
    result = {}
    for name, value in (ex.checkpoints or {}).items():
        if value.get("type") == "file":
            with open(value["path"]) as f:
                result[name] = json.load(f)
        else:
            result[name] = value.get("value")
    return result


def test_carried_across_suspend(worker):
    """A checkpoint written before suspending is visible to the successor."""
    targets = [workflow("test", "poller")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "poller")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()
        assert values(ex0) == {}
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=5)
        ex0.conn.suspend(ex0.execution_id)

        ex1 = ctx.executor.next_execute()
        assert ex1.execution_id != ex0.execution_id
        assert values(ex1) == {"cursor": 5}

        ex1.conn.complete(ex1.execution_id, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_carried_across_retry(worker):
    """A failing attempt's checkpoint is visible to its retry."""
    targets = [
        workflow(
            "test",
            "flaky",
            retries={"limit": 1, "backoff_min_ms": 0, "backoff_max_ms": 0},
        )
    ]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "flaky")

        ex0 = ctx.executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, progress=17)
        ex0.conn.fail(ex0.execution_id, "RuntimeError", "transient failure")

        ex1 = ctx.executor.next_execute()
        assert values(ex1) == {"progress": 17}

        ex1.conn.complete(ex1.execution_id, value="recovered")
        assert ctx.result(resp["runId"])["value"]["data"] == "recovered"


def test_carried_across_recurrence(worker):
    """Each recurrence sees what the previous iteration wrote."""
    targets = [workflow("test", "ticker", recurrent=True)]

    with worker(targets) as ctx:
        ctx.submit("test", "ticker")

        seen = []
        for i in range(3):
            ex = ctx.executor.next_execute()
            seen.append(values(ex).get("count"))
            ex.conn.checkpoint_set(ex.execution_id, count=i)
            # Recurrent targets recur while they return None.
            ex.conn.complete(ex.execution_id, value=None)

        assert seen == [None, 0, 1]


def test_carried_across_manual_rerun(worker):
    """Re-running a step from the CLI preserves its checkpoint."""
    targets = [workflow("test", "main")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "main")

        ex0 = ctx.executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, cursor="a")
        ex0.conn.complete(ex0.execution_id, value="first")
        ctx.result(resp["runId"])

        ctx.rerun(resp["stepId"])

        ex1 = ctx.executor.next_execute()
        assert values(ex1) == {"cursor": "a"}
        ex1.conn.complete(ex1.execution_id, value="second")


def test_scoped_to_step(worker):
    """A checkpoint is not visible to another step, or to another run."""
    targets = [workflow("test", "main"), workflow("test", "other")]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=1)
        ex0.conn.complete(ex0.execution_id, value="done")
        ctx.result(resp["runId"])

        # A different target — different step, different run.
        resp2 = ctx.submit("test", "other")
        ex1 = ctx.executor.next_execute()
        assert values(ex1) == {}
        ex1.conn.complete(ex1.execution_id, value="done")
        ctx.result(resp2["runId"])

        # The same target again — still a new run, so still a new step.
        resp3 = ctx.submit("test", "main")
        ex2 = ctx.executor.next_execute()
        assert values(ex2) == {}
        ex2.conn.complete(ex2.execution_id, value="done")
        ctx.result(resp3["runId"])


def test_names_carried_forward_independently(worker):
    """A name the latest attempt didn't write is still carried forward."""
    targets = [workflow("test", "poller")]

    with worker(targets) as ctx:
        ctx.submit("test", "poller")

        ex0 = ctx.executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=1, batch="a")
        ex0.conn.suspend(ex0.execution_id)

        ex1 = ctx.executor.next_execute()
        assert values(ex1) == {"cursor": 1, "batch": "a"}
        ex1.conn.checkpoint_set(ex1.execution_id, cursor=2)
        ex1.conn.suspend(ex1.execution_id)

        ex2 = ctx.executor.next_execute()
        assert values(ex2) == {"cursor": 2, "batch": "a"}
        ex2.conn.complete(ex2.execution_id, value="done")


def test_reset(worker):
    """A reset name is gone for the next attempt, not restored from an older one."""
    targets = [workflow("test", "poller")]

    with worker(targets) as ctx:
        ctx.submit("test", "poller")

        ex0 = ctx.executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=1, batch="a")
        ex0.conn.suspend(ex0.execution_id)

        ex1 = ctx.executor.next_execute()
        ex1.conn.checkpoint_reset(ex1.execution_id, "cursor")
        ex1.conn.suspend(ex1.execution_id)

        ex2 = ctx.executor.next_execute()
        assert values(ex2) == {"batch": "a"}

        # And it stays gone through an attempt that touches nothing.
        ex2.conn.suspend(ex2.execution_id)
        ex3 = ctx.executor.next_execute()
        assert values(ex3) == {"batch": "a"}
        ex3.conn.complete(ex3.execution_id, value="done")


def test_reset_every_name(worker):
    """Resetting everything leaves an empty checkpoint, not the pre-reset state.

    An attempt whose entire row-set is tombstones still has to count as having
    touched checkpoints — otherwise the read falls back to the previous attempt
    and resurrects exactly what was just cleared.
    """
    targets = [workflow("test", "poller")]

    with worker(targets) as ctx:
        ctx.submit("test", "poller")

        ex0 = ctx.executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=1, batch="a")
        ex0.conn.suspend(ex0.execution_id)

        ex1 = ctx.executor.next_execute()
        assert values(ex1) == {"cursor": 1, "batch": "a"}
        ex1.conn.checkpoint_reset(ex1.execution_id, "cursor", "batch")
        ex1.conn.suspend(ex1.execution_id)

        ex2 = ctx.executor.next_execute()
        assert values(ex2) == {}
        ex2.conn.complete(ex2.execution_id, value="done")


def test_null_value_is_not_a_reset(worker):
    """A checkpoint set to null is stored; only reset clears it."""
    targets = [workflow("test", "poller")]

    with worker(targets) as ctx:
        ctx.submit("test", "poller")

        ex0 = ctx.executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=None)
        ex0.conn.suspend(ex0.execution_id)

        ex1 = ctx.executor.next_execute()
        assert "cursor" in (ex1.checkpoints or {})
        assert values(ex1) == {"cursor": None}

        ex1.conn.checkpoint_reset(ex1.execution_id, "cursor")
        ex1.conn.suspend(ex1.execution_id)

        ex2 = ctx.executor.next_execute()
        assert "cursor" not in (ex2.checkpoints or {})
        ex2.conn.complete(ex2.execution_id, value="done")


def test_repeated_writes_coalesce(worker):
    """Rapid writes are throttled, but the final value always lands."""
    targets = [workflow("test", "poller")]

    with worker(targets) as ctx:
        ctx.submit("test", "poller")

        ex0 = ctx.executor.next_execute()
        for i in range(50):
            ex0.conn.checkpoint_set(ex0.execution_id, cursor=i)
        ex0.conn.suspend(ex0.execution_id)

        ex1 = ctx.executor.next_execute()
        assert values(ex1) == {"cursor": 49}
        ex1.conn.complete(ex1.execution_id, value="done")


def test_flush_is_acknowledged(worker):
    """flush returns only once buffered state has reached the server."""
    targets = [workflow("test", "poller")]

    with worker(targets) as ctx:
        ctx.submit("test", "poller")

        ex0 = ctx.executor.next_execute()
        # Two writes inside one throttle window: the second is buffered, and
        # the flush is what gets it to the server.
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=1)
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=2)
        resp = ex0.conn.flush(ex0.execution_id)
        assert "error" not in resp

        ex0.conn.suspend(ex0.execution_id)
        ex1 = ctx.executor.next_execute()
        assert values(ex1) == {"cursor": 2}
        ex1.conn.complete(ex1.execution_id, value="done")


def test_buffered_write_survives_a_crash(worker):
    """A write still in the throttle buffer lands when the adapter dies.

    The first write of an execution goes out on the leading edge, so it's the
    second — coalesced into the open window — that has to survive a process
    that never reports anything. Covers the crash path end to end; it doesn't
    pin down *which* flush delivers it, since the retry is not in practice
    assigned early enough to distinguish them.
    """
    targets = [
        workflow(
            "test",
            "poller",
            retries={"limit": 1, "backoff_min_ms": 0, "backoff_max_ms": 0},
        )
    ]

    with worker(targets) as ctx:
        ctx.submit("test", "poller")

        ex0 = ctx.executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=1)
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=2)
        # Die without reporting a result or an error.
        ex0.conn.close()

        ex1 = ctx.executor.next_execute()
        assert values(ex1) == {"cursor": 2}
        ex1.conn.complete(ex1.execution_id, value="done")


def test_buffered_write_survives_a_timeout(worker):
    """A write still in the throttle buffer lands when the execution times out.

    Same shape as the crash case: the timeout is reported by the worker rather
    than the adapter, so it needs its own flush ahead of the report.
    """
    targets = [
        workflow(
            "test",
            "poller",
            timeout=300,
            retries={"limit": 1, "backoff_min_ms": 0, "backoff_max_ms": 0},
        )
    ]

    with worker(targets) as ctx:
        ctx.submit("test", "poller")

        ex0 = ctx.executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=1)
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=2)
        # Never report anything — let the timeout fire. It's shorter than the
        # throttle window, so the second write is still buffered when it does.

        ex1 = ctx.executor.next_execute()
        assert values(ex1) == {"cursor": 2}
        ex1.conn.complete(ex1.execution_id, value="done")


def test_write_after_completion_ignored(worker):
    """A write from a superseded attempt can't affect the current one.

    The old connection may already be torn down by the abort that follows a
    suspension, in which case nothing is sent at all — either way the
    successor must see the pre-suspend value.
    """
    targets = [workflow("test", "poller")]

    with worker(targets) as ctx:
        ctx.submit("test", "poller")

        ex0 = ctx.executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=1)
        ex0.conn.suspend(ex0.execution_id)

        try:
            ex0.conn.checkpoint_set(ex0.execution_id, cursor=999)
        except (ConnectionError, OSError):
            pass

        ex1 = ctx.executor.next_execute()
        assert values(ex1) == {"cursor": 1}
        ex1.conn.complete(ex1.execution_id, value="done")


def test_blob_backed_value(worker):
    """A checkpoint too large to inline round-trips via the blob store."""
    targets = [workflow("test", "poller")]
    payload = "x" * 5000

    with worker(targets) as ctx:
        ctx.submit("test", "poller")

        ex0 = ctx.executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, blob=payload)
        ex0.conn.suspend(ex0.execution_id)

        ex1 = ctx.executor.next_execute()
        assert values(ex1) == {"blob": payload}
        ex1.conn.complete(ex1.execution_id, value="done")


def test_workspace_inheritance(worker):
    """A derived workspace reads the base's state but writes only its own."""
    targets = [workflow("test", "poller")]

    with worker(targets, workspace="base") as ctx_base:
        resp = ctx_base.submit("test", "poller")
        run_id = resp["runId"]
        step_id = resp["stepId"]

        ex0 = ctx_base.executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, cursor="base-1")
        ex0.conn.complete(ex0.execution_id, value="done")
        ctx_base.result(run_id)

        saved_host = ctx_base.host

    cli.workspaces_create("derived", base="base", host=saved_host, workspace="derived")

    with worker(targets, workspace="derived") as ctx_derived:
        # Inherited from the base, since the derived workspace has no state.
        ctx_derived.rerun(step_id)
        ex1 = ctx_derived.executor.next_execute()
        assert values(ex1) == {"cursor": "base-1"}

        ex1.conn.checkpoint_set(ex1.execution_id, cursor="derived-1")
        ex1.conn.complete(ex1.execution_id, value="done")

        # Now the derived workspace has its own, which wins outright.
        ctx_derived.rerun(step_id)
        ex2 = ctx_derived.executor.next_execute()
        assert values(ex2) == {"cursor": "derived-1"}
        ex2.conn.complete(ex2.execution_id, value="done")

    # The base is untouched by any of that.
    with worker(targets, workspace="base") as ctx_base:
        ctx_base.rerun(step_id)
        ex3 = ctx_base.executor.next_execute()
        assert values(ex3) == {"cursor": "base-1"}
        ex3.conn.complete(ex3.execution_id, value="done")


def test_survives_epoch_rotation(isolated_server, tmp_path):
    """Rotation carries the effective checkpoint into the new epoch."""
    server, host, project_id = isolated_server
    targets = [workflow("test", "poller")]

    with managed_worker(targets, host, tmp_path) as executor:
        resp = cli.submit("test/poller", host=host)
        run_id = resp["runId"]
        step_id = resp["stepId"]

        ex0 = executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=7, batch="a")
        ex0.conn.suspend(ex0.execution_id)

        ex1 = executor.next_execute()
        assert values(ex1) == {"cursor": 7, "batch": "a"}
        # Reset one name, so the snapshot carried over is the post-reset state
        # rather than everything ever written.
        ex1.conn.checkpoint_reset(ex1.execution_id, "batch")
        ex1.conn.complete(ex1.execution_id, value="done")
        poll_result(run_id, host)

        api_post(server.port, project_id, "rotate_epoch")

        cli.runs_rerun(step_id, host=host)
        ex2 = executor.next_execute()
        assert values(ex2) == {"cursor": 7}
        ex2.conn.complete(ex2.execution_id, value="after-rotation")

        result = poll_result(run_id, host)
        assert result["value"]["data"] == "after-rotation"


def test_reset_in_derived_workspace_survives_epoch_rotation(isolated_server, tmp_path):
    """A reset in a derived workspace keeps masking the base after rotation.

    The tombstone is the only thing keeping the derived workspace's row-set
    non-empty, and an empty row-set falls back to the base — so discarding it
    at rotation would resurrect the value the reset removed.
    """
    server, host, project_id = isolated_server
    targets = [workflow("test", "poller")]

    with managed_worker(targets, host, tmp_path / "base") as executor:
        resp = cli.submit("test/poller", host=host)
        run_id = resp["runId"]
        step_id = resp["stepId"]

        ex0 = executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=7)
        ex0.conn.complete(ex0.execution_id, value="done")
        poll_result(run_id, host)

    cli.workspaces_create("derived", base="default", host=host, workspace="derived")

    with managed_worker(
        targets, host, tmp_path / "derived", workspace="derived"
    ) as executor:
        cli.runs_rerun(step_id, host=host, workspace="derived")
        ex1 = executor.next_execute()
        assert values(ex1) == {"cursor": 7}

        # Reset the only name it holds, so its entire row-set is tombstones.
        ex1.conn.checkpoint_reset(ex1.execution_id, "cursor")
        ex1.conn.complete(ex1.execution_id, value="done")
        poll_result(run_id, host, workspace="derived")

        api_post(server.port, project_id, "rotate_epoch")

        cli.runs_rerun(step_id, host=host, workspace="derived")
        ex2 = executor.next_execute()
        assert values(ex2) == {}
        ex2.conn.complete(ex2.execution_id, value="done")
        poll_result(run_id, host, workspace="derived")

    # The base's own state is untouched by any of that.
    with managed_worker(targets, host, tmp_path / "base-again") as executor:
        cli.runs_rerun(step_id, host=host)
        ex3 = executor.next_execute()
        assert values(ex3) == {"cursor": 7}
        ex3.conn.complete(ex3.execution_id, value="done")


def test_exposed_in_run_topic(worker):
    """The run topic carries each execution's checkpoint, for Studio."""
    targets = [workflow("test", "poller")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "poller")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=5, batch="a")
        ex0.conn.suspend(ex0.execution_id)

        ex1 = ctx.executor.next_execute()
        ex1.conn.checkpoint_reset(ex1.execution_id, "batch")
        ex1.conn.complete(ex1.execution_id, value="done")
        ctx.result(run_id)

        executions = next(iter(ctx.inspect(run_id)["steps"].values()))["executions"]

        # Each attempt is reported as a transition: what it was handed when it
        # started, and what it ended up holding. The first attempt started from
        # nothing; the second inherited both names and reset one of them, so the
        # reset shows up as an absence on the "after" side only.
        assert executions["1"]["checkpoints"]["before"] == {}
        assert executions["1"]["checkpoints"]["after"]["cursor"]["data"] == 5
        assert executions["1"]["checkpoints"]["after"]["batch"]["data"] == "a"

        assert executions["2"]["checkpoints"]["before"]["cursor"]["data"] == 5
        assert executions["2"]["checkpoints"]["before"]["batch"]["data"] == "a"
        assert executions["2"]["checkpoints"]["after"]["cursor"]["data"] == 5
        assert "batch" not in executions["2"]["checkpoints"]["after"]


def test_run_topic_reports_untouched_state(worker):
    """An attempt that didn't write reports the state it left untouched."""
    targets = [workflow("test", "poller")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "poller")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=5)
        ex0.conn.suspend(ex0.execution_id)

        ex1 = ctx.executor.next_execute()
        ex1.conn.complete(ex1.execution_id, value="done")
        ctx.result(run_id)

        executions = next(iter(ctx.inspect(run_id)["steps"].values()))["executions"]

        # No rows of its own — but it still held the checkpoint, rather than
        # nothing.
        checkpoints = executions["2"]["checkpoints"]
        assert checkpoints["before"]["cursor"]["data"] == 5
        assert checkpoints["after"]["cursor"]["data"] == 5


def test_no_checkpoints_sent_when_empty(worker):
    """An execution with no checkpoint state gets no payload at all."""
    targets = [workflow("test", "main")]

    with worker(targets) as ctx:
        ctx.submit("test", "main")
        ex = ctx.executor.next_execute()
        assert ex.checkpoints == {}
        ex.conn.complete(ex.execution_id, value="done")


@pytest.mark.parametrize("value", [0, "", False, []])
def test_falsy_values_round_trip(worker, value):
    """Falsy checkpoint values survive — absence is distinct from emptiness."""
    targets = [workflow("test", "poller")]

    with worker(targets) as ctx:
        ctx.submit("test", "poller")

        ex0 = ctx.executor.next_execute()
        ex0.conn.checkpoint_set(ex0.execution_id, cursor=value)
        ex0.conn.suspend(ex0.execution_id)

        ex1 = ctx.executor.next_execute()
        assert values(ex1) == {"cursor": value}
        ex1.conn.complete(ex1.execution_id, value="done")
