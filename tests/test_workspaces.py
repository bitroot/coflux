"""Tests for workspace management: pause/resume, validation, archive, discover."""

import subprocess

import pytest

import support.cli as cli
from support.manifest import task, workflow
from support.protocol import execution_result


def test_workspace_pause_resume(worker):
    """Paused workspace stops dispatch; resuming continues dispatch."""
    targets = [workflow("test", "main")]

    def handler(eid, module, target, args):
        return execution_result(eid, value="done")

    with worker(targets, handler) as ctx:
        # Pause the workspace before submitting
        ctx.pause()

        resp = ctx.submit("test", "main")

        # Execution should NOT be dispatched while paused
        with pytest.raises(TimeoutError):
            ctx.executor.next_execute(timeout=1)

        # Resume the workspace
        ctx.resume()

        # Now the execution should be dispatched
        ex = ctx.executor.next_execute(timeout=5)
        assert ex.target == "main"
        ex.conn.complete(ex.execution_id, value="resumed")

        result = ctx.result(resp["runId"])
        assert result["value"]["data"] == "resumed"


def test_duplicate_workspace_name(worker):
    """Creating a workspace with a duplicate name fails."""
    targets = [workflow("test", "main")]

    with worker(targets) as ctx:
        # The "default" workspace is already created by the fixture.
        # Trying to create another with the same name should fail.
        with pytest.raises(subprocess.CalledProcessError):
            cli.workspaces_create("default", host=ctx.host, workspace=ctx.workspace)


def test_archive_module(worker):
    """Archiving a module removes it from the manifest listing."""
    targets = [workflow("test", "main")]

    def handler(eid, module, target, args):
        return execution_result(eid, value="done")

    with worker(targets, handler) as ctx:
        # Module should appear in manifests before archiving
        manifests = ctx.inspect_manifests()
        assert "test" in manifests
        assert "main" in manifests["test"]

        # Archive the "test" module
        ctx.archive_module("test")

        # Module should no longer appear in manifests
        manifests = ctx.inspect_manifests()
        assert "test" not in manifests


def test_rerun_step_in_derived_workspace(worker):
    """A step from a base workspace run can be re-run in a derived workspace."""
    targets = [workflow("test", "main")]

    # Step 1: Run workflow in "base" workspace
    with worker(targets, workspace="base") as ctx_base:
        resp = ctx_base.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx_base.executor.next_execute()
        ex0.conn.complete(ex0.execution_id, value="original")

        result = ctx_base.result(run_id)
        assert result["value"]["data"] == "original"

        # Get the step ID
        data = ctx_base.inspect(run_id)
        step_ids = list(data["steps"].keys())
        assert len(step_ids) == 1
        step_id = step_ids[0]

        saved_host = ctx_base.host

    # Step 2: Create "derived" workspace inheriting from "base"
    cli.workspaces_create("derived", base="base", host=saved_host, workspace="derived")

    # Step 3: Start worker in "derived", re-run the step there
    with worker(targets, workspace="derived") as ctx_derived:
        rerun_resp = ctx_derived.rerun(step_id)
        assert rerun_resp["attempt"] > 1

        ex0 = ctx_derived.executor.next_execute()
        assert ex0.target == "main"
        ex0.conn.complete(ex0.execution_id, value="rerun-in-derived")

        result = ctx_derived.result(run_id)
        assert result["value"]["data"] == "rerun-in-derived"


def test_discover_targets(worker):
    """Discover workflows from local code without registering.

    Only workflows are shown (matching what 'manifests register' sends).
    """
    targets = [
        workflow("test", "my_workflow", parameters=["x"]),
        task("test", "my_task", parameters=["a", "b"]),
    ]

    with worker(targets) as ctx:
        result = ctx.discover_modules("test")
        discovered = result["targets"]

        # Only workflows should appear (tasks are filtered out)
        names = {t["name"] for t in discovered}
        assert "my_workflow" in names
        assert "my_task" not in names

        assert len(discovered) == 1
        assert discovered[0]["type"] == "workflow"
        assert [p["name"] for p in discovered[0]["parameters"]] == ["x"]
