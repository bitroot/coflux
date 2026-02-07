"""Tests for workspace management: pause/resume, validation, archive, discover."""

import subprocess

import pytest

import support.cli as cli
from support.manifest import task, workflow
from support.protocol import execution_result


def test_workspace_pause_resume(worker):
    """Paused workspace stops dispatch; resuming continues dispatch."""
    targets = [workflow("test.main")]

    def handler(eid, target, args):
        return execution_result(eid, value="done")

    with worker(targets, handler) as ctx:
        # Pause the workspace before submitting
        ctx.pause()

        resp = ctx.submit("test.main")
        conn0 = ctx.executor.connections[0]

        # Execution should NOT be dispatched while paused
        with pytest.raises(TimeoutError):
            conn0.recv_execute(timeout=1)

        # Resume the workspace
        ctx.resume()

        # Now the execution should be dispatched
        eid, target, _ = conn0.recv_execute(timeout=5)
        assert target == "test.main"
        conn0.complete(eid, value="resumed")

        result = ctx.result(resp["runId"])
        assert result["value"]["data"] == "resumed"


def test_duplicate_workspace_name(worker):
    """Creating a workspace with a duplicate name fails."""
    targets = [workflow("test.main")]

    with worker(targets) as ctx:
        # The "default" workspace is already created by the fixture.
        # Trying to create another with the same name should fail.
        with pytest.raises(subprocess.CalledProcessError):
            cli.workspaces_create("default", env=ctx.env)


def test_archive_module(worker):
    """Archiving a module removes it from the manifest listing."""
    targets = [workflow("test.main")]

    def handler(eid, target, args):
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
    targets = [workflow("test.main")]

    # Step 1: Run workflow in "base" workspace
    with worker(targets, workspace="base") as ctx_base:
        resp = ctx_base.submit("test.main")
        run_id = resp["runId"]
        conn0 = ctx_base.executor.connections[0]

        eid, _, _ = conn0.recv_execute()
        conn0.complete(eid, value="original")

        result = ctx_base.result(run_id)
        assert result["value"]["data"] == "original"

        # Get the step ID
        data = ctx_base.inspect(run_id)
        step_ids = list(data["steps"].keys())
        assert len(step_ids) == 1
        step_id = step_ids[0]

        saved_env = ctx_base.env.copy()

    # Step 2: Create "derived" workspace inheriting from "base"
    saved_env["COFLUX_WORKSPACE"] = "derived"
    cli.workspaces_create("derived", base="base", env=saved_env)

    # Step 3: Start worker in "derived", re-run the step there
    with worker(
        targets, workspace="derived", create_workspace=False
    ) as ctx_derived:
        rerun_resp = ctx_derived.rerun(step_id)
        assert rerun_resp["attempt"] > 1

        conn0 = ctx_derived.executor.connections[0]
        eid, target, _ = conn0.recv_execute()
        assert target == "test.main"
        conn0.complete(eid, value="rerun-in-derived")

        result = ctx_derived.result(run_id)
        assert result["value"]["data"] == "rerun-in-derived"


def test_discover_targets(worker):
    """Discover workflows from local code without registering.

    Only workflows are shown (matching what 'manifests register' sends).
    """
    targets = [
        workflow("test.my_workflow", parameters=["x"]),
        task("test.my_task", parameters=["a", "b"]),
    ]

    with worker(targets) as ctx:
        result = ctx.discover_modules("test")
        discovered = result["targets"]

        # Only workflows should appear (tasks are filtered out)
        names = {t["name"] for t in discovered}
        assert "test.my_workflow" in names
        assert "test.my_task" not in names

        assert len(discovered) == 1
        assert discovered[0]["type"] == "workflow"
        assert [p["name"] for p in discovered[0]["parameters"]] == ["x"]
