"""Tests for epoch database rotation and cross-epoch behavior."""

import support.cli as cli
from support.helpers import api_post, managed_worker, poll_result
from support.manifest import task, workflow
from support.protocol import json_args


def _rotate_epoch(port, project_id):
    """Force an epoch rotation via the management API."""
    api_post(port, project_id, "rotate_epoch")


def test_epoch_rotation_creates_new_epoch(isolated_server, tmp_path):
    """Rotation creates a new epoch file and server remains functional."""
    server, host, project_id = isolated_server
    targets = [workflow("test", "my_workflow")]

    with managed_worker(targets, host, tmp_path) as executor:
        # Submit and complete a workflow
        resp = cli.submit("test/my_workflow", host=host)
        run_id = resp["runId"]

        ex = executor.next_execute()
        assert ex.target == "my_workflow"
        ex.conn.complete(ex.execution_id, value=42)

        result = poll_result(run_id, host)
        assert result["type"] == "value"
        assert result["value"]["data"] == 42

        # Force rotation
        _rotate_epoch(server.port, project_id)

        # Server still works after rotation — submit another workflow
        resp2 = cli.submit("test/my_workflow", host=host)
        run_id2 = resp2["runId"]

        ex2 = executor.next_execute()
        assert ex2.target == "my_workflow"
        ex2.conn.complete(ex2.execution_id, value=99)

        result2 = poll_result(run_id2, host)
        assert result2["type"] == "value"
        assert result2["value"]["data"] == 99


def test_rerun_across_epoch_boundary(isolated_server, tmp_path):
    """Re-running a step whose data is in an old epoch works (copy-on-reference)."""
    server, host, project_id = isolated_server
    targets = [workflow("test", "my_workflow")]

    with managed_worker(targets, host, tmp_path) as executor:
        resp = cli.submit("test/my_workflow", host=host)
        run_id = resp["runId"]
        step_id = resp["stepId"]

        ex = executor.next_execute()
        assert ex.target == "my_workflow"
        ex.conn.complete(ex.execution_id, value=42)

        result = poll_result(run_id, host)
        assert result["type"] == "value"
        assert result["value"]["data"] == 42

        # Force rotation — original run is now in old epoch
        _rotate_epoch(server.port, project_id)

        # Re-run the step (this triggers copy-on-reference from old epoch)
        rerun_resp = cli.runs_rerun(step_id, host=host)
        assert rerun_resp["attempt"] == 2

        # Execute the re-run
        ex2 = executor.next_execute()
        assert ex2.target == "my_workflow"
        ex2.conn.complete(ex2.execution_id, value=99)

        result2 = poll_result(run_id, host)
        assert result2["type"] == "value"
        assert result2["value"]["data"] == 99


def test_cache_hit_across_epoch_boundary(isolated_server, tmp_path):
    """Cached task result from old epoch is found after rotation."""
    server, host, project_id = isolated_server
    targets = [
        workflow("test", "main"),
        task("test", "expensive", parameters=["x"]),
    ]

    with managed_worker(targets, host, tmp_path, concurrency=2) as executor:
            # First workflow: execute a cached task
            resp1 = cli.submit("test/main", host=host)
            run_id1 = resp1["runId"]

            ex0 = executor.next_execute()

            ref1 = ex0.conn.submit_task(
                ex0.execution_id,
                "test", "expensive",
                json_args(42),
                cache={"params": True},
            )

            ex1 = executor.next_execute()
            ex1.conn.complete(ex1.execution_id, value=84)

            assert ex0.conn.resolve(ex0.execution_id, ref1)["value"] == 84

            ex0.conn.complete(ex0.execution_id, value="done1")
            result1 = poll_result(run_id1, host)
            assert result1["type"] == "value"
            assert result1["value"]["data"] == "done1"

            # Force rotation — cached execution is now in old epoch
            _rotate_epoch(server.port, project_id)

            # Second workflow: submit same cached task — should hit cache.
            resp2 = cli.submit("test/main", host=host)
            run_id2 = resp2["runId"]

            ex = executor.next_execute(timeout=5)

            ref2 = ex.conn.submit_task(
                ex.execution_id,
                "test", "expensive",
                json_args(42),
                cache={"params": True},
            )

            # Should resolve immediately from cache (no new execution)
            assert ex.conn.resolve(ex.execution_id, ref2)["value"] == 84

            ex.conn.complete(ex.execution_id, value="done2")
            result2 = poll_result(run_id2, host)
            assert result2["type"] == "value"
            assert result2["value"]["data"] == "done2"


def test_parent_child_run_across_epoch_boundary(isolated_server, tmp_path):
    """Rerunning a child step copies its parent run from an old epoch (recursive copy)."""
    server, host, project_id = isolated_server
    targets = [
        workflow("test", "main"),
        workflow("test", "child"),
    ]

    with managed_worker(targets, host, tmp_path, concurrency=2) as executor:
            # Submit parent workflow
            resp = cli.submit("test/main", host=host)
            run_id = resp["runId"]

            ex0 = executor.next_execute()
            assert ex0.target == "main"

            # From parent, submit child workflow
            child_ref = ex0.conn.submit_workflow(ex0.execution_id, "test", "child", [])

            ex1 = executor.next_execute()
            assert ex1.target == "child"
            ex1.conn.complete(ex1.execution_id, value=42)

            # Parent resolves child reference
            resolved = ex0.conn.resolve(ex0.execution_id, child_ref)
            assert resolved["value"] == 42

            ex0.conn.complete(ex0.execution_id, value="done")
            result = poll_result(run_id, host)
            assert result["type"] == "value"
            assert result["value"]["data"] == "done"

            # child_ref is the execution ID string, e.g. "abc:1:1"
            parts = child_ref.rsplit(":", 2)
            child_step_id = f"{parts[0]}:{parts[1]}"  # "abc:1"

            # Rotate epoch — both parent and child runs now in old epoch
            _rotate_epoch(server.port, project_id)

            # Rerun the child step — triggers copy_run on child, which recursively copies parent
            rerun_resp = cli.runs_rerun(child_step_id, host=host)
            assert rerun_resp["attempt"] == 2

            # Execute the rerun — the fact that the execution was dispatched means
            # copy_run succeeded (including recursive parent copy)
            ex = executor.next_execute()
            assert ex.target == "child"
            ex.conn.complete(ex.execution_id, value=99)


def test_resolve_execution_reference_from_old_epoch(isolated_server, tmp_path):
    """Resolving an execution reference from an old epoch finds and copies the run."""
    server, host, project_id = isolated_server
    targets = [
        workflow("test", "main"),
        task("test", "producer", parameters=["x"]),
    ]

    with managed_worker(targets, host, tmp_path, concurrency=2) as executor:
            # First workflow: submit a task and capture the execution reference
            resp1 = cli.submit("test/main", host=host)
            run_id1 = resp1["runId"]

            ex0 = executor.next_execute()

            producer_ref = ex0.conn.submit_task(
                ex0.execution_id,
                "test", "producer",
                json_args(42),
            )

            ex1 = executor.next_execute()
            ex1.conn.complete(ex1.execution_id, value=84)

            assert ex0.conn.resolve(ex0.execution_id, producer_ref)["value"] == 84

            ex0.conn.complete(ex0.execution_id, value="done1")
            result1 = poll_result(run_id1, host)
            assert result1["type"] == "value"
            assert result1["value"]["data"] == "done1"

            # Rotate epoch — first run with task is now in old epoch
            _rotate_epoch(server.port, project_id)

            # Second workflow: resolve the OLD producer reference
            resp2 = cli.submit("test/main", host=host)
            run_id2 = resp2["runId"]

            ex = executor.next_execute()
            assert ex.target == "main"

            # Resolve the old execution reference — server must find it in archived epoch
            resolved = ex.conn.resolve(ex.execution_id, producer_ref)
            assert resolved["value"] == 84

            ex.conn.complete(ex.execution_id, value="done2")
            result2 = poll_result(run_id2, host)
            assert result2["type"] == "value"
            assert result2["value"]["data"] == "done2"


def test_multiple_rotations_cache_hit_from_oldest_epoch(isolated_server, tmp_path):
    """Cache lookup across 3 epochs finds the match in the oldest one via bloom filter."""
    server, host, project_id = isolated_server
    targets = [
        workflow("test", "main"),
        task("test", "expensive", parameters=["x"]),
    ]

    with managed_worker(targets, host, tmp_path, concurrency=2) as executor:
            # --- Epoch 1: cached task with args(42) → value 84 ---
            resp1 = cli.submit("test/main", host=host)
            run_id1 = resp1["runId"]

            ex0 = executor.next_execute()
            ref1 = ex0.conn.submit_task(
                ex0.execution_id,
                "test", "expensive",
                json_args(42),
                cache={"params": True},
            )

            ex1 = executor.next_execute()
            ex1.conn.complete(ex1.execution_id, value=84)

            assert ex0.conn.resolve(ex0.execution_id, ref1)["value"] == 84
            ex0.conn.complete(ex0.execution_id, value="done1")
            result1 = poll_result(run_id1, host)
            assert result1["value"]["data"] == "done1"

            # Rotate → epoch 2
            _rotate_epoch(server.port, project_id)

            # --- Epoch 2: cached task with args(99) → value 198 (different args) ---
            resp2 = cli.submit("test/main", host=host)
            run_id2 = resp2["runId"]

            ex_wf2 = executor.next_execute()

            ref2 = ex_wf2.conn.submit_task(
                ex_wf2.execution_id,
                "test", "expensive",
                json_args(99),
                cache={"params": True},
            )

            ex_task2 = executor.next_execute()
            ex_task2.conn.complete(ex_task2.execution_id, value=198)

            assert ex_wf2.conn.resolve(ex_wf2.execution_id, ref2)["value"] == 198
            ex_wf2.conn.complete(ex_wf2.execution_id, value="done2")
            result2 = poll_result(run_id2, host)
            assert result2["value"]["data"] == "done2"

            # Rotate → epoch 3
            _rotate_epoch(server.port, project_id)

            # --- Epoch 3: cached task with args(42) again — should hit epoch 1's cache ---
            resp3 = cli.submit("test/main", host=host)
            run_id3 = resp3["runId"]

            ex_wf3 = executor.next_execute()

            ref3 = ex_wf3.conn.submit_task(
                ex_wf3.execution_id,
                "test", "expensive",
                json_args(42),
                cache={"params": True},
            )

            # Should resolve from cache (epoch 1) — no new task execution
            resolved3 = ex_wf3.conn.resolve(ex_wf3.execution_id, ref3)
            assert resolved3["value"] == 84  # from epoch 1, NOT 198

            ex_wf3.conn.complete(ex_wf3.execution_id, value="done3")
            result3 = poll_result(run_id3, host)
            assert result3["value"]["data"] == "done3"


def test_asset_reference_across_epoch_boundary(isolated_server, tmp_path):
    """Assets are preserved when a run is copied from an old epoch via rerun."""
    server, host, project_id = isolated_server
    targets = [
        workflow("test", "main"),
        task("test", "producer"),
        task("test", "consumer"),
    ]

    with managed_worker(targets, host, tmp_path, concurrency=2) as executor:
            # Submit workflow
            resp = cli.submit("test/main", host=host)
            run_id = resp["runId"]

            ex0 = executor.next_execute()

            # Submit producer task
            ref_prod = ex0.conn.submit_task(ex0.execution_id, "test", "producer", [])
            ex1 = executor.next_execute()

            # Create a temp file and persist as asset
            asset_file = str(tmp_path / "epoch_asset.txt")
            with open(asset_file, "w") as f:
                f.write("asset data for epoch test")

            asset_result = ex1.conn.persist_asset(
                ex1.execution_id,
                [asset_file],
                metadata={"name": "epoch_asset"},
            )
            assert "asset_id" in asset_result
            asset_id = asset_result["asset_id"]

            ex1.conn.complete(ex1.execution_id, value="produced")
            assert ex0.conn.resolve(ex0.execution_id, ref_prod)["value"] == "produced"

            # Submit consumer task
            ref_cons = ex0.conn.submit_task(ex0.execution_id, "test", "consumer", [])
            ex2 = executor.next_execute()

            # Consumer retrieves the asset — capture original entries
            original_asset = ex2.conn.get_asset(ex2.execution_id, asset_id)
            assert "entries" in original_asset
            original_entries = original_asset["entries"]
            assert len(original_entries) > 0

            # Capture original blob key and size
            original_entry = list(original_entries.values())[0]
            original_blob_key = original_entry[0]
            original_size = original_entry[1]

            ex2.conn.complete(ex2.execution_id, value="consumed")
            assert ex0.conn.resolve(ex0.execution_id, ref_cons)["value"] == "consumed"

            ex0.conn.complete(ex0.execution_id, value="done")
            result = poll_result(run_id, host)
            assert result["type"] == "value"
            assert result["value"]["data"] == "done"

            # ref_cons is the execution ID string, e.g. "abc:3:1"
            parts = ref_cons.rsplit(":", 2)
            cons_step_id = f"{parts[0]}:{parts[1]}"

            # Rotate epoch — run with asset is now in old epoch
            _rotate_epoch(server.port, project_id)

            # Rerun the consumer step — copy_run copies run + assets to active epoch
            rerun_resp = cli.runs_rerun(cons_step_id, host=host)
            assert rerun_resp["attempt"] == 2

            # Execute the consumer rerun
            ex = executor.next_execute()
            assert ex.target == "consumer"

            # The old asset reference should still resolve after copy
            rerun_asset = ex.conn.get_asset(ex.execution_id, asset_id)
            assert "entries" in rerun_asset
            rerun_entries = rerun_asset["entries"]
            assert len(rerun_entries) > 0

            # Verify the asset data matches original
            rerun_entry = list(rerun_entries.values())[0]
            assert rerun_entry[0] == original_blob_key
            assert rerun_entry[1] == original_size

            ex.conn.complete(ex.execution_id, value="consumed again")


def test_idempotency_across_epoch_boundary(isolated_server, tmp_path):
    """Idempotency key is found in archived epoch after rotation."""
    server, host, project_id = isolated_server
    targets = [workflow("test", "my_workflow")]

    with managed_worker(targets, host, tmp_path) as executor:
            # Submit with idempotency key and complete
            resp1 = cli.submit(
                "test/my_workflow", idempotency_key="epoch-key", host=host
            )
            run_id1 = resp1["runId"]

            ex = executor.next_execute()
            assert ex.target == "my_workflow"
            ex.conn.complete(ex.execution_id, value=42)

            result = poll_result(run_id1, host)
            assert result["type"] == "value"

            # Force epoch rotation
            _rotate_epoch(server.port, project_id)

            # Submit again with the same idempotency key — should return existing run
            resp2 = cli.submit(
                "test/my_workflow", idempotency_key="epoch-key", host=host
            )
            run_id2 = resp2["runId"]

            assert run_id1 == run_id2
