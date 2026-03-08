"""Tests for epoch database rotation and cross-epoch behavior."""

import urllib.request
import uuid

import support.cli as cli
from support.helpers import managed_worker, poll_result
from support.manifest import task, workflow
from support.protocol import json_args
from support.server import ManagedServer


def _rotate_epoch(port, project_id):
    """Force an epoch rotation via the management API."""
    url = f"http://{project_id}.localhost:{port}/api/rotate_epoch"
    req = urllib.request.Request(
        url,
        method="POST",
        data=b"{}",
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=10)


def test_epoch_rotation_creates_new_epoch(tmp_path):
    """Rotation creates a new epoch file and server remains functional."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [workflow("test.my_workflow")]

    server = ManagedServer(str(tmp_path / "data"))
    server.start()

    try:
        host = f"{project_id}.localhost:{server.port}"
        cli.workspaces_create("default", host=host)

        with managed_worker(targets, host, tmp_path) as executor:
            # Submit and complete a workflow
            resp = cli.submit("test.my_workflow", host=host)
            run_id = resp["runId"]

            conn, eid, target, _ = executor.next_execute()
            assert target == "test.my_workflow"
            conn.complete(eid, value=42)

            result = poll_result(run_id, host)
            assert result["type"] == "value"
            assert result["value"]["data"] == 42

            # Force rotation
            _rotate_epoch(server.port, project_id)

            # Server still works after rotation — submit another workflow
            resp2 = cli.submit("test.my_workflow", host=host)
            run_id2 = resp2["runId"]

            conn2, eid2, target2, _ = executor.next_execute()
            assert target2 == "test.my_workflow"
            conn2.complete(eid2, value=99)

            result2 = poll_result(run_id2, host)
            assert result2["type"] == "value"
            assert result2["value"]["data"] == 99
    finally:
        server.stop()


def test_rerun_across_epoch_boundary(tmp_path):
    """Re-running a step whose data is in an old epoch works (copy-on-reference)."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [workflow("test.my_workflow")]

    server = ManagedServer(str(tmp_path / "data"))
    server.start()

    try:
        host = f"{project_id}.localhost:{server.port}"
        cli.workspaces_create("default", host=host)

        with managed_worker(targets, host, tmp_path) as executor:
            resp = cli.submit("test.my_workflow", host=host)
            run_id = resp["runId"]
            step_id = resp["stepId"]

            conn, eid, target, _ = executor.next_execute()
            assert target == "test.my_workflow"
            conn.complete(eid, value=42)

            result = poll_result(run_id, host)
            assert result["type"] == "value"
            assert result["value"]["data"] == 42

            # Force rotation — original run is now in old epoch
            _rotate_epoch(server.port, project_id)

            # Re-run the step (this triggers copy-on-reference from old epoch)
            rerun_resp = cli.runs_rerun(step_id, host=host)
            assert rerun_resp["attempt"] == 2

            # Execute the re-run
            conn2, eid2, target2, _ = executor.next_execute()
            assert target2 == "test.my_workflow"
            conn2.complete(eid2, value=99)

            result2 = poll_result(run_id, host)
            assert result2["type"] == "value"
            assert result2["value"]["data"] == 99
    finally:
        server.stop()


def test_cache_hit_across_epoch_boundary(tmp_path):
    """Cached task result from old epoch is found after rotation."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [
        workflow("test.main"),
        task("test.expensive", parameters=["x"]),
    ]

    server = ManagedServer(str(tmp_path / "data"))
    server.start()

    try:
        host = f"{project_id}.localhost:{server.port}"
        cli.workspaces_create("default", host=host)

        with managed_worker(targets, host, tmp_path, concurrency=2) as executor:
            # First workflow: execute a cached task
            resp1 = cli.submit("test.main", host=host)
            run_id1 = resp1["runId"]

            conn0, wf_eid1, _, _ = executor.next_execute()

            ref1 = conn0.submit_task(
                wf_eid1,
                "test.expensive",
                json_args(42),
                cache={"params": True},
            )

            conn1, task_eid, _, _ = executor.next_execute()
            conn1.complete(task_eid, value=84)

            assert conn0.resolve(wf_eid1, ref1)["value"] == 84

            conn0.complete(wf_eid1, value="done1")
            result1 = poll_result(run_id1, host)
            assert result1["type"] == "value"
            assert result1["value"]["data"] == "done1"

            # Force rotation — cached execution is now in old epoch
            _rotate_epoch(server.port, project_id)

            # Second workflow: submit same cached task — should hit cache.
            resp2 = cli.submit("test.main", host=host)
            run_id2 = resp2["runId"]

            wf_conn, wf_eid2, _, _ = executor.next_execute(timeout=5)

            ref2 = wf_conn.submit_task(
                wf_eid2,
                "test.expensive",
                json_args(42),
                cache={"params": True},
            )

            # Should resolve immediately from cache (no new execution)
            assert wf_conn.resolve(wf_eid2, ref2)["value"] == 84

            wf_conn.complete(wf_eid2, value="done2")
            result2 = poll_result(run_id2, host)
            assert result2["type"] == "value"
            assert result2["value"]["data"] == "done2"
    finally:
        server.stop()


def test_parent_child_run_across_epoch_boundary(tmp_path):
    """Rerunning a child step copies its parent run from an old epoch (recursive copy)."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [
        workflow("test.main"),
        workflow("test.child"),
    ]

    server = ManagedServer(str(tmp_path / "data"))
    server.start()

    try:
        host = f"{project_id}.localhost:{server.port}"
        cli.workspaces_create("default", host=host)

        with managed_worker(targets, host, tmp_path, concurrency=2) as executor:
            # Submit parent workflow
            resp = cli.submit("test.main", host=host)
            run_id = resp["runId"]

            conn0, wf_eid, target, _ = executor.next_execute()
            assert target == "test.main"

            # From parent, submit child workflow
            child_ref = conn0.submit_workflow(wf_eid, "test.child", [])

            conn1, child_eid, target, _ = executor.next_execute()
            assert target == "test.child"
            conn1.complete(child_eid, value=42)

            # Parent resolves child reference
            resolved = conn0.resolve(wf_eid, child_ref)
            assert resolved["value"] == 42

            conn0.complete(wf_eid, value="done")
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
            conn, eid, target, _ = executor.next_execute()
            assert target == "test.child"
            conn.complete(eid, value=99)
    finally:
        server.stop()


def test_resolve_execution_reference_from_old_epoch(tmp_path):
    """Resolving an execution reference from an old epoch finds and copies the run."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [
        workflow("test.main"),
        task("test.producer", parameters=["x"]),
    ]

    server = ManagedServer(str(tmp_path / "data"))
    server.start()

    try:
        host = f"{project_id}.localhost:{server.port}"
        cli.workspaces_create("default", host=host)

        with managed_worker(targets, host, tmp_path, concurrency=2) as executor:
            # First workflow: submit a task and capture the execution reference
            resp1 = cli.submit("test.main", host=host)
            run_id1 = resp1["runId"]

            conn0, wf_eid1, _, _ = executor.next_execute()

            producer_ref = conn0.submit_task(
                wf_eid1,
                "test.producer",
                json_args(42),
            )

            conn1, task_eid, _, _ = executor.next_execute()
            conn1.complete(task_eid, value=84)

            assert conn0.resolve(wf_eid1, producer_ref)["value"] == 84

            conn0.complete(wf_eid1, value="done1")
            result1 = poll_result(run_id1, host)
            assert result1["type"] == "value"
            assert result1["value"]["data"] == "done1"

            # Rotate epoch — first run with task is now in old epoch
            _rotate_epoch(server.port, project_id)

            # Second workflow: resolve the OLD producer reference
            resp2 = cli.submit("test.main", host=host)
            run_id2 = resp2["runId"]

            conn, wf_eid2, target, _ = executor.next_execute()
            assert target == "test.main"

            # Resolve the old execution reference — server must find it in archived epoch
            resolved = conn.resolve(wf_eid2, producer_ref)
            assert resolved["value"] == 84

            conn.complete(wf_eid2, value="done2")
            result2 = poll_result(run_id2, host)
            assert result2["type"] == "value"
            assert result2["value"]["data"] == "done2"
    finally:
        server.stop()


def test_multiple_rotations_cache_hit_from_oldest_epoch(tmp_path):
    """Cache lookup across 3 epochs finds the match in the oldest one via bloom filter."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [
        workflow("test.main"),
        task("test.expensive", parameters=["x"]),
    ]

    server = ManagedServer(str(tmp_path / "data"))
    server.start()

    try:
        host = f"{project_id}.localhost:{server.port}"
        cli.workspaces_create("default", host=host)

        with managed_worker(targets, host, tmp_path, concurrency=2) as executor:
            # --- Epoch 1: cached task with args(42) → value 84 ---
            resp1 = cli.submit("test.main", host=host)
            run_id1 = resp1["runId"]

            conn0, wf_eid1, _, _ = executor.next_execute()
            ref1 = conn0.submit_task(
                wf_eid1,
                "test.expensive",
                json_args(42),
                cache={"params": True},
            )

            conn1, task_eid1, _, _ = executor.next_execute()
            conn1.complete(task_eid1, value=84)

            assert conn0.resolve(wf_eid1, ref1)["value"] == 84
            conn0.complete(wf_eid1, value="done1")
            result1 = poll_result(run_id1, host)
            assert result1["value"]["data"] == "done1"

            # Rotate → epoch 2
            _rotate_epoch(server.port, project_id)

            # --- Epoch 2: cached task with args(99) → value 198 (different args) ---
            resp2 = cli.submit("test.main", host=host)
            run_id2 = resp2["runId"]

            conn_wf2, wf_eid2, _, _ = executor.next_execute()

            ref2 = conn_wf2.submit_task(
                wf_eid2,
                "test.expensive",
                json_args(99),
                cache={"params": True},
            )

            conn_task2, task_eid2, _, _ = executor.next_execute()
            conn_task2.complete(task_eid2, value=198)

            assert conn_wf2.resolve(wf_eid2, ref2)["value"] == 198
            conn_wf2.complete(wf_eid2, value="done2")
            result2 = poll_result(run_id2, host)
            assert result2["value"]["data"] == "done2"

            # Rotate → epoch 3
            _rotate_epoch(server.port, project_id)

            # --- Epoch 3: cached task with args(42) again — should hit epoch 1's cache ---
            resp3 = cli.submit("test.main", host=host)
            run_id3 = resp3["runId"]

            conn_wf3, wf_eid3, _, _ = executor.next_execute()

            ref3 = conn_wf3.submit_task(
                wf_eid3,
                "test.expensive",
                json_args(42),
                cache={"params": True},
            )

            # Should resolve from cache (epoch 1) — no new task execution
            resolved3 = conn_wf3.resolve(wf_eid3, ref3)
            assert resolved3["value"] == 84  # from epoch 1, NOT 198

            conn_wf3.complete(wf_eid3, value="done3")
            result3 = poll_result(run_id3, host)
            assert result3["value"]["data"] == "done3"
    finally:
        server.stop()


def test_asset_reference_across_epoch_boundary(tmp_path):
    """Assets are preserved when a run is copied from an old epoch via rerun."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [
        workflow("test.main"),
        task("test.producer"),
        task("test.consumer"),
    ]

    server = ManagedServer(str(tmp_path / "data"))
    server.start()

    try:
        host = f"{project_id}.localhost:{server.port}"
        cli.workspaces_create("default", host=host)

        with managed_worker(targets, host, tmp_path, concurrency=2) as executor:
            # Submit workflow
            resp = cli.submit("test.main", host=host)
            run_id = resp["runId"]

            conn0, wf_eid, _, _ = executor.next_execute()

            # Submit producer task
            ref_prod = conn0.submit_task(wf_eid, "test.producer", [])
            conn1, prod_eid, _, _ = executor.next_execute()

            # Create a temp file and persist as asset
            asset_file = str(tmp_path / "epoch_asset.txt")
            with open(asset_file, "w") as f:
                f.write("asset data for epoch test")

            asset_result = conn1.persist_asset(
                prod_eid,
                [asset_file],
                metadata={"name": "epoch_asset"},
            )
            assert "asset_id" in asset_result
            asset_id = asset_result["asset_id"]

            conn1.complete(prod_eid, value="produced")
            assert conn0.resolve(wf_eid, ref_prod)["value"] == "produced"

            # Submit consumer task
            ref_cons = conn0.submit_task(wf_eid, "test.consumer", [])
            conn2, cons_eid, _, _ = executor.next_execute()

            # Consumer retrieves the asset — capture original entries
            original_asset = conn2.get_asset(cons_eid, asset_id)
            assert "entries" in original_asset
            original_entries = original_asset["entries"]
            assert len(original_entries) > 0

            # Capture original blob key and size
            original_entry = list(original_entries.values())[0]
            original_blob_key = original_entry[0]
            original_size = original_entry[1]

            conn2.complete(cons_eid, value="consumed")
            assert conn0.resolve(wf_eid, ref_cons)["value"] == "consumed"

            conn0.complete(wf_eid, value="done")
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
            conn, rerun_eid, target, _ = executor.next_execute()
            assert target == "test.consumer"

            # The old asset reference should still resolve after copy
            rerun_asset = conn.get_asset(rerun_eid, asset_id)
            assert "entries" in rerun_asset
            rerun_entries = rerun_asset["entries"]
            assert len(rerun_entries) > 0

            # Verify the asset data matches original
            rerun_entry = list(rerun_entries.values())[0]
            assert rerun_entry[0] == original_blob_key
            assert rerun_entry[1] == original_size

            conn.complete(rerun_eid, value="consumed again")
    finally:
        server.stop()


def test_idempotency_across_epoch_boundary(tmp_path):
    """Idempotency key is found in archived epoch after rotation."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [workflow("test.my_workflow")]

    server = ManagedServer(str(tmp_path / "data"))
    server.start()

    try:
        host = f"{project_id}.localhost:{server.port}"
        cli.workspaces_create("default", host=host)

        with managed_worker(targets, host, tmp_path) as executor:
            # Submit with idempotency key and complete
            resp1 = cli.submit(
                "test.my_workflow", idempotency_key="epoch-key", host=host
            )
            run_id1 = resp1["runId"]

            conn, eid, target, _ = executor.next_execute()
            assert target == "test.my_workflow"
            conn.complete(eid, value=42)

            result = poll_result(run_id1, host)
            assert result["type"] == "value"

            # Force epoch rotation
            _rotate_epoch(server.port, project_id)

            # Submit again with the same idempotency key — should return existing run
            resp2 = cli.submit(
                "test.my_workflow", idempotency_key="epoch-key", host=host
            )
            run_id2 = resp2["runId"]

            assert run_id1 == run_id2
    finally:
        server.stop()
