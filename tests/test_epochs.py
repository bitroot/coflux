"""Tests for epoch database rotation and cross-epoch behavior."""

import json
import os
import signal
import subprocess
import time
import urllib.request
import uuid

import support.cli as cli
from support.executor import Executor
from support.manifest import manifest, task, workflow
from support.protocol import json_args
from support.server import ManagedServer

ADAPTER_SCRIPT = os.path.join(os.path.dirname(__file__), "support", "adapter.py")


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


def _epoch_files(data_dir, project_id):
    """Return sorted list of epoch .sqlite files for a project."""
    epochs_dir = os.path.join(data_dir, "projects", project_id, "epochs")
    if not os.path.isdir(epochs_dir):
        return []
    return sorted(f for f in os.listdir(epochs_dir) if f.endswith(".sqlite"))


def _make_env(host):
    return {
        "PATH": os.environ["PATH"],
        "COFLUX_SERVER_HOST": host,
        "COFLUX_WORKSPACE": "default",
    }


def _poll_result(run_id, env, timeout=15):
    """Poll for a run result until it completes or times out."""
    deadline = time.time() + timeout
    interval = 0.1
    while time.time() < deadline:
        try:
            return cli.runs_result(run_id, env=env)
        except (subprocess.CalledProcessError, json.JSONDecodeError):
            time.sleep(interval)
            interval = min(interval * 2, 1.0)
    raise TimeoutError(f"run {run_id} did not complete within {timeout}s")


def test_epoch_rotation_creates_new_epoch(tmp_path):
    """Rotation creates a new epoch file and server remains functional."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [workflow("test.my_workflow")]
    manifest_json = json.dumps(manifest(targets))
    socket_path = str(tmp_path / "executor.sock")
    data_dir = str(tmp_path / "data")

    server = ManagedServer(data_dir)
    server.start()

    try:
        env = _make_env(f"{project_id}.localhost:{server.port}")
        env["COFLUX_TEST_MANIFEST"] = manifest_json
        env["COFLUX_TEST_SOCKET"] = socket_path
        env["COFLUX_LOGS_STORE_FLUSH_INTERVAL"] = "0"

        cli.workspaces_create("default", env=env)

        executor = Executor(socket_path)
        executor.start()

        adapter = f"python3,{ADAPTER_SCRIPT}"
        worker_proc = cli.worker(["test"], adapter, concurrency=1, env=env)

        try:
            executor.accept(count=1, timeout=15)
            conn = executor.connections[0]
            conn.send_ready()

            # Submit and complete a workflow
            resp = cli.submit("test.my_workflow", env=env)
            run_id = resp["runId"]

            eid, target, _ = conn.recv_execute()
            assert target == "test.my_workflow"
            conn.complete(eid, value=42)

            result = _poll_result(run_id, env)
            assert result["type"] == "value"
            assert result["value"]["data"] == 42

            # Should have exactly 1 epoch file
            assert len(_epoch_files(data_dir, project_id)) == 1

            # Force rotation
            _rotate_epoch(server.port, project_id)

            # Should now have 2 epoch files
            assert len(_epoch_files(data_dir, project_id)) == 2

            # Server still works after rotation — submit another workflow
            resp2 = cli.submit("test.my_workflow", env=env)
            run_id2 = resp2["runId"]

            eid2, target2, _ = conn.recv_execute()
            assert target2 == "test.my_workflow"
            conn.complete(eid2, value=99)

            result2 = _poll_result(run_id2, env)
            assert result2["type"] == "value"
            assert result2["value"]["data"] == 99

        finally:
            worker_proc.send_signal(signal.SIGTERM)
            try:
                worker_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker_proc.kill()
                worker_proc.wait(timeout=5)
            executor.close()
    finally:
        server.stop()


def test_rerun_across_epoch_boundary(tmp_path):
    """Re-running a step whose data is in an old epoch works (copy-on-reference)."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [workflow("test.my_workflow")]
    manifest_json = json.dumps(manifest(targets))
    socket_path = str(tmp_path / "executor.sock")
    data_dir = str(tmp_path / "data")

    server = ManagedServer(data_dir)
    server.start()

    try:
        env = _make_env(f"{project_id}.localhost:{server.port}")
        env["COFLUX_TEST_MANIFEST"] = manifest_json
        env["COFLUX_TEST_SOCKET"] = socket_path
        env["COFLUX_LOGS_STORE_FLUSH_INTERVAL"] = "0"

        cli.workspaces_create("default", env=env)

        executor = Executor(socket_path)
        executor.start()

        adapter = f"python3,{ADAPTER_SCRIPT}"
        worker_proc = cli.worker(["test"], adapter, concurrency=1, env=env)

        try:
            executor.accept(count=1, timeout=15)
            conn = executor.connections[0]
            conn.send_ready()

            # Submit and complete a workflow
            resp = cli.submit("test.my_workflow", env=env)
            run_id = resp["runId"]
            step_id = resp["stepId"]

            eid, target, _ = conn.recv_execute()
            assert target == "test.my_workflow"
            conn.complete(eid, value=42)

            result = _poll_result(run_id, env)
            assert result["type"] == "value"
            assert result["value"]["data"] == 42

            # Force rotation — original run is now in old epoch
            _rotate_epoch(server.port, project_id)
            assert len(_epoch_files(data_dir, project_id)) == 2

            # Re-run the step (this triggers copy-on-reference from old epoch)
            rerun_resp = cli.runs_rerun(step_id, env=env)
            assert rerun_resp["attempt"] == 2

            # Execute the re-run
            eid2, target2, _ = conn.recv_execute()
            assert target2 == "test.my_workflow"
            conn.complete(eid2, value=99)

            result2 = _poll_result(run_id, env)
            assert result2["type"] == "value"
            assert result2["value"]["data"] == 99

        finally:
            worker_proc.send_signal(signal.SIGTERM)
            try:
                worker_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker_proc.kill()
                worker_proc.wait(timeout=5)
            executor.close()
    finally:
        server.stop()


def test_cache_hit_across_epoch_boundary(tmp_path):
    """Cached task result from old epoch is found after rotation."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [
        workflow("test.main"),
        task("test.expensive", parameters=["x"]),
    ]
    manifest_json = json.dumps(manifest(targets))
    socket_path = str(tmp_path / "executor.sock")
    data_dir = str(tmp_path / "data")

    server = ManagedServer(data_dir)
    server.start()

    try:
        env = _make_env(f"{project_id}.localhost:{server.port}")
        env["COFLUX_TEST_MANIFEST"] = manifest_json
        env["COFLUX_TEST_SOCKET"] = socket_path
        env["COFLUX_LOGS_STORE_FLUSH_INTERVAL"] = "0"

        cli.workspaces_create("default", env=env)

        executor = Executor(socket_path)
        executor.start()

        adapter = f"python3,{ADAPTER_SCRIPT}"
        worker_proc = cli.worker(["test"], adapter, concurrency=2, env=env)

        try:
            for _ in range(2):
                executor.accept(count=1, timeout=15)
                executor.connections[-1].send_ready()

            conn0 = executor.connections[0]
            conn1 = executor.connections[1]

            # First workflow: execute a cached task
            resp1 = cli.submit("test.main", env=env)
            run_id1 = resp1["runId"]

            wf_eid1, _, _ = conn0.recv_execute()

            ref1 = conn0.submit_task(
                wf_eid1,
                "test.expensive",
                json_args(42),
                cache={"params": True},
            )

            task_eid, _, _ = conn1.recv_execute()
            conn1.complete(task_eid, value=84)

            assert conn0.resolve(wf_eid1, ref1)["value"] == 84

            conn0.complete(wf_eid1, value="done1")
            result1 = _poll_result(run_id1, env)
            assert result1["type"] == "value"
            assert result1["value"]["data"] == "done1"

            # Force rotation — cached execution is now in old epoch
            _rotate_epoch(server.port, project_id)
            assert len(_epoch_files(data_dir, project_id)) == 2

            # Second workflow: submit same cached task — should hit cache.
            # The workflow execution may land on either connection.
            resp2 = cli.submit("test.main", env=env)
            run_id2 = resp2["runId"]

            try:
                wf_eid2, _, _ = conn0.recv_execute(timeout=5)
                wf_conn = conn0
            except TimeoutError:
                wf_eid2, _, _ = conn1.recv_execute(timeout=5)
                wf_conn = conn1

            ref2 = wf_conn.submit_task(
                wf_eid2,
                "test.expensive",
                json_args(42),
                cache={"params": True},
            )

            # Should resolve immediately from cache (no new execution on other conn)
            assert wf_conn.resolve(wf_eid2, ref2)["value"] == 84

            wf_conn.complete(wf_eid2, value="done2")
            result2 = _poll_result(run_id2, env)
            assert result2["type"] == "value"
            assert result2["value"]["data"] == "done2"

        finally:
            worker_proc.send_signal(signal.SIGTERM)
            try:
                worker_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker_proc.kill()
                worker_proc.wait(timeout=5)
            executor.close()
    finally:
        server.stop()


def test_server_starts_after_old_epoch_deleted(tmp_path):
    """Server starts and functions after an old epoch file is deleted."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [workflow("test.my_workflow")]
    manifest_json = json.dumps(manifest(targets))
    socket_path = str(tmp_path / "executor.sock")
    data_dir = str(tmp_path / "data")

    server = ManagedServer(data_dir)
    server.start()

    try:
        env = _make_env(f"{project_id}.localhost:{server.port}")
        env["COFLUX_TEST_MANIFEST"] = manifest_json
        env["COFLUX_TEST_SOCKET"] = socket_path
        env["COFLUX_LOGS_STORE_FLUSH_INTERVAL"] = "0"

        cli.workspaces_create("default", env=env)

        executor = Executor(socket_path)
        executor.start()

        adapter = f"python3,{ADAPTER_SCRIPT}"
        worker_proc = cli.worker(["test"], adapter, concurrency=1, env=env)

        try:
            executor.accept(count=1, timeout=15)
            conn = executor.connections[0]
            conn.send_ready()

            # Submit and complete a workflow
            resp = cli.submit("test.my_workflow", env=env)
            run_id = resp["runId"]

            eid, target, _ = conn.recv_execute()
            assert target == "test.my_workflow"
            conn.complete(eid, value=42)

            result = _poll_result(run_id, env)
            assert result["type"] == "value"
            assert result["value"]["data"] == 42

        finally:
            worker_proc.send_signal(signal.SIGTERM)
            try:
                worker_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker_proc.kill()
                worker_proc.wait(timeout=5)
            executor.close()

        # Rotate so we have 2 epochs
        _rotate_epoch(server.port, project_id)
        epoch_files = _epoch_files(data_dir, project_id)
        assert len(epoch_files) == 2

    finally:
        server.stop()

    # Delete the oldest epoch file and the epoch index
    epochs_dir = os.path.join(data_dir, "projects", project_id, "epochs")
    oldest = epoch_files[0]
    os.remove(os.path.join(epochs_dir, oldest))

    index_path = os.path.join(data_dir, "projects", project_id, "epoch_index.bin")
    if os.path.exists(index_path):
        os.remove(index_path)

    # Restart with same data dir and port
    server.start()

    try:
        socket_path2 = str(tmp_path / "executor2.sock")
        env["COFLUX_TEST_SOCKET"] = socket_path2

        executor2 = Executor(socket_path2)
        executor2.start()

        worker_proc2 = cli.worker(["test"], adapter, concurrency=1, env=env)

        try:
            executor2.accept(count=1, timeout=15)
            conn2 = executor2.connections[0]
            conn2.send_ready()

            # Submit a new workflow — server should work fine
            resp2 = cli.submit("test.my_workflow", env=env)
            run_id2 = resp2["runId"]

            eid2, target2, _ = conn2.recv_execute()
            assert target2 == "test.my_workflow"
            conn2.complete(eid2, value=99)

            result2 = _poll_result(run_id2, env)
            assert result2["type"] == "value"
            assert result2["value"]["data"] == 99

        finally:
            worker_proc2.send_signal(signal.SIGTERM)
            try:
                worker_proc2.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker_proc2.kill()
                worker_proc2.wait(timeout=5)
            executor2.close()
    finally:
        server.stop()


def _recv_from_either(conn0, conn1, timeout=10):
    """Try to receive an execute message from either connection.

    Returns (connection, execution_id, target, arguments).
    """
    try:
        eid, target, args = conn0.recv_execute(timeout=1)
        return conn0, eid, target, args
    except TimeoutError:
        eid, target, args = conn1.recv_execute(timeout=timeout)
        return conn1, eid, target, args


def test_parent_child_run_across_epoch_boundary(tmp_path):
    """Rerunning a child step copies its parent run from an old epoch (recursive copy)."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [
        workflow("test.main"),
        workflow("test.child"),
    ]
    manifest_json = json.dumps(manifest(targets))
    socket_path = str(tmp_path / "executor.sock")
    data_dir = str(tmp_path / "data")

    server = ManagedServer(data_dir)
    server.start()

    try:
        env = _make_env(f"{project_id}.localhost:{server.port}")
        env["COFLUX_TEST_MANIFEST"] = manifest_json
        env["COFLUX_TEST_SOCKET"] = socket_path
        env["COFLUX_LOGS_STORE_FLUSH_INTERVAL"] = "0"

        cli.workspaces_create("default", env=env)

        executor = Executor(socket_path)
        executor.start()

        adapter = f"python3,{ADAPTER_SCRIPT}"
        worker_proc = cli.worker(["test"], adapter, concurrency=2, env=env)

        try:
            for _ in range(2):
                executor.accept(count=1, timeout=15)
                executor.connections[-1].send_ready()

            conn0 = executor.connections[0]
            conn1 = executor.connections[1]

            # Submit parent workflow
            resp = cli.submit("test.main", env=env)
            run_id = resp["runId"]

            wf_eid, target, _ = conn0.recv_execute()
            assert target == "test.main"

            # From parent, submit child workflow
            child_ref = conn0.submit_workflow(wf_eid, "test.child", [])

            child_eid, target, _ = conn1.recv_execute()
            assert target == "test.child"
            conn1.complete(child_eid, value=42)

            # Parent resolves child reference
            resolved = conn0.resolve(wf_eid, child_ref)
            assert resolved["value"] == 42

            conn0.complete(wf_eid, value="done")
            result = _poll_result(run_id, env)
            assert result["type"] == "value"
            assert result["value"]["data"] == "done"

            # Extract child step_id from reference: ["execution", "{run_id}:{step}:{attempt}"]
            child_exec_id = child_ref[1]  # e.g. "abc:1:1"
            parts = child_exec_id.rsplit(":", 2)
            child_step_id = f"{parts[0]}:{parts[1]}"  # "abc:1"

            # Rotate epoch — both parent and child runs now in old epoch
            _rotate_epoch(server.port, project_id)
            assert len(_epoch_files(data_dir, project_id)) == 2

            # Rerun the child step — triggers copy_run on child, which recursively copies parent
            rerun_resp = cli.runs_rerun(child_step_id, env=env)
            assert rerun_resp["attempt"] == 2

            # Execute the rerun — the fact that the execution was dispatched means
            # copy_run succeeded (including recursive parent copy)
            conn, eid, target, _ = _recv_from_either(conn0, conn1)
            assert target == "test.child"
            conn.complete(eid, value=99)

        finally:
            worker_proc.send_signal(signal.SIGTERM)
            try:
                worker_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker_proc.kill()
                worker_proc.wait(timeout=5)
            executor.close()
    finally:
        server.stop()


def test_resolve_execution_reference_from_old_epoch(tmp_path):
    """Resolving an execution reference from an old epoch finds and copies the run."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [
        workflow("test.main"),
        task("test.producer", parameters=["x"]),
    ]
    manifest_json = json.dumps(manifest(targets))
    socket_path = str(tmp_path / "executor.sock")
    data_dir = str(tmp_path / "data")

    server = ManagedServer(data_dir)
    server.start()

    try:
        env = _make_env(f"{project_id}.localhost:{server.port}")
        env["COFLUX_TEST_MANIFEST"] = manifest_json
        env["COFLUX_TEST_SOCKET"] = socket_path
        env["COFLUX_LOGS_STORE_FLUSH_INTERVAL"] = "0"

        cli.workspaces_create("default", env=env)

        executor = Executor(socket_path)
        executor.start()

        adapter = f"python3,{ADAPTER_SCRIPT}"
        worker_proc = cli.worker(["test"], adapter, concurrency=2, env=env)

        try:
            for _ in range(2):
                executor.accept(count=1, timeout=15)
                executor.connections[-1].send_ready()

            conn0 = executor.connections[0]
            conn1 = executor.connections[1]

            # First workflow: submit a task and capture the execution reference
            resp1 = cli.submit("test.main", env=env)
            run_id1 = resp1["runId"]

            wf_eid1, _, _ = conn0.recv_execute()

            producer_ref = conn0.submit_task(
                wf_eid1, "test.producer", json_args(42),
            )

            task_eid, _, _ = conn1.recv_execute()
            conn1.complete(task_eid, value=84)

            assert conn0.resolve(wf_eid1, producer_ref)["value"] == 84

            conn0.complete(wf_eid1, value="done1")
            result1 = _poll_result(run_id1, env)
            assert result1["type"] == "value"
            assert result1["value"]["data"] == "done1"

            # Rotate epoch — first run with task is now in old epoch
            _rotate_epoch(server.port, project_id)
            assert len(_epoch_files(data_dir, project_id)) == 2

            # Second workflow: resolve the OLD producer reference
            resp2 = cli.submit("test.main", env=env)
            run_id2 = resp2["runId"]

            conn, wf_eid2, target, _ = _recv_from_either(conn0, conn1)
            assert target == "test.main"

            # Resolve the old execution reference — server must find it in archived epoch
            resolved = conn.resolve(wf_eid2, producer_ref)
            assert resolved["value"] == 84

            conn.complete(wf_eid2, value="done2")
            result2 = _poll_result(run_id2, env)
            assert result2["type"] == "value"
            assert result2["value"]["data"] == "done2"

        finally:
            worker_proc.send_signal(signal.SIGTERM)
            try:
                worker_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker_proc.kill()
                worker_proc.wait(timeout=5)
            executor.close()
    finally:
        server.stop()


def test_multiple_rotations_cache_hit_from_oldest_epoch(tmp_path):
    """Cache lookup across 3 epochs finds the match in the oldest one via bloom filter."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [
        workflow("test.main"),
        task("test.expensive", parameters=["x"]),
    ]
    manifest_json = json.dumps(manifest(targets))
    socket_path = str(tmp_path / "executor.sock")
    data_dir = str(tmp_path / "data")

    server = ManagedServer(data_dir)
    server.start()

    try:
        env = _make_env(f"{project_id}.localhost:{server.port}")
        env["COFLUX_TEST_MANIFEST"] = manifest_json
        env["COFLUX_TEST_SOCKET"] = socket_path
        env["COFLUX_LOGS_STORE_FLUSH_INTERVAL"] = "0"

        cli.workspaces_create("default", env=env)

        executor = Executor(socket_path)
        executor.start()

        adapter = f"python3,{ADAPTER_SCRIPT}"
        worker_proc = cli.worker(["test"], adapter, concurrency=2, env=env)

        try:
            for _ in range(2):
                executor.accept(count=1, timeout=15)
                executor.connections[-1].send_ready()

            conn0 = executor.connections[0]
            conn1 = executor.connections[1]

            # --- Epoch 1: cached task with args(42) → value 84 ---
            resp1 = cli.submit("test.main", env=env)
            run_id1 = resp1["runId"]

            wf_eid1, _, _ = conn0.recv_execute()
            ref1 = conn0.submit_task(
                wf_eid1, "test.expensive", json_args(42),
                cache={"params": True},
            )

            task_eid1, _, _ = conn1.recv_execute()
            conn1.complete(task_eid1, value=84)

            assert conn0.resolve(wf_eid1, ref1)["value"] == 84
            conn0.complete(wf_eid1, value="done1")
            result1 = _poll_result(run_id1, env)
            assert result1["value"]["data"] == "done1"

            # Rotate → epoch 2
            _rotate_epoch(server.port, project_id)
            assert len(_epoch_files(data_dir, project_id)) == 2

            # --- Epoch 2: cached task with args(99) → value 198 (different args) ---
            resp2 = cli.submit("test.main", env=env)
            run_id2 = resp2["runId"]

            conn_wf2, wf_eid2, _, _ = _recv_from_either(conn0, conn1)
            conn_task2 = conn1 if conn_wf2 is conn0 else conn0

            ref2 = conn_wf2.submit_task(
                wf_eid2, "test.expensive", json_args(99),
                cache={"params": True},
            )

            task_eid2, _, _ = conn_task2.recv_execute()
            conn_task2.complete(task_eid2, value=198)

            assert conn_wf2.resolve(wf_eid2, ref2)["value"] == 198
            conn_wf2.complete(wf_eid2, value="done2")
            result2 = _poll_result(run_id2, env)
            assert result2["value"]["data"] == "done2"

            # Rotate → epoch 3
            _rotate_epoch(server.port, project_id)
            assert len(_epoch_files(data_dir, project_id)) == 3

            # --- Epoch 3: cached task with args(42) again — should hit epoch 1's cache ---
            resp3 = cli.submit("test.main", env=env)
            run_id3 = resp3["runId"]

            conn_wf3, wf_eid3, _, _ = _recv_from_either(conn0, conn1)

            ref3 = conn_wf3.submit_task(
                wf_eid3, "test.expensive", json_args(42),
                cache={"params": True},
            )

            # Should resolve from cache (epoch 1) — no new task execution
            resolved3 = conn_wf3.resolve(wf_eid3, ref3)
            assert resolved3["value"] == 84  # from epoch 1, NOT 198

            conn_wf3.complete(wf_eid3, value="done3")
            result3 = _poll_result(run_id3, env)
            assert result3["value"]["data"] == "done3"

        finally:
            worker_proc.send_signal(signal.SIGTERM)
            try:
                worker_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker_proc.kill()
                worker_proc.wait(timeout=5)
            executor.close()
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
    manifest_json = json.dumps(manifest(targets))
    socket_path = str(tmp_path / "executor.sock")
    data_dir = str(tmp_path / "data")

    server = ManagedServer(data_dir)
    server.start()

    try:
        env = _make_env(f"{project_id}.localhost:{server.port}")
        env["COFLUX_TEST_MANIFEST"] = manifest_json
        env["COFLUX_TEST_SOCKET"] = socket_path
        env["COFLUX_LOGS_STORE_FLUSH_INTERVAL"] = "0"

        cli.workspaces_create("default", env=env)

        executor = Executor(socket_path)
        executor.start()

        adapter = f"python3,{ADAPTER_SCRIPT}"
        worker_proc = cli.worker(["test"], adapter, concurrency=2, env=env)

        try:
            for _ in range(2):
                executor.accept(count=1, timeout=15)
                executor.connections[-1].send_ready()

            conn0 = executor.connections[0]
            conn1 = executor.connections[1]

            # Submit workflow
            resp = cli.submit("test.main", env=env)
            run_id = resp["runId"]

            wf_eid, _, _ = conn0.recv_execute()

            # Submit producer task
            ref_prod = conn0.submit_task(wf_eid, "test.producer", [])
            prod_eid, _, _ = conn1.recv_execute()

            # Create a temp file and persist as asset
            asset_file = str(tmp_path / "epoch_asset.txt")
            with open(asset_file, "w") as f:
                f.write("asset data for epoch test")

            asset_result = conn1.persist_asset(
                prod_eid, [asset_file], metadata={"name": "epoch_asset"},
            )
            assert "reference" in asset_result
            asset_ref = asset_result["reference"]

            conn1.complete(prod_eid, value="produced")
            assert conn0.resolve(wf_eid, ref_prod)["value"] == "produced"

            # Submit consumer task
            ref_cons = conn0.submit_task(wf_eid, "test.consumer", [])
            cons_eid, _, _ = conn1.recv_execute()

            # Consumer retrieves the asset — capture original entries
            original_asset = conn1.get_asset(cons_eid, asset_ref)
            assert "entries" in original_asset
            original_entries = original_asset["entries"]
            assert len(original_entries) > 0

            # Capture original blob key and size
            original_entry = list(original_entries.values())[0]
            original_blob_key = original_entry[0]
            original_size = original_entry[1]

            conn1.complete(cons_eid, value="consumed")
            assert conn0.resolve(wf_eid, ref_cons)["value"] == "consumed"

            conn0.complete(wf_eid, value="done")
            result = _poll_result(run_id, env)
            assert result["type"] == "value"
            assert result["value"]["data"] == "done"

            # Extract consumer step_id for rerun
            cons_exec_id = ref_cons[1]  # e.g. "abc:3:1"
            parts = cons_exec_id.rsplit(":", 2)
            cons_step_id = f"{parts[0]}:{parts[1]}"

            # Rotate epoch — run with asset is now in old epoch
            _rotate_epoch(server.port, project_id)
            assert len(_epoch_files(data_dir, project_id)) == 2

            # Rerun the consumer step — copy_run copies run + assets to active epoch
            rerun_resp = cli.runs_rerun(cons_step_id, env=env)
            assert rerun_resp["attempt"] == 2

            # Execute the consumer rerun
            conn, rerun_eid, target, _ = _recv_from_either(conn0, conn1)
            assert target == "test.consumer"

            # The old asset reference should still resolve after copy
            rerun_asset = conn.get_asset(rerun_eid, asset_ref)
            assert "entries" in rerun_asset
            rerun_entries = rerun_asset["entries"]
            assert len(rerun_entries) > 0

            # Verify the asset data matches original
            rerun_entry = list(rerun_entries.values())[0]
            assert rerun_entry[0] == original_blob_key
            assert rerun_entry[1] == original_size

            conn.complete(rerun_eid, value="consumed again")

        finally:
            worker_proc.send_signal(signal.SIGTERM)
            try:
                worker_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker_proc.kill()
                worker_proc.wait(timeout=5)
            executor.close()
    finally:
        server.stop()
