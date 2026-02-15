"""Tests for asset persistence and retrieval."""

import os
import tempfile

from support.manifest import task, workflow


def test_persist_and_get_asset(worker):
    """Executor persists a file as an asset, then retrieves it in another step."""
    targets = [
        workflow("test.main"),
        task("test.producer"),
        task("test.consumer"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.main")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]
        conn1 = ctx.executor.connections[1]

        wf_eid, _, _ = conn0.recv_execute()

        # Submit producer task
        ref_prod = conn0.submit_task(wf_eid, "test.producer", [])

        prod_eid, _, _ = conn1.recv_execute()

        # Create a temp file to persist as an asset
        fd, tmp_path = tempfile.mkstemp(suffix=".txt")
        with open(fd, "w") as f:
            f.write("hello asset")

        # Persist the file as an asset
        asset_result = conn1.persist_asset(
            prod_eid,
            [tmp_path],
            metadata={"name": "my_asset"},
        )
        assert "asset_id" in asset_result
        asset_id = asset_result["asset_id"]

        conn1.complete(prod_eid, value="produced")
        assert conn0.resolve(wf_eid, ref_prod)["value"] == "produced"

        # Submit consumer task (conn1 is reused after completing)
        ref_cons = conn0.submit_task(wf_eid, "test.consumer", [])
        cons_eid, _, _ = conn1.recv_execute()

        # Retrieve the asset (response is {"entries": {path: [blob_key, size, metadata]}})
        result = conn1.get_asset(cons_eid, asset_id)
        assert "entries" in result
        entries = result["entries"]
        assert isinstance(entries, dict)
        assert len(entries) > 0

        # Each entry should have [blob_key, size, metadata]
        for _, entry in entries.items():
            assert len(entry) == 3
            blob_key, size, metadata = entry
            assert isinstance(blob_key, str)
            assert size > 0

        conn1.complete(cons_eid, value="consumed")
        assert conn0.resolve(wf_eid, ref_cons)["value"] == "consumed"

        conn0.complete(wf_eid, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"

        # Clean up
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_asset_inspect_and_download(worker, tmp_path):
    """CLI can inspect and download a persisted asset."""
    targets = [
        workflow("test.main"),
        task("test.producer"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.main")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]
        conn1 = ctx.executor.connections[1]

        wf_eid, _, _ = conn0.recv_execute()

        # Submit producer task
        ref_prod = conn0.submit_task(wf_eid, "test.producer", [])
        prod_eid, _, _ = conn1.recv_execute()

        # Create a temp file and persist as asset
        src_path = str(tmp_path / "data.txt")
        with open(src_path, "w") as f:
            f.write("asset content for CLI test")

        asset_result = conn1.persist_asset(
            prod_eid,
            [src_path],
            metadata={"name": "cli_test_asset"},
        )
        asset_id = asset_result["asset_id"]

        conn1.complete(prod_eid, value="done")
        assert conn0.resolve(wf_eid, ref_prod)["value"] == "done"
        conn0.complete(wf_eid, value="done")
        ctx.result(run_id)

        # Inspect the asset via CLI
        info = ctx.inspect_asset(asset_id)
        assert info["name"] == "cli_test_asset"
        assert "entries" in info
        entries = info["entries"]
        assert len(entries) == 1

        # Get the blob key from the inspect output
        entry_path = list(entries.keys())[0]
        blob_key = entries[entry_path]["blobKey"]
        assert entries[entry_path]["size"] > 0

        # Download the asset via CLI
        download_dir = str(tmp_path / "downloaded")
        os.makedirs(download_dir)
        ctx.download_asset(asset_id, download_dir)

        downloaded_file = os.path.join(download_dir, entry_path)
        assert os.path.exists(downloaded_file)
        with open(downloaded_file) as f:
            assert f.read() == "asset content for CLI test"

        # Get the raw blob via CLI
        blob_output = str(tmp_path / "blob_output.txt")
        ctx.get_blob(blob_key, blob_output)
        with open(blob_output) as f:
            assert f.read() == "asset content for CLI test"
