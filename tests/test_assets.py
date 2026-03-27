"""Tests for asset persistence and retrieval."""

import os

from support.manifest import task, workflow


def test_persist_and_get_asset(worker, tmp_path):
    """Executor persists a file as an asset, then retrieves it in another step."""
    targets = [
        workflow("test", "main"),
        task("test", "producer"),
        task("test", "consumer"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        # Submit producer task
        ref_prod = ex0.conn.submit_task(ex0.execution_id, "test", "producer", [])

        ex1 = ctx.executor.next_execute()

        # Create a temp file to persist as an asset
        asset_file = tmp_path / "asset.txt"
        asset_file.write_text("hello asset")

        # Persist the file as an asset
        asset_result = ex1.conn.persist_asset(
            ex1.execution_id,
            [str(asset_file)],
            metadata={"name": "my_asset"},
        )
        assert "asset_id" in asset_result
        asset_id = asset_result["asset_id"]

        ex1.conn.complete(ex1.execution_id, value="produced")
        assert ex0.conn.resolve(ex0.execution_id, ref_prod)["value"] == "produced"

        # Submit consumer task (fresh connection)
        ref_cons = ex0.conn.submit_task(ex0.execution_id, "test", "consumer", [])
        ex2 = ctx.executor.next_execute()

        # Retrieve the asset (response is {"entries": {path: [blob_key, size, metadata]}})
        result = ex2.conn.get_asset(ex2.execution_id, asset_id)
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

        ex2.conn.complete(ex2.execution_id, value="consumed")
        assert ex0.conn.resolve(ex0.execution_id, ref_cons)["value"] == "consumed"

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_asset_inspect_and_download(worker, tmp_path):
    """CLI can inspect and download a persisted asset."""
    targets = [
        workflow("test", "main"),
        task("test", "producer"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        # Submit producer task
        ref_prod = ex0.conn.submit_task(ex0.execution_id, "test", "producer", [])
        ex1 = ctx.executor.next_execute()

        # Create a temp file and persist as asset
        src_path = str(tmp_path / "data.txt")
        with open(src_path, "w") as f:
            f.write("asset content for CLI test")

        asset_result = ex1.conn.persist_asset(
            ex1.execution_id,
            [src_path],
            metadata={"name": "cli_test_asset"},
        )
        asset_id = asset_result["asset_id"]

        ex1.conn.complete(ex1.execution_id, value="done")
        assert ex0.conn.resolve(ex0.execution_id, ref_prod)["value"] == "done"
        ex0.conn.complete(ex0.execution_id, value="done")
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
