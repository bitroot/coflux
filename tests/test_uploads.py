"""Assets assembled from outside a run — the path Studio takes when someone
uploads a file. The client stores the blobs itself and the server is handed
keys, so an upload straight to an S3 blob store works the same way."""

import json
import urllib.error

import pytest
from support import cli
from support.helpers import api_post, managed_worker, put_blob
from support.manifest import workflow


@pytest.fixture
def host(server, project_id):
    return f"{project_id}.localhost:{server.port}"


@pytest.fixture
def workspace_id(server, project_id):
    """The default workspace's external id — what the API expects, as
    distinct from the name the CLI takes."""
    created = api_post(
        server.port, project_id, "create_workspace", body={"name": "default"}
    )
    return created["id"]


def _create_asset(server, project_id, workspace_id, entries, name=None):
    body = {"workspaceId": workspace_id, "entries": entries}
    if name is not None:
        body["name"] = name
    return api_post(server.port, project_id, "create_asset", body=body)


def _entry(server, project_id, path, content, metadata=None):
    key = put_blob(server.port, project_id, content)
    entry = {"path": path, "blobKey": key, "size": len(content)}
    if metadata is not None:
        entry["metadata"] = metadata
    return entry


def _post_error(server, project_id, path, body):
    """The status and decoded body of a request expected to fail."""
    with pytest.raises(urllib.error.HTTPError) as exc:
        api_post(server.port, project_id, path, body=body)
    return exc.value.code, json.loads(exc.value.read())


# --- Creating -------------------------------------------------------------


def test_create_asset_from_uploaded_blobs(server, project_id, workspace_id, host):
    """Blobs are stored first, then assembled into an asset by key. The
    server never sees the bytes."""
    entries = [
        _entry(server, project_id, "train.csv", b"a,b\n1,2\n"),
        _entry(server, project_id, "test.csv", b"a,b\n3,4\n"),
    ]
    result = _create_asset(server, project_id, workspace_id, entries, name="dataset")

    assert result["name"] == "dataset"
    assert result["totalCount"] == 2
    assert result["totalSize"] == 16

    inspected = cli.assets_inspect(result["assetId"], host=host)
    assert sorted(inspected["entries"]) == ["test.csv", "train.csv"]


def test_create_asset_is_content_addressed(server, project_id, workspace_id):
    """The same entries give the same asset — the dedup every other asset
    gets, so an upload repeated after a failure costs nothing."""
    entries = [_entry(server, project_id, "data.txt", b"hello")]
    first = _create_asset(server, project_id, workspace_id, entries)
    second = _create_asset(server, project_id, workspace_id, entries)

    assert first["assetId"] == second["assetId"]


def test_create_asset_keeps_directory_paths(server, project_id, workspace_id, host):
    """A directory upload arrives as one entry per file, keyed by its path
    within the tree."""
    entries = [
        _entry(server, project_id, "images/a.png", b"one"),
        _entry(server, project_id, "images/nested/b.png", b"two"),
    ]
    result = _create_asset(server, project_id, workspace_id, entries, name="images")

    inspected = cli.assets_inspect(result["assetId"], host=host)
    assert sorted(inspected["entries"]) == ["images/a.png", "images/nested/b.png"]


@pytest.mark.parametrize(
    "path",
    ["../escape.txt", "/absolute.txt", "a/../../b.txt", "a//b.txt", "./a.txt", ""],
)
def test_create_asset_rejects_unsafe_paths(server, project_id, workspace_id, path):
    """Entry paths are restored to disk relative to a directory the reader
    chooses, so they have to stay inside it."""
    key = put_blob(server.port, project_id, b"x")
    status, body = _post_error(
        server,
        project_id,
        "create_asset",
        {
            "workspaceId": workspace_id,
            "entries": [{"path": path, "blobKey": key, "size": 1}],
        },
    )
    assert status == 400
    assert body["error"] == "bad_request"


def test_create_asset_rejects_bad_entries(server, project_id, workspace_id):
    """A blob key is a sha256 hex digest, sizes are non-negative, and one
    path appears once."""
    key = put_blob(server.port, project_id, b"x")

    for entries in [
        [],
        [{"path": "a.txt", "blobKey": "nothex", "size": 1}],
        [{"path": "a.txt", "blobKey": key, "size": -1}],
        [{"path": "a.txt", "blobKey": key}],
        [
            {"path": "a.txt", "blobKey": key, "size": 1},
            {"path": "a.txt", "blobKey": key, "size": 1},
        ],
    ]:
        status, _ = _post_error(
            server,
            project_id,
            "create_asset",
            {"workspaceId": workspace_id, "entries": entries},
        )
        assert status == 400


# --- Using ----------------------------------------------------------------


def test_submit_with_asset_argument(server, project_id, workspace_id, host, tmp_path):
    """An uploaded asset can be a run argument: the value is a single
    reference, and the worker is given the asset with its entries."""
    entries = [_entry(server, project_id, "input.csv", b"a,b\n1,2\n")]
    asset = _create_asset(server, project_id, workspace_id, entries, name="input")

    with managed_worker(
        [workflow("test", "main")], host, tmp_path / "worker"
    ) as executor:
        response = api_post(
            server.port,
            project_id,
            "submit_workflow",
            body={
                "workspaceId": workspace_id,
                "module": "test",
                "target": "main",
                "arguments": [["asset", asset["assetId"]]],
            },
        )

        ex = executor.next_execute()
        [argument] = ex.arguments
        assert argument["value"] == {"type": "ref", "index": 0}
        assert argument["references"] == [["asset", asset["assetId"], "input", 1, 8]]

        ex.conn.complete(ex.execution_id, value="done")
        assert cli.runs_result(response["runId"], host=host)["value"]["data"] == "done"


def test_submit_rejects_unknown_asset(server, project_id, workspace_id):
    """A reference to an asset that doesn't exist fails the submit rather
    than writing a value against it."""
    status, body = _post_error(
        server,
        project_id,
        "submit_workflow",
        {
            "workspaceId": workspace_id,
            "module": "test",
            "target": "main",
            "arguments": [["asset", "Axxxxxxx"]],
        },
    )
    assert status == 404
    assert body["details"] == {"arguments": "asset_unknown"}


def test_submit_rejects_malformed_argument(server, project_id, workspace_id):
    """An argument that is neither a JSON value nor a reference is a bad
    request, not a crash."""
    status, _ = _post_error(
        server,
        project_id,
        "submit_workflow",
        {
            "workspaceId": workspace_id,
            "module": "test",
            "target": "main",
            "arguments": [["nonsense", "x"]],
        },
    )
    assert status == 400
