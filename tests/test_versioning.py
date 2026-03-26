"""Tests for versioning and compatibility."""

import json
import urllib.error
import urllib.request

from support import protocol
from support.manifest import workflow
from support.protocol import execution_result


def _discover_url(project_id, port):
    return f"http://{project_id}.localhost:{port}/api/discover"


def test_discover_returns_api_version(server, project_id):
    """The discover endpoint should return both version and api_version."""
    url = _discover_url(project_id, server.port)
    with urllib.request.urlopen(url) as resp:
        data = json.loads(resp.read())

    assert "version" in data
    assert "api_version" in data

    # api_version should be derivable from version (0.X.Y -> "0.X")
    parts = data["version"].split(".")
    expected_api = f"{parts[0]}.{parts[1]}" if int(parts[0]) == 0 else parts[0]
    assert data["api_version"] == expected_api


def test_version_mismatch_rejected(server, project_id):
    """The server should reject requests with a mismatched API version."""
    url = _discover_url(project_id, server.port)
    req = urllib.request.Request(url, headers={"X-API-Version": "99.99"})
    try:
        urllib.request.urlopen(req)
        assert False, "expected 409 response"
    except urllib.error.HTTPError as e:
        assert e.code == 409
        data = json.loads(e.read())
        assert data["error"] == "version_mismatch"
        assert "server" in data["details"]
        assert data["details"]["expected"] == "99.99"


def test_no_version_header_accepted(server, project_id):
    """Requests without a version header should be accepted."""
    url = _discover_url(project_id, server.port)
    with urllib.request.urlopen(url) as resp:
        assert resp.status == 200


def test_matching_version_accepted(server, project_id):
    """Requests with a matching API version should be accepted."""
    url = _discover_url(project_id, server.port)

    # First discover the API version
    with urllib.request.urlopen(url) as resp:
        data = json.loads(resp.read())
    api_version = data["api_version"]

    # Now make a request with the matching version
    req = urllib.request.Request(url, headers={"X-API-Version": api_version})
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200


def test_ready_message_includes_version():
    """The protocol ready message should include a version param."""
    msg = protocol.ready_message()
    assert msg["method"] == "ready"
    assert "params" in msg
    assert "version" in msg["params"]


def test_worker_accepts_matching_adapter_version(worker):
    """A worker should start successfully when the adapter sends a matching version."""
    # This implicitly tests that the ready message format with version is accepted,
    # since the test executor sends ready_message() which now includes version.
    targets = [workflow("test", "noop")]

    def handler(execution_id, module, target, arguments):
        return execution_result(execution_id)

    with worker(targets, handler) as ctx:
        result = ctx.run("test", "noop")
        assert result == {"value": None}
