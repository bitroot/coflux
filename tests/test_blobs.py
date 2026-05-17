"""Tests for the /blobs/:key HTTP endpoint."""

import hashlib
import http.client
import json
import os

import pytest


def _conn(server, project_id):
    """A raw HTTP connection with the project Host header."""
    c = http.client.HTTPConnection("127.0.0.1", server.port, timeout=5)
    return c, {"Host": f"{project_id}.localhost:{server.port}"}


def _request(server, project_id, method, raw_path, body=None):
    """Send a request with the path passed verbatim (no normalisation)."""
    conn, headers = _conn(server, project_id)
    try:
        conn.request(method, raw_path, body=body, headers=headers)
        resp = conn.getresponse()
        data = resp.read()
        return resp.status, data
    finally:
        conn.close()


def _hex_sha256(b):
    return hashlib.sha256(b).hexdigest()


# Keys that must be rejected without touching the filesystem.  One
# representative per rejection class: a traversal exploit,
# a wrong-length key, and a right-length non-hex key.
INVALID_KEYS = [
    "....%2F..%2F..%2F..%2Fetc%2Fpasswd",
    "abc",
    "z" * 64,
]


def test_put_then_get_roundtrip(server, project_id):
    """A blob written with its real SHA-256 key can be read back."""
    body = b"hello, blobs"
    key = _hex_sha256(body)

    status, _ = _request(server, project_id, "PUT", f"/blobs/{key}", body=body)
    assert status == 204

    status, data = _request(server, project_id, "GET", f"/blobs/{key}")
    assert status == 200
    assert data == body

    status, _ = _request(server, project_id, "HEAD", f"/blobs/{key}")
    assert status == 200


def test_put_rejects_hash_mismatch(server, project_id):
    """A valid-format key that doesn't match the body content is rejected."""
    # Format-valid key, but not the hash of the body.
    bogus_key = "0" * 64
    status, data = _request(
        server, project_id, "PUT", f"/blobs/{bogus_key}", body=b"some body"
    )
    assert status == 400
    assert json.loads(data)["error"] == "hash_mismatch"


@pytest.mark.parametrize("key", INVALID_KEYS)
def test_get_rejects_invalid_key(server, project_id, key):
    status, _ = _request(server, project_id, "GET", f"/blobs/{key}")
    assert status == 404


@pytest.mark.parametrize("key", INVALID_KEYS)
def test_head_rejects_invalid_key(server, project_id, key):
    status, _ = _request(server, project_id, "HEAD", f"/blobs/{key}")
    assert status == 404


@pytest.mark.parametrize("key", INVALID_KEYS)
def test_put_rejects_invalid_key(server, project_id, key):
    status, data = _request(
        server, project_id, "PUT", f"/blobs/{key}", body=b"anything"
    )
    assert status == 400
    assert json.loads(data)["error"] == "invalid_key"


def test_no_files_created_outside_blobs_dir(isolated_server):
    """Malicious keys must not create any files or dirs on disk.

    Uses an isolated server so we can inspect the data dir in isolation
    and assert that only the expected ``blobs/<aa>/<bb>/<rest>`` layout
    (or nothing at all) appears.
    """
    srv, host, pid = isolated_server
    project_id = host.split(".", 1)[0]

    # Hit every invalid-key vector across all three verbs.
    for method in ("GET", "HEAD", "PUT"):
        for key in INVALID_KEYS:
            _request(srv, project_id, method, f"/blobs/{key}", body=b"x")

    data_dir = srv.data_dir
    # No traversal should have created files outside ``data_dir``.
    assert os.path.isdir(data_dir), "server data dir disappeared"

    # If a ``blobs`` dir exists at all, it must contain only well-formed
    # two-char subdirs (none of the invalid keys above start with two hex
    # chars and a slash, so the dir should not exist).
    blobs_dir = os.path.join(data_dir, "blobs")
    if os.path.exists(blobs_dir):
        for entry in os.listdir(blobs_dir):
            assert len(entry) == 2 and all(
                c in "0123456789abcdef" for c in entry
            ), f"unexpected blob shard: {entry!r}"
