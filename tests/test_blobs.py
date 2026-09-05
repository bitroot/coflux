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


def _request_headers(server, project_id, method, raw_path, headers=None):
    """Like ``_request``, but also returns the response headers (lowercased)."""
    conn, request_headers = _conn(server, project_id)
    if headers:
        request_headers = {**request_headers, **headers}
    try:
        conn.request(method, raw_path, headers=request_headers)
        resp = conn.getresponse()
        data = resp.read()
        response_headers = {k.lower(): v for k, v in resp.getheaders()}
        return resp.status, response_headers, data
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
    srv, host, _pid = isolated_server
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
            assert len(entry) == 2 and all(c in "0123456789abcdef" for c in entry), (
                f"unexpected blob shard: {entry!r}"
            )


def _put_blob(server, project_id, body):
    """PUT a blob and return its key."""
    key = _hex_sha256(body)
    status, _ = _request(server, project_id, "PUT", f"/blobs/{key}", body=body)
    assert status == 204
    return key


@pytest.fixture
def sample_blob(server, project_id):
    """A ~1000 byte blob, returning ``(key, body)``."""
    body = (bytes(range(256)) * 4)[:1000]
    return _put_blob(server, project_id, body), body


def test_get_full_advertises_ranges(server, project_id, sample_blob):
    """A rangeless GET returns the whole blob and advertises range support."""
    key, body = sample_blob
    status, headers, data = _request_headers(server, project_id, "GET", f"/blobs/{key}")
    assert status == 200
    assert headers["accept-ranges"] == "bytes"
    assert headers["content-length"] == str(len(body))
    assert data == body


def test_head_returns_size(server, project_id, sample_blob):
    """HEAD reports the size without a body."""
    key, body = sample_blob
    status, headers, data = _request_headers(
        server, project_id, "HEAD", f"/blobs/{key}"
    )
    assert status == 200
    assert headers["content-length"] == str(len(body))
    assert headers["accept-ranges"] == "bytes"
    assert data == b""


def test_get_range_prefix(server, project_id, sample_blob):
    key, body = sample_blob
    status, headers, data = _request_headers(
        server, project_id, "GET", f"/blobs/{key}", {"Range": "bytes=0-9"}
    )
    assert status == 206
    assert data == body[:10]
    assert headers["content-range"] == f"bytes 0-9/{len(body)}"
    assert headers["content-length"] == "10"


def test_get_range_open_ended(server, project_id, sample_blob):
    key, body = sample_blob
    status, headers, data = _request_headers(
        server, project_id, "GET", f"/blobs/{key}", {"Range": "bytes=990-"}
    )
    assert status == 206
    assert data == body[990:]
    assert headers["content-range"] == f"bytes 990-999/{len(body)}"


def test_get_range_suffix(server, project_id, sample_blob):
    key, body = sample_blob
    status, headers, data = _request_headers(
        server, project_id, "GET", f"/blobs/{key}", {"Range": "bytes=-10"}
    )
    assert status == 206
    assert data == body[-10:]
    assert headers["content-range"] == f"bytes 990-999/{len(body)}"


def test_get_range_clamped_to_size(server, project_id, sample_blob):
    """A last-byte beyond the end is clamped rather than rejected."""
    key, body = sample_blob
    status, headers, data = _request_headers(
        server, project_id, "GET", f"/blobs/{key}", {"Range": "bytes=990-5000"}
    )
    assert status == 206
    assert data == body[990:]
    assert headers["content-range"] == f"bytes 990-999/{len(body)}"


def test_get_range_beyond_end_unsatisfiable(server, project_id, sample_blob):
    key, body = sample_blob
    status, headers, data = _request_headers(
        server, project_id, "GET", f"/blobs/{key}", {"Range": "bytes=1000-"}
    )
    assert status == 416
    assert headers["content-range"] == f"bytes */{len(body)}"
    assert data == b""


@pytest.mark.parametrize(
    "range_header",
    [
        "bytes=5-2",  # inverted
        "items=0-1",  # unsupported unit
        "bytes=0-1,5-6",  # multiple ranges
        "bytes=abc",  # unparsable
    ],
)
def test_get_ignores_unsupported_ranges(server, project_id, sample_blob, range_header):
    """Ranges we don't support fall back to serving the whole blob."""
    key, body = sample_blob
    status, headers, data = _request_headers(
        server, project_id, "GET", f"/blobs/{key}", {"Range": range_header}
    )
    assert status == 200
    assert data == body
    assert "content-range" not in headers


def test_empty_blob_ranges(server, project_id):
    """An empty blob serves a zero-length 200, and any range is unsatisfiable."""
    key = _put_blob(server, project_id, b"")

    status, headers, data = _request_headers(server, project_id, "GET", f"/blobs/{key}")
    assert status == 200
    assert headers["content-length"] == "0"
    assert data == b""

    status, headers, _ = _request_headers(
        server, project_id, "GET", f"/blobs/{key}", {"Range": "bytes=0-"}
    )
    assert status == 416
    assert headers["content-range"] == "bytes */0"


def test_options_advertises_range_cors(server, project_id):
    """Preflight allows the Range header and exposes the range response headers."""
    key = "0" * 64
    status, headers, _ = _request_headers(
        server, project_id, "OPTIONS", f"/blobs/{key}"
    )
    assert status == 204
    assert "range" in headers["access-control-allow-headers"].split(",")
    exposed = headers["access-control-expose-headers"].split(",")
    assert "content-range" in exposed
