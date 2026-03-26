"""Tests for server authentication mechanisms.

Covers Studio JWT auth, super token auth, and unauthenticated access
control. A dedicated server instance is started with authentication
enabled so these tests don't interfere with the rest of the suite.
"""

import json
import tempfile
import time
import urllib.error
import urllib.request
import uuid

import jwt as pyjwt
import pytest
from support.jwks import JWKSServer, generate_keypair, mint_jwt
from support.server import SUPER_TOKEN, ManagedServer

TEAM_ID = "test-team-1"
KID = "test-key-1"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def keypair():
    """Ed25519 keypair for the whole module."""
    return generate_keypair(kid=KID)


@pytest.fixture(scope="module")
def jwks_server(keypair):
    """JWKS HTTP server serving the test public key."""
    _private_key, jwk = keypair
    srv = JWKSServer([jwk])
    srv.start()
    yield srv
    srv.stop()


@pytest.fixture(scope="module")
def auth_server(jwks_server):
    """A Coflux server with authentication enabled."""
    data_dir = tempfile.mkdtemp(prefix="coflux-test-auth-")
    srv = ManagedServer(
        data_dir,
        extra_env={
            "COFLUX_REQUIRE_AUTH": "true",
            "COFLUX_STUDIO_TEAMS": TEAM_ID,
            "COFLUX_STUDIO_URL": jwks_server.url,
            "COFLUX_SECRET": "test-secret-for-service-tokens",
        },
    )
    srv.start(timeout=30)
    yield srv
    srv.stop()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _api_request(server_port, project_id, path, *, token=None, body=None):
    """Make an HTTP request to the server API.

    Returns ``(status, parsed_json_or_None)``.
    """
    url = f"http://127.0.0.1:{server_port}/api/{path}"
    headers = {"Host": f"{project_id}.localhost:{server_port}"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        raw = resp.read()
        return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            resp_body = json.loads(raw) if raw else None
        except Exception:
            resp_body = None
        return e.code, resp_body


def _discover(server_port, project_id, token=None):
    """Call the discover endpoint."""
    return _api_request(server_port, project_id, "discover", token=token)


def _make_host(project_id, port):
    return f"{project_id}.localhost:{port}"


def _mint(keypair, auth_server, project_id, **kwargs):
    """Convenience: mint a JWT for the given project against the test server."""
    private_key, _jwk = keypair
    defaults = dict(
        kid=KID,
        issuer=auth_server._extra_env["COFLUX_STUDIO_URL"],
        team_id=TEAM_ID,
        host=_make_host(project_id, auth_server.port),
    )
    defaults.update(kwargs)
    return mint_jwt(private_key, **defaults)


# ---------------------------------------------------------------------------
# JWT Authentication — Happy Path
# ---------------------------------------------------------------------------


class TestJWTHappyPath:
    def test_valid_jwt_accepted(self, keypair, auth_server):
        """A correctly signed JWT with valid claims is accepted."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"
        token = _mint(keypair, auth_server, project_id)
        status, body = _discover(auth_server.port, project_id, token=token)
        assert status == 200
        assert "version" in body
        assert body["access"]["workspaces"] == ["*"]

    def test_jwt_workspace_restriction(self, keypair, auth_server):
        """The ``workspaces`` claim limits the reported access."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"
        token = _mint(keypair, auth_server, project_id, workspaces=["staging", "dev/*"])
        status, body = _discover(auth_server.port, project_id, token=token)
        assert status == 200
        assert set(body["access"]["workspaces"]) == {"staging", "dev/*"}

    def test_jwt_creates_principal(self, keypair, auth_server):
        """Repeated requests with the same ``sub`` claim are idempotent."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"
        subject = f"user-{uuid.uuid4().hex[:8]}"
        for _ in range(2):
            token = _mint(keypair, auth_server, project_id, subject=subject)
            status, _body = _discover(auth_server.port, project_id, token=token)
            assert status == 200

    def test_jwt_default_workspaces(self, keypair, auth_server):
        """When the ``workspaces`` claim is absent, all workspaces are accessible."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"
        token = _mint(keypair, auth_server, project_id)
        status, body = _discover(auth_server.port, project_id, token=token)
        assert status == 200
        assert body["access"]["workspaces"] == ["*"]


# ---------------------------------------------------------------------------
# JWT Authentication — Rejection Cases
# ---------------------------------------------------------------------------


class TestJWTRejection:
    def test_expired_jwt(self, keypair, auth_server):
        """A JWT whose ``exp`` is in the past is rejected."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"
        token = _mint(keypair, auth_server, project_id, expires_in=-60)
        status, body = _discover(auth_server.port, project_id, token=token)
        assert status == 401
        assert body["error"] == "unauthorized"

    def test_not_yet_valid_jwt(self, keypair, auth_server):
        """A JWT whose ``nbf`` is in the future is rejected."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"
        token = _mint(
            keypair,
            auth_server,
            project_id,
            not_before=int(time.time()) + 3600,
        )
        status, body = _discover(auth_server.port, project_id, token=token)
        assert status == 401

    def test_wrong_issuer(self, keypair, auth_server):
        """A JWT with a non-matching ``iss`` is rejected."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"
        token = _mint(
            keypair,
            auth_server,
            project_id,
            issuer="https://evil.example.com",
        )
        status, body = _discover(auth_server.port, project_id, token=token)
        assert status == 401

    def test_wrong_team(self, keypair, auth_server):
        """A JWT with a team ID not in COFLUX_STUDIO_TEAMS is rejected."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"
        token = _mint(keypair, auth_server, project_id, team_id="unknown-team")
        status, body = _discover(auth_server.port, project_id, token=token)
        assert status == 401

    def test_wrong_host(self, keypair, auth_server):
        """A JWT whose ``aud`` host doesn't match the request host is rejected."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"
        private_key, _jwk = keypair
        token = mint_jwt(
            private_key,
            kid=KID,
            issuer=auth_server._extra_env["COFLUX_STUDIO_URL"],
            team_id=TEAM_ID,
            host="wrong-project.localhost:9999",
        )
        status, body = _discover(auth_server.port, project_id, token=token)
        assert status == 401

    def test_wrong_signature(self, keypair, auth_server):
        """A JWT signed with a different key is rejected."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"
        other_private_key, _other_jwk = generate_keypair(kid=KID)
        token = mint_jwt(
            other_private_key,
            kid=KID,
            issuer=auth_server._extra_env["COFLUX_STUDIO_URL"],
            team_id=TEAM_ID,
            host=_make_host(project_id, auth_server.port),
        )
        status, body = _discover(auth_server.port, project_id, token=token)
        assert status == 401

    def test_unknown_kid(self, keypair, auth_server):
        """A JWT with an unknown ``kid`` header is rejected."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"
        token = _mint(keypair, auth_server, project_id, kid="nonexistent-key")
        status, body = _discover(auth_server.port, project_id, token=token)
        assert status == 401

    def test_missing_audience(self, keypair, auth_server):
        """A JWT without an ``aud`` claim is rejected."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"
        private_key, _jwk = keypair
        now = int(time.time())
        claims = {
            "iss": auth_server._extra_env["COFLUX_STUDIO_URL"],
            "sub": "test-user",
            "exp": now + 3600,
            "iat": now,
        }
        token = pyjwt.encode(
            claims, private_key, algorithm="EdDSA", headers={"kid": KID}
        )
        status, body = _discover(auth_server.port, project_id, token=token)
        assert status == 401


# ---------------------------------------------------------------------------
# Super Token Authentication
# ---------------------------------------------------------------------------


class TestSuperToken:
    def test_super_token_accepted(self, auth_server):
        """The super token still works when Studio auth is also enabled."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"
        status, body = _discover(auth_server.port, project_id, token=SUPER_TOKEN)
        assert status == 200
        assert body["access"]["workspaces"] == ["*"]

    def test_invalid_super_token_rejected(self, auth_server):
        """A token that isn't the super token and isn't a JWT is rejected."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"
        status, body = _discover(
            auth_server.port, project_id, token="not-the-right-token"
        )
        assert status == 401

    def test_super_token_required_for_rotate_epoch(self, keypair, auth_server):
        """Admin endpoints reject non-super tokens (including valid JWTs)."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"

        # JWT should be forbidden
        jwt_token = _mint(keypair, auth_server, project_id)
        status, body = _api_request(
            auth_server.port,
            project_id,
            "rotate_epoch",
            token=jwt_token,
            body={},
        )
        assert status == 403

        # Super token should work
        status, _body = _api_request(
            auth_server.port,
            project_id,
            "rotate_epoch",
            token=SUPER_TOKEN,
            body={},
        )
        assert status == 204 or status == 200


# ---------------------------------------------------------------------------
# Unauthenticated Access
# ---------------------------------------------------------------------------


class TestUnauthenticated:
    def test_no_token_rejected(self, auth_server):
        """Without a token, ``COFLUX_REQUIRE_AUTH=true`` returns 401."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"
        status, body = _discover(auth_server.port, project_id)
        assert status == 401
        assert body["error"] == "unauthorized"


# ---------------------------------------------------------------------------
# JWKS Key Rotation
# ---------------------------------------------------------------------------


class TestKeyRotation:
    def test_new_key_accepted_after_rotation(self, keypair, auth_server, jwks_server):
        """When the JWKS is updated with a new key, JWTs signed with
        the new key are accepted (the server re-fetches on cache miss)."""
        project_id = f"auth-{uuid.uuid4().hex[:8]}"
        new_kid = "rotated-key-1"
        new_private_key, new_jwk = generate_keypair(kid=new_kid)
        _orig_private_key, orig_jwk = keypair

        # Add the new key to the JWKS (keep the old one too)
        jwks_server.set_keys([orig_jwk, new_jwk])

        token = mint_jwt(
            new_private_key,
            kid=new_kid,
            issuer=auth_server._extra_env["COFLUX_STUDIO_URL"],
            team_id=TEAM_ID,
            host=_make_host(project_id, auth_server.port),
        )
        status, body = _discover(auth_server.port, project_id, token=token)
        assert status == 200

        # Restore original keys for other tests
        jwks_server.set_keys([orig_jwk])
