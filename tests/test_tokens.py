"""Service tokens.

Tokens live in the admin store, which isn't rotated, so a token outlives
epoch rotations and server restarts, and is gone for good once revoked.
"""

import json
import tempfile
import urllib.error
import urllib.request
import uuid

import pytest
from support.helpers import api_post
from support.server import ManagedServer


@pytest.fixture(scope="module")
def token_server():
    """A server with authentication on and a secret to sign tokens with."""
    data_dir = tempfile.mkdtemp(prefix="coflux-test-tokens-")
    srv = ManagedServer(
        data_dir,
        extra_env={
            "COFLUX_REQUIRE_AUTH": "true",
            "COFLUX_SECRET": "test-secret-for-service-tokens",
        },
    )
    srv.start(timeout=30)
    yield srv
    srv.stop()


def _discover(port, project_id, token):
    """What a token is granted: (status, workspaces or None)."""
    req = urllib.request.Request(
        f"http://{project_id}.localhost:{port}/api/discover",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read())["access"]["workspaces"]
    except urllib.error.HTTPError as e:
        return e.code, None


def _create(port, project_id, token=None, **body):
    return api_post(port, project_id, "create_token", token=token, body=body)


def _revoke(port, project_id, external_id, token=None):
    """Returns the HTTP status."""
    try:
        api_post(
            port,
            project_id,
            "revoke_token",
            token=token,
            body={"externalId": external_id},
        )
        return 204
    except urllib.error.HTTPError as e:
        return e.code


class TestServiceTokens:
    def test_token_grants_its_workspaces_until_revoked(self, token_server):
        port = token_server.port
        project_id = f"tok-{uuid.uuid4().hex[:8]}"

        created = _create(port, project_id, name="ci", workspaces=["staging"])
        assert _discover(port, project_id, created["token"]) == (200, ["staging"])

        assert _revoke(port, project_id, created["externalId"]) == 204
        assert _discover(port, project_id, created["token"]) == (401, None)

    def test_token_outlives_rotation_and_restart(self, token_server):
        """The admin store isn't an epoch: rotating one, or restarting the
        server, changes nothing about which tokens exist."""
        port = token_server.port
        project_id = f"tok-{uuid.uuid4().hex[:8]}"

        created = _create(port, project_id, name="deploy")
        api_post(port, project_id, "rotate_epoch")
        assert _discover(port, project_id, created["token"]) == (200, ["*"])

        token_server.restart(timeout=30)
        assert _discover(port, project_id, created["token"]) == (200, ["*"])

    def test_setting_a_secret_takes_operator_access_to_its_scope(self, token_server):
        """A token for 'development/*' can set a secret for a development
        workspace, but not for the project or for production."""
        port = token_server.port
        project_id = f"tok-{uuid.uuid4().hex[:8]}"
        restricted = _create(port, project_id, name="dev", workspaces=["development/*"])

        def set_secret(scope):
            try:
                api_post(
                    port,
                    project_id,
                    "set_secret",
                    token=restricted["token"],
                    body={"name": "key", "scope": scope, "value": "v"},
                )
                return 200
            except urllib.error.HTTPError as e:
                return e.code

        assert set_secret("development/joe") == 200
        assert set_secret("") == 403
        assert set_secret("production") == 403

    def test_only_the_creator_or_full_access_can_revoke(self, token_server):
        """Who created a token is kept with it, and resolved to a principal
        in whichever epoch asks - here, after a rotation."""
        port = token_server.port
        project_id = f"tok-{uuid.uuid4().hex[:8]}"

        parent = _create(port, project_id, name="parent", workspaces=["staging"])
        other = _create(port, project_id, name="other", workspaces=["staging"])
        child = _create(port, project_id, token=parent["token"], name="child")

        api_post(port, project_id, "rotate_epoch")

        assert (
            _revoke(port, project_id, child["externalId"], token=other["token"]) == 403
        )
        assert _discover(port, project_id, child["token"]) == (200, ["staging"])
        assert (
            _revoke(port, project_id, child["externalId"], token=parent["token"]) == 204
        )
        assert _discover(port, project_id, child["token"]) == (401, None)
