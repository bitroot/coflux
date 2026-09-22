"""Service tokens.

Tokens live in the admin store, which isn't rotated, so a token outlives
epoch rotations and server restarts, and is gone for good once revoked.
"""

import json
import urllib.error
import urllib.request
import uuid

import pytest
from support.helpers import api_post
from support.server import ManagedServer


@pytest.fixture(scope="module")
def token_server(tmp_path_factory):
    """A server with authentication on and a secret to sign tokens with."""
    srv = ManagedServer(
        str(tmp_path_factory.mktemp("token-server")),
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

    def _set_secret(self, port, project_id, token, workspaces):
        """The status of setting a secret for some workspace patterns - a
        convenient probe for what a grant contains."""
        try:
            api_post(
                port,
                project_id,
                "set_secret",
                token=token,
                body={"name": "key", "workspaces": workspaces, "value": "v"},
            )
            return 200
        except urllib.error.HTTPError as e:
            return e.code

    def test_setting_a_secret_takes_a_grant_containing_the_whole_scope(self, token_server):
        """A token for 'development/*' can set a secret for that, or for
        anything inside it - but not for the project, and not for a
        workspace outside it."""
        port = token_server.port
        project_id = f"tok-{uuid.uuid4().hex[:8]}"
        restricted = _create(port, project_id, name="dev", workspaces=["development/*"])

        def set_secret(*workspaces):
            return self._set_secret(port, project_id, restricted["token"], list(workspaces))

        assert set_secret("development/*") == 200
        assert set_secret("development/joe") == 200
        assert set_secret("development/joe/*") == 200

        assert set_secret("*") == 403
        assert set_secret("production") == 403
        assert set_secret("development") == 403

        # Every pattern has to be allowed, not just one of them.
        assert set_secret("development/joe", "production") == 403

    def test_holding_a_workspace_is_not_holding_a_scope_that_reaches_past_it(
        self, token_server
    ):
        """The case that used to leak: a token for exactly 'staging' can
        set a secret there, but not one reaching the workspaces beneath
        it, which it has no access to."""
        port = token_server.port
        project_id = f"tok-{uuid.uuid4().hex[:8]}"
        restricted = _create(port, project_id, name="staging", workspaces=["staging"])

        def set_secret(*workspaces):
            return self._set_secret(port, project_id, restricted["token"], list(workspaces))

        assert set_secret("staging") == 200

        assert set_secret("staging/*") == 403
        assert set_secret("staging/feature-1") == 403

    def test_a_token_cannot_be_granted_more_than_its_creator_has(self, token_server):
        """A token can hand on a scope its own contains, and no more."""
        port = token_server.port
        project_id = f"tok-{uuid.uuid4().hex[:8]}"
        parent = _create(port, project_id, name="parent", workspaces=["development/*"])

        def create(workspaces):
            try:
                _create(
                    port,
                    project_id,
                    token=parent["token"],
                    name="child",
                    workspaces=workspaces,
                )
                return 200
            except urllib.error.HTTPError as e:
                return e.code

        assert create(["development/*"]) == 200
        assert create(["development/joe"]) == 200
        assert create(["development/joe/*"]) == 200

        assert create(["*"]) == 403
        assert create(["development"]) == 403
        assert create(["production"]) == 403
        assert create(["development-2/*"]) == 403

        # A pattern that names no scope is rejected outright.
        assert create([""]) == 400

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
