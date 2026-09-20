"""Secrets: set, listed and deleted by name, with values never shown."""

import json
import subprocess

import pytest
from support import cli


class TestSecrets:
    def test_set_list_delete(self, server, project_id):
        host = f"{project_id}.localhost:{server.port}"

        cli.secrets_set("api-key", "first", workspaces="default", host=host)
        [secret] = cli.secrets_list(host=host)
        assert secret["name"] == "api-key"
        assert secret["scope"] == "default"
        assert secret["version"] == 1

        # Setting it again replaces the value and bumps the version.
        cli.secrets_set("api-key", "second\n", workspaces="default", host=host)
        [secret] = cli.secrets_list(host=host)
        assert secret["version"] == 2

        cli.secrets_set("shared", "x", workspaces="*", host=host)
        cli.secrets_set("shared", "y", workspaces="development/*", host=host)
        listed = {(s["scope"], s["name"]) for s in cli.secrets_list(host=host)}
        assert listed == {
            ("default", "api-key"),
            ("*", "shared"),
            ("development/*", "shared"),
        }

        # Values are nowhere in the listing.
        assert "first" not in json.dumps(cli.secrets_list(host=host))
        assert "second" not in json.dumps(cli.secrets_list(host=host))

        cli.secrets_delete("shared", workspaces="*", host=host)
        cli.secrets_delete("api-key", workspaces="default", host=host)
        listed = {(s["scope"], s["name"]) for s in cli.secrets_list(host=host)}
        assert listed == {("development/*", "shared")}

        with pytest.raises(subprocess.CalledProcessError):
            cli.secrets_delete("api-key", workspaces="default", host=host)

    def test_one_value_can_be_set_for_several_workspaces(self, server, project_id):
        """Commas set the same value once per pattern: each is listed,
        rotated and deleted on its own."""
        host = f"{project_id}.localhost:{server.port}"

        cli.secrets_set("api-key", "shared", workspaces="staging,production/*", host=host)
        listed = {(s["scope"], s["version"]) for s in cli.secrets_list(host=host)}
        assert listed == {("staging", 1), ("production/*", 1)}

        # Rotating one leaves the other where it was.
        cli.secrets_set("api-key", "rotated", workspaces="staging", host=host)
        listed = {(s["scope"], s["version"]) for s in cli.secrets_list(host=host)}
        assert listed == {("staging", 2), ("production/*", 1)}

        # Deleting takes the patterns it was actually set for.
        cli.secrets_delete("api-key", workspaces="staging,production/*", host=host)
        assert cli.secrets_list(host=host) == []

    def test_names_and_workspaces_are_validated(self, server, project_id):
        host = f"{project_id}.localhost:{server.port}"

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            cli.secrets_set("not valid!", "v", workspaces="default", host=host)
        assert "bad_request" in exc_info.value.stderr

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            cli.secrets_set("ok", "v", workspaces="trailing/", host=host)
        assert "bad_request" in exc_info.value.stderr

        # The workspaces are required - there is no implicit scope.
        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            cli._coflux(
                "secrets", "set", "ok", host=host, workspace="default", output=None, input="v"
            )
        assert "workspaces" in exc_info.value.stderr

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            cli.secrets_set("ok", "", workspaces="default", host=host)
        assert "no value given" in exc_info.value.stderr
