"""Secrets: set, listed and deleted by name, with values never shown."""

import json
import subprocess

import pytest
from support import cli


class TestSecrets:
    def test_set_list_delete(self, server, project_id):
        host = f"{project_id}.localhost:{server.port}"

        # The default scope is the current workspace.
        cli.secrets_set("api-key", "first", host=host)
        [secret] = cli.secrets_list(host=host)
        assert secret["name"] == "api-key"
        assert secret["scope"] == "default"
        assert secret["version"] == 1

        # Setting it again replaces the value and bumps the version.
        cli.secrets_set("api-key", "second\n", host=host)
        [secret] = cli.secrets_list(host=host)
        assert secret["version"] == 2

        cli.secrets_set("shared", "x", global_=True, host=host)
        cli.secrets_set("shared", "y", scope="development", host=host)
        listed = {(s["scope"], s["name"]) for s in cli.secrets_list(host=host)}
        assert listed == {
            ("default", "api-key"),
            ("", "shared"),
            ("development", "shared"),
        }

        # Values are nowhere in the listing.
        assert "first" not in json.dumps(cli.secrets_list(host=host))
        assert "second" not in json.dumps(cli.secrets_list(host=host))

        cli.secrets_delete("shared", global_=True, host=host)
        cli.secrets_delete("api-key", host=host)
        listed = {(s["scope"], s["name"]) for s in cli.secrets_list(host=host)}
        assert listed == {("development", "shared")}

        with pytest.raises(subprocess.CalledProcessError):
            cli.secrets_delete("api-key", host=host)

    def test_names_and_scopes_are_validated(self, server, project_id):
        host = f"{project_id}.localhost:{server.port}"

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            cli.secrets_set("not valid!", "v", host=host)
        assert "bad_request" in exc_info.value.stderr

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            cli.secrets_set("ok", "v", scope="trailing/", host=host)
        assert "bad_request" in exc_info.value.stderr

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            cli.secrets_set("ok", "", host=host)
        assert "no value given" in exc_info.value.stderr
