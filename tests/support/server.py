"""Managed Elixir server for integration tests."""

import hashlib
import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid

_SERVER_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "server")


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_ready(port, token=None, timeout=15):
    """Wait until the server is accepting HTTP requests.

    When the server has ``COFLUX_REQUIRE_AUTH=true``, the discover endpoint
    returns 401 without a token.  We accept any non-connection-error response
    (including 401) as proof the server is up, but if *token* is supplied we
    send it so we get a clean 200.
    """
    url = f"http://127.0.0.1:{port}/api/discover"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            headers = {"Host": f"healthcheck.localhost:{port}"}
            if token:
                headers["Authorization"] = f"Bearer {token}"
            req = urllib.request.Request(url, headers=headers)
            urllib.request.urlopen(req, timeout=1)
            return
        except urllib.error.HTTPError:
            # Any HTTP response (including 401/403) means the server is up.
            return
        except (urllib.error.URLError, OSError):
            time.sleep(0.1)
    raise TimeoutError(f"server not ready on port {port} within {timeout}s")


SUPER_TOKEN = "test-super-token"


class ManagedServer:
    """Start/stop a Coflux server for tests.

    Runs the Elixir server directly when the source is available,
    or via Docker when COFLUX_IMAGE is set (e.g. in CI).
    Auth is disabled so tests don't need tokens for normal operations.
    A super token is configured for management endpoints (rotate, etc.).

    Pass ``extra_env`` to override or extend the default environment
    variables (e.g. to enable authentication).
    """

    def __init__(self, data_dir, port=None, extra_env=None):
        self.port = port or _find_free_port()
        self.data_dir = data_dir
        self._proc = None
        self._container = None
        self._image = os.environ.get("COFLUX_IMAGE")
        self._extra_env = extra_env or {}

    def start(self, timeout=30):
        if self._image:
            self._start_docker(self._image, timeout)
        else:
            self._start_local(timeout)

    def _start_local(self, timeout):
        cli_path = os.path.abspath(os.environ.get("COFLUX_BIN", "coflux"))
        env = {
            "PATH": os.environ["PATH"],
            "HOME": os.environ.get("HOME", "/tmp"),
            "PORT": str(self.port),
            "COFLUX_DATA_DIR": self.data_dir,
            "COFLUX_PUBLIC_HOST": "%.localhost:" + str(self.port),
            "COFLUX_REQUIRE_AUTH": "false",
            "COFLUX_LAUNCHER_TYPES": "process,docker",
            "COFLUX_SUPER_TOKEN_HASH": hashlib.sha256(SUPER_TOKEN.encode()).hexdigest(),
            "COFLUX_CLI_PATH": cli_path,
            **self._extra_env,
        }
        self._proc = subprocess.Popen(
            ["elixir", "-S", "mix", "run", "--no-halt"],
            cwd=_SERVER_DIR,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _wait_for_ready(self.port, timeout=timeout)

    def _start_docker(self, image, timeout):
        self._container = f"coflux-test-{uuid.uuid4().hex[:8]}"
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                self._container,
                "-p",
                f"{self.port}:7777",
                "-v",
                f"{self.data_dir}:/data",
                "-e",
                "COFLUX_PUBLIC_HOST=%.localhost:7777",
                "-e",
                "COFLUX_REQUIRE_AUTH=false",
                "-e",
                f"COFLUX_SUPER_TOKEN_HASH={hashlib.sha256(SUPER_TOKEN.encode()).hexdigest()}",
                image,
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        _wait_for_ready(self.port, timeout=timeout)

    def stop(self):
        if self._container:
            subprocess.run(
                ["docker", "rm", "-f", self._container],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            self._container = None
        elif self._proc:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait()
            self._proc = None

    def restart(self, timeout=30):
        self.stop()
        self.start(timeout=timeout)
