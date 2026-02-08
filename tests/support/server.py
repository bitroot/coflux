"""Managed Elixir server for integration tests."""

import os
import socket
import subprocess
import time
import uuid

_SERVER_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "server")


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)
    raise TimeoutError(f"server not ready on port {port} within {timeout}s")


class ManagedServer:
    """Start/stop a Coflux server for tests.

    Runs the Elixir server directly when the source is available,
    or via Docker when COFLUX_IMAGE is set (e.g. in CI).
    Auth is disabled so tests don't need tokens.
    """

    def __init__(self, data_dir, port=None):
        self.port = port or _find_free_port()
        self.data_dir = data_dir
        self._proc = None
        self._container = None
        self._image = os.environ.get("COFLUX_IMAGE")

    def start(self, timeout=30):
        if self._image:
            self._start_docker(self._image, timeout)
        else:
            self._start_local(timeout)

    def _start_local(self, timeout):
        env = {
            "PATH": os.environ["PATH"],
            "HOME": os.environ.get("HOME", "/tmp"),
            "PORT": str(self.port),
            "COFLUX_DATA_DIR": self.data_dir,
            "COFLUX_BASE_DOMAIN": "localhost",
            "COFLUX_REQUIRE_AUTH": "false",
        }
        self._proc = subprocess.Popen(
            ["elixir", "-S", "mix", "run", "--no-halt"],
            cwd=_SERVER_DIR,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _wait_for_port(self.port, timeout=timeout)

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
                "COFLUX_BASE_DOMAIN=localhost",
                "-e",
                "COFLUX_REQUIRE_AUTH=false",
                image,
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        _wait_for_port(self.port, timeout=timeout)

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
