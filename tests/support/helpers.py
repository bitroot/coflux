"""Shared helpers for tests that manage workers directly."""

import json
import os
import signal
import subprocess
import time
from contextlib import contextmanager

from . import cli
from .executor import Executor
from .manifest import manifest

ADAPTER_SCRIPT = os.path.join(os.path.dirname(__file__), "adapter.py")


def make_env(host):
    """Build a minimal env dict for CLI commands."""
    return {
        "PATH": os.environ["PATH"],
        "COFLUX_SERVER_HOST": host,
        "COFLUX_WORKSPACE": "default",
    }


def poll_result(run_id, env, timeout=15):
    """Poll for a run result until it completes or times out."""
    deadline = time.time() + timeout
    interval = 0.1
    while time.time() < deadline:
        try:
            return cli.runs_result(run_id, env=env)
        except (subprocess.CalledProcessError, json.JSONDecodeError):
            time.sleep(interval)
            interval = min(interval * 2, 1.0)
    raise TimeoutError(f"run {run_id} did not complete within {timeout}s")


@contextmanager
def managed_worker(targets, env, socket_path, concurrency=1):
    """Start a worker with an executor and clean up on exit.

    Yields (executor, worker_proc). The caller is responsible for interacting
    with the executor and polling for results.
    """
    manifest_json = json.dumps(manifest(targets))
    env = {
        **env,
        "COFLUX_TEST_MANIFEST": manifest_json,
        "COFLUX_TEST_SOCKET": socket_path,
        "COFLUX_LOGS_STORE_FLUSH_INTERVAL": "0",
    }

    executor = Executor(socket_path)
    executor.start()

    adapter = f"python3,{ADAPTER_SCRIPT}"
    worker_proc = cli.worker(["test"], adapter, concurrency=concurrency, env=env)

    try:
        warm_count = min(concurrency, 4)
        executor.wait_connections(warm_count, timeout=15)
        yield executor
    finally:
        worker_proc.send_signal(signal.SIGTERM)
        try:
            worker_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            worker_proc.kill()
            worker_proc.wait(timeout=5)
        executor.close()
