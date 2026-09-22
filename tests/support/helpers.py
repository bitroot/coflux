"""Shared helpers for tests that manage workers directly."""

import hashlib
import json
import os
import signal
import subprocess
import time
import urllib.request
from contextlib import contextmanager

from . import cli
from .executor import Executor
from .manifest import manifest
from .server import SUPER_TOKEN

ADAPTER_SCRIPT = os.path.join(os.path.dirname(__file__), "adapter.py")


def poll_result(
    run_id, host, workspace="default", timeout=15, interval=0.1, max_interval=1.0
):
    """Poll for a run result until it completes or times out."""
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            return cli.runs_result(run_id, host=host, workspace=workspace)
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            last_error = e
            time.sleep(interval)
            interval = min(interval * 2, max_interval)
    raise TimeoutError(
        f"run {run_id} did not complete within {timeout}s (last error: {last_error})"
    )


def get_topic(host, *route):
    """Capture a topic snapshot over the REST endpoint."""
    url = f"http://{host}/topics/" + "/".join(str(part) for part in route)
    with urllib.request.urlopen(urllib.request.Request(url), timeout=5) as resp:
        return json.load(resp)


def workspace_id(host, name="default"):
    """The external id of the workspace called ``name``."""
    workspaces = get_topic(host, "workspaces")
    return next(k for k, v in workspaces.items() if v["name"] == name)


def api_post(port, project_id, path, token=None, body=None):
    """POST to a server management API endpoint, returning the decoded reply."""
    if token is None:
        token = SUPER_TOKEN
    url = f"http://{project_id}.localhost:{port}/api/{path}"
    req = urllib.request.Request(
        url,
        method="POST",
        data=json.dumps(body if body is not None else {}).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        content = resp.read()
    return json.loads(content) if content else {}


def put_blob(port, project_id, content, token=None):
    """Store a blob the way a browser would: PUT to its own content hash."""
    if token is None:
        token = SUPER_TOKEN
    key = hashlib.sha256(content).hexdigest()
    url = f"http://{project_id}.localhost:{port}/blobs/{key}"
    req = urllib.request.Request(
        url,
        method="PUT",
        data=content,
        headers={"Authorization": f"Bearer {token}"},
    )
    urllib.request.urlopen(req, timeout=10)
    return key


@contextmanager
def managed_worker(
    targets,
    host,
    worker_dir,
    workspace="default",
    concurrency=1,
    modules=None,
    provides=None,
    accepts=None,
):
    """Start a worker with an executor and clean up on exit.

    Yields an Executor. The caller is responsible for interacting
    with the executor and polling for results.
    """
    if modules is None:
        modules = ["test"]
    worker_dir.mkdir(exist_ok=True)
    socket_path = str(worker_dir / "executor.sock")
    manifest_path = str(worker_dir / "manifest.json")

    with open(manifest_path, "w") as f:
        json.dump(manifest(targets), f)

    executor = Executor(socket_path)
    executor.start()

    adapter = (
        f"python3,{ADAPTER_SCRIPT},--manifest,{manifest_path},--socket,{socket_path}"
    )
    worker_proc = cli.worker(
        modules,
        adapter,
        concurrency=concurrency,
        provides=provides,
        accepts=accepts,
        host=host,
        workspace=workspace,
        env_vars={"logs_store_flush_interval": "0"},
    )

    def worker_stderr():
        if worker_proc.stderr:
            return worker_proc.stderr.read().decode()
        return ""

    try:
        try:
            warm_count = min(concurrency, 4)
            executor.wait_connections(warm_count, timeout=15)
        except (OSError, TimeoutError) as e:
            raise OSError(f"{e}\n--- worker stderr ---\n{worker_stderr()}") from None
        yield executor
    finally:
        worker_proc.send_signal(signal.SIGTERM)
        try:
            worker_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            worker_proc.kill()
            worker_proc.wait(timeout=5)
        executor.close()
