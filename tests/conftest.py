import json
import os
import signal
import subprocess
import time
import uuid
from contextlib import contextmanager

import pytest
import support.cli as cli
from support.executor import TestExecutor
from support.manifest import manifest

SERVER_PORT = os.environ.get("COFLUX_TEST_PORT", "7777")
SERVER_TOKEN = os.environ.get("COFLUX_SERVER_TOKEN", "")
ADAPTER_SCRIPT = os.path.join(os.path.dirname(__file__), "support", "adapter.py")


@pytest.fixture
def project_id():
    return f"test-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def worker(project_id, tmp_path):
    _worker_count = [0]

    @contextmanager
    def _worker(
        targets,
        handler=None,
        modules=None,
        concurrency=1,
        provides=None,
        workspace="default",
        create_workspace=True,
    ):
        if handler is None:
            handler = lambda eid, target, args: None
        modules = modules or ["test"]
        manifest_json = json.dumps(manifest(targets))
        _worker_count[0] += 1
        socket_path = str(tmp_path / f"executor-{_worker_count[0]}.sock")
        host = f"{project_id}.localhost:{SERVER_PORT}"

        env = os.environ.copy()
        env["COFLUX_SERVER_HOST"] = host
        env["COFLUX_SERVER_TOKEN"] = SERVER_TOKEN
        env["COFLUX_WORKSPACE"] = workspace
        env["COFLUX_TEST_MANIFEST"] = manifest_json
        env["COFLUX_TEST_SOCKET"] = socket_path
        env["COFLUX_LOGS_STORE_FLUSH_INTERVAL"] = "0"

        # Create the workspace (each test gets a fresh project)
        if create_workspace:
            cli.workspaces_create(workspace, env=env)

        # Start executor socket server
        executor = TestExecutor(socket_path)
        executor.start()

        adapter = f"python3,{ADAPTER_SCRIPT}"
        worker_proc = cli.worker(
            modules,
            adapter,
            concurrency=concurrency,
            provides=provides,
            env=env,
        )

        def worker_stderr():
            if worker_proc.stderr:
                return worker_proc.stderr.read().decode()
            return ""

        try:
            # Accept connections from test-adapter shim instances (one per
            # concurrency slot). The pool spawns executors sequentially,
            # waiting for each to be ready before starting the next.
            try:
                for _ in range(concurrency):
                    executor.accept(count=1, timeout=15)
                    executor.connections[-1].send_ready()
            except OSError as e:
                raise OSError(
                    f"{e}\n--- worker stderr ---\n{worker_stderr()}"
                ) from None

            class WorkerContext:
                def __init__(self):
                    self.env = env
                    self.executor = executor
                    self.project_id = project_id

                def handle_one(self, conn_index=0):
                    """Handle one execution on the specified connection."""
                    executor.connections[conn_index].run_one(handler)

                def submit(self, target, *arguments):
                    """Submit a workflow and return the parsed JSON response."""
                    return cli.submit(target, *arguments, env=env)

                def result(self, run_id, timeout=10):
                    """Poll for a run result."""
                    deadline = time.time() + timeout
                    last_error = None
                    interval = 0.05
                    while time.time() < deadline:
                        try:
                            return cli.runs_result(run_id, env=env)
                        except (
                            subprocess.CalledProcessError,
                            json.JSONDecodeError,
                        ) as e:
                            last_error = e
                            time.sleep(interval)
                            interval = min(interval * 2, 0.5)
                    raise TimeoutError(
                        f"run {run_id} did not complete within {timeout}s"
                        f" (last error: {last_error})"
                    )

                def inspect(self, run_id):
                    """Get the full run topic snapshot."""
                    return cli.runs_inspect(run_id, env=env)

                def rerun(self, step_id):
                    """Re-run a step and return the parsed JSON response."""
                    return cli.runs_rerun(step_id, env=env)

                def cancel(self, execution_id):
                    """Cancel an execution."""
                    cli.runs_cancel(execution_id, env=env)

                def pause(self):
                    """Pause the workspace."""
                    cli.workspaces_pause(env=env)

                def resume(self):
                    """Resume the workspace."""
                    cli.workspaces_resume(env=env)

                def discover_modules(self, *modules):
                    """Discover targets from local code."""
                    adapter_arg = f"python3,{ADAPTER_SCRIPT}"
                    return cli.manifests_discover(
                        *modules, adapter=adapter_arg, env=env
                    )

                def archive_module(self, module):
                    """Archive a module."""
                    cli.manifests_archive(module, env=env)

                def inspect_manifests(self):
                    """Get the current manifests for the workspace."""
                    return cli.manifests_inspect(env=env)

                def inspect_asset(self, asset_id):
                    """Get asset metadata by ID."""
                    return cli.assets_inspect(asset_id, env=env)

                def download_asset(self, asset_id, dest_dir):
                    """Download asset files to a directory."""
                    cli.assets_download(asset_id, dest_dir, env=env)

                def get_blob(self, key, output_path):
                    """Download a blob by key to a file."""
                    cli.blobs_get(key, output_path, env=env)

                def logs(
                    self,
                    run_id,
                    step_attempt=None,
                    json_output=True,
                    min_entries=None,
                    timeout=5,
                ):
                    """Fetch logs, polling until min_entries are available."""
                    deadline = time.time() + timeout if min_entries else 0
                    while True:
                        data = cli.logs_get(
                            run_id,
                            step_attempt=step_attempt,
                            env=env,
                            json_output=json_output,
                        )
                        if not min_entries or time.time() >= deadline:
                            return data
                        if json_output:
                            if len(data.get("logs", [])) >= min_entries:
                                return data
                        else:
                            if data.strip():
                                return data
                        time.sleep(0.05)

                def run(self, target, *arguments):
                    """Submit, handle one execution, and return the run result.

                    Returns a dict with either {"value": <data>} or
                    {"error": {"type": ..., "message": ...}}.
                    """
                    resp = self.submit(target, *arguments)
                    self.handle_one()
                    run_result = self.result(resp["runId"])
                    if run_result["type"] == "value":
                        return {"value": run_result["value"]["data"]}
                    elif run_result["type"] == "error":
                        err = run_result["error"]
                        return {
                            "error": {
                                k: v for k, v in err.items() if k in ("type", "message")
                            }
                        }
                    return run_result

            ctx = WorkerContext()
            yield ctx

        finally:
            worker_proc.send_signal(signal.SIGTERM)
            try:
                worker_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker_proc.kill()
                worker_proc.wait(timeout=5)
            executor.close()

    return _worker
