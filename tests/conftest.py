import json
import subprocess
import tempfile
import time
import uuid
from contextlib import contextmanager

import pytest
import support.cli as cli
from support.helpers import ADAPTER_SCRIPT, managed_worker
from support.server import ManagedServer


class WorkerContext:
    def __init__(self, host, workspace, executor, handler, project_id, worker_dir):
        self.host = host
        self.workspace = workspace
        self.executor = executor
        self.project_id = project_id
        self._handler = handler
        self._worker_dir = worker_dir

    def handle_one(self):
        """Handle one execution."""
        conn, eid, target, args = self.executor.next_execute()
        response = self._handler(eid, target, args)
        if response is not None:
            conn.send(response)

    def submit(self, target, *arguments, idempotency_key=None):
        """Submit a workflow and return the parsed JSON response."""
        return cli.submit(
            target,
            *arguments,
            idempotency_key=idempotency_key,
            host=self.host,
            workspace=self.workspace,
        )

    def result(self, run_id, timeout=10):
        """Poll for a run result."""
        deadline = time.time() + timeout
        last_error = None
        interval = 0.05
        while time.time() < deadline:
            try:
                return cli.runs_result(run_id, host=self.host, workspace=self.workspace)
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
        return cli.runs_inspect(run_id, host=self.host, workspace=self.workspace)

    def rerun(self, step_id):
        """Re-run a step and return the parsed JSON response."""
        return cli.runs_rerun(step_id, host=self.host, workspace=self.workspace)

    def cancel(self, execution_id):
        """Cancel an execution."""
        cli.runs_cancel(execution_id, host=self.host, workspace=self.workspace)

    def pause(self):
        """Pause the workspace."""
        cli.workspaces_pause(host=self.host, workspace=self.workspace)

    def resume(self):
        """Resume the workspace."""
        cli.workspaces_resume(host=self.host, workspace=self.workspace)

    def discover_modules(self, *modules):
        """Discover targets from local code."""
        manifest_path = self._worker_dir / "manifest.json"
        socket_path = self._worker_dir / "executor.sock"
        adapter = f"python3,{ADAPTER_SCRIPT},--manifest,{manifest_path},--socket,{socket_path}"
        return cli.manifests_discover(
            *modules, adapter=adapter, host=self.host, workspace=self.workspace
        )

    def archive_module(self, module):
        """Archive a module."""
        cli.manifests_archive(module, host=self.host, workspace=self.workspace)

    def inspect_manifests(self):
        """Get the current manifests for the workspace."""
        return cli.manifests_inspect(host=self.host, workspace=self.workspace)

    def inspect_asset(self, asset_id):
        """Get asset metadata by ID."""
        return cli.assets_inspect(asset_id, host=self.host, workspace=self.workspace)

    def download_asset(self, asset_id, dest_dir):
        """Download asset files to a directory."""
        cli.assets_download(
            asset_id, dest_dir, host=self.host, workspace=self.workspace
        )

    def get_blob(self, key, output_path):
        """Download a blob by key to a file."""
        cli.blobs_get(key, output_path, host=self.host, workspace=self.workspace)

    def logs(
        self,
        run_id,
        step_attempt=None,
        from_ts=None,
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
                from_ts=from_ts,
                host=self.host,
                workspace=self.workspace,
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
            value = run_result["value"]
            if value["type"] == "raw":
                return {"value": value["data"]}
            elif value["type"] == "blob":
                blob_file = str(self._worker_dir / f"blob-{value['key'][:16]}")
                self.get_blob(value["key"], blob_file)
                with open(blob_file) as f:
                    return {"value": json.load(f)}
        elif run_result["type"] == "error":
            err = run_result["error"]
            return {"error": {k: v for k, v in err.items() if k in ("type", "message")}}
        return run_result


def pytest_configure(config):
    """Start a shared test server (runs on the controller and in non-xdist mode)."""
    if not hasattr(config, "workerinput"):
        data_dir = tempfile.mkdtemp(prefix="coflux-test-server-")
        srv = ManagedServer(data_dir)
        srv.start()
        config._server = srv


def pytest_configure_node(node):
    """Pass server port to each xdist worker."""
    node.workerinput["server_port"] = node.config._server.port


def pytest_unconfigure(config):
    """Stop the shared test server (runs on the controller and in non-xdist mode)."""
    srv = getattr(config, "_server", None)
    if srv:
        srv.stop()


@pytest.fixture(scope="session")
def server(request):
    if hasattr(request.config, "workerinput"):
        # xdist worker — server is running on the controller.
        port = request.config.workerinput["server_port"]
        return ManagedServer("", port=port)
    else:
        # No xdist — server was started in pytest_configure.
        return request.config._server


@pytest.fixture
def project_id():
    return f"test-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def worker(server, project_id, tmp_path):
    _worker_count = 0

    @contextmanager
    def _worker(
        targets,
        handler=None,
        modules=None,
        concurrency=1,
        provides=None,
        workspace="default",
    ):
        nonlocal _worker_count
        if handler is None:
            handler = lambda eid, target, args: None
        _worker_count += 1

        host = f"{project_id}.localhost:{server.port}"
        worker_dir = tmp_path / f"worker-{_worker_count}"

        with managed_worker(
            targets,
            host,
            worker_dir,
            workspace=workspace,
            concurrency=concurrency,
            modules=modules,
            provides=provides,
        ) as executor:
            ctx = WorkerContext(
                host, workspace, executor, handler, project_id, worker_dir
            )
            yield ctx

    return _worker
