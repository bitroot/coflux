"""Tests for pool management and the process launcher.

A pool with a process launcher causes the server to automatically start
worker processes when executions are submitted for matching modules.
"""

import json
import subprocess

import pytest
import support.cli as cli
from support.executor import Executor
from support.helpers import ADAPTER_SCRIPT, poll_result
from support.manifest import manifest, workflow, task
from support.protocol import json_args


# Launcher-managed workers are slower to start than direct workers.
_LAUNCH_TIMEOUT = 30  # wait_connections: launcher startup + worker init
_EXEC_TIMEOUT = 15    # next_execute: execution dispatch after worker is ready
_RESULT_TIMEOUT = 15  # poll_result: result propagation


@pytest.fixture
def pool_env(server, project_id, tmp_path):
    """Provide helpers for pool-based tests.

    Sets up an Executor and manifest file, and yields a context dict
    with everything needed to configure a pool and interact with
    launched workers.
    """
    host = f"{project_id}.localhost:{server.port}"
    worker_dir = tmp_path / "pool-worker"
    worker_dir.mkdir()
    socket_path = str(worker_dir / "executor.sock")
    manifest_path = str(worker_dir / "manifest.json")

    executor = Executor(socket_path)
    executor.start()

    try:
        yield {
            "host": host,
            "worker_dir": worker_dir,
            "socket_path": socket_path,
            "manifest_path": manifest_path,
            "executor": executor,
        }
    finally:
        executor.close()


def _setup_pool(pool_env, targets, modules=None, pool_name="test-pool", provides=None, **kwargs):
    """Write manifest and create a process-launcher pool.

    Creates the workspace (if needed) and configures a pool whose launcher
    starts a ``coflux worker`` process pointing at the test adapter.
    Extra keyword arguments are forwarded to ``cli.pools_create``.
    """
    modules = modules or ["test"]
    manifest_path = pool_env["manifest_path"]
    socket_path = pool_env["socket_path"]
    host = pool_env["host"]

    with open(manifest_path, "w") as f:
        json.dump(manifest(targets), f)

    adapter = ["python3", ADAPTER_SCRIPT, "--manifest", manifest_path, "--socket", socket_path]

    cli.pools_create(
        pool_name,
        type="process",
        modules=modules,
        provides=provides,
        process_dir=str(pool_env["worker_dir"]),
        adapter=adapter,
        host=host,
        **kwargs,
    )

    # Register manifests so the server knows about the workflows
    adapter_str = ",".join(adapter)
    cli.manifests_register(*modules, adapter=adapter_str, host=host)


class TestPoolLifecycle:
    def test_pool_create_and_list(self, pool_env):
        """Creating a pool makes it visible in the pool list."""
        host = pool_env["host"]
        targets = [workflow("test", "my_workflow")]
        _setup_pool(pool_env, targets)

        pools = cli.pools_list(host=host)
        assert "test-pool" in pools

    def test_pool_get(self, pool_env):
        """Pool details can be retrieved by name."""
        host = pool_env["host"]
        targets = [workflow("test", "my_workflow")]
        _setup_pool(pool_env, targets)

        pool = cli.pools_get("test-pool", host=host)
        assert pool["launcher"]["type"] == "process"
        assert pool["launcher"]["directory"] == str(pool_env["worker_dir"])
        assert "test" in pool["modules"]

    def test_pool_delete(self, pool_env):
        """Deleting a pool removes it from the list."""
        host = pool_env["host"]
        targets = [workflow("test", "my_workflow")]
        _setup_pool(pool_env, targets)

        cli.pools_delete("test-pool", host=host)
        pools = cli.pools_list(host=host)
        assert "test-pool" not in pools

    def test_create_already_exists(self, pool_env):
        """Creating a pool that already exists fails."""
        targets = [workflow("test", "my_workflow")]
        _setup_pool(pool_env, targets)

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            _setup_pool(pool_env, targets)
        assert "already_exists" in exc_info.value.stderr

    def test_update_nonexistent(self, pool_env):
        """Updating a pool that doesn't exist fails."""
        host = pool_env["host"]

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            cli.pools_update("no-such-pool", concurrency=2, host=host)
        assert "not_found" in exc_info.value.stderr


class TestProcessLauncher:
    def test_auto_launch_worker(self, pool_env):
        """Submitting a workflow for a pooled module auto-launches a worker
        that executes the workflow and returns the result."""
        host = pool_env["host"]
        executor = pool_env["executor"]
        targets = [workflow("test", "greet", parameters=["name"])]
        _setup_pool(pool_env, targets)

        resp = cli.submit("test/greet", '"world"', host=host)

        # The server should launch a worker process which connects to our
        # executor.  Wait for that connection.
        executor.wait_connections(1, timeout=_LAUNCH_TIMEOUT)

        ex = executor.next_execute(timeout=_EXEC_TIMEOUT)
        assert ex.target == "greet"
        assert ex.arguments[0]["value"] == "world"
        ex.conn.complete(ex.execution_id, value="hello world")

        result = poll_result(resp["runId"], host, timeout=_RESULT_TIMEOUT)
        assert result["type"] == "value"
        assert result["value"]["data"] == "hello world"

    def test_multiple_executions(self, pool_env):
        """A pool-launched worker can handle multiple sequential executions."""
        host = pool_env["host"]
        executor = pool_env["executor"]
        targets = [workflow("test", "add", parameters=["a", "b"])]
        _setup_pool(pool_env, targets)

        # Submit first
        resp1 = cli.submit("test/add", "1", "2", host=host)
        executor.wait_connections(1, timeout=_LAUNCH_TIMEOUT)
        ex1 = executor.next_execute(timeout=_EXEC_TIMEOUT)
        ex1.conn.complete(ex1.execution_id, value=3)
        result1 = poll_result(resp1["runId"], host, timeout=_RESULT_TIMEOUT)
        assert result1["value"]["data"] == 3

        # Submit second (reuses existing worker)
        resp2 = cli.submit("test/add", "10", "20", host=host)
        ex2 = executor.next_execute(timeout=_EXEC_TIMEOUT)
        ex2.conn.complete(ex2.execution_id, value=30)
        result2 = poll_result(resp2["runId"], host, timeout=_RESULT_TIMEOUT)
        assert result2["value"]["data"] == 30

    def test_workflow_with_child_task(self, pool_env):
        """A pool-launched worker can submit child tasks during execution."""
        host = pool_env["host"]
        executor = pool_env["executor"]
        targets = [
            workflow("test", "orchestrator"),
            task("test", "double", parameters=["x"]),
        ]
        _setup_pool(pool_env, targets)

        resp = cli.submit("test/orchestrator", host=host)
        executor.wait_connections(1, timeout=_LAUNCH_TIMEOUT)

        # Handle the orchestrator: submit a child task
        ex0 = executor.next_execute(timeout=_EXEC_TIMEOUT)
        assert ex0.target == "orchestrator"
        ref = ex0.conn.submit_task(ex0.execution_id, "test", "double", json_args(5))

        # Handle the child task
        ex1 = executor.next_execute(timeout=_EXEC_TIMEOUT)
        assert ex1.target == "double"
        assert ex1.arguments[0]["value"] == 5
        ex1.conn.complete(ex1.execution_id, value=10)

        # Resolve and complete the orchestrator
        resolved = ex0.conn.resolve(ex0.execution_id, ref)
        assert resolved["value"] == 10
        ex0.conn.complete(ex0.execution_id, value="done")

        result = poll_result(resp["runId"], host, timeout=_RESULT_TIMEOUT)
        assert result["value"]["data"] == "done"

    def test_error_propagation(self, pool_env):
        """Errors from pool-launched workers are reported correctly."""
        host = pool_env["host"]
        executor = pool_env["executor"]
        targets = [workflow("test", "failing")]
        _setup_pool(pool_env, targets)

        resp = cli.submit("test/failing", host=host)
        executor.wait_connections(1, timeout=_LAUNCH_TIMEOUT)

        ex = executor.next_execute(timeout=_EXEC_TIMEOUT)
        ex.conn.fail(ex.execution_id, "RuntimeError", "something broke")

        result = poll_result(resp["runId"], host, timeout=_RESULT_TIMEOUT)
        assert result["type"] == "error"
        assert result["error"]["type"] == "RuntimeError"
        assert result["error"]["message"] == "something broke"

    def test_pool_with_provides(self, pool_env):
        """A pool with provides tags matches executions that require them."""
        host = pool_env["host"]
        executor = pool_env["executor"]
        targets = [
            workflow("test", "gpu_job", requires={"gpu": ["A100"]}),
        ]
        _setup_pool(pool_env, targets, provides={"gpu": ["A100"]})

        resp = cli.submit("test/gpu_job", host=host)
        executor.wait_connections(1, timeout=_LAUNCH_TIMEOUT)

        ex = executor.next_execute(timeout=_EXEC_TIMEOUT)
        assert ex.target == "gpu_job"
        ex.conn.complete(ex.execution_id, value="computed")

        result = poll_result(resp["runId"], host, timeout=_RESULT_TIMEOUT)
        assert result["value"]["data"] == "computed"


class TestCommonLauncherFields:
    def test_get_returns_adapter(self, pool_env):
        """Adapter configured on a pool is returned in pool details."""
        host = pool_env["host"]
        targets = [workflow("test", "my_workflow")]
        _setup_pool(pool_env, targets)

        pool = cli.pools_get("test-pool", host=host)
        adapter = pool["launcher"]["adapter"]
        assert adapter[0] == "python3"
        assert "--manifest" in adapter
        assert "--socket" in adapter

    def test_get_returns_concurrency(self, pool_env):
        """Concurrency configured on a pool is returned in pool details."""
        host = pool_env["host"]
        targets = [workflow("test", "my_workflow")]
        _setup_pool(pool_env, targets, pool_name="conc-pool", concurrency=4)

        pool = cli.pools_get("conc-pool", host=host)
        assert pool["launcher"]["concurrency"] == 4

    def test_get_returns_env(self, pool_env):
        """Custom env vars configured on a pool are returned in pool details."""
        host = pool_env["host"]
        targets = [workflow("test", "my_workflow")]
        _setup_pool(
            pool_env, targets, pool_name="env-pool",
            env={"MY_VAR": "hello", "OTHER_VAR": "world"},
        )

        pool = cli.pools_get("env-pool", host=host)
        assert pool["launcher"]["env"]["MY_VAR"] == "hello"
        assert pool["launcher"]["env"]["OTHER_VAR"] == "world"

    def test_update_common_fields(self, pool_env):
        """Common launcher fields can be updated on an existing pool."""
        host = pool_env["host"]
        targets = [workflow("test", "my_workflow")]
        _setup_pool(pool_env, targets)

        # Update concurrency and env on the existing pool
        cli.pools_update("test-pool", concurrency=8, env={"EXTRA": "val"}, host=host)

        pool = cli.pools_get("test-pool", host=host)
        assert pool["launcher"]["concurrency"] == 8
        assert pool["launcher"]["env"]["EXTRA"] == "val"
        # Original fields should be preserved
        assert pool["launcher"]["type"] == "process"
        assert pool["launcher"]["directory"] == str(pool_env["worker_dir"])

    def test_env_reaches_worker(self, pool_env):
        """Custom env vars set on a pool are visible in the launched worker."""
        host = pool_env["host"]
        worker_dir = pool_env["worker_dir"]
        executor = pool_env["executor"]

        # Use a marker file to prove env vars reach the adapter process.
        # The adapter script doesn't use env vars directly, but we can
        # verify the worker launches successfully with them set, since
        # the server injects them into the process environment.
        marker = str(worker_dir / "env_marker.txt")
        targets = [workflow("test", "check_env")]

        # Create a small adapter wrapper that writes an env var to a file
        # before delegating to the real adapter.
        wrapper_script = str(worker_dir / "env_wrapper.py")
        manifest_path = pool_env["manifest_path"]
        socket_path = pool_env["socket_path"]

        with open(wrapper_script, "w") as f:
            f.write(
                "import os, sys, subprocess\n"
                f"with open({marker!r}, 'w') as f:\n"
                "    f.write(os.environ.get('TEST_POOL_VAR', ''))\n"
                "result = subprocess.run(\n"
                f"    ['python3', {ADAPTER_SCRIPT!r}] + sys.argv[1:],\n"
                "    stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr\n"
                ")\n"
                "sys.exit(result.returncode)\n"
            )

        with open(manifest_path, "w") as f:
            json.dump(manifest(targets), f)

        cli.pools_create(
            "env-worker-pool",
            type="process",
            modules=["test"],
            process_dir=str(worker_dir),
            adapter=["python3", wrapper_script, "--manifest", manifest_path, "--socket", socket_path],
            env={"TEST_POOL_VAR": "pool-env-works"},
            host=host,
        )

        # Register manifests using the real adapter (not the env wrapper)
        real_adapter = f"python3,{ADAPTER_SCRIPT},--manifest,{manifest_path}"
        cli.manifests_register("test", adapter=real_adapter, host=host)

        resp = cli.submit("test/check_env", host=host)
        executor.wait_connections(1, timeout=_LAUNCH_TIMEOUT)

        ex = executor.next_execute(timeout=_EXEC_TIMEOUT)
        ex.conn.complete(ex.execution_id, value="ok")
        poll_result(resp["runId"], host, timeout=_RESULT_TIMEOUT)

        with open(marker) as f:
            assert f.read() == "pool-env-works"

    def test_multiple_modules(self, pool_env):
        """A pool with multiple modules handles targets from both."""
        host = pool_env["host"]
        executor = pool_env["executor"]
        targets = [
            workflow("module_a", "job_a"),
            workflow("module_b", "job_b"),
        ]
        _setup_pool(pool_env, targets, modules=["module_a", "module_b"])

        pool = cli.pools_get("test-pool", host=host)
        assert "module_a" in pool["modules"]
        assert "module_b" in pool["modules"]

        # Submit to first module
        resp_a = cli.submit("module_a/job_a", host=host)
        executor.wait_connections(1, timeout=_LAUNCH_TIMEOUT)

        ex_a = executor.next_execute(timeout=_EXEC_TIMEOUT)
        assert ex_a.target == "job_a"
        ex_a.conn.complete(ex_a.execution_id, value="from_a")

        result_a = poll_result(resp_a["runId"], host, timeout=_RESULT_TIMEOUT)
        assert result_a["value"]["data"] == "from_a"

        # Submit to second module (reuses the same worker)
        resp_b = cli.submit("module_b/job_b", host=host)

        ex_b = executor.next_execute(timeout=_EXEC_TIMEOUT)
        assert ex_b.target == "job_b"
        ex_b.conn.complete(ex_b.execution_id, value="from_b")

        result_b = poll_result(resp_b["runId"], host, timeout=_RESULT_TIMEOUT)
        assert result_b["value"]["data"] == "from_b"
