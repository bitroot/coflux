"""Tests for pool management and the process launcher.

A pool with a process launcher causes the server to automatically start
worker processes when executions are submitted for matching modules.
"""

import json
import os
import subprocess
import time

import pytest
from support import cli
from support.ecs import FakeEcs
from support.executor import Executor
from support.helpers import ADAPTER_SCRIPT, poll_result
from support.manifest import manifest, task, workflow
from support.protocol import json_args

# Launcher-managed workers are slower to start than direct workers.
_LAUNCH_TIMEOUT = 30  # wait_connections: launcher startup + worker init
_EXEC_TIMEOUT = 15  # next_execute: execution dispatch after worker is ready
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


def _setup_pool(
    pool_env, targets, modules=None, pool_name="test-pool", provides=None, **kwargs
):
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

    adapter = [
        "python3",
        ADAPTER_SCRIPT,
        "--manifest",
        manifest_path,
        "--socket",
        socket_path,
    ]

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
            pool_env,
            targets,
            pool_name="env-pool",
            env={"MY_VAR": "hello", "OTHER_VAR": "world"},
        )

        pool = cli.pools_get("env-pool", host=host)
        assert pool["launcher"]["env"]["MY_VAR"] == "hello"
        assert pool["launcher"]["env"]["OTHER_VAR"] == "world"

    def test_idle_timeout_is_a_pool_field(self, pool_env, tmp_path):
        """An idle timeout is set, exported, unset and imported at the pool
        level, and zero is a value rather than an absence."""
        host = pool_env["host"]
        targets = [workflow("test", "my_workflow")]
        _setup_pool(pool_env, targets, pool_name="idle-pool", idle_timeout=300)

        pool = cli.pools_get("idle-pool", host=host)
        assert pool["idleTimeout"] == 300
        assert "idleTimeout" not in pool["launcher"]

        exported = cli.pools_export(host=host)
        assert "idle_timeout = 300\n" in exported

        cli.pools_update("idle-pool", idle_timeout=0, host=host)
        assert cli.pools_get("idle-pool", host=host)["idleTimeout"] == 0

        cli._coflux(
            "pools",
            "update",
            "idle-pool",
            "--unset",
            "idleTimeout",
            host=host,
            output=None,
        )
        assert "idleTimeout" not in cli.pools_get("idle-pool", host=host)

        path = tmp_path / "pools.toml"
        path.write_text(exported)
        cli.pools_import(path, host=host)
        assert cli.pools_get("idle-pool", host=host)["idleTimeout"] == 300

    def test_idle_timeout_keeps_worker_warm(self, pool_env):
        """A worker with an idle timeout outlives the gap between runs, so
        the second run reuses it rather than paying for another launch."""
        host = pool_env["host"]
        executor = pool_env["executor"]
        targets = [workflow("test", "greet", parameters=["name"])]
        _setup_pool(pool_env, targets, pool_name="warm-pool", idle_timeout=60)

        resp = cli.submit("test/greet", '"one"', host=host)
        executor.wait_connections(1, timeout=_LAUNCH_TIMEOUT)
        ex = executor.next_execute(timeout=_EXEC_TIMEOUT)
        ex.conn.complete(ex.execution_id, value="one")
        poll_result(resp["runId"], host, timeout=_RESULT_TIMEOUT)

        # Longer than the default idle timeout and the sweep that enforces it.
        time.sleep(12)
        workers = cli.pools_launches("warm-pool", host=host)
        assert len(workers) == 1
        assert all(w["stoppingAt"] is None for w in workers.values())

        resp = cli.submit("test/greet", '"two"', host=host)
        ex = executor.next_execute(timeout=_EXEC_TIMEOUT)
        ex.conn.complete(ex.execution_id, value="two")
        result = poll_result(resp["runId"], host, timeout=_RESULT_TIMEOUT)
        assert result["value"]["data"] == "two"

        # Same worker, same connection: nothing else was launched.
        executor.wait_connections(1, timeout=1)
        assert len(cli.pools_launches("warm-pool", host=host)) == 1

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
            adapter=[
                "python3",
                wrapper_script,
                "--manifest",
                manifest_path,
                "--socket",
                socket_path,
            ],
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


class TestWorkerReadiness:
    def test_slow_starting_worker_is_not_drained(self, pool_env):
        """A worker slower to start than the idle timeout still gets work.

        The server creates the session when it launches the worker, but the
        worker can't accept anything until it has connected and declared
        its targets. Anything that measures idleness from before that point
        drains the worker while it is still starting, and it is stopped
        having never run a thing.
        """
        host = pool_env["host"]
        executor = pool_env["executor"]
        manifest_path = pool_env["manifest_path"]
        socket_path = pool_env["socket_path"]

        with open(manifest_path, "w") as f:
            json.dump(manifest([workflow("test", "my_workflow")]), f)

        base_adapter = [
            "python3",
            ADAPTER_SCRIPT,
            "--manifest",
            manifest_path,
            "--socket",
            socket_path,
        ]

        # Comfortably longer than the server's 5s idle timeout.
        slow_adapter = base_adapter + ["--discover-delay", "8"]

        cli.pools_create(
            "test-pool",
            type="process",
            modules=["test"],
            process_dir=str(pool_env["worker_dir"]),
            adapter=slow_adapter,
            host=host,
        )

        # Registered with the prompt adapter: only the launched worker
        # should be slow.
        cli.manifests_register("test", adapter=",".join(base_adapter), host=host)

        resp = cli.submit("test/my_workflow", host=host)

        executor.wait_connections(1, timeout=_LAUNCH_TIMEOUT)
        ex = executor.next_execute(timeout=_EXEC_TIMEOUT)
        ex.conn.complete(ex.execution_id, value="ok")

        result = poll_result(resp["runId"], host, timeout=_RESULT_TIMEOUT)
        assert result["value"]["data"] == "ok"


class TestPoolState:
    def test_enable_unknown_pool_is_rejected(self, pool_env):
        """Enabling a pool that doesn't exist fails, and changes nothing.

        The name is not a pool, so there is nothing to enable. Recording
        the state anyway would leave behind an entry that looks like a pool
        but has no launcher and no modules, which the scheduler then trips
        over on its next pass.
        """
        host = pool_env["host"]
        targets = [workflow("test", "my_workflow")]
        _setup_pool(pool_env, targets)

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            cli.pools_enable("no-such-pool", host=host)
        assert "not_found" in exc_info.value.stderr

        assert "no-such-pool" not in cli.pools_list(host=host)

        # The next pass still runs: a phantom pool would crash it.
        executor = pool_env["executor"]
        resp = cli.submit("test/my_workflow", host=host)
        executor.wait_connections(1, timeout=_LAUNCH_TIMEOUT)
        ex = executor.next_execute(timeout=_EXEC_TIMEOUT)
        ex.conn.complete(ex.execution_id, value="ok")
        poll_result(resp["runId"], host, timeout=_RESULT_TIMEOUT)

    def test_disable_unknown_pool_is_rejected(self, pool_env):
        """Disabling a pool that doesn't exist fails, and changes nothing."""
        host = pool_env["host"]
        _setup_pool(pool_env, [workflow("test", "my_workflow")])

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            cli.pools_disable("no-such-pool", host=host)
        assert "not_found" in exc_info.value.stderr

        assert "no-such-pool" not in cli.pools_list(host=host)

    def test_disable_and_enable_round_trip(self, pool_env):
        """A real pool can be disabled and enabled again."""
        host = pool_env["host"]
        _setup_pool(pool_env, [workflow("test", "my_workflow")])

        cli.pools_disable("test-pool", host=host)
        assert cli.pools_get("test-pool", host=host)["state"] == "disabled"

        cli.pools_enable("test-pool", host=host)
        assert cli.pools_get("test-pool", host=host)["state"] == "active"


class TestPoolModules:
    def test_wildcard_modules_are_rejected(self, pool_env):
        """A pool's modules are names, not patterns.

        The same list is handed to the launcher as the worker's arguments,
        so a wildcard would be passed to the worker to import as well as
        matching no execution - a pool that silently never runs anything.
        """
        host = pool_env["host"]

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            cli.pools_create(
                "wildcard-pool",
                type="process",
                modules=["myapp.*"],
                process_dir=str(pool_env["worker_dir"]),
                host=host,
            )
        assert "bad_request" in exc_info.value.stderr

        assert "wildcard-pool" not in cli.pools_list(host=host)


class TestPoolSecrets:
    """Pools name secrets; launchers get their values, and nothing else does."""

    def _kubernetes_pool(
        self, host, name="k8s-pool", token_secret="k8s-token", **kwargs
    ):
        cli._coflux(
            "pools",
            "create",
            name,
            "--type",
            "kubernetes",
            "--set",
            "image=myorg/worker:latest",
            "--set",
            f"tokenSecret={token_secret}",
            "--set",
            "apiServer=https://k8s.example.com",
            "--modules",
            "test",
            host=host,
            output=None,
            **kwargs,
        )

    def test_secret_env_reaches_worker(self, pool_env):
        """A secret named in envSecrets is in the worker's environment, and
        its value shows up nowhere a pool is described."""
        host = pool_env["host"]
        worker_dir = pool_env["worker_dir"]
        executor = pool_env["executor"]
        marker = str(worker_dir / "secret_marker.txt")
        wrapper_script = str(worker_dir / "secret_wrapper.py")
        manifest_path = pool_env["manifest_path"]
        socket_path = pool_env["socket_path"]

        with open(wrapper_script, "w") as f:
            f.write(
                "import os, sys, subprocess\n"
                f"with open({marker!r}, 'w') as f:\n"
                "    f.write(os.environ.get('TEST_SECRET', ''))\n"
                "result = subprocess.run(\n"
                f"    ['python3', {ADAPTER_SCRIPT!r}] + sys.argv[1:],\n"
                "    stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr\n"
                ")\n"
                "sys.exit(result.returncode)\n"
            )

        with open(manifest_path, "w") as f:
            json.dump(manifest([workflow("test", "check_env")]), f)

        adapter = [
            "python3",
            wrapper_script,
            "--manifest",
            manifest_path,
            "--socket",
            socket_path,
        ]

        # Set for the current workspace, which is where the pool is.
        cli.secrets_set("api-key", "s3cr3t-value", host=host)

        cli.pools_create(
            "secret-pool",
            type="process",
            modules=["test"],
            process_dir=str(worker_dir),
            adapter=adapter,
            host=host,
        )
        cli._coflux(
            "pools",
            "update",
            "secret-pool",
            "--set",
            "envSecrets.TEST_SECRET=api-key",
            host=host,
            output=None,
        )
        cli.manifests_register("test", adapter=",".join(adapter), host=host)

        cli.submit("test/check_env", host=host)
        executor.wait_connections(1, timeout=_LAUNCH_TIMEOUT)

        with open(marker) as f:
            assert f.read() == "s3cr3t-value"

        pool = cli.pools_get("secret-pool", host=host)
        assert pool["launcher"]["envSecrets"] == {"TEST_SECRET": "api-key"}
        assert "s3cr3t-value" not in json.dumps(pool)
        assert "s3cr3t-value" not in cli.pools_export(host=host)

    def test_missing_secret_is_refused(self, pool_env):
        """A pool naming a secret that doesn't exist for its workspace is
        refused when it's created or updated, naming the secret."""
        host = pool_env["host"]

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            self._kubernetes_pool(host, token_secret="nope")
        assert "secrets_not_found" in exc_info.value.stderr
        assert "nope" in exc_info.value.stderr

        cli.secrets_set("k8s-token", "bearer", host=host)
        self._kubernetes_pool(host)

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            cli._coflux(
                "pools",
                "update",
                "k8s-pool",
                "--set",
                "envSecrets.X=missing",
                host=host,
                output=None,
            )
        assert "secrets_not_found" in exc_info.value.stderr

    def test_scope_follows_workspace_names(self, pool_env):
        """A secret for 'development' serves 'development/joe' and not
        'production', whatever the workspaces inherit from."""
        host = pool_env["host"]
        cli.secrets_set("k8s-token", "bearer", scope="development", host=host)

        self._kubernetes_pool(host, workspace="development/joe")

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            self._kubernetes_pool(host, workspace="production")
        assert "secrets_not_found" in exc_info.value.stderr

    def test_export_names_secrets(self, pool_env, tmp_path):
        """An export carries the names, never the values, and imports back
        as long as the secrets exist."""
        host = pool_env["host"]
        cli.secrets_set("k8s-token", "super-secret-token", host=host)
        self._kubernetes_pool(host)
        cli._coflux(
            "pools",
            "update",
            "k8s-pool",
            "--set",
            "envSecrets.API_KEY=k8s-token",
            host=host,
            output=None,
        )

        exported = cli.pools_export(host=host)
        assert 'token_secret = "k8s-token"' in exported
        assert "env_secrets = {" in exported
        assert 'API_KEY = "k8s-token"' in exported
        assert "super-secret-token" not in exported

        path = tmp_path / "pools.toml"
        path.write_text(exported)
        cli.pools_import(path, host=host)

        launcher = cli.pools_get("k8s-pool", host=host)["launcher"]
        assert launcher["tokenSecret"] == "k8s-token"
        assert launcher["envSecrets"] == {"API_KEY": "k8s-token"}


# ---------------------------------------------------------------------------
# ECS launcher


@pytest.fixture
def ecs_env(pool_env):
    """A pool environment with a stand-in for the ECS API (see support.ecs)."""
    cli_path = os.path.abspath(os.environ.get("COFLUX_BIN", "coflux"))
    fake = FakeEcs(cli_path, cwd=str(pool_env["worker_dir"]))
    fake.start()
    try:
        yield {**pool_env, "ecs": fake}
    finally:
        fake.close()


def _setup_ecs_pool(ecs_env, targets, modules=None, pool_name="ecs-pool", sets=()):
    """Write the manifest and create an ECS pool pointed at the fake API.

    ``sets`` are extra ``--set`` fields; a later one overrides a default.
    """
    modules = modules or ["test"]
    host = ecs_env["host"]
    fake = ecs_env["ecs"]

    with open(ecs_env["manifest_path"], "w") as f:
        json.dump(manifest(targets), f)

    adapter = [
        "python3",
        ADAPTER_SCRIPT,
        "--manifest",
        ecs_env["manifest_path"],
        "--socket",
        ecs_env["socket_path"],
    ]

    fields = [
        f"cluster={fake.cluster}",
        "taskDefinition=worker-task",
        "region=us-east-1",
        f"endpoint={fake.endpoint}",
        "credentialsSecret=aws-test",
        'subnets=["subnet-1", "subnet-2"]',
        "securityGroups=sg-1",
        "assignPublicIp=true",
        f"adapter={json.dumps(adapter)}",
        *sets,
    ]
    # In the shape `aws configure export-credentials` produces.
    cli.secrets_set(
        "aws-test",
        json.dumps({"AccessKeyId": "AKIATEST", "SecretAccessKey": "test-secret-key"}),
        global_=True,
        host=host,
    )

    args = ["pools", "create", pool_name, "--type", "ecs"]
    for field in fields:
        args.extend(["--set", field])
    args.extend(["--modules", ",".join(modules)])
    cli._coflux(*args, host=host, output=None)

    cli.manifests_register(*modules, adapter=",".join(adapter), host=host)


def _wait_for_worker(host, pool_name, predicate, timeout=30):
    """Poll the pool's launches until a worker satisfies the predicate."""
    deadline = time.time() + timeout
    workers = {}
    while time.time() < deadline:
        workers = cli.pools_launches(pool_name, host=host)
        for worker in workers.values():
            if predicate(worker):
                return worker
        time.sleep(0.5)
    raise TimeoutError(f"no worker matched within {timeout}s: {workers}")


class TestEcsLauncher:
    def test_runs_worker_as_task(self, ecs_env):
        """A worker is a task run from the pool's task definition, with the
        modules as its command and the connection details as its environment,
        in a signed request."""
        host = ecs_env["host"]
        executor = ecs_env["executor"]
        fake = ecs_env["ecs"]
        targets = [workflow("test", "greet", parameters=["name"])]
        _setup_ecs_pool(ecs_env, targets)

        resp = cli.submit("test/greet", '"world"', host=host)
        executor.wait_connections(1, timeout=_LAUNCH_TIMEOUT)

        ex = executor.next_execute(timeout=_EXEC_TIMEOUT)
        assert ex.target == "greet"
        assert ex.arguments[0]["value"] == "world"
        ex.conn.complete(ex.execution_id, value="hello world")

        result = poll_result(resp["runId"], host, timeout=_RESULT_TIMEOUT)
        assert result["type"] == "value"
        assert result["value"]["data"] == "hello world"

        # Without a container named, the task definition says which to override.
        assert len(fake.requests_for("DescribeTaskDefinition")) == 1

        [(run_task, headers)] = fake.requests_for("RunTask")
        assert run_task["cluster"] == fake.cluster
        assert run_task["taskDefinition"] == "worker-task"
        assert run_task["count"] == 1
        assert run_task["launchType"] == "FARGATE"
        assert run_task["startedBy"] == "coflux:ecs-pool"
        assert run_task["networkConfiguration"] == {
            "awsvpcConfiguration": {
                "subnets": ["subnet-1", "subnet-2"],
                "securityGroups": ["sg-1"],
                "assignPublicIp": "ENABLED",
            }
        }
        [override] = run_task["overrides"]["containerOverrides"]
        assert override["name"] == fake.container_name
        assert override["command"] == ["test"]
        env = {e["name"]: e["value"] for e in override["environment"]}
        assert env["COFLUX_HOST"] == host
        assert env["COFLUX_WORKSPACE"] == "default"
        assert env["COFLUX_SESSION"]

        assert headers["content-type"] == "application/x-amz-json-1.1"
        authorization = headers["authorization"]
        assert authorization.startswith("AWS4-HMAC-SHA256 Credential=AKIATEST/")
        assert "/us-east-1/ecs/aws4_request" in authorization
        assert "x-amz-date" in headers

    def test_idle_worker_is_stopped(self, ecs_env):
        """An idle worker's task is stopped, and a task stopped on request
        isn't reported as having failed."""
        host = ecs_env["host"]
        executor = ecs_env["executor"]
        fake = ecs_env["ecs"]
        _setup_ecs_pool(ecs_env, [workflow("test", "greet")])

        resp = cli.submit("test/greet", host=host)
        executor.wait_connections(1, timeout=_LAUNCH_TIMEOUT)
        ex = executor.next_execute(timeout=_EXEC_TIMEOUT)
        ex.conn.complete(ex.execution_id, value="done")
        poll_result(resp["runId"], host, timeout=_RESULT_TIMEOUT)

        [(stop_task, _)] = fake.wait_for("StopTask", timeout=30)
        assert stop_task["cluster"] == fake.cluster
        assert stop_task["task"] in fake.task_arns()

        worker = _wait_for_worker(
            host, "ecs-pool", lambda w: w["deactivatedAt"] is not None
        )
        assert worker["stopError"] is None
        assert worker["error"] is None

    def test_oom_killed_task_is_reported(self, ecs_env):
        """A task that ECS stops for exceeding its memory is reported as
        such, with the task's stopped reason in place of a log tail."""
        host = ecs_env["host"]
        executor = ecs_env["executor"]
        fake = ecs_env["ecs"]
        _setup_ecs_pool(ecs_env, [workflow("test", "greet")])

        cli.submit("test/greet", host=host)
        executor.wait_connections(1, timeout=_LAUNCH_TIMEOUT)
        # Mid-execution, so the worker isn't idle and stopped first.
        executor.next_execute(timeout=_EXEC_TIMEOUT)

        [arn] = fake.task_arns()
        fake.kill_with_oom(arn)

        worker = _wait_for_worker(
            host, "ecs-pool", lambda w: w["deactivatedAt"] is not None, timeout=45
        )
        assert worker["error"] == "oom_killed"
        assert worker["logs"] == "Essential container in task exited"

    def test_refused_launch_is_reported(self, ecs_env):
        """A RunTask the API refuses fails the worker, with the API's own
        message kept as the worker's logs."""
        host = ecs_env["host"]
        fake = ecs_env["ecs"]
        fake.run_task_error = ("ClusterNotFoundException", "Cluster not found.")
        _setup_ecs_pool(ecs_env, [workflow("test", "greet")])

        cli.submit("test/greet", host=host)

        worker = _wait_for_worker(
            host, "ecs-pool", lambda w: w["startError"] is not None
        )
        assert worker["startError"] == "launch_cluster_not_found"
        assert worker["logs"] == "Cluster not found."

    def test_missing_task_definition_is_reported(self, ecs_env):
        """The API doesn't say a task definition wasn't found, only that it
        couldn't be described; the worker says which it means."""
        host = ecs_env["host"]
        fake = ecs_env["ecs"]
        fake.task_definition_error = (
            "ClientException",
            "Unable to describe task definition.",
        )
        _setup_ecs_pool(ecs_env, [workflow("test", "greet")])

        cli.submit("test/greet", host=host)

        worker = _wait_for_worker(
            host, "ecs-pool", lambda w: w["startError"] is not None
        )
        assert worker["startError"] == "launch_task_definition_not_found"
        assert worker["logs"] == "Unable to describe task definition."

    def test_capacity_provider_replaces_launch_type(self, ecs_env):
        """A capacity provider is a strategy rather than a launch type, and a
        named container isn't looked up."""
        host = ecs_env["host"]
        executor = ecs_env["executor"]
        fake = ecs_env["ecs"]
        _setup_ecs_pool(
            ecs_env,
            [workflow("test", "greet")],
            sets=["capacityProvider=FARGATE_SPOT", "containerName=app"],
        )

        resp = cli.submit("test/greet", host=host)
        executor.wait_connections(1, timeout=_LAUNCH_TIMEOUT)
        ex = executor.next_execute(timeout=_EXEC_TIMEOUT)
        ex.conn.complete(ex.execution_id, value="done")
        poll_result(resp["runId"], host, timeout=_RESULT_TIMEOUT)

        assert fake.requests_for("DescribeTaskDefinition") == []
        [(run_task, _)] = fake.requests_for("RunTask")
        assert "launchType" not in run_task
        assert run_task["capacityProviderStrategy"] == [
            {"capacityProvider": "FARGATE_SPOT", "weight": 1}
        ]
        assert run_task["overrides"]["containerOverrides"][0]["name"] == "app"

    def test_launch_type_and_capacity_provider_are_exclusive(self, ecs_env):
        with pytest.raises(subprocess.CalledProcessError):
            _setup_ecs_pool(
                ecs_env,
                [workflow("test", "greet")],
                sets=["launchType=EC2", "capacityProvider=FARGATE_SPOT"],
            )

    def test_single_ids_are_accepted_for_lists(self, ecs_env):
        """A lone subnet or security group ID needn't be written as JSON."""
        host = ecs_env["host"]
        _setup_ecs_pool(ecs_env, [workflow("test", "greet")], sets=["subnets=subnet-9"])

        launcher = cli.pools_get("ecs-pool", host=host)["launcher"]
        assert launcher["type"] == "ecs"
        assert launcher["subnets"] == ["subnet-9"]
        assert launcher["securityGroups"] == ["sg-1"]
        assert launcher["assignPublicIp"] is True
        assert launcher["credentialsSecret"] == "aws-test"

        cli._coflux(
            "pools",
            "update",
            "ecs-pool",
            "--set",
            "subnets=subnet-10",
            host=host,
            output=None,
        )
        assert cli.pools_get("ecs-pool", host=host)["launcher"]["subnets"] == [
            "subnet-10"
        ]

    def test_export_names_the_credentials_secret(self, ecs_env, tmp_path):
        """An export carries the secret's name, never its value, and
        imports back while the secret exists."""
        host = ecs_env["host"]
        _setup_ecs_pool(ecs_env, [workflow("test", "greet")])

        exported = cli.pools_export(host=host)
        assert 'credentials_secret = "aws-test"' in exported
        assert 'task_definition = "worker-task"' in exported
        assert "test-secret-key" not in exported
        assert "AKIATEST" not in exported

        path = tmp_path / "pools.toml"
        path.write_text(exported)
        cli.pools_import(path, host=host)
        launcher = cli.pools_get("ecs-pool", host=host)["launcher"]
        assert launcher["credentialsSecret"] == "aws-test"
