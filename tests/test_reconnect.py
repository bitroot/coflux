"""Tests for result buffering across worker reconnection and server restart."""

import json
import os
import signal
import subprocess
import time
import uuid

import support.cli as cli
from support.executor import Executor
from support.manifest import manifest, workflow
from support.proxy import TCPProxy
from support.server import ManagedServer

ADAPTER_SCRIPT = os.path.join(os.path.dirname(__file__), "support", "adapter.py")


def _make_env(host):
    return {
        "PATH": os.environ["PATH"],
        "COFLUX_SERVER_HOST": host,
        "COFLUX_WORKSPACE": "default",
    }


def _poll_result(run_id, env, timeout=15):
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


def test_result_buffered_on_disconnect(server, tmp_path):
    """Result completed while disconnected is delivered after reconnect."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [workflow("test.my_workflow")]
    manifest_json = json.dumps(manifest(targets))
    socket_path = str(tmp_path / "executor.sock")

    # Direct env for CLI commands (submit, result) - bypasses proxy
    direct_env = _make_env(f"{project_id}.localhost:{server.port}")
    cli.workspaces_create("default", env=direct_env)

    proxy = TCPProxy("127.0.0.1", server.port)
    try:
        # Worker env goes through proxy so we can disrupt the WebSocket
        worker_env = _make_env(f"{project_id}.localhost:{proxy.port}")
        worker_env["COFLUX_TEST_MANIFEST"] = manifest_json
        worker_env["COFLUX_TEST_SOCKET"] = socket_path
        worker_env["COFLUX_LOGS_STORE_FLUSH_INTERVAL"] = "0"

        executor = Executor(socket_path)
        executor.start()

        adapter = f"python3,{ADAPTER_SCRIPT}"
        worker_proc = cli.worker(["test"], adapter, concurrency=1, env=worker_env)

        try:
            executor.accept(count=1, timeout=15)
            conn = executor.connections[0]
            conn.send_ready()

            # Submit workflow via direct connection
            resp = cli.submit("test.my_workflow", env=direct_env)
            run_id = resp["runId"]

            # Executor receives the execution
            eid, target, _ = conn.recv_execute()
            assert target == "test.my_workflow"

            # Kill proxy connections — Go worker loses its WebSocket
            proxy.disconnect()

            # Wait for Go CLI to detect the broken connection
            time.sleep(0.5)

            # Complete the execution while the worker is disconnected.
            # The Go worker will buffer the result.
            conn.complete(eid, value=42)

            # Worker reconnects through the proxy (still accepting),
            # server sends session message with execution IDs,
            # worker flushes the buffered result.
            result = _poll_result(run_id, direct_env, timeout=15)
            assert result["type"] == "value"
            assert result["value"]["data"] == 42

        finally:
            worker_proc.send_signal(signal.SIGTERM)
            try:
                worker_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker_proc.kill()
                worker_proc.wait(timeout=5)
            executor.close()
    finally:
        proxy.close()


def test_error_buffered_on_disconnect(server, tmp_path):
    """Error reported while disconnected is delivered after reconnect."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [workflow("test.my_workflow")]
    manifest_json = json.dumps(manifest(targets))
    socket_path = str(tmp_path / "executor.sock")

    direct_env = _make_env(f"{project_id}.localhost:{server.port}")
    cli.workspaces_create("default", env=direct_env)

    proxy = TCPProxy("127.0.0.1", server.port)
    try:
        worker_env = _make_env(f"{project_id}.localhost:{proxy.port}")
        worker_env["COFLUX_TEST_MANIFEST"] = manifest_json
        worker_env["COFLUX_TEST_SOCKET"] = socket_path
        worker_env["COFLUX_LOGS_STORE_FLUSH_INTERVAL"] = "0"

        executor = Executor(socket_path)
        executor.start()

        adapter = f"python3,{ADAPTER_SCRIPT}"
        worker_proc = cli.worker(["test"], adapter, concurrency=1, env=worker_env)

        try:
            executor.accept(count=1, timeout=15)
            conn = executor.connections[0]
            conn.send_ready()

            resp = cli.submit("test.my_workflow", env=direct_env)
            run_id = resp["runId"]

            eid, target, _ = conn.recv_execute()
            assert target == "test.my_workflow"

            # Kill proxy connections
            proxy.disconnect()
            time.sleep(0.5)

            # Report error while disconnected
            conn.fail(eid, "RuntimeError", "something broke")

            result = _poll_result(run_id, direct_env, timeout=15)
            assert result["type"] == "error"
            assert result["error"]["type"] == "RuntimeError"
            assert result["error"]["message"] == "something broke"

        finally:
            worker_proc.send_signal(signal.SIGTERM)
            try:
                worker_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker_proc.kill()
                worker_proc.wait(timeout=5)
            executor.close()
    finally:
        proxy.close()


def test_result_buffered_across_server_restart(tmp_path):
    """Result completed while server is down is delivered after server restart."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [workflow("test.my_workflow")]
    manifest_json = json.dumps(manifest(targets))
    socket_path = str(tmp_path / "executor.sock")

    server = ManagedServer(str(tmp_path / "data"))
    server.start()

    try:
        env = _make_env(f"{project_id}.localhost:{server.port}")
        env["COFLUX_TEST_MANIFEST"] = manifest_json
        env["COFLUX_TEST_SOCKET"] = socket_path
        env["COFLUX_LOGS_STORE_FLUSH_INTERVAL"] = "0"

        cli.workspaces_create("default", env=env)

        executor = Executor(socket_path)
        executor.start()

        adapter = f"python3,{ADAPTER_SCRIPT}"
        worker_proc = cli.worker(["test"], adapter, concurrency=1, env=env)

        try:
            executor.accept(count=1, timeout=15)
            conn = executor.connections[0]
            conn.send_ready()

            resp = cli.submit("test.my_workflow", env=env)
            run_id = resp["runId"]

            eid, target, _ = conn.recv_execute()
            assert target == "test.my_workflow"

            # Kill server
            server.stop()
            time.sleep(0.5)

            # Complete execution while server is down
            conn.complete(eid, value=42)

            # Restart server — same port, same data directory
            server.start()

            # Worker reconnects, server restores session from DB,
            # sends execution IDs, worker flushes buffered result
            result = _poll_result(run_id, env, timeout=20)
            assert result["type"] == "value"
            assert result["value"]["data"] == 42

        finally:
            worker_proc.send_signal(signal.SIGTERM)
            try:
                worker_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker_proc.kill()
                worker_proc.wait(timeout=5)
            executor.close()
    finally:
        server.stop()
