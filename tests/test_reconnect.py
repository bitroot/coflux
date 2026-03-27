"""Tests for result buffering across worker reconnection and server restart."""

import time
import uuid

import support.cli as cli
from support.helpers import managed_worker, poll_result
from support.manifest import workflow
from support.proxy import TCPProxy


def test_result_buffered_on_disconnect(server, tmp_path):
    """Result completed while disconnected is delivered after reconnect."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [workflow("test", "my_workflow")]

    # Direct host for CLI commands (submit, result) - bypasses proxy
    direct_host = f"{project_id}.localhost:{server.port}"
    cli.workspaces_create("default", host=direct_host)

    proxy = TCPProxy("127.0.0.1", server.port)
    try:
        # Worker host goes through proxy so we can disrupt the WebSocket
        worker_host = f"{project_id}.localhost:{proxy.port}"

        with managed_worker(targets, worker_host, tmp_path) as executor:
            resp = cli.submit("test/my_workflow", host=direct_host)
            run_id = resp["runId"]

            ex = executor.next_execute()
            assert ex.target == "my_workflow"

            # Kill proxy connections — Go worker loses its WebSocket
            proxy.disconnect()
            time.sleep(0.5)

            # Complete the execution while the worker is disconnected.
            # The Go worker will buffer the result.
            ex.conn.complete(ex.execution_id, value=42)

            # Worker reconnects through the proxy (still accepting),
            # server sends session message with execution IDs,
            # worker flushes the buffered result.
            result = poll_result(run_id, direct_host, timeout=15)
            assert result["type"] == "value"
            assert result["value"]["data"] == 42
    finally:
        proxy.close()


def test_error_buffered_on_disconnect(server, tmp_path):
    """Error reported while disconnected is delivered after reconnect."""
    project_id = f"test-{uuid.uuid4().hex[:12]}"
    targets = [workflow("test", "my_workflow")]

    direct_host = f"{project_id}.localhost:{server.port}"
    cli.workspaces_create("default", host=direct_host)

    proxy = TCPProxy("127.0.0.1", server.port)
    try:
        worker_host = f"{project_id}.localhost:{proxy.port}"

        with managed_worker(targets, worker_host, tmp_path) as executor:
            resp = cli.submit("test/my_workflow", host=direct_host)
            run_id = resp["runId"]

            ex = executor.next_execute()
            assert ex.target == "my_workflow"

            proxy.disconnect()
            time.sleep(0.5)

            ex.conn.fail(ex.execution_id, "RuntimeError", "something broke")

            result = poll_result(run_id, direct_host, timeout=15)
            assert result["type"] == "error"
            assert result["error"]["type"] == "RuntimeError"
            assert result["error"]["message"] == "something broke"
    finally:
        proxy.close()


def test_result_buffered_across_server_restart(isolated_server, tmp_path):
    """Result completed while server is down is delivered after server restart."""
    server, host, project_id = isolated_server
    targets = [workflow("test", "my_workflow")]

    with managed_worker(targets, host, tmp_path) as executor:
        resp = cli.submit("test/my_workflow", host=host)
        run_id = resp["runId"]

        ex = executor.next_execute()
        assert ex.target == "my_workflow"

        # Kill server
        server.stop()
        time.sleep(0.5)

        # Complete execution while server is down
        ex.conn.complete(ex.execution_id, value=42)

        # Restart server — same port, same data directory
        server.start()

        # Worker reconnects, server restores session from DB,
        # sends execution IDs, worker flushes buffered result
        result = poll_result(run_id, host, timeout=20)
        assert result["type"] == "value"
        assert result["value"]["data"] == 42
