import socket

from . import protocol


class ExecutorConnection:
    """A single connection from a test-adapter shim instance."""

    def __init__(self, conn):
        self._conn = conn
        self._file = conn.makefile("rb")
        self._next_request_id = 1

    def send(self, msg: dict):
        self._conn.sendall(protocol.encode_message(msg))

    def recv(self, timeout=10) -> dict:
        self._conn.settimeout(timeout)
        try:
            line = self._file.readline()
        except (TimeoutError, OSError) as e:
            # After a timeout, the buffered file wrapper is poisoned.
            # Recreate it so future reads work.
            self._file = self._conn.makefile("rb")
            raise TimeoutError(f"no message received within {timeout}s") from e
        if not line:
            raise ConnectionError("connection closed")
        return protocol.decode_message(line)

    def send_ready(self):
        self.send(protocol.ready_message())

    def run_one(self, handler):
        """Wait for execute, call handler, send response.

        The initial ready is sent by the fixture during setup. After each
        execution completes, the pool automatically makes the executor
        available for the next one.
        """
        msg = self.recv()
        assert msg["method"] == "execute", f"expected execute, got {msg['method']}"
        params = msg["params"]
        response = handler(
            params["execution_id"],
            params["target"],
            params["arguments"],
        )
        if response is not None:
            self.send(response)

    def recv_execute(self, **kwargs):
        """Receive an execute message, return (execution_id, target, arguments)."""
        msg = self.recv(**kwargs)
        assert msg["method"] == "execute", f"expected execute, got {msg['method']}"
        p = msg["params"]
        return p["execution_id"], p["target"], p.get("arguments", [])

    def _request(self, msg):
        """Send a request message (with auto-assigned ID) and return the response."""
        rid = self._next_request_id
        self._next_request_id += 1
        msg["id"] = rid
        self.send(msg)
        resp = self.recv()
        assert resp["id"] == rid
        return resp

    def submit_task(self, execution_id, target, arguments, **kwargs):
        """Submit a child task execution and return the target execution ID."""
        msg = protocol.submit_execution_request(
            None,
            execution_id,
            target,
            arguments,
            **kwargs,
        )
        resp = self._request(msg)
        return resp["result"]["execution_id"]

    def submit_workflow(self, execution_id, target, arguments, **kwargs):
        """Submit a child workflow execution and return the target execution ID."""
        msg = protocol.submit_execution_request(
            None,
            execution_id,
            target,
            arguments,
            type="workflow",
            **kwargs,
        )
        resp = self._request(msg)
        return resp["result"]["execution_id"]

    def resolve(self, execution_id, target_execution_id):
        """Resolve a reference and return the result dict (or error dict)."""
        msg = protocol.resolve_reference_request(None, execution_id, target_execution_id)
        resp = self._request(msg)
        return resp.get("result", resp.get("error"))

    def cancel(self, execution_id, target_execution_id):
        """Cancel a child execution."""
        msg = protocol.cancel_execution_request(None, execution_id, target_execution_id)
        return self._request(msg)

    def suspend(self, execution_id, execute_after=None):
        """Suspend the current execution."""
        msg = protocol.suspend_request(None, execution_id, execute_after)
        return self._request(msg)

    def persist_asset(self, execution_id, paths, metadata=None):
        """Persist files as an asset and return the result."""
        msg = protocol.persist_asset_request(None, execution_id, paths, metadata)
        resp = self._request(msg)
        return resp.get("result", resp.get("error"))

    def get_asset(self, execution_id, asset_id):
        """Get asset entries by asset ID."""
        msg = protocol.get_asset_request(None, execution_id, asset_id)
        resp = self._request(msg)
        return resp.get("result", resp.get("error"))

    def complete(self, execution_id, value=None):
        """Send execution_result."""
        self.send(protocol.execution_result(execution_id, value=value))

    def fail(self, execution_id, error_type, message, traceback=""):
        """Send execution_error."""
        self.send(protocol.execution_error(execution_id, error_type, message, traceback))

    def close(self):
        self._file.close()
        self._conn.close()


class Executor:
    """Manages the Unix socket server and accepts connections from test-adapter shims."""

    def __init__(self, socket_path: str):
        self.socket_path = socket_path
        self._server_sock = None
        self.connections = []

    def start(self):
        self._server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server_sock.bind(self.socket_path)
        self._server_sock.listen(5)

    def accept(self, count=1, timeout=30):
        """Accept `count` connections from test-adapter shim instances."""
        self._server_sock.settimeout(timeout)
        for _ in range(count):
            conn, _ = self._server_sock.accept()
            self.connections.append(ExecutorConnection(conn))

    def close(self):
        for c in self.connections:
            c.close()
        if self._server_sock:
            self._server_sock.close()
