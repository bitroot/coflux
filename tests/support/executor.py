import socket
import threading
import time
from collections import namedtuple

from . import protocol

Execution = namedtuple("Execution", ["conn", "execution_id", "module", "target", "arguments"])


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
        """Wait for execute, call handler, send response."""
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
        """Receive an execute message, return (execution_id, module, target, arguments)."""
        msg = self.recv(**kwargs)
        assert msg["method"] == "execute", f"expected execute, got {msg['method']}"
        p = msg["params"]
        return p["execution_id"], p.get("module", ""), p["target"], p.get("arguments", [])

    def _request(self, msg):
        """Send a request message (with auto-assigned ID) and return the response."""
        rid = self._next_request_id
        self._next_request_id += 1
        msg["id"] = rid
        self.send(msg)
        resp = self.recv()
        assert resp["id"] == rid
        return resp

    def submit_task(self, execution_id, module, target, arguments, **kwargs):
        """Submit a child task execution and return the target execution ID."""
        msg = protocol.submit_execution_request(
            None,
            execution_id,
            module,
            target,
            arguments,
            **kwargs,
        )
        resp = self._request(msg)
        return resp["result"]["execution_id"]

    def submit_workflow(self, execution_id, module, target, arguments, **kwargs):
        """Submit a child workflow execution and return the target execution ID."""
        msg = protocol.submit_execution_request(
            None,
            execution_id,
            module,
            target,
            arguments,
            type="workflow",
            **kwargs,
        )
        resp = self._request(msg)
        return resp["result"]["execution_id"]

    def resolve(self, execution_id, target_execution_id):
        """Resolve a reference and return the result dict (or error dict)."""
        msg = protocol.resolve_reference_request(
            None, execution_id, target_execution_id
        )
        resp = self._request(msg)
        return resp.get("result", resp.get("error"))

    def poll(self, execution_id, target_execution_id, timeout_ms=0):
        """Poll for a reference result without suspending.

        Returns the result dict, or None if not yet available.
        """
        msg = protocol.resolve_reference_request(
            None, execution_id, target_execution_id,
            timeout_ms=timeout_ms, suspend=False,
        )
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

    def submit_input(self, execution_id, template, **kwargs):
        """Submit an input request and return the input external ID.

        Returns the input_id string on success, or an error dict on failure.
        """
        msg = protocol.submit_input_request(None, execution_id, template, **kwargs)
        resp = self._request(msg)
        if "error" in resp:
            return resp["error"]
        result = resp.get("result")
        if isinstance(result, dict) and "input_id" in result:
            return result["input_id"]
        return result

    def resolve_input(self, input_external_id, from_execution_id, **kwargs):
        """Resolve an input and return the result dict.

        Returns the raw result dict from the server. For a value response
        this looks like {"type": "inline", "format": "json", "value": ...}.
        For dismissed: {"status": "dismissed"}.
        For suspended: {"status": "suspended"}.
        Returns None if the poll timed out with no response.
        Raises RuntimeError if the server returned an error.
        """
        msg = protocol.resolve_input_request(
            None, input_external_id, from_execution_id, **kwargs
        )
        resp = self._request(msg)
        if "error" in resp:
            raise RuntimeError(f"resolve_input error: {resp['error']}")
        return resp.get("result")

    def complete(self, execution_id, value=None):
        """Send execution_result."""
        self.send(protocol.execution_result(execution_id, value=value))

    def fail(self, execution_id, error_type, message, traceback="", retryable=None):
        """Send execution_error."""
        self.send(
            protocol.execution_error(execution_id, error_type, message, traceback, retryable=retryable)
        )

    def close(self):
        try:
            self._file.close()
        except OSError:
            pass
        try:
            self._conn.close()
        except OSError:
            pass


class Executor:
    """Manages the Unix socket server and accepts connections from test-adapter shims.

    Connections are accepted in a background thread. Each new connection
    automatically sends "ready". Use next_execute() to wait for the next
    execution to arrive on any connection.
    """

    def __init__(self, socket_path: str):
        self.socket_path = socket_path
        self._server_sock = None
        self.connections = []
        self._lock = threading.Lock()
        self._accept_thread = None
        self._stopped = threading.Event()
        self._consumed = (
            set()
        )  # indices of connections already returned by next_execute

    def start(self):
        self._server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server_sock.bind(self.socket_path)
        self._server_sock.listen(16)
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    def _accept_loop(self):
        """Continuously accept connections and send ready."""
        while not self._stopped.is_set():
            self._server_sock.settimeout(0.5)
            try:
                conn, _ = self._server_sock.accept()
            except (socket.timeout, OSError):
                continue
            ec = ExecutorConnection(conn)
            ec.send_ready()
            with self._lock:
                self.connections.append(ec)

    def wait_connections(self, count, timeout=15):
        """Wait until at least `count` connections have been accepted."""
        deadline = time.time() + timeout
        while True:
            with self._lock:
                if len(self.connections) >= count:
                    return
            if time.time() >= deadline:
                with self._lock:
                    n = len(self.connections)
                raise TimeoutError(
                    f"expected {count} connections within {timeout}s, got {n}"
                )
            time.sleep(0.05)

    def next_execute(self, timeout=10):
        """Wait for the next execution to arrive on any connection.

        Each execution gets its own connection with one-shot executors.
        Previously consumed or dead connections are skipped.

        Returns an Execution named tuple.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                conns = list(enumerate(self.connections))
            for idx, conn in conns:
                if idx in self._consumed:
                    continue
                try:
                    eid, module, target, args = conn.recv_execute(timeout=0.1)
                    self._consumed.add(idx)
                    return Execution(conn, eid, module, target, args)
                except TimeoutError:
                    continue
                except (ConnectionError, OSError):
                    self._consumed.add(idx)
                    continue
            time.sleep(0.05)
        raise TimeoutError(f"no execute received within {timeout}s")

    def close(self):
        self._stopped.set()
        if self._accept_thread:
            self._accept_thread.join(timeout=2)
        with self._lock:
            for c in self.connections:
                try:
                    c.close()
                except OSError:
                    pass
        if self._server_sock:
            self._server_sock.close()
