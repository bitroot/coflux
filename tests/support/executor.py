import socket
import threading
import time
from collections import namedtuple

from . import protocol


def _unwrap_select_result(result):
    """Translate a select result to the legacy resolve_reference / resolve_input shape.

    The select protocol always returns a dict with a status field. The old
    wait RPCs returned:
      - value:     {"type": "inline", ..., "value": ...} (the value directly)
      - error:     {"status": "error", "error_type": "...", "error_message": "..."}
      - cancelled: {"status": "cancelled"}
      - dismissed: {"status": "dismissed"}
      - timeout:   None

    Convert so existing tests that compare against those shapes keep working.
    """
    if result is None:
        return None
    if not isinstance(result, dict):
        return result
    status = result.get("status")
    if status == "ok":
        return result.get("value")
    if status == "timeout":
        return None
    if status == "error":
        err = result.get("error") or {}
        return {
            "status": "error",
            "error_type": err.get("type", ""),
            "error_message": err.get("message", ""),
        }
    if status in ("cancelled", "dismissed"):
        return {"status": status}
    return result

Execution = namedtuple("Execution", ["conn", "execution_id", "module", "target", "arguments"])


class ExecutorConnection:
    """A single connection from a test-adapter shim instance."""

    def __init__(self, conn):
        self._conn = conn
        self._file = conn.makefile("rb")
        self._next_request_id = 1
        # When a test mixes RPCs with async pushes (stream_items, stream_closed),
        # we may read a push while waiting for a response and vice-versa. Park
        # mismatched messages here so the next `recv` / helper picks them up.
        self._buffer = []

    def send(self, msg: dict):
        self._conn.sendall(protocol.encode_message(msg))

    def recv(self, timeout=10) -> dict:
        if self._buffer:
            return self._buffer.pop(0)
        return self._recv_raw(timeout)

    def _recv_raw(self, timeout) -> dict:
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
        """Send a request message (with auto-assigned ID) and return the response.

        If async pushes (stream_items / stream_closed) arrive ahead of the
        response, they're parked in the buffer so tests can fetch them later
        via recv_push.
        """
        rid = self._next_request_id
        self._next_request_id += 1
        msg["id"] = rid
        self.send(msg)
        while True:
            incoming = self.recv()
            if incoming.get("id") == rid:
                return incoming
            # Park non-matching messages (typically notifications).
            self._buffer.append(incoming)

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

    def select(
        self,
        execution_id,
        handles,
        timeout_ms=None,
        suspend=True,
        cancel_remaining=False,
    ):
        """Send a select request and return the result dict (or error dict).

        ``handles`` is a list of handle specs built via
        ``protocol.execution_handle(id)`` or ``protocol.input_handle(id)``.
        """
        msg = protocol.select_request(
            None,
            execution_id,
            handles,
            timeout_ms=timeout_ms,
            suspend=suspend,
            cancel_remaining=cancel_remaining,
        )
        resp = self._request(msg)
        return resp.get("result", resp.get("error"))

    def resolve(self, execution_id, target_execution_id):
        """Resolve a single execution reference (convenience over select).

        Returns the legacy resolve_reference shape for backward compatibility.
        """
        return _unwrap_select_result(
            self.select(
                execution_id,
                [protocol.execution_handle(target_execution_id)],
            )
        )

    def poll(self, execution_id, target_execution_id, timeout_ms=0):
        """Poll for an execution result without suspending.

        Returns the legacy shape (or None if the poll timed out).
        """
        return _unwrap_select_result(
            self.select(
                execution_id,
                [protocol.execution_handle(target_execution_id)],
                timeout_ms=timeout_ms,
                suspend=False,
            )
        )

    def cancel(self, execution_id, target_execution_id):
        """Cancel a child execution (convenience wrapper over cancel_request)."""
        msg = protocol.cancel_request(
            None,
            execution_id,
            [protocol.execution_handle(target_execution_id)],
        )
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

    def resolve_input(
        self, input_external_id, from_execution_id, timeout_ms=None, suspend=True
    ):
        """Resolve an input (convenience over select).

        Returns the legacy resolve_input shape:
          - value: the raw value dict
          - dismissed: {"status": "dismissed"}
          - timeout: None
        Raises RuntimeError if the server returned a protocol-level error.
        """
        msg = protocol.select_request(
            None,
            from_execution_id,
            [protocol.input_handle(input_external_id)],
            timeout_ms=timeout_ms,
            suspend=suspend,
        )
        resp = self._request(msg)
        if "error" in resp:
            raise RuntimeError(f"select error: {resp['error']}")
        return _unwrap_select_result(resp.get("result"))

    # --- Stream producer helpers ---

    def stream_register(self, execution_id, index):
        """Notify that a new stream exists."""
        self.send(protocol.stream_register(execution_id, index))

    def stream_append(self, execution_id, index, sequence, value, format="json"):
        """Append an item (raw JSON value) to a stream."""
        self.send(protocol.stream_append(execution_id, index, sequence, value, format=format))

    def stream_close(self, execution_id, index, error=None):
        """Close a stream (optionally with an error {type, message, traceback})."""
        self.send(protocol.stream_close(execution_id, index, error=error))

    # --- Stream consumer helpers ---

    def stream_subscribe(
        self,
        execution_id,
        subscription_id,
        producer_execution_id,
        index,
        from_sequence=0,
        filter=None,
    ):
        """Subscribe to a stream. ``filter`` is an optional dict built via
        protocol.slice_filter / partition_filter / chain_filter."""
        self.send(
            protocol.stream_subscribe(
                execution_id,
                subscription_id,
                producer_execution_id,
                index,
                from_sequence=from_sequence,
                filter=filter,
            )
        )

    def stream_unsubscribe(self, execution_id, subscription_id):
        self.send(protocol.stream_unsubscribe(execution_id, subscription_id))

    def recv_push(self, method, subscription_id=None, timeout=10):
        """Read messages until one matching ``method`` (and subscription) arrives.

        Returns the params dict. Non-matching messages are re-buffered in
        order so later calls can consume them.
        """
        held = []
        deadline = time.time() + timeout
        try:
            while True:
                remaining = max(0.01, deadline - time.time())
                msg = self.recv(timeout=remaining)
                if msg.get("method") == method:
                    params = msg.get("params", {})
                    if (
                        subscription_id is None
                        or params.get("subscription_id") == subscription_id
                    ):
                        # Put held messages back (preserve order) before returning.
                        self._buffer[:0] = held
                        return params
                held.append(msg)
        except TimeoutError:
            # Restore buffer and propagate.
            self._buffer[:0] = held
            raise

    def drain_stream(self, subscription_id, timeout=10):
        """Collect every pushed item + final closure for ``subscription_id``.

        Returns ``(items, closed_params)`` where ``items`` is a list of
        ``[position, value_dict]`` pairs in arrival order. Messages for other
        subscriptions are re-buffered so later calls can fetch them.
        """
        items = []
        deadline = time.time() + timeout
        while True:
            remaining = max(0.01, deadline - time.time())
            msg = self.recv(timeout=remaining)
            method = msg.get("method")
            params = msg.get("params", {})
            if params.get("subscription_id") != subscription_id or method not in (
                "stream_items",
                "stream_closed",
            ):
                self._buffer.append(msg)
                continue
            if method == "stream_items":
                items.extend(params.get("items", []))
                continue
            # stream_closed — terminal
            return items, params

    def complete(self, execution_id, value=None):
        """Send execution_result and signal the mock adapter we're done.

        Closing the socket mirrors what a real adapter does — it exits after
        the task finishes (and streams drain). Without this, the CLI's
        post-result receive loop never hits EOF, so the mock adapter hangs
        and ties up the worker's concurrency slot.
        """
        self.send(protocol.execution_result(execution_id, value=value))
        self._close_sending_side()

    def fail(self, execution_id, error_type, message, traceback="", retryable=None):
        """Send execution_error."""
        self.send(
            protocol.execution_error(execution_id, error_type, message, traceback, retryable=retryable)
        )
        self._close_sending_side()

    def _close_sending_side(self):
        """Shut down the socket's write side; reads stay open for any pushes
        in flight (e.g. final stream_closed) so asserts can still collect.
        """
        try:
            self._conn.shutdown(socket.SHUT_WR)
        except OSError:
            pass

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
