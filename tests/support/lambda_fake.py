"""A stand-in for the AWS Lambda API, for exercising the Lambda launcher.

The real service runs the function in a sandbox. This runs the reference
handler (``examples/lambda/handler.py``) in a subprocess, with a context
whose remaining time counts down from the function's timeout, and holds
the synchronous Invoke open until the handler returns or the timeout
ends it - answering the way Lambda does, with the function's error and
the tail of its log in headers. So the launcher's whole path (request
signing, GetFunctionConfiguration, the held Invoke, how a result is read)
and the handler are exercised together, without an AWS account.

Requests aren't authenticated (there's no secret to check against), but
what the launcher sends is kept for tests to inspect.
"""

import base64
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

RUNNER = os.path.join(os.path.dirname(__file__), "lambda_runner.py")
HANDLER_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "examples", "lambda")
)

# Lambda returns the last 4KB of the log.
LOG_TAIL_BYTES = 4096


class FakeLambda:
    def __init__(self, cli_path, cwd, function_name="coflux-worker", timeout=60):
        self.cwd = cwd
        self.function_name = function_name
        # The function's timeout, in seconds.
        self.timeout = timeout
        # (method, action, path, query, body, headers) in the order received.
        self.requests = []
        # Errors (status, type, message) to answer Invoke with, one per
        # request, before any invocation runs.
        self.invoke_errors = []
        # Set to make the handler raise before it starts the worker.
        self.handler_error = None
        self.invocations = {}
        self._lock = threading.Lock()
        # The handler runs `coflux` from PATH, which the binary under test
        # may not be named.
        self._bin_dir = tempfile.mkdtemp(prefix="fake-lambda-bin-")
        os.symlink(cli_path, os.path.join(self._bin_dir, "coflux"))
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._make_handler())
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self._server.server_port}"

    def start(self):
        self._thread.start()

    def close(self):
        with self._lock:
            procs = [i["proc"] for i in self.invocations.values()]
        for proc in procs:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=5)
        self._server.shutdown()
        self._server.server_close()

    def requests_for(self, action):
        with self._lock:
            return [r for r in self.requests if r["action"] == action]

    def wait_for(self, action, count=1, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            received = self.requests_for(action)
            if len(received) >= count:
                return received
            time.sleep(0.1)
        raise TimeoutError(f"{action} not received {count} time(s) within {timeout}s")

    def running(self):
        with self._lock:
            return [i for i in self.invocations.values() if i["proc"].poll() is None]

    # --- Handling ---

    def _make_handler(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format, *args):
                pass

            def do_GET(self):
                self._handle("GET")

            def do_POST(self):
                self._handle("POST")

            def _handle(self, method):
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length else b""
                url = urlparse(self.path)
                parts = url.path.split("/")
                # /2015-03-31/functions/{name}/{action}
                name = unquote(parts[3]) if len(parts) > 4 else None
                action = parts[4] if len(parts) > 4 else None
                request = {
                    "method": method,
                    "action": action,
                    "function": name,
                    "query": {k: v[0] for k, v in parse_qs(url.query).items()},
                    "body": json.loads(raw) if raw else None,
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                }
                with fake._lock:
                    fake.requests.append(request)
                status, headers, body = fake._dispatch(request)
                self.send_response(status)
                for key, value in headers.items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return Handler

    def _dispatch(self, request):
        if request["function"] != self.function_name:
            return _error(
                404,
                "ResourceNotFoundException",
                f"Function not found: {request['function']}",
            )
        if request["action"] == "configuration" and request["method"] == "GET":
            return (
                200,
                {"Content-Type": "application/json"},
                json.dumps(
                    {"FunctionName": self.function_name, "Timeout": self.timeout}
                ).encode(),
            )
        if request["action"] == "invocations" and request["method"] == "POST":
            with self._lock:
                error = self.invoke_errors.pop(0) if self.invoke_errors else None
            if error:
                return _error(*error)
            return self._invoke(request["body"])
        return _error(400, "InvalidParameterValueException", "Unknown action")

    def _invoke(self, event):
        invocation_id = uuid.uuid4().hex
        log_path = os.path.join(self.cwd, f"invocation-{invocation_id}.log")
        result_path = os.path.join(self.cwd, f"invocation-{invocation_id}.json")
        deadline_ms = int(time.time() * 1000) + self.timeout * 1000
        env = {
            "PATH": f"{self._bin_dir}{os.pathsep}{os.environ['PATH']}",
            "FAKE_LAMBDA_DEADLINE_MS": str(deadline_ms),
            "FAKE_LAMBDA_RESULT": result_path,
            "FAKE_LAMBDA_HANDLER_DIR": HANDLER_DIR,
        }
        if self.handler_error:
            env["FAKE_LAMBDA_HANDLER_ERROR"] = self.handler_error

        with open(log_path, "wb") as log:
            proc = subprocess.Popen(
                [sys.executable, RUNNER],
                cwd=self.cwd,
                env=env,
                stdin=subprocess.PIPE,
                stdout=log,
                stderr=subprocess.STDOUT,
                # Its own group, so a timeout takes the worker with it, as
                # Lambda ending the sandbox would.
                start_new_session=True,
            )
        with self._lock:
            self.invocations[invocation_id] = {"proc": proc, "event": event}
        stdin = proc.stdin
        assert stdin is not None
        stdin.write(json.dumps(event).encode())
        stdin.close()

        timed_out = False
        try:
            proc.wait(timeout=self.timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()

        with open(log_path, "rb") as f:
            log = f.read()[-LOG_TAIL_BYTES:]
        headers = {
            "Content-Type": "application/json",
            "X-Amz-Log-Result": base64.b64encode(log).decode(),
        }
        if timed_out:
            headers["X-Amz-Function-Error"] = "Unhandled"
            body = {
                "errorType": "Sandbox.Timedout",
                "errorMessage": f"Task timed out after {self.timeout:.2f} seconds",
            }
        else:
            try:
                with open(result_path) as f:
                    result = json.load(f)
            except FileNotFoundError:
                # The runtime died without the handler returning - killed
                # as the fake closed, say.
                result = {
                    "error": {
                        "errorType": "Runtime.ExitError",
                        "errorMessage": f"Runtime exited with code {proc.returncode}",
                    }
                }
            if "error" in result:
                headers["X-Amz-Function-Error"] = "Unhandled"
                body = result["error"]
            else:
                body = result["value"]
        return 200, headers, json.dumps(body).encode()


def _error(status, error_type, message):
    return (
        status,
        {"Content-Type": "application/json", "X-Amzn-ErrorType": error_type},
        json.dumps({"Type": "User", "message": message}).encode(),
    )
