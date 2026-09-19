"""A stand-in for the Amazon ECS API, for exercising the ECS launcher.

The real service runs containers. This runs ``coflux worker`` processes
from the command and environment overrides that a RunTask carries, and
describes them the way ECS describes tasks, so the launcher's whole path -
request signing, the RunTask it builds, polling, stopping, and how a
stopped task's state is read - is exercised without an AWS account.

Requests aren't authenticated (there's no secret to check against), but
what the launcher sends is kept for tests to inspect.
"""

import json
import os
import signal
import subprocess
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TARGET_PREFIX = "AmazonEC2ContainerServiceV20141113."


class FakeEcs:
    def __init__(self, cli_path, cwd, cluster="test-cluster", container_name="worker"):
        self.cli_path = cli_path
        self.cwd = cwd
        self.cluster = cluster
        self.container_name = container_name
        # (action, body, headers) in the order received.
        self.requests: list[tuple[str, dict, dict]] = []
        # task ARN -> {"proc", "stop_code", "stopped_reason", "container_reason",
        # "exit_code"}
        self.tasks = {}
        # (type, message) to reject every RunTask with, or None.
        self.run_task_error = None
        # Likewise for DescribeTaskDefinition.
        self.task_definition_error = None
        self._lock = threading.Lock()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._make_handler())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self._server.server_port}"

    def start(self):
        self._thread.start()

    def close(self):
        self._server.shutdown()
        self._server.server_close()
        with self._lock:
            tasks = list(self.tasks.values())
        for task in tasks:
            proc = task["proc"]
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)

    def requests_for(self, action):
        with self._lock:
            return [
                (body, headers) for a, body, headers in self.requests if a == action
            ]

    def wait_for(self, action, count=1, timeout=30):
        """Wait until ``count`` requests for ``action`` have been received."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            received = self.requests_for(action)
            if len(received) >= count:
                return received
            time.sleep(0.1)
        raise TimeoutError(f"{action} not received {count} time(s) within {timeout}s")

    def task_arns(self):
        with self._lock:
            return list(self.tasks)

    def kill_with_oom(self, arn):
        """Kill a task's process the way an OOM kill looks from ECS."""
        with self._lock:
            task = self.tasks[arn]
            task["stop_code"] = "EssentialContainerExited"
            task["stopped_reason"] = "Essential container in task exited"
            task["container_reason"] = (
                "OutOfMemoryError: Container killed due to memory usage"
            )
            task["exit_code"] = 137
            proc = task["proc"]
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=10)

    # --- Handling ---

    def _make_handler(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length) or b"{}")
                target = self.headers.get("X-Amz-Target", "")
                action = (
                    target[len(TARGET_PREFIX) :]
                    if target.startswith(TARGET_PREFIX)
                    else target
                )
                headers = {k.lower(): v for k, v in self.headers.items()}
                with fake._lock:
                    fake.requests.append((action, body, headers))
                status, response = fake._dispatch(action, body)
                payload = json.dumps(response).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/x-amz-json-1.1")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        return Handler

    def _dispatch(self, action, body):
        if action == "DescribeTaskDefinition":
            if self.task_definition_error:
                error_type, message = self.task_definition_error
                return 400, {"__type": error_type, "message": message}
            return 200, {
                "taskDefinition": {
                    "family": body.get("taskDefinition"),
                    "containerDefinitions": [{"name": self.container_name}],
                }
            }
        if action == "RunTask":
            return self._run_task(body)
        if action == "DescribeTasks":
            return self._describe_tasks(body)
        if action == "StopTask":
            return self._stop_task(body)
        return 400, {
            "__type": "InvalidParameterException",
            "message": f"Unknown action {action}",
        }

    def _run_task(self, body):
        if self.run_task_error:
            error_type, message = self.run_task_error
            return 400, {"__type": error_type, "message": message}
        if body.get("cluster") != self.cluster:
            return 400, {
                "__type": "ClusterNotFoundException",
                "message": "Cluster not found.",
            }

        override = body["overrides"]["containerOverrides"][0]
        command = override.get("command", [])
        env = {e["name"]: e["value"] for e in override.get("environment", [])}

        task_id = uuid.uuid4().hex
        # A container has only the environment its task gives it - not the
        # test runner's shell - and ECS sends its output to the task
        # definition's log driver; here that's a file next to the worker,
        # for when a test needs to see why one didn't start.
        with open(os.path.join(self.cwd, f"task-{task_id}.log"), "wb") as log:
            proc = subprocess.Popen(
                [self.cli_path, "worker", *command],
                cwd=self.cwd,
                env={"PATH": os.environ["PATH"], **env},
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        arn = f"arn:aws:ecs:us-east-1:123456789012:task/{self.cluster}/{task_id}"
        with self._lock:
            self.tasks[arn] = {
                "proc": proc,
                "stop_code": None,
                "stopped_reason": None,
                "container_reason": None,
                "exit_code": None,
            }
        return 200, {
            "tasks": [{"taskArn": arn, "lastStatus": "PROVISIONING"}],
            "failures": [],
        }

    def _describe_tasks(self, body):
        tasks = []
        failures = []
        for arn in body.get("tasks", []):
            with self._lock:
                task = self.tasks.get(arn)
            if task is None:
                failures.append({"arn": arn, "reason": "MISSING"})
            else:
                tasks.append(self._describe(arn, task))
        return 200, {"tasks": tasks, "failures": failures}

    def _stop_task(self, body):
        arn = body.get("task")
        with self._lock:
            task = self.tasks.get(arn)
            if task is not None and task["stop_code"] is None:
                task["stop_code"] = "UserInitiated"
                task["stopped_reason"] = body.get("reason", "")
        if task is None:
            return 400, {
                "__type": "InvalidParameterException",
                "message": "The referenced task was not found.",
            }
        proc = task["proc"]
        if proc.poll() is None:
            proc.terminate()
        return 200, {"task": self._describe(arn, task)}

    def _describe(self, arn, task):
        proc = task["proc"]
        returncode = proc.poll()
        if returncode is None:
            return {"taskArn": arn, "lastStatus": "RUNNING", "containers": []}
        exit_code = task["exit_code"] if task["exit_code"] is not None else returncode
        container = {"name": self.container_name, "exitCode": exit_code}
        if task["container_reason"]:
            container["reason"] = task["container_reason"]
        described = {
            "taskArn": arn,
            "lastStatus": "STOPPED",
            "stopCode": task["stop_code"] or "EssentialContainerExited",
            "containers": [container],
        }
        if task["stopped_reason"]:
            described["stoppedReason"] = task["stopped_reason"]
        return described
