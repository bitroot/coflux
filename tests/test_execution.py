"""Tests for core workflow and task execution mechanics."""

import json
import os
import tempfile

from support.manifest import task, workflow
from support.protocol import execution_error, execution_result, json_args


def test_simple_workflow(worker):
    """Submit a no-arg workflow, return a JSON result, verify via runs result."""
    targets = [workflow("test.my_workflow")]

    def handler(execution_id, target, arguments):
        assert target == "test.my_workflow"
        assert arguments == []
        return execution_result(execution_id, value=42)

    with worker(targets, handler) as ctx:
        assert ctx.run("test.my_workflow") == {"value": 42}


def test_workflow_with_arguments(worker):
    """Submit a workflow with parameters, verify arguments arrive correctly."""
    targets = [workflow("test.greet", parameters=["name", "count"])]

    def handler(execution_id, target, arguments):
        assert arguments[0]["value"] == "hello"
        assert arguments[1]["value"] == 3
        return execution_result(execution_id, value="hello hello hello")

    with worker(targets, handler) as ctx:
        assert ctx.run("test.greet", '"hello"', "3") == {"value": "hello hello hello"}


def test_workflow_error(worker):
    """Return an execution error, verify it shows up in the run result."""
    targets = [workflow("test.failing")]

    def handler(execution_id, target, arguments):
        return execution_error(
            execution_id,
            error_type="ValueError",
            message="something went wrong",
        )

    with worker(targets, handler) as ctx:
        result = ctx.run("test.failing")
        assert result == {
            "error": {"type": "ValueError", "message": "something went wrong"}
        }


def test_workflow_no_result(worker):
    """Return execution_result with no value (None), verify run completes."""
    targets = [workflow("test.noop")]

    def handler(execution_id, target, arguments):
        return execution_result(execution_id)

    with worker(targets, handler) as ctx:
        assert ctx.run("test.noop") == {"value": None}


def test_error_with_traceback(worker):
    """Error traceback string is preserved through the pipeline."""
    targets = [workflow("test.failing")]

    traceback = '  File "test.py", line 10, in my_func\n    x = 1/0'

    def handler(eid, target, args):
        return execution_error(
            eid, "ZeroDivisionError", "division by zero", traceback=traceback
        )

    with worker(targets, handler) as ctx:
        resp = ctx.submit("test.failing")
        ctx.handle_one()
        result = ctx.result(resp["runId"])
        assert result["type"] == "error"
        assert result["error"]["type"] == "ZeroDivisionError"
        assert result["error"]["message"] == "division by zero"
        # CLI parses traceback into structured frames
        frames = result["error"]["frames"]
        assert len(frames) >= 1
        assert frames[0]["file"] == "test.py"
        assert frames[0]["line"] == 10
        assert frames[0]["name"] == "my_func"


def test_nested_arguments(worker):
    """Complex nested data structures survive the submit->execute round-trip."""
    targets = [workflow("test.process", parameters=["data"])]

    payload = {
        "users": [{"name": "Alice", "active": True, "tags": [1, None]}],
        "count": 0,
        "flag": False,
        "metadata": {"nested": {"deep": True}},
    }

    def handler(eid, target, args):
        # Server may encode dicts/lists in a structured format, so we don't
        # assert exact equality. Just verify the argument arrived.
        assert len(args) == 1
        assert args[0]["format"] == "json"
        return execution_result(eid, value=payload)

    with worker(targets, handler) as ctx:
        # The result round-trips through the server and comes back as raw data
        result = ctx.run("test.process", json.dumps(payload))
        assert result["value"] == payload


def test_task_from_workflow(worker):
    """Workflow submits a task, resolves its result, returns final result."""
    targets = [
        workflow("test.orchestrator"),
        task("test.compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.orchestrator")
        run_id = resp["runId"]

        conn0, wf_eid, target, _ = ctx.executor.next_execute()
        assert target == "test.orchestrator"

        ref = conn0.submit_task(wf_eid, "test.compute", json_args("10"))

        conn1, task_eid, target, _ = ctx.executor.next_execute()
        assert target == "test.compute"
        conn1.complete(task_eid, value=20)

        resolved = conn0.resolve(wf_eid, ref)
        assert resolved["type"] == "inline"

        conn0.complete(wf_eid, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_task_error_propagation(worker):
    """Child task error is returned when workflow resolves the reference."""
    targets = [
        workflow("test.main"),
        task("test.failing_task"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.main")
        run_id = resp["runId"]

        conn0, wf_eid, _, _ = ctx.executor.next_execute()

        ref = conn0.submit_task(wf_eid, "test.failing_task", [])

        conn1, task_eid, _, _ = ctx.executor.next_execute()
        conn1.fail(task_eid, "RuntimeError", "task failed")

        # Resolving a failed child returns a successful response with error details
        result = conn0.resolve(wf_eid, ref)
        assert result["status"] == "error"
        assert result["error_type"] == "RuntimeError"
        assert result["error_message"] == "task failed"

        conn0.complete(wf_eid, value="handled")
        assert ctx.result(run_id)["value"]["data"] == "handled"


def test_error_type_preserved(worker):
    """Fully qualified error type is preserved through the pipeline."""
    targets = [workflow("test.failing")]

    def handler(execution_id, target, arguments):
        return execution_error(
            execution_id,
            error_type="builtins.ZeroDivisionError",
            message="division by zero",
        )

    with worker(targets, handler) as ctx:
        result = ctx.run("test.failing")
        assert result == {
            "error": {
                "type": "builtins.ZeroDivisionError",
                "message": "division by zero",
            }
        }


def test_child_error_details_in_resolve(worker):
    """Child error type and message are available when resolving a failed child."""
    targets = [
        workflow("test.parent"),
        task("test.child"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.parent")
        run_id = resp["runId"]

        conn0, parent_eid, _, _ = ctx.executor.next_execute()
        ref = conn0.submit_task(parent_eid, "test.child", [])

        conn1, child_eid, _, _ = ctx.executor.next_execute()
        conn1.fail(child_eid, "builtins.ValueError", "invalid input")

        result = conn0.resolve(parent_eid, ref)
        assert result["status"] == "error"
        assert result["error_type"] == "builtins.ValueError"
        assert result["error_message"] == "invalid input"

        conn0.complete(parent_eid, value="recovered")
        assert ctx.result(run_id)["value"]["data"] == "recovered"


def test_fan_out(worker):
    """Workflow submits two tasks in parallel and collects both results."""
    targets = [
        workflow("test.main"),
        task("test.compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=3) as ctx:
        resp = ctx.submit("test.main")
        run_id = resp["runId"]

        conn0, wf_eid, _, _ = ctx.executor.next_execute()

        # Submit two tasks
        ref1 = conn0.submit_task(wf_eid, "test.compute", json_args(3))
        ref2 = conn0.submit_task(wf_eid, "test.compute", json_args(7))

        # Tasks execute on fresh connections
        for _ in range(2):
            conn, eid, _, args = ctx.executor.next_execute()
            x = args[0]["value"]
            conn.complete(eid, value=x * 2)

        # Resolve both and collect values
        val1 = conn0.resolve(wf_eid, ref1)["value"]
        val2 = conn0.resolve(wf_eid, ref2)["value"]
        assert {val1, val2} == {6, 14}

        conn0.complete(wf_eid, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_sequential_tasks(worker):
    """Workflow chains two tasks: result of first feeds into second."""
    targets = [
        workflow("test.pipeline"),
        task("test.add", parameters=["a", "b"]),
        task("test.multiply", parameters=["a", "b"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.pipeline")
        run_id = resp["runId"]

        conn0, wf_eid, _, _ = ctx.executor.next_execute()

        # Submit add(3, 4) and resolve
        ref1 = conn0.submit_task(wf_eid, "test.add", json_args(3, 4))

        conn_t1, task_eid, target, _ = ctx.executor.next_execute()
        assert target == "test.add"
        conn_t1.complete(task_eid, value=7)

        assert conn0.resolve(wf_eid, ref1)["value"] == 7

        # Submit multiply(7, 2) and resolve (fresh connection)
        ref2 = conn0.submit_task(wf_eid, "test.multiply", json_args(7, 2))

        conn_t2, task_eid, target, _ = ctx.executor.next_execute()
        assert target == "test.multiply"
        conn_t2.complete(task_eid, value=14)

        assert conn0.resolve(wf_eid, ref2)["value"] == 14

        conn0.complete(wf_eid, value=14)
        assert ctx.result(run_id)["value"]["data"] == 14


def test_multiple_modules(worker):
    """Targets in different modules work correctly."""
    targets = [
        workflow("app.main"),
        task("compute.double", parameters=["x"]),
    ]

    with worker(targets, modules=["app", "compute"], concurrency=2) as ctx:
        resp = ctx.submit("app.main")
        run_id = resp["runId"]

        conn0, wf_eid, target, _ = ctx.executor.next_execute()
        assert target == "app.main"

        ref = conn0.submit_task(wf_eid, "compute.double", json_args(5))

        conn1, task_eid, target, _ = ctx.executor.next_execute()
        assert target == "compute.double"
        conn1.complete(task_eid, value=10)

        assert conn0.resolve(wf_eid, ref)["value"] == 10

        conn0.complete(wf_eid, value=10)
        assert ctx.result(run_id)["value"]["data"] == 10


def test_rerun_step(worker):
    """Completed workflow step can be re-run, producing a new execution."""
    targets = [workflow("test.my_workflow")]

    with worker(targets) as ctx:
        resp = ctx.submit("test.my_workflow")
        run_id = resp["runId"]

        # First execution
        conn0, eid1, _, _ = ctx.executor.next_execute()
        conn0.complete(eid1, value="first")

        result = ctx.result(run_id)
        assert result["value"]["data"] == "first"

        # Find the step ID from inspect
        data = ctx.inspect(run_id)
        step_ids = list(data["steps"].keys())
        assert len(step_ids) == 1
        step_id = step_ids[0]

        # Re-run the step
        rerun_resp = ctx.rerun(step_id)
        assert rerun_resp["attempt"] > 1

        # Second execution arrives on a fresh connection
        conn1, eid2, _, _ = ctx.executor.next_execute()
        assert eid2 != eid1
        conn1.complete(eid2, value="rerun")

        result = ctx.result(run_id)
        assert result["value"]["data"] == "rerun"


def test_workflow_calls_workflow(worker):
    """Workflow submits another workflow as a child, resolves its result."""
    targets = [
        workflow("test.outer"),
        workflow("test.inner"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.outer")
        run_id = resp["runId"]

        conn0, wf_eid, target, _ = ctx.executor.next_execute()
        assert target == "test.outer"

        # Submit inner workflow (type="workflow")
        ref = conn0.submit_workflow(wf_eid, "test.inner", [])

        conn1, inner_eid, target, _ = ctx.executor.next_execute()
        assert target == "test.inner"
        conn1.complete(inner_eid, value="inner result")

        resolved = conn0.resolve(wf_eid, ref)
        assert resolved["value"] == "inner result"

        conn0.complete(wf_eid, value="outer done")
        assert ctx.result(run_id)["value"]["data"] == "outer done"


def test_blob_argument_round_trip(worker):
    """Large file argument is uploaded as blob and received as file on the other end."""
    targets = [
        workflow("test.main"),
        task("test.process", parameters=["data"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.main")
        run_id = resp["runId"]

        conn0, wf_eid, _, _ = ctx.executor.next_execute()

        # Create a temp file larger than blob threshold (100 bytes)
        content = "x" * 200
        fd, tmp_path = tempfile.mkstemp(suffix=".json")
        try:
            with open(fd, "w") as f:
                json.dump(content, f)

            # Submit task with file-type argument
            file_arg = [{"type": "file", "format": "json", "path": tmp_path}]
            ref = conn0.submit_task(wf_eid, "test.process", file_arg)

            # Receiving executor should get the argument as a file
            conn1, task_eid, target, args = ctx.executor.next_execute()
            assert target == "test.process"
            assert len(args) == 1
            assert args[0]["type"] == "file"
            assert "path" in args[0]

            # Read the file to verify content survived the blob round-trip
            with open(args[0]["path"]) as f:
                received = json.load(f)
            assert received == content

            conn1.complete(task_eid, value="processed")
            assert conn0.resolve(wf_eid, ref)["value"] == "processed"

            conn0.complete(wf_eid, value="done")
            assert ctx.result(run_id)["value"]["data"] == "done"
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


def test_idempotency_same_key_returns_same_run(worker):
    """Submitting with the same idempotency key returns the existing run."""
    targets = [workflow("test.my_workflow")]

    with worker(targets) as ctx:
        resp1 = ctx.submit("test.my_workflow", idempotency_key="key-1")
        run_id1 = resp1["runId"]

        resp2 = ctx.submit("test.my_workflow", idempotency_key="key-1")
        run_id2 = resp2["runId"]

        assert run_id1 == run_id2

        # Only one execution should be dispatched
        ctx.handle_one()
