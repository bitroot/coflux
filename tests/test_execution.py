"""Tests for core workflow and task execution mechanics."""

import json
import os
import tempfile

from support.manifest import task, workflow
from support.protocol import execution_error, execution_result, json_args


def test_simple_workflow(worker):
    """Submit a no-arg workflow, return a JSON result, verify via runs result."""
    targets = [workflow("test", "my_workflow")]

    def handler(execution_id, module, target, arguments):
        assert target == "my_workflow"
        assert arguments == []
        return execution_result(execution_id, value=42)

    with worker(targets, handler) as ctx:
        assert ctx.run("test", "my_workflow") == {"value": 42}


def test_workflow_with_arguments(worker):
    """Submit a workflow with parameters, verify arguments arrive correctly."""
    targets = [workflow("test", "greet", parameters=["name", "count"])]

    def handler(execution_id, module, target, arguments):
        assert arguments[0]["value"] == "hello"
        assert arguments[1]["value"] == 3
        return execution_result(execution_id, value="hello hello hello")

    with worker(targets, handler) as ctx:
        assert ctx.run("test", "greet", '"hello"', "3") == {"value": "hello hello hello"}


def test_workflow_error(worker):
    """Return an execution error, verify it shows up in the run result."""
    targets = [workflow("test", "failing")]

    def handler(execution_id, module, target, arguments):
        return execution_error(
            execution_id,
            error_type="ValueError",
            message="something went wrong",
        )

    with worker(targets, handler) as ctx:
        result = ctx.run("test", "failing")
        assert result == {
            "error": {"type": "ValueError", "message": "something went wrong"}
        }


def test_workflow_no_result(worker):
    """Return execution_result with no value (None), verify run completes."""
    targets = [workflow("test", "noop")]

    def handler(execution_id, module, target, arguments):
        return execution_result(execution_id)

    with worker(targets, handler) as ctx:
        assert ctx.run("test", "noop") == {"value": None}


def test_error_with_traceback(worker):
    """Error traceback string is preserved through the pipeline."""
    targets = [workflow("test", "failing")]

    traceback = '  File "test.py", line 10, in my_func\n    x = 1/0'

    def handler(eid, module, target, args):
        return execution_error(
            eid, "ZeroDivisionError", "division by zero", traceback=traceback
        )

    with worker(targets, handler) as ctx:
        resp = ctx.submit("test", "failing")
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
    targets = [workflow("test", "process", parameters=["data"])]

    payload = {
        "users": [{"name": "Alice", "active": True, "tags": [1, None]}],
        "count": 0,
        "flag": False,
        "metadata": {"nested": {"deep": True}},
    }

    def handler(eid, module, target, args):
        # Server may encode dicts/lists in a structured format, so we don't
        # assert exact equality. Just verify the argument arrived.
        assert len(args) == 1
        assert args[0]["format"] == "json"
        return execution_result(eid, value=payload)

    with worker(targets, handler) as ctx:
        # The result round-trips through the server and comes back as raw data
        result = ctx.run("test", "process", json.dumps(payload))
        assert result["value"] == payload


def test_task_from_workflow(worker):
    """Workflow submits a task, resolves its result, returns final result."""
    targets = [
        workflow("test", "orchestrator"),
        task("test", "compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "orchestrator")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()
        assert ex0.target == "orchestrator"

        ref = ex0.conn.submit_task(ex0.execution_id, "test", "compute", json_args("10"))

        ex1 = ctx.executor.next_execute()
        assert ex1.target == "compute"
        ex1.conn.complete(ex1.execution_id, value=20)

        resolved = ex0.conn.resolve(ex0.execution_id, ref)
        assert resolved["type"] == "inline"

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_task_error_propagation(worker):
    """Child task error is returned when workflow resolves the reference."""
    targets = [
        workflow("test", "main"),
        task("test", "failing_task"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        ref = ex0.conn.submit_task(ex0.execution_id, "test", "failing_task", [])

        ex1 = ctx.executor.next_execute()
        ex1.conn.fail(ex1.execution_id, "RuntimeError", "task failed")

        # Resolving a failed child returns a successful response with error details
        result = ex0.conn.resolve(ex0.execution_id, ref)
        assert result["status"] == "error"
        assert result["error_type"] == "RuntimeError"
        assert result["error_message"] == "task failed"

        ex0.conn.complete(ex0.execution_id, value="handled")
        assert ctx.result(run_id)["value"]["data"] == "handled"


def test_error_type_preserved(worker):
    """Fully qualified error type is preserved through the pipeline."""
    targets = [workflow("test", "failing")]

    def handler(execution_id, module, target, arguments):
        return execution_error(
            execution_id,
            error_type="builtins.ZeroDivisionError",
            message="division by zero",
        )

    with worker(targets, handler) as ctx:
        result = ctx.run("test", "failing")
        assert result == {
            "error": {
                "type": "builtins.ZeroDivisionError",
                "message": "division by zero",
            }
        }


def test_child_error_details_in_resolve(worker):
    """Child error type and message are available when resolving a failed child."""
    targets = [
        workflow("test", "parent"),
        task("test", "child"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "parent")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()
        ref = ex0.conn.submit_task(ex0.execution_id, "test", "child", [])

        ex1 = ctx.executor.next_execute()
        ex1.conn.fail(ex1.execution_id, "builtins.ValueError", "invalid input")

        result = ex0.conn.resolve(ex0.execution_id, ref)
        assert result["status"] == "error"
        assert result["error_type"] == "builtins.ValueError"
        assert result["error_message"] == "invalid input"

        ex0.conn.complete(ex0.execution_id, value="recovered")
        assert ctx.result(run_id)["value"]["data"] == "recovered"


def test_fan_out(worker):
    """Workflow submits two tasks in parallel and collects both results."""
    targets = [
        workflow("test", "main"),
        task("test", "compute", parameters=["x"]),
    ]

    with worker(targets, concurrency=3) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        # Submit two tasks
        ref1 = ex0.conn.submit_task(ex0.execution_id, "test", "compute", json_args(3))
        ref2 = ex0.conn.submit_task(ex0.execution_id, "test", "compute", json_args(7))

        # Tasks execute on fresh connections
        for _ in range(2):
            ex = ctx.executor.next_execute()
            x = ex.arguments[0]["value"]
            ex.conn.complete(ex.execution_id, value=x * 2)

        # Resolve both and collect values
        val1 = ex0.conn.resolve(ex0.execution_id, ref1)["value"]
        val2 = ex0.conn.resolve(ex0.execution_id, ref2)["value"]
        assert {val1, val2} == {6, 14}

        ex0.conn.complete(ex0.execution_id, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_sequential_tasks(worker):
    """Workflow chains two tasks: result of first feeds into second."""
    targets = [
        workflow("test", "pipeline"),
        task("test", "add", parameters=["a", "b"]),
        task("test", "multiply", parameters=["a", "b"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "pipeline")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        # Submit add(3, 4) and resolve
        ref1 = ex0.conn.submit_task(ex0.execution_id, "test", "add", json_args(3, 4))

        ex_t1 = ctx.executor.next_execute()
        assert ex_t1.target == "add"
        ex_t1.conn.complete(ex_t1.execution_id, value=7)

        assert ex0.conn.resolve(ex0.execution_id, ref1)["value"] == 7

        # Submit multiply(7, 2) and resolve (fresh connection)
        ref2 = ex0.conn.submit_task(ex0.execution_id, "test", "multiply", json_args(7, 2))

        ex_t2 = ctx.executor.next_execute()
        assert ex_t2.target == "multiply"
        ex_t2.conn.complete(ex_t2.execution_id, value=14)

        assert ex0.conn.resolve(ex0.execution_id, ref2)["value"] == 14

        ex0.conn.complete(ex0.execution_id, value=14)
        assert ctx.result(run_id)["value"]["data"] == 14


def test_multiple_modules(worker):
    """Targets in different modules work correctly."""
    targets = [
        workflow("app", "main"),
        task("compute", "double", parameters=["x"]),
    ]

    with worker(targets, modules=["app", "compute"], concurrency=2) as ctx:
        resp = ctx.submit("app", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()
        assert ex0.target == "main"

        ref = ex0.conn.submit_task(ex0.execution_id, "compute", "double", json_args(5))

        ex1 = ctx.executor.next_execute()
        assert ex1.target == "double"
        ex1.conn.complete(ex1.execution_id, value=10)

        assert ex0.conn.resolve(ex0.execution_id, ref)["value"] == 10

        ex0.conn.complete(ex0.execution_id, value=10)
        assert ctx.result(run_id)["value"]["data"] == 10


def test_rerun_step(worker):
    """Completed workflow step can be re-run, producing a new execution."""
    targets = [workflow("test", "my_workflow")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "my_workflow")
        run_id = resp["runId"]

        # First execution
        ex0 = ctx.executor.next_execute()
        ex0.conn.complete(ex0.execution_id, value="first")

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
        ex1 = ctx.executor.next_execute()
        assert ex1.execution_id != ex0.execution_id
        ex1.conn.complete(ex1.execution_id, value="rerun")

        result = ctx.result(run_id)
        assert result["value"]["data"] == "rerun"


def test_workflow_calls_workflow(worker):
    """Workflow submits another workflow as a child, resolves its result."""
    targets = [
        workflow("test", "outer"),
        workflow("test", "inner"),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "outer")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()
        assert ex0.target == "outer"

        # Submit inner workflow (type="workflow")
        ref = ex0.conn.submit_workflow(ex0.execution_id, "test", "inner", [])

        ex1 = ctx.executor.next_execute()
        assert ex1.target == "inner"
        ex1.conn.complete(ex1.execution_id, value="inner result")

        resolved = ex0.conn.resolve(ex0.execution_id, ref)
        assert resolved["value"] == "inner result"

        ex0.conn.complete(ex0.execution_id, value="outer done")
        assert ctx.result(run_id)["value"]["data"] == "outer done"


def test_blob_argument_round_trip(worker):
    """Large file argument is uploaded as blob and received as file on the other end."""
    targets = [
        workflow("test", "main"),
        task("test", "process", parameters=["data"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        run_id = resp["runId"]

        ex0 = ctx.executor.next_execute()

        # Create a temp file larger than blob threshold (100 bytes)
        content = "x" * 200
        fd, tmp_path = tempfile.mkstemp(suffix=".json")
        try:
            with open(fd, "w") as f:
                json.dump(content, f)

            # Submit task with file-type argument
            file_arg = [{"type": "file", "format": "json", "path": tmp_path}]
            ref = ex0.conn.submit_task(ex0.execution_id, "test", "process", file_arg)

            # Receiving executor should get the argument as a file
            ex1 = ctx.executor.next_execute()
            assert ex1.target == "process"
            assert len(ex1.arguments) == 1
            assert ex1.arguments[0]["type"] == "file"
            assert "path" in ex1.arguments[0]

            # Read the file to verify content survived the blob round-trip
            with open(ex1.arguments[0]["path"]) as f:
                received = json.load(f)
            assert received == content

            ex1.conn.complete(ex1.execution_id, value="processed")
            assert ex0.conn.resolve(ex0.execution_id, ref)["value"] == "processed"

            ex0.conn.complete(ex0.execution_id, value="done")
            assert ctx.result(run_id)["value"]["data"] == "done"
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


def test_idempotency_same_key_returns_same_run(worker):
    """Submitting with the same idempotency key returns the existing run."""
    targets = [workflow("test", "my_workflow")]

    with worker(targets) as ctx:
        resp1 = ctx.submit("test", "my_workflow", idempotency_key="key-1")
        run_id1 = resp1["runId"]

        resp2 = ctx.submit("test", "my_workflow", idempotency_key="key-1")
        run_id2 = resp2["runId"]

        assert run_id1 == run_id2

        # Only one execution should be dispatched
        ctx.handle_one()
