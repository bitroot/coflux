"""Tests for input submission, resolution, and lifecycle."""

import json
import threading
import time

import support.cli as cli
import support.protocol as protocol
from support.helpers import api_post, managed_worker, poll_result
from support.manifest import task, workflow
from support.protocol import execution_result, json_args


# ---------------------------------------------------------------------------
# Basic lifecycle
# ---------------------------------------------------------------------------


def test_submit_and_respond_input(worker):
    """Submit an input, respond via CLI, resolve it, complete the workflow."""
    targets = [workflow("test", "ask")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "ask")
        ex = ctx.executor.next_execute()

        input_id = ex.conn.submit_input(ex.execution_id, "What is your name?")
        assert isinstance(input_id, str)
        assert "/i" in input_id  # Run-scoped format: R<run_id>/i<number>

        # Respond via CLI before resolving
        cli.inputs_respond(input_id, "Alice", host=ctx.host)

        # Resolve — should return the value immediately
        result = ex.conn.resolve_input(input_id, ex.execution_id)
        assert result is not None
        assert result["value"] == "Alice"

        ex.conn.complete(ex.execution_id, value="done")
        run_result = ctx.result(resp["runId"])
        assert run_result["type"] == "value"
        assert run_result["value"]["data"] == "done"


def test_submit_and_dismiss_input(worker):
    """Submit an input, dismiss via CLI, resolve returns dismissed status."""
    targets = [workflow("test", "ask")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "ask")
        ex = ctx.executor.next_execute()

        input_id = ex.conn.submit_input(ex.execution_id, "Approve?")

        cli.inputs_dismiss(input_id, host=ctx.host)

        result = ex.conn.resolve_input(input_id, ex.execution_id)
        assert result is not None
        assert result["status"] == "dismissed"

        ex.conn.complete(ex.execution_id, value="dismissed")
        run_result = ctx.result(resp["runId"])
        assert run_result["type"] == "value"


def test_input_no_schema(worker):
    """Input with just a template (no schema, no key) works."""
    targets = [workflow("test", "simple")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "simple")
        ex = ctx.executor.next_execute()

        input_id = ex.conn.submit_input(ex.execution_id, "Enter something")
        cli.inputs_respond(input_id, {"nested": "data"}, host=ctx.host)

        result = ex.conn.resolve_input(input_id, ex.execution_id)
        assert result["value"] == {"nested": "data"}

        ex.conn.complete(ex.execution_id, value="ok")
        assert ctx.result(resp["runId"])["type"] == "value"


def test_input_respond_null(worker):
    """Responding with null is valid and can be inspected."""
    targets = [workflow("test", "nullcheck")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "nullcheck")
        ex = ctx.executor.next_execute()

        input_id = ex.conn.submit_input(ex.execution_id, "value?")
        cli.inputs_respond(input_id, None, host=ctx.host)

        # Verify the response was recorded via inspect
        info = cli.inputs_inspect(input_id, host=ctx.host)
        assert info["response"] is not None
        assert info["response"]["type"] == "value"

        ex.conn.complete(ex.execution_id, value="ok")
        assert ctx.result(resp["runId"])["type"] == "value"


# ---------------------------------------------------------------------------
# Memoization (key-based dedup)
# ---------------------------------------------------------------------------


def test_input_memoization_same_key(worker):
    """Two inputs with the same key in the same run return the same input ID."""
    targets = [workflow("test", "memo")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "memo")
        ex = ctx.executor.next_execute()

        input_id_1 = ex.conn.submit_input(
            ex.execution_id, "Enter value", key="my-key"
        )
        input_id_2 = ex.conn.submit_input(
            ex.execution_id, "Enter value", key="my-key"
        )

        assert input_id_1 == input_id_2

        cli.inputs_respond(input_id_1, 42, host=ctx.host)
        result = ex.conn.resolve_input(input_id_1, ex.execution_id)
        assert result["value"] == 42

        ex.conn.complete(ex.execution_id, value="ok")
        assert ctx.result(resp["runId"])["type"] == "value"


def test_input_no_key_creates_separate_inputs(worker):
    """Two inputs without a key always create separate inputs."""
    targets = [workflow("test", "nokey")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "nokey")
        ex = ctx.executor.next_execute()

        input_id_1 = ex.conn.submit_input(ex.execution_id, "First?")
        input_id_2 = ex.conn.submit_input(ex.execution_id, "Second?")

        assert input_id_1 != input_id_2

        cli.inputs_respond(input_id_1, "a", host=ctx.host)
        cli.inputs_respond(input_id_2, "b", host=ctx.host)

        r1 = ex.conn.resolve_input(input_id_1, ex.execution_id)
        r2 = ex.conn.resolve_input(input_id_2, ex.execution_id)
        assert r1["value"] == "a"
        assert r2["value"] == "b"

        ex.conn.complete(ex.execution_id, value="ok")
        assert ctx.result(resp["runId"])["type"] == "value"


# ---------------------------------------------------------------------------
# Suspension and waiting
# ---------------------------------------------------------------------------


def test_input_suspends_execution(worker):
    """resolve_input with suspend=true suspends, then resumes when responded."""
    targets = [workflow("test", "suspend")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "suspend")
        ex = ctx.executor.next_execute()

        input_id = ex.conn.submit_input(ex.execution_id, "Waiting...")

        # Resolve with suspend — the worker will suspend and kill this executor.
        # We send the request but the connection closes before we get a response.
        msg = protocol.select_request(
            None,
            ex.execution_id,
            [protocol.input_handle(input_id)],
            timeout_ms=0,
            suspend=True,
        )
        msg["id"] = 999
        ex.conn.send(msg)

        # Give the server time to process the suspend
        time.sleep(0.5)

        # Respond to the input via CLI
        cli.inputs_respond(input_id, "hello", host=ctx.host)

        # The execution should be re-scheduled after the response
        ex2 = ctx.executor.next_execute()

        # In the re-execution, resolve should return the value immediately
        result2 = ex2.conn.resolve_input(input_id, ex2.execution_id)
        assert result2["value"] == "hello"

        ex2.conn.complete(ex2.execution_id, value="resumed")
        run_result = ctx.result(resp["runId"])
        assert run_result["type"] == "value"
        assert run_result["value"]["data"] == "resumed"


def test_input_poll_no_response_yet(worker):
    """Polling with timeout_ms=0 and suspend=false returns null when pending."""
    targets = [workflow("test", "poll")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "poll")
        ex = ctx.executor.next_execute()

        input_id = ex.conn.submit_input(ex.execution_id, "Poll me")

        # Poll — no response yet, should return None
        result = ex.conn.resolve_input(
            input_id, ex.execution_id, timeout_ms=0, suspend=False
        )
        assert result is None

        # Now respond
        cli.inputs_respond(input_id, "polled!", host=ctx.host)

        # Poll again — should return value
        result2 = ex.conn.resolve_input(
            input_id, ex.execution_id, timeout_ms=0, suspend=False
        )
        assert result2 is not None
        assert result2["value"] == "polled!"

        ex.conn.complete(ex.execution_id, value="ok")
        assert ctx.result(resp["runId"])["type"] == "value"


def test_input_cancel_via_cancel_rpc(worker):
    """Cancelling an input via the cancel RPC transitions it to cancelled.

    After cancellation, resolving returns status=cancelled (distinct from
    the dismissed path), and any select waiting on the input is notified.
    """
    targets = [workflow("test", "cancel_input")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "cancel_input")
        ex = ctx.executor.next_execute()

        input_id = ex.conn.submit_input(ex.execution_id, "Please cancel me")

        # Cancel the input via the unified cancel RPC
        cancel_msg = protocol.cancel_request(
            None,
            ex.execution_id,
            [protocol.input_handle(input_id)],
        )
        cancel_msg["id"] = 9001
        ex.conn.send(cancel_msg)
        cancel_resp = ex.conn.recv()
        assert cancel_resp["id"] == 9001

        # Resolving the input should now surface the cancellation
        resolve_result = ex.conn.resolve_input(input_id, ex.execution_id)
        assert resolve_result == {"status": "cancelled"}

        ex.conn.complete(ex.execution_id, value="cancelled-ok")
        assert ctx.result(resp["runId"])["type"] == "value"


def test_input_wait_then_respond(worker):
    """resolve_input blocks until the input is responded to."""
    targets = [workflow("test", "blocking")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "blocking")
        ex = ctx.executor.next_execute()

        input_id = ex.conn.submit_input(ex.execution_id, "Block me")

        # Resolve in a thread (it will block)
        result_holder = [None]
        error_holder = [None]

        def do_resolve():
            try:
                result_holder[0] = ex.conn.resolve_input(
                    input_id, ex.execution_id, timeout_ms=5000
                )
            except Exception as e:
                error_holder[0] = e

        t = threading.Thread(target=do_resolve)
        t.start()

        # Give the resolve a moment to register
        time.sleep(0.3)

        # Respond via CLI — this should unblock the resolve
        cli.inputs_respond(input_id, "unblocked", host=ctx.host)

        t.join(timeout=10)
        assert not t.is_alive(), "resolve_input did not unblock"
        assert error_holder[0] is None, f"resolve_input raised: {error_holder[0]}"
        assert result_holder[0] is not None
        assert result_holder[0]["value"] == "unblocked"

        ex.conn.complete(ex.execution_id, value="ok")
        assert ctx.result(resp["runId"])["type"] == "value"


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


def test_input_schema_validation_accept(worker):
    """Responding with a value matching the schema succeeds."""
    targets = [workflow("test", "schema_ok")]
    schema = json.dumps({"type": "integer"})

    with worker(targets) as ctx:
        resp = ctx.submit("test", "schema_ok")
        ex = ctx.executor.next_execute()

        input_id = ex.conn.submit_input(
            ex.execution_id, "Enter a number", schema=schema
        )

        cli.inputs_respond(input_id, 42, host=ctx.host)

        result = ex.conn.resolve_input(input_id, ex.execution_id)
        assert result["value"] == 42

        ex.conn.complete(ex.execution_id, value="ok")
        assert ctx.result(resp["runId"])["type"] == "value"


def test_input_schema_validation_reject(worker):
    """Responding with a value that doesn't match the schema fails."""
    targets = [workflow("test", "schema_bad")]
    schema = json.dumps({"type": "integer"})

    with worker(targets) as ctx:
        resp = ctx.submit("test", "schema_bad")
        ex = ctx.executor.next_execute()

        input_id = ex.conn.submit_input(
            ex.execution_id, "Enter a number", schema=schema
        )

        # Respond with a string — should be rejected by the server
        try:
            cli.inputs_respond(input_id, "not a number", host=ctx.host)
            rejected = False
        except Exception:
            rejected = True

        assert rejected, "Server should reject value that doesn't match schema"

        # Clean up — respond with valid value so execution can complete
        cli.inputs_respond(input_id, 99, host=ctx.host)
        ex.conn.resolve_input(input_id, ex.execution_id)
        ex.conn.complete(ex.execution_id, value="ok")


# ---------------------------------------------------------------------------
# Response idempotency
# ---------------------------------------------------------------------------


def test_input_already_responded(worker):
    """Responding to an input twice — second attempt is rejected."""
    targets = [workflow("test", "double")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "double")
        ex = ctx.executor.next_execute()

        input_id = ex.conn.submit_input(ex.execution_id, "Once only")

        cli.inputs_respond(input_id, "first", host=ctx.host)

        # Second response should fail
        try:
            cli.inputs_respond(input_id, "second", host=ctx.host)
            second_failed = False
        except Exception:
            second_failed = True

        assert second_failed, "Second response should be rejected"

        result = ex.conn.resolve_input(input_id, ex.execution_id)
        assert result["value"] == "first"

        ex.conn.complete(ex.execution_id, value="ok")
        assert ctx.result(resp["runId"])["type"] == "value"


# ---------------------------------------------------------------------------
# Cross-run input passing
# ---------------------------------------------------------------------------


def test_input_passed_between_runs(worker):
    """Run A creates an input; Run B resolves it by input_id."""
    targets = [
        workflow("test", "producer"),
        workflow("test", "consumer"),
    ]

    with worker(targets, concurrency=2) as ctx:
        # --- Run A: create input, return input_id ---
        resp_a = ctx.submit("test", "producer")
        ex_a = ctx.executor.next_execute()
        assert ex_a.target == "producer"

        input_id = ex_a.conn.submit_input(ex_a.execution_id, "Shared input")
        ex_a.conn.complete(ex_a.execution_id, value=input_id)

        result_a = ctx.result(resp_a["runId"])
        assert result_a["type"] == "value"
        assert result_a["value"]["data"] == input_id

        # --- Run B: resolve the same input ---
        resp_b = ctx.submit("test", "consumer")
        ex_b = ctx.executor.next_execute()
        assert ex_b.target == "consumer"

        # Respond to the input (could be from either run's perspective)
        cli.inputs_respond(input_id, "shared value", host=ctx.host)

        # Run B resolves the input created by Run A
        result = ex_b.conn.resolve_input(input_id, ex_b.execution_id)
        assert result["value"] == "shared value"

        ex_b.conn.complete(ex_b.execution_id, value="consumed")
        result_b = ctx.result(resp_b["runId"])
        assert result_b["type"] == "value"
        assert result_b["value"]["data"] == "consumed"


def test_input_ref_passed_as_task_argument(worker):
    """Pass an input reference as an argument to a child task."""
    targets = [
        workflow("test", "parent"),
        task("test", "child", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "parent")
        ex = ctx.executor.next_execute()
        assert ex.target == "parent"

        # Submit an input and respond to it
        input_id = ex.conn.submit_input(ex.execution_id, "Enter value")
        cli.inputs_respond(input_id, "hello", host=ctx.host)

        # Resolve the input to get the value
        result = ex.conn.resolve_input(input_id, ex.execution_id)
        assert result["value"] == "hello"

        # Pass the input as a reference argument to a child task
        input_ref_arg = {
            "type": "raw",
            "data": {"type": "ref", "index": 0},
            "references": [["input", input_id]],
        }
        child_exec_id = ex.conn.submit_task(
            ex.execution_id,
            "test",
            "child",
            [input_ref_arg],
            wait_for=[0],
        )

        # The child should execute with the resolved input value
        ex_child = ctx.executor.next_execute()
        assert ex_child.target == "child"
        ex_child.conn.complete(ex_child.execution_id, value="got it")

        # Resolve the child and complete the parent
        child_result = ex.conn.resolve(ex.execution_id, child_exec_id)
        assert child_result["value"] == "got it"

        ex.conn.complete(ex.execution_id, value="done")
        final = ctx.result(resp["runId"])
        assert final["type"] == "value"
        assert final["value"]["data"] == "done"


# ---------------------------------------------------------------------------
# Inspection
# ---------------------------------------------------------------------------


def test_inspect_input(worker):
    """Inspect an input via CLI and verify metadata."""
    targets = [workflow("test", "inspectable")]
    schema = json.dumps({"type": "string"})

    with worker(targets) as ctx:
        resp = ctx.submit("test", "inspectable")
        ex = ctx.executor.next_execute()

        input_id = ex.conn.submit_input(
            ex.execution_id,
            "Enter your name",
            schema=schema,
            key="name-key",
            title="Name Input",
        )

        info = cli.inputs_inspect(input_id, host=ctx.host)
        assert info["key"] == "name-key"
        assert info["template"] == "Enter your name"
        assert info["schema"] is not None
        assert "response" not in info or info["response"] is None

        # Respond and inspect again
        cli.inputs_respond(input_id, "Bob", host=ctx.host)
        info2 = cli.inputs_inspect(input_id, host=ctx.host)
        assert info2["response"] is not None
        assert info2["response"]["type"] == "value"
        assert info2["response"]["value"] == "Bob"

        ex.conn.resolve_input(input_id, ex.execution_id)
        ex.conn.complete(ex.execution_id, value="ok")
        assert ctx.result(resp["runId"])["type"] == "value"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_input_with_initial_requires_schema(worker):
    """Submitting an input with initial but no schema is rejected."""
    targets = [workflow("test", "bad_initial")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "bad_initial")
        ex = ctx.executor.next_execute()

        result = ex.conn.submit_input(
            ex.execution_id, "Init without schema", initial={"value": 1}
        )

        # Should return an error dict (not a string input_id)
        assert isinstance(result, dict), f"Expected error dict, got {result!r}"

        ex.conn.complete(ex.execution_id, value="ok")


def test_input_with_title_and_actions(worker):
    """Input title and actions are preserved in inspection."""
    targets = [workflow("test", "actions")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "actions")
        ex = ctx.executor.next_execute()

        input_id = ex.conn.submit_input(
            ex.execution_id,
            "Approve this?",
            title="Approval",
            actions=["Approve", "Reject"],
        )

        info = cli.inputs_inspect(input_id, host=ctx.host)
        assert info.get("title") == "Approval"

        cli.inputs_respond(input_id, True, host=ctx.host)
        ex.conn.resolve_input(input_id, ex.execution_id)
        ex.conn.complete(ex.execution_id, value="ok")
        assert ctx.result(resp["runId"])["type"] == "value"


# ---------------------------------------------------------------------------
# Epoch boundary tests
# ---------------------------------------------------------------------------


def _rotate_epoch(port, project_id):
    """Force an epoch rotation via the management API."""
    api_post(port, project_id, "rotate_epoch")


def test_input_survives_epoch_rotation(isolated_server, tmp_path):
    """Input and response are copied when a run is rerun across epoch boundary."""
    server, host, project_id = isolated_server
    targets = [workflow("test", "ask")]

    with managed_worker(targets, host, tmp_path) as executor:
        resp = cli.submit("test/ask", host=host)
        run_id = resp["runId"]
        step_id = resp["stepId"]

        ex = executor.next_execute()
        assert ex.target == "ask"

        input_id = ex.conn.submit_input(ex.execution_id, "Question?", key="q1")
        cli.inputs_respond(input_id, "answer", host=host)

        result = ex.conn.resolve_input(input_id, ex.execution_id)
        assert result["value"] == "answer"

        ex.conn.complete(ex.execution_id, value="done")
        assert poll_result(run_id, host)["type"] == "value"

        # Rotate epoch
        _rotate_epoch(server.port, project_id)

        # Rerun the step — triggers copy_run from old epoch
        rerun = cli.runs_rerun(step_id, host=host)
        assert rerun["attempt"] == 2

        ex2 = executor.next_execute()
        assert ex2.target == "ask"

        # The keyed input should be memoized (same key, same run)
        input_id_2 = ex2.conn.submit_input(ex2.execution_id, "Question?", key="q1")
        assert input_id_2 == input_id  # Same input (memoized across epoch copy)

        # Response should already be available
        result2 = ex2.conn.resolve_input(input_id_2, ex2.execution_id)
        assert result2["value"] == "answer"

        ex2.conn.complete(ex2.execution_id, value="done2")
        result_final = poll_result(run_id, host)
        assert result_final["type"] == "value"
        assert result_final["value"]["data"] == "done2"


def test_input_resolve_across_epoch(isolated_server, tmp_path):
    """Input created in epoch 1 can be resolved by a new run in epoch 2."""
    server, host, project_id = isolated_server
    targets = [
        workflow("test", "creator"),
        workflow("test", "resolver"),
    ]

    with managed_worker(targets, host, tmp_path, concurrency=2) as executor:
        # --- Epoch 1: create input ---
        resp1 = cli.submit("test/creator", host=host)
        run_id1 = resp1["runId"]

        ex1 = executor.next_execute()
        assert ex1.target == "creator"

        input_id = ex1.conn.submit_input(ex1.execution_id, "Cross-epoch input")
        cli.inputs_respond(input_id, "epoch1-value", host=host)

        ex1.conn.complete(ex1.execution_id, value=input_id)
        result1 = poll_result(run_id1, host)
        assert result1["type"] == "value"
        assert result1["value"]["data"] == input_id

        # Rotate — input now in old epoch
        _rotate_epoch(server.port, project_id)

        # --- Epoch 2: resolve the old input ---
        resp2 = cli.submit("test/resolver", host=host)
        run_id2 = resp2["runId"]

        ex2 = executor.next_execute()
        assert ex2.target == "resolver"

        # Resolve the input from epoch 1 — should find it via cross-epoch lookup
        result = ex2.conn.resolve_input(input_id, ex2.execution_id)
        assert result is not None
        assert result["value"] == "epoch1-value"

        ex2.conn.complete(ex2.execution_id, value="resolved")
        result2 = poll_result(run_id2, host)
        assert result2["type"] == "value"
        assert result2["value"]["data"] == "resolved"
