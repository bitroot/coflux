"""Tests for logging and execution groups."""

from support.manifest import task, workflow
from support.protocol import json_args, log_message, register_group_notification


def test_log_messages(worker):
    """Log messages during execution don't interfere with result."""
    targets = [workflow("test.logged")]

    with worker(targets) as ctx:
        resp = ctx.submit("test.logged")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]

        eid, _, _ = conn0.recv_execute()

        # Send log messages at various levels
        conn0.send(log_message(eid, "info", "starting work"))
        conn0.send(log_message(eid, "warning", "something is off"))
        conn0.send(log_message(eid, "error", "recoverable issue"))
        conn0.send(log_message(eid, "debug", "detail"))

        # Complete execution normally
        conn0.complete(eid, value="logged")

        assert ctx.result(run_id)["value"]["data"] == "logged"


def test_execution_groups(worker):
    """Workflow registers a group and submits tasks within it."""
    targets = [
        workflow("test.grouped"),
        task("test.item", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.grouped")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]
        conn1 = ctx.executor.connections[1]

        wf_eid, _, _ = conn0.recv_execute()

        # Register a group
        conn0.send(register_group_notification(wf_eid, group_id=1, name="batch"))

        # Submit tasks in the group
        ref1 = conn0.submit_task(
            wf_eid,
            "test.item",
            json_args("a"),
            group_id=1,
        )
        ref2 = conn0.submit_task(
            wf_eid,
            "test.item",
            json_args("b"),
            group_id=1,
        )

        # Handle both tasks (first goes to conn1, second reuses conn1 after complete)
        task_eid1, _, _ = conn1.recv_execute()
        conn1.complete(task_eid1, value="ok")
        task_eid2, _, _ = conn1.recv_execute()
        conn1.complete(task_eid2, value="ok")

        # Resolve both
        conn0.resolve(wf_eid, ref1)
        conn0.resolve(wf_eid, ref2)

        conn0.complete(wf_eid, value="grouped")

        # Verify groups show in inspect
        data = ctx.inspect(run_id)
        child_steps = {
            sid: s for sid, s in data["steps"].items() if s.get("target") == "item"
        }
        assert len(child_steps) == 2

        assert ctx.result(run_id)["value"]["data"] == "grouped"


def test_log_retrieval_via_cli(worker):
    """Fetch logs for a run via the CLI and verify content."""
    targets = [workflow("test.log_retrieve")]

    with worker(targets) as ctx:
        resp = ctx.submit("test.log_retrieve")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]

        eid, _, _ = conn0.recv_execute()

        # Send log messages at various levels
        conn0.send(log_message(eid, "info", "hello from cli"))
        conn0.send(log_message(eid, "warning", "a warning"))
        conn0.send(log_message(eid, "error", "an error"))

        # Complete execution
        conn0.complete(eid, value="done")

        # Wait for result to ensure execution finished
        assert ctx.result(run_id)["value"]["data"] == "done"

        # Fetch logs via CLI (3 messages sent)
        log_data = ctx.logs(run_id, min_entries=3)
        logs = log_data["logs"]

        templates = [l["template"] for l in logs]
        assert "hello from cli" in templates
        assert "a warning" in templates
        assert "an error" in templates


def test_log_filter_by_execution(worker):
    """Filter logs by step:attempt to get only one execution's logs."""
    targets = [
        workflow("test.log_parent"),
        task("test.log_child", parameters=["x"]),
    ]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test.log_parent")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]
        conn1 = ctx.executor.connections[1]

        wf_eid, _, _ = conn0.recv_execute()

        # Parent logs a message
        conn0.send(log_message(wf_eid, "info", "parent message"))

        # Submit child task
        ref1 = conn0.submit_task(wf_eid, "test.log_child", json_args("a"))

        # Handle child task
        child_eid, _, _ = conn1.recv_execute()
        conn1.send(log_message(child_eid, "info", "child message"))
        conn1.complete(child_eid, value="child_done")

        # Resolve and complete parent
        conn0.resolve(wf_eid, ref1)
        conn0.complete(wf_eid, value="parent_done")

        assert ctx.result(run_id)["value"]["data"] == "parent_done"

        # Fetch all logs - should have both messages (2 sent)
        all_logs = ctx.logs(run_id, min_entries=2)["logs"]
        all_templates = [l["template"] for l in all_logs]
        assert "parent message" in all_templates
        assert "child message" in all_templates

        # Find the child step ID from the run topic
        data = ctx.inspect(run_id)
        child_step_id = None
        for sid, s in data["steps"].items():
            if s.get("target") == "log_child":
                child_step_id = sid
                break
        assert child_step_id is not None, "child step not found in run topic"

        # Filter by step:attempt (attempt 1)
        child_logs = ctx.logs(run_id, step_attempt=f"{child_step_id}:1")["logs"]
        child_templates = [l["template"] for l in child_logs]
        assert "child message" in child_templates
        assert "parent message" not in child_templates


def test_log_with_values(worker):
    """Log messages with values are stored and returned correctly."""
    targets = [workflow("test.log_values")]

    with worker(targets) as ctx:
        resp = ctx.submit("test.log_values")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]

        eid, _, _ = conn0.recv_execute()

        # Send log with values (wire format: ["raw", data, refs])
        conn0.send(
            log_message(
                eid,
                "info",
                "processing {data}",
                values={
                    "data": ["raw", {"id": 42}, []],
                },
            )
        )

        conn0.complete(eid, value="done")

        assert ctx.result(run_id)["value"]["data"] == "done"

        # Fetch logs via CLI (JSON mode, 1 message sent)
        log_data = ctx.logs(run_id, min_entries=1)
        logs = log_data["logs"]

        entry = next(l for l in logs if l["template"] == "processing {data}")
        assert "values" in entry
        assert "data" in entry["values"]
        val = entry["values"]["data"]
        assert val["type"] == "raw"
        assert val["data"] == {"id": 42}


def test_log_display_format(worker):
    """CLI text output shows labels and interpolated values."""
    targets = [workflow("test.log_display")]

    with worker(targets) as ctx:
        resp = ctx.submit("test.log_display")
        run_id = resp["runId"]
        conn0 = ctx.executor.connections[0]

        eid, _, _ = conn0.recv_execute()

        # Send log with value placeholder
        conn0.send(
            log_message(
                eid,
                "info",
                "count is {n}",
                values={
                    "n": ["raw", 7, []],
                },
            )
        )

        conn0.complete(eid, value="done")

        assert ctx.result(run_id)["value"]["data"] == "done"

        # Fetch logs in text mode (not --json, 1 message sent)
        output = ctx.logs(run_id, json_output=False, min_entries=1)

        # Should contain the label [test.log_display]
        assert "[test.log_display]" in output
        # Should contain the interpolated value
        assert "count is 7" in output
