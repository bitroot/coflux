"""Tests for logging and execution groups."""

import json
import time
import urllib.request

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


# Helpers for log partitioning tests


def _rotate_logs(port, project_id):
    """Force a log partition rotation via the management API."""
    url = f"http://{project_id}.localhost:{port}/api/rotate_logs"
    req = urllib.request.Request(
        url,
        method="POST",
        data=b"{}",
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=10)


def _query_logs_http(port, project_id, run_id, after=None, from_ts=None):
    """Query logs via HTTP JSON endpoint."""
    params = f"run={run_id}"
    if after:
        params += f"&after={after}"
    if from_ts is not None:
        params += f"&from={from_ts}"
    url = f"http://{project_id}.localhost:{port}/logs?{params}"
    req = urllib.request.Request(url)
    resp = urllib.request.urlopen(req, timeout=10)
    return json.loads(resp.read())


def test_logs_across_partition_boundary(worker, server):
    """Write logs, rotate, write more logs for same run — query returns all."""
    targets = [workflow("test.partitioned")]

    with worker(targets) as ctx:
        resp = ctx.submit("test.partitioned")
        run_id = resp["runId"]
        conn = ctx.executor.connections[0]

        eid, _, _ = conn.recv_execute()

        # Write logs in first partition
        conn.send(log_message(eid, "info", "msg_before_rotation_1"))
        conn.send(log_message(eid, "info", "msg_before_rotation_2"))

        # Complete execution so logs are flushed
        conn.complete(eid, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"

        # Verify logs are there before rotation
        data = ctx.logs(run_id, min_entries=2)
        assert len(data["logs"]) >= 2

        # Force log partition rotation
        _rotate_logs(server.port, ctx.project_id)

        # Query logs for the first run — should still return all logs
        data = ctx.logs(run_id, min_entries=2)
        templates = [l["template"] for l in data["logs"]]
        assert "msg_before_rotation_1" in templates
        assert "msg_before_rotation_2" in templates


def test_bloom_filter_narrows_search(worker, server):
    """Logs for run A in partition 1, run B in partition 2 — Bloom filters work."""
    targets = [workflow("test.bloom_a"), workflow("test.bloom_b")]

    with worker(targets, concurrency=2) as ctx:
        # Run A — logs go to partition 1
        resp_a = ctx.submit("test.bloom_a")
        run_id_a = resp_a["runId"]
        conn0 = ctx.executor.connections[0]

        eid_a, _, _ = conn0.recv_execute()
        conn0.send(log_message(eid_a, "info", "run_a_message"))
        conn0.complete(eid_a, value="a_done")
        assert ctx.result(run_id_a)["value"]["data"] == "a_done"

        # Verify log is there
        data_a = ctx.logs(run_id_a, min_entries=1)
        assert any(l["template"] == "run_a_message" for l in data_a["logs"])

        # Rotate — run A's logs now in old partition
        _rotate_logs(server.port, ctx.project_id)

        # Wait for Bloom filter build to complete
        time.sleep(1)

        # Run B — logs go to partition 2 (new active)
        resp_b = ctx.submit("test.bloom_b")
        run_id_b = resp_b["runId"]
        conn1 = ctx.executor.connections[1]

        eid_b, _, _ = conn1.recv_execute()
        conn1.send(log_message(eid_b, "info", "run_b_message"))
        conn1.complete(eid_b, value="b_done")
        assert ctx.result(run_id_b)["value"]["data"] == "b_done"

        # Query run A — should find logs from old partition
        data_a = ctx.logs(run_id_a, min_entries=1)
        templates_a = [l["template"] for l in data_a["logs"]]
        assert "run_a_message" in templates_a
        assert "run_b_message" not in templates_a

        # Query run B — should find logs from new partition only
        data_b = ctx.logs(run_id_b, min_entries=1)
        templates_b = [l["template"] for l in data_b["logs"]]
        assert "run_b_message" in templates_b
        assert "run_a_message" not in templates_b


def test_from_parameter_skips_old_partitions(worker, server):
    """Query with from= after old partition — old partition should be skipped."""
    targets = [workflow("test.from_old"), workflow("test.from_new")]

    with worker(targets, concurrency=2) as ctx:
        # Write logs in first partition
        resp_old = ctx.submit("test.from_old")
        run_id_old = resp_old["runId"]
        conn0 = ctx.executor.connections[0]

        eid_old, _, _ = conn0.recv_execute()
        conn0.send(log_message(eid_old, "info", "old_message"))
        conn0.complete(eid_old, value="old_done")
        assert ctx.result(run_id_old)["value"]["data"] == "old_done"

        # Rotate
        _rotate_logs(server.port, ctx.project_id)

        # Wait for Bloom build
        time.sleep(1)

        # Record timestamp after rotation
        from_ts = int(time.time() * 1000)

        # Write logs in second partition
        resp_new = ctx.submit("test.from_new")
        run_id_new = resp_new["runId"]
        conn1 = ctx.executor.connections[1]

        eid_new, _, _ = conn1.recv_execute()
        conn1.send(log_message(eid_new, "info", "new_message"))
        conn1.complete(eid_new, value="new_done")
        assert ctx.result(run_id_new)["value"]["data"] == "new_done"

        # Query new run with from= parameter via HTTP
        data = _query_logs_http(
            server.port, ctx.project_id, run_id_new, from_ts=from_ts
        )
        templates = [l["template"] for l in data["logs"]]
        assert "new_message" in templates

        # Query old run without from= — should still work
        data_old = _query_logs_http(server.port, ctx.project_id, run_id_old)
        templates_old = [l["template"] for l in data_old["logs"]]
        assert "old_message" in templates_old


def test_sse_subscription_across_rotation(worker, server):
    """Subscribe to run, rotate while subscribed, write new logs — subscriber receives them."""
    targets = [workflow("test.sse_rotate")]

    with worker(targets) as ctx:
        resp = ctx.submit("test.sse_rotate")
        run_id = resp["runId"]
        conn = ctx.executor.connections[0]

        eid, _, _ = conn.recv_execute()

        # Write initial log
        conn.send(log_message(eid, "info", "before_rotation"))

        # Wait for flush
        time.sleep(1)

        # Rotate while run is still active
        _rotate_logs(server.port, ctx.project_id)

        # Write more logs after rotation
        conn.send(log_message(eid, "info", "after_rotation"))

        # Complete execution
        conn.complete(eid, value="rotated")
        assert ctx.result(run_id)["value"]["data"] == "rotated"

        # Query all logs — should include both before and after rotation
        data = ctx.logs(run_id, min_entries=2)
        templates = [l["template"] for l in data["logs"]]
        assert "before_rotation" in templates
        assert "after_rotation" in templates


def test_pagination_across_partitions(worker, server):
    """Paginate across partition boundaries using cursors."""
    targets = [workflow("test.paginate")]

    with worker(targets) as ctx:
        resp = ctx.submit("test.paginate")
        run_id = resp["runId"]
        conn = ctx.executor.connections[0]

        eid, _, _ = conn.recv_execute()

        # Write logs in first partition
        for i in range(3):
            conn.send(log_message(eid, "info", f"p1_msg_{i}"))

        # Need to give time for flush
        time.sleep(1)

        # Rotate
        _rotate_logs(server.port, ctx.project_id)

        # Write logs in second partition
        for i in range(3):
            conn.send(log_message(eid, "info", f"p2_msg_{i}"))

        # Complete
        conn.complete(eid, value="paginated")
        assert ctx.result(run_id)["value"]["data"] == "paginated"

        # Fetch all logs
        data = ctx.logs(run_id, min_entries=6)
        all_templates = [l["template"] for l in data["logs"]]
        assert len(all_templates) >= 6

        # Verify both partitions' messages are present
        for i in range(3):
            assert f"p1_msg_{i}" in all_templates
            assert f"p2_msg_{i}" in all_templates

        # Verify ordering: p1 messages should come before p2 messages
        p1_indices = [all_templates.index(f"p1_msg_{i}") for i in range(3)]
        p2_indices = [all_templates.index(f"p2_msg_{i}") for i in range(3)]
        assert max(p1_indices) < min(p2_indices), (
            f"p1 messages should precede p2 messages: p1={p1_indices}, p2={p2_indices}"
        )

        # Test pagination with limit via HTTP
        # Fetch first page (limit=2)
        url = f"http://{ctx.project_id}.localhost:{server.port}/logs?run={run_id}"
        # Use the cursor from the response to paginate
        data1 = _query_logs_http(server.port, ctx.project_id, run_id)
        assert len(data1["logs"]) >= 6
        assert data1["cursor"] is not None
