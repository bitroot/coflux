"""Characterisation tests for the workspace-level topics.

Each test drives a scenario and reads a topic over REST at several points.
Topical keeps a topic process alive for ten seconds after its last capture,
so successive reads inside a test go through the same process and exercise
the live-update path as well as the initial snapshot.
"""

import threading
import time

from support import cli
from support.helpers import get_topic, workspace_id
from support.manifest import task, workflow
from support.protocol import json_args


def wait_for(fn, timeout=5, interval=0.1):
    deadline = time.time() + timeout
    while True:
        value = fn()
        if value is not None:
            return value
        if time.time() > deadline:
            raise TimeoutError("condition not met")
        time.sleep(interval)


def execution_ref(execution_id):
    return {
        "type": "inline",
        "format": "json",
        "value": None,
        "references": [["execution", execution_id]],
    }


def test_queue_topic(worker):
    targets = [
        workflow("test", "main"),
        task("test", "producer"),
        task("test", "consumer", parameters=["data"], wait_for=[0]),
    ]

    with worker(targets, concurrency=1) as ctx:
        run_id = ctx.submit("test", "main")["runId"]
        root = ctx.executor.next_execute()

        producer = root.conn.submit_task(
            root.execution_id, "test", "producer", json_args()
        )
        consumer = root.conn.submit_task(
            root.execution_id,
            "test",
            "consumer",
            [execution_ref(producer)],
            wait_for=[0],
        )

        # The root is running; the producer waits for a worker slot; the
        # consumer waits for the producer.
        queue = wait_for(
            lambda: (lambda q: q if {producer, consumer} <= set(q) else None)(
                ctx.queue()
            )
        )
        assert set(queue) == {root.execution_id, producer, consumer}

        entry = queue[root.execution_id]
        assert entry["module"] == "test"
        assert entry["target"] == "main"
        assert entry["runId"] == run_id
        assert entry["stepId"] == f"{run_id}:1"
        assert entry["stepNumber"] == 1
        assert entry["attempt"] == 1
        assert entry["assignedAt"] is not None
        assert entry["dependencies"] == []

        entry = queue[producer]
        assert entry["module"] == "test"
        assert entry["target"] == "producer"
        assert entry["runId"] == run_id
        assert entry["stepId"] == f"{run_id}:2"
        assert entry["stepNumber"] == 2
        assert entry["attempt"] == 1
        assert entry["executeAfter"] is None
        assert isinstance(entry["createdAt"], int)
        assert entry["assignedAt"] is None
        assert entry["dependencies"] == []
        assert entry["requires"] == {}

        assert queue[consumer]["dependencies"] == [
            {"type": "execution", "executionId": producer}
        ]

        # Completing the root frees the slot: the producer is assigned and
        # the root leaves the queue.
        root.conn.complete(root.execution_id, value="done")
        ex = ctx.executor.next_execute(timeout=5)
        assert ex.execution_id == producer

        queue = wait_for(
            lambda: (lambda q: q if q.get(producer, {}).get("assignedAt") else None)(
                ctx.queue()
            )
        )
        assert root.execution_id not in queue
        assert queue[consumer]["assignedAt"] is None

        # Completing the producer resolves the consumer's dependency.
        ex.conn.complete(ex.execution_id, value=42)
        ex = ctx.executor.next_execute(timeout=5)
        assert ex.execution_id == consumer

        queue = wait_for(
            lambda: (lambda q: q if q.get(consumer, {}).get("assignedAt") else None)(
                ctx.queue()
            )
        )
        assert set(queue) == {consumer}
        assert queue[consumer]["dependencies"] == []

        ex.conn.complete(ex.execution_id, value="x")
        assert ctx.result(run_id)["value"]["data"] == "done"
        wait_for(lambda: True if ctx.queue() == {} else None)


def test_modules_topic(worker):
    targets = [workflow("test", "main"), task("test", "child")]

    with worker(targets, concurrency=1) as ctx:
        ws = workspace_id(ctx.host, ctx.workspace)

        def modules():
            return get_topic(ctx.host, "workspaces", ws, "modules")

        def active_runs():
            return modules()["test"]["workflows"]["main"]["activeRuns"]

        assert modules() == {"test": {"workflows": {"main": {"activeRuns": {}}}}}

        # A run submitted while the workspace is paused is queued; resuming
        # gets it assigned, which makes it running.
        ctx.pause()
        run_id = ctx.submit("test", "main")["runId"]
        wait_for(lambda: True if active_runs().get(run_id) == "queued" else None)
        ctx.resume()
        root = ctx.executor.next_execute(timeout=5)
        wait_for(lambda: True if active_runs().get(run_id) == "running" else None)

        # The run stays active while any of its executions is in flight.
        child = root.conn.submit_task(root.execution_id, "test", "child", json_args())
        root.conn.complete(root.execution_id, value="done")
        ex = ctx.executor.next_execute(timeout=5)
        assert ex.execution_id == child
        assert active_runs().get(run_id) == "running"
        ex.conn.complete(ex.execution_id, value=1)
        assert ctx.result(run_id)["value"]["data"] == "done"
        wait_for(lambda: True if active_runs() == {} else None)

        # Archiving the module removes it.
        ctx.archive_module("test")
        wait_for(lambda: True if modules() == {} else None)


def test_workflow_topic(worker):
    targets = [
        workflow("test", "main", parameters=["x"], retries={"limit": 2}, delay=5),
        task("test", "child"),
    ]

    with worker(targets, concurrency=1) as ctx:
        ws = workspace_id(ctx.host, ctx.workspace)

        def topic():
            return get_topic(ctx.host, "workspaces", ws, "workflows", "test", "main")

        value = topic()
        assert value["parameters"] == [
            {"name": "x", "default": None, "annotation": None}
        ]
        assert value["instruction"] is None
        assert value["configuration"] == {
            "waitFor": [],
            "cache": None,
            "defer": None,
            "delay": 5,
            "retries": {"limit": 2, "backoffMin": 0, "backoffMax": 0},
            "recurrent": False,
            "timeout": 0,
            "requires": {},
            "memo": False,
            "streams": None,
            "concurrency": None,
        }
        assert value["runs"] == {}

        ctx.pause()
        run_id = ctx.submit("test", "main", '"a"')["runId"]
        runs = wait_for(
            lambda: (
                lambda r: r if r.get(run_id, {}).get("active") == "queued" else None
            )(topic()["runs"])
        )
        run = runs[run_id]
        assert run["id"] == run_id
        assert isinstance(run["createdAt"], int)
        assert run["createdBy"] is None
        assert run["outcome"] is None

        ctx.resume()
        root = ctx.executor.next_execute(timeout=5)
        wait_for(
            lambda: True if topic()["runs"][run_id]["active"] == "running" else None
        )

        root.conn.complete(root.execution_id, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"
        run = wait_for(
            lambda: (lambda r: r if r["outcome"] == "completed" else None)(
                topic()["runs"][run_id]
            )
        )
        assert run["active"] is None


def test_workspaces_topic(worker):
    with worker([workflow("test", "main")]) as ctx:
        ws = workspace_id(ctx.host, ctx.workspace)

        def topic():
            return get_topic(ctx.host, "workspaces")

        assert topic()[ws] == {"name": "default", "baseId": None, "state": "active"}

        cli.workspaces_create("child", base="default", host=ctx.host)
        child = wait_for(
            lambda: next((v for v in topic().values() if v["name"] == "child"), None)
        )
        assert child == {"name": "child", "baseId": ws, "state": "active"}

        ctx.pause()
        wait_for(lambda: True if topic()[ws]["state"] == "paused" else None)
        ctx.resume()
        wait_for(lambda: True if topic()[ws]["state"] == "active" else None)


def test_sessions_topic(worker):
    with worker([workflow("test", "main")], concurrency=2) as ctx:
        ws = workspace_id(ctx.host, ctx.workspace)

        def topic():
            return get_topic(ctx.host, "workspaces", ws, "sessions")

        [(session_id, session)] = topic().items()
        assert session == {
            "connected": True,
            "executing": 0,
            "concurrency": 2,
            "poolName": None,
            "targets": {"test": ["main"]},
            "provides": {},
            "accepts": {},
            "workerState": None,
            "executions": 0,
        }

        run_id = ctx.submit("test", "main")["runId"]
        ex = ctx.executor.next_execute()
        wait_for(lambda: True if topic()[session_id]["executing"] == 1 else None)
        assert topic()[session_id]["executions"] == 1

        ex.conn.complete(ex.execution_id, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"
        wait_for(lambda: True if topic()[session_id]["executing"] == 0 else None)


def test_manifests_topic(worker):
    targets = [
        workflow("test", "main", parameters=["x"], retries={"limit": 1}, delay=3),
        task("test", "child"),
    ]

    with worker(targets) as ctx:
        ws = workspace_id(ctx.host, ctx.workspace)

        def topic():
            return get_topic(ctx.host, "workspaces", ws, "manifests")

        assert topic() == {
            "test": {
                "main": {
                    "parameters": [{"name": "x", "default": None, "annotation": None}],
                    "waitFor": [],
                    "cache": None,
                    "defer": None,
                    "delay": 3,
                    "retries": {"limit": 1, "backoffMin": 0, "backoffMax": 0},
                    "requires": {},
                    "concurrency": None,
                }
            }
        }

        ctx.archive_module("test")
        wait_for(lambda: True if topic() == {} else None)


def test_pools_topics(worker, tmp_path):
    with worker([workflow("test", "main")]) as ctx:
        ws = workspace_id(ctx.host, ctx.workspace)

        def pools():
            return get_topic(ctx.host, "workspaces", ws, "pools")

        def pool():
            return get_topic(ctx.host, "workspaces", ws, "pools", "p1")

        assert pools() == {}
        cli.pools_create(
            "p1", "process", modules=["test"], process_dir=str(tmp_path), host=ctx.host
        )
        expected = {
            "modules": ["test"],
            "provides": {},
            "accepts": {},
            "launcher": {"type": "process", "directory": str(tmp_path)},
            "state": "active",
        }
        assert wait_for(lambda: pools().get("p1")) == expected
        assert pool() == {"pool": expected, "workers": {}}

        cli._coflux("pools", "disable", "p1", host=ctx.host)
        wait_for(lambda: True if pools()["p1"]["state"] == "disabled" else None)
        assert pool()["pool"]["state"] == "disabled"
        cli._coflux("pools", "enable", "p1", host=ctx.host)
        wait_for(lambda: True if pools()["p1"]["state"] == "active" else None)

        cli.pools_delete("p1", host=ctx.host)
        wait_for(lambda: True if pools() == {} else None)


def test_inputs_topics(worker):
    with worker([workflow("test", "ask")]) as ctx:
        ws = workspace_id(ctx.host, ctx.workspace)

        def inputs():
            return get_topic(ctx.host, "workspaces", ws, "inputs")

        run_id = ctx.submit("test", "ask")["runId"]
        ex = ctx.executor.next_execute()
        input_id = ex.conn.submit_input(ex.execution_id, "Name?")

        assert inputs() == {}

        # Waiting on the input makes it active until it is answered and the
        # execution that waited has finished.
        resolved = {}
        thread = threading.Thread(
            target=lambda: resolved.update(
                ex.conn.resolve_input(input_id, ex.execution_id)
            )
        )
        thread.start()
        entry = wait_for(lambda: inputs().get(input_id))
        assert entry["runId"] == run_id
        assert entry["title"] is None
        assert entry["requires"] == {}
        assert isinstance(entry["createdAt"], int)

        cli.inputs_respond(input_id, "Alice", host=ctx.host)
        thread.join(timeout=5)
        assert resolved["value"] == "Alice"
        wait_for(lambda: True if inputs() == {} else None)

        ex.conn.complete(ex.execution_id, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"


def test_catalog_topic(worker):
    with worker([workflow("test", "main")]) as ctx:
        ws = workspace_id(ctx.host, ctx.workspace)

        def topic():
            return get_topic(ctx.host, "workspaces", ws, "catalog")

        assert topic() == {}
        ctx.catalog_publish("a/b", value={"x": 1})
        version = wait_for(lambda: topic().get("a/b"))
        assert version["path"] == "a/b"
        assert version["number"] == 1
        assert version["workspaceId"] == ws
        assert version["publishedBy"] is None
        assert version["value"]["type"] == "raw"
        assert isinstance(version["createdAt"], int)

        ctx.catalog_publish("a/b", value={"x": 2})
        wait_for(lambda: True if topic()["a/b"]["number"] == 2 else None)


def test_stream_topic(worker):
    with worker([workflow("test", "producer")]) as ctx:
        ws = workspace_id(ctx.host, ctx.workspace)
        resp = ctx.submit("test", "producer")
        ex = ctx.executor.next_execute()
        stream_id = f"{resp['stepId']}_0"

        def topic():
            return get_topic(ctx.host, "streams", stream_id)

        ex.conn.stream_register(ex.execution_id, 0)
        ex.conn.stream_append(ex.execution_id, 0, 0, "a")
        ex.conn.stream_append(ex.execution_id, 0, 1, "b")

        value = wait_for(
            lambda: (lambda v: v if v["totalCount"] == 2 else None)(topic())
        )
        assert value["id"] == stream_id
        assert value["step"] == {
            "stepId": resp["stepId"],
            "module": "test",
            "target": "producer",
        }
        assert value["workspaceId"] == ws
        assert value["index"] == 0
        assert value["attempts"] == [1]
        assert value["closure"] is None
        assert [item["sequence"] for item in value["items"]] == [0, 1]
        assert [item["value"]["data"] for item in value["items"]] == ["a", "b"]
        assert value["tailSize"] == 200

        ex.conn.stream_append(ex.execution_id, 0, 2, "c")
        ex.conn.stream_close(ex.execution_id, 0)
        ex.conn.complete(ex.execution_id, value=1)
        assert ctx.result(resp["runId"])["type"] == "value"

        value = wait_for(lambda: (lambda v: v if v["closure"] else None)(topic()))
        assert value["totalCount"] == 3
        assert [item["sequence"] for item in value["items"]] == [0, 1, 2]
        assert value["closure"]["attempt"] == 1
        assert value["closure"]["error"] is None
        assert isinstance(value["closure"]["closedAt"], int)
