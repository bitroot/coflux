"""The catalog: publishing values, snapshot-bound reads, workspace
inheritance, waiting for the next version, and the surfaces around it."""

import os
import subprocess
import time

import pytest
from support import cli, protocol
from support.helpers import api_post, managed_worker
from support.manifest import task, workflow


def _persist(ex, tmp_path, name, content):
    """Persist a one-file asset from ``ex`` and return its id."""
    path = tmp_path / f"{name}.txt"
    path.write_text(content)
    result = ex.conn.persist_asset(
        ex.execution_id, {f"{name}.txt": str(path)}, metadata={"name": name}
    )
    return result["asset_id"]


def _asset_id(found):
    """The asset a ``catalog_get`` result holds."""
    assert found["value"]["value"] == {"type": "ref", "index": 0}
    return found["value"]["references"][0][1]


def _dict(**items):
    """A dict in the encoded JSON value format."""
    return {"type": "dict", "items": [x for kv in items.items() for x in kv]}


# --- Publishing and reading ----------------------------------------------------


def test_publish_and_read_latest(worker, tmp_path):
    """A version is a value at a path — here an asset. Publishing gives
    the number; reading gives the number and the value, and records
    lineage, which the run topic shows on both sides."""
    targets = [workflow("test", "main"), task("test", "producer")]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        ref = ex0.conn.submit_task(ex0.execution_id, "test", "producer", [])
        ex1 = ctx.executor.next_execute()
        asset_id = _persist(ex1, tmp_path, "model", "weights v1")

        assert (
            ex1.conn.catalog_publish_asset(ex1.execution_id, "models/churn", asset_id)
            == 1
        )
        ex1.conn.complete(ex1.execution_id, value="published")
        assert ex0.conn.resolve(ex0.execution_id, ref)["value"] == "published"

        # The parent was assigned before the publish, but it is the same
        # run, so the read sees past its snapshot.
        latest = ex0.conn.catalog_get(ex0.execution_id, "models/churn")
        assert latest["number"] == 1
        assert _asset_id(latest) == asset_id
        ex0.conn.complete(ex0.execution_id, value="done")
        ctx.result(resp["runId"])

        run = ctx.inspect(resp["runId"])
        producer = run["steps"][f"{resp['runId']}:2"]["executions"]["1"]
        assert set(producer["published"]) == {"models/churn@1"}
        published = producer["published"]["models/churn@1"]
        assert published["number"] == 1
        assert published["value"]["data"] == {"type": "ref", "index": 0}
        assert published["value"]["references"][0]["assetId"] == asset_id
        assert published["publishedBy"] == ex1.execution_id

        reader = run["steps"][resp["stepId"]]["executions"]["1"]
        dependency = reader["dependencies"]["models/churn@1"]
        assert dependency["type"] == "catalog"
        assert dependency["version"]["number"] == 1
        assert dependency["version"]["value"]["references"][0]["assetId"] == asset_id
        assert dependency["pending"] is False


def test_publish_and_read_a_plain_value(worker):
    """Any value can be published — a config, a number, a locator for
    something external — and a named read records lineage just as a read
    of the head does."""
    targets = [workflow("test", "main"), task("test", "consumer")]

    with worker(targets, concurrency=2) as ctx:
        resp = ctx.submit("test", "main")
        ex0 = ctx.executor.next_execute()

        config = _dict(threshold=0.7, table="s3://bucket/customers")
        assert (
            ex0.conn.catalog_publish(ex0.execution_id, "configs/training", config) == 1
        )
        found = ex0.conn.catalog_get(ex0.execution_id, "configs/training")
        assert found["number"] == 1
        assert found["value"]["type"] == "inline"
        assert found["value"]["value"] == config
        assert found["value"].get("references", []) == []

        # A consumer told which number to read reads it by name.
        ref = ex0.conn.submit_task(ex0.execution_id, "test", "consumer", [])
        ex1 = ctx.executor.next_execute()
        found = ex1.conn.catalog_get(ex1.execution_id, "configs/training", 1)
        assert found["value"]["value"] == config
        ex1.conn.complete(ex1.execution_id, value="consumed")
        assert ex0.conn.resolve(ex0.execution_id, ref)["value"] == "consumed"
        ex0.conn.complete(ex0.execution_id, value="done")
        ctx.result(resp["runId"])

        run = ctx.inspect(resp["runId"])
        consumer = run["steps"][f"{resp['runId']}:2"]["executions"]["1"]
        dependency = consumer["dependencies"]["configs/training@1"]
        assert dependency["type"] == "catalog"
        assert dependency["version"]["value"]["data"] == config


def test_republishing_the_head_is_a_noop(worker, tmp_path):
    """The same value writes nothing; a different one is a new version.
    Values are content-hashed, so this holds for plain data as much as for
    assets."""
    targets = [workflow("test", "main")]

    with worker(targets) as ctx:
        ctx.submit("test", "main")
        ex = ctx.executor.next_execute()
        asset_id = _persist(ex, tmp_path, "data", "rows")

        first = ex.conn.catalog_publish_asset(ex.execution_id, "data/rows", asset_id)
        again = ex.conn.catalog_publish_asset(ex.execution_id, "data/rows", asset_id)
        assert first == again == 1

        other = _persist(ex, tmp_path, "data2", "more rows")
        assert ex.conn.catalog_publish_asset(ex.execution_id, "data/rows", other) == 2

        # A plain value dedups the same way.
        assert ex.conn.catalog_publish(ex.execution_id, "data/rows", "v3") == 3
        assert ex.conn.catalog_publish(ex.execution_id, "data/rows", "v3") == 3
        assert ex.conn.catalog_publish(ex.execution_id, "data/rows", "v4") == 4

        assert [v["number"] for v in ctx.catalog_inspect("data/rows")] == [4, 3, 2, 1]
        ex.conn.complete(ex.execution_id, value="done")


def test_invalid_path_or_asset_is_refused(worker):
    targets = [workflow("test", "main")]

    with worker(targets) as ctx:
        ctx.submit("test", "main")
        ex = ctx.executor.next_execute()
        for path in ["/abs", "a//b", "a/../b", "a@1", "a:b"]:
            error = ex.conn.catalog_publish(ex.execution_id, path, 1)
            assert error["code"] == "invalid_path", path
        assert (
            ex.conn.catalog_get(ex.execution_id, "bad path")["code"] == "invalid_path"
        )
        # A value pointing at an asset that doesn't exist is refused too.
        error = ex.conn.catalog_publish_asset(ex.execution_id, "data/x", "Anope")
        assert error["code"] == "asset_not_found"
        ex.conn.complete(ex.execution_id, value="done")


# --- Snapshots ------------------------------------------------------------------


def test_reads_are_bound_to_the_snapshot_at_assignment(worker):
    """An execution sees the catalog as it was when it was assigned. What
    other runs publish afterwards is invisible to it, however many times
    it asks; a fresh execution sees everything."""
    targets = [workflow("test", "reader"), workflow("test", "writer")]

    with worker(targets, concurrency=3) as ctx:
        ctx.submit("test", "reader")
        reader = ctx.executor.next_execute()
        assert reader.conn.catalog_get(reader.execution_id, "models/m") is None

        ctx.submit("test", "writer")
        writer = ctx.executor.next_execute()
        assert writer.conn.catalog_publish(writer.execution_id, "models/m", "v1") == 1
        writer.conn.complete(writer.execution_id, value="done")

        # Still nothing, from where the reader stands.
        assert reader.conn.catalog_get(reader.execution_id, "models/m") is None
        # But a named version is an immutable reference, exempt from the pin.
        named = reader.conn.catalog_get(reader.execution_id, "models/m", 1)
        assert named["number"] == 1
        assert named["value"]["value"] == "v1"
        reader.conn.complete(reader.execution_id, value="done")

        ctx.submit("test", "reader")
        later = ctx.executor.next_execute()
        assert later.conn.catalog_get(later.execution_id, "models/m")["number"] == 1
        later.conn.complete(later.execution_id, value="done")


# --- Workspaces -----------------------------------------------------------------


def test_derived_workspace_inherits_and_is_overtaken(worker):
    """A derived workspace reads its base's versions before and after
    publishing its own; the base never sees the derived workspace's; and
    numbers are allocated once per path across both."""
    targets = [workflow("test", "main")]

    with worker(targets, workspace="base") as ctx_base:
        ctx_base.submit("test", "main")
        ex = ctx_base.executor.next_execute()
        assert ex.conn.catalog_publish(ex.execution_id, "models/m", "one") == 1
        ex.conn.complete(ex.execution_id, value="done")
        saved_host = ctx_base.host

    cli.workspaces_create("derived", base="base", host=saved_host, workspace="derived")

    with worker(targets, workspace="derived") as ctx_derived:
        ctx_derived.submit("test", "main")
        ex = ctx_derived.executor.next_execute()
        # Inherited.
        assert ex.conn.catalog_get(ex.execution_id, "models/m")["number"] == 1

        assert ex.conn.catalog_publish(ex.execution_id, "models/m", "two") == 2
        # Its own publish is visible to its own run past the pin.
        assert ex.conn.catalog_get(ex.execution_id, "models/m")["number"] == 2
        ex.conn.complete(ex.execution_id, value="done")

        assert [
            e["path"] + "@" + str(e["number"]) for e in ctx_derived.catalog_list()
        ] == ["models/m@2"]
        assert [v["number"] for v in ctx_derived.catalog_inspect("models/m")] == [2, 1]

    with worker(targets, workspace="base") as ctx_base:
        # The base doesn't see the derived workspace's @2...
        assert [e["number"] for e in ctx_base.catalog_list()] == [1]
        ctx_base.submit("test", "main")
        ex = ctx_base.executor.next_execute()
        assert (
            ex.conn.catalog_get(ex.execution_id, "models/m", 2)["code"] == "invisible"
        )
        # ...and its next publish takes @3, past the number it can't see.
        assert ex.conn.catalog_publish(ex.execution_id, "models/m", "three") == 3
        ex.conn.complete(ex.execution_id, value="done")
        assert [v["number"] for v in ctx_base.catalog_inspect("models/m")] == [3, 1]

    with worker(targets, workspace="derived") as ctx_derived:
        # The base's newer version overtakes the derived workspace's own.
        assert [e["number"] for e in ctx_derived.catalog_list()] == [3]
        ctx_derived.submit("test", "main")
        ex = ctx_derived.executor.next_execute()
        assert ex.conn.catalog_get(ex.execution_id, "models/m")["number"] == 3
        named = ex.conn.catalog_get(ex.execution_id, "models/m", 2)
        assert named["number"] == 2
        assert named["value"]["value"] == "two"
        ex.conn.complete(ex.execution_id, value="done")


# --- Snapshots across attempts ---------------------------------------------------


def _publish(ctx, path, value):
    """Publish ``value`` at ``path`` from a fresh writer run."""
    ctx.submit("test", "writer")
    writer = ctx.executor.next_execute()
    number = writer.conn.catalog_publish(writer.execution_id, path, value)
    writer.conn.complete(writer.execution_id, value="done")
    return number


def test_a_rerun_inherits_the_previous_attempts_snapshot(worker):
    """Re-running a step re-runs it against the catalog the previous
    attempt saw, so a re-run reproduces a run rather than silently picking
    up whatever landed since. Choosing a snapshot is explicit: ``latest``,
    or a version to run as of."""
    targets = [workflow("test", "reader"), workflow("test", "writer")]

    with worker(targets, concurrency=3) as ctx:
        assert _publish(ctx, "models/m", "v1") == 1

        resp = ctx.submit("test", "reader")
        first = ctx.executor.next_execute()
        assert first.conn.catalog_get(first.execution_id, "models/m")["number"] == 1
        first.conn.complete(first.execution_id, value="done")

        assert _publish(ctx, "models/m", "v2") == 2

        ctx.rerun(resp["stepId"])
        second = ctx.executor.next_execute()
        assert second.execution_id == f"{resp['stepId']}:2"
        assert second.conn.catalog_get(second.execution_id, "models/m")["number"] == 1
        second.conn.complete(second.execution_id, value="done")

        ctx.rerun(resp["stepId"], catalog="latest")
        third = ctx.executor.next_execute()
        assert third.conn.catalog_get(third.execution_id, "models/m")["number"] == 2
        third.conn.complete(third.execution_id, value="done")

        # A plain re-run of *that* attempt keeps its choice.
        ctx.rerun(resp["stepId"])
        fourth = ctx.executor.next_execute()
        assert fourth.conn.catalog_get(fourth.execution_id, "models/m")["number"] == 2
        fourth.conn.complete(fourth.execution_id, value="done")

        ctx.rerun(resp["stepId"], catalog="models/m@1")
        fifth = ctx.executor.next_execute()
        assert fifth.conn.catalog_get(fifth.execution_id, "models/m")["number"] == 1
        fifth.conn.complete(fifth.execution_id, value="done")


def test_a_resumed_suspension_keeps_its_snapshot(worker):
    """The execution that resumes a suspended step sees what the suspended
    one saw. Only a catalog wait — ``next()`` — moves the snapshot on."""
    targets = [workflow("test", "reader"), workflow("test", "writer")]

    with worker(targets, concurrency=3) as ctx:
        _publish(ctx, "models/m", "v1")

        resp = ctx.submit("test", "reader")
        first = ctx.executor.next_execute()
        assert first.conn.catalog_get(first.execution_id, "models/m")["number"] == 1

        _publish(ctx, "models/m", "v2")

        first.conn.suspend(first.execution_id)
        resumed = ctx.executor.next_execute()
        assert resumed.execution_id == f"{resp['stepId']}:2"
        assert resumed.conn.catalog_get(resumed.execution_id, "models/m")["number"] == 1

        resumed.conn.suspend(resumed.execution_id, catalog_wait="models/m")
        woken = ctx.executor.next_execute()
        assert woken.execution_id == f"{resp['stepId']}:3"
        assert woken.conn.catalog_get(woken.execution_id, "models/m")["number"] == 2
        woken.conn.complete(woken.execution_id, value="done")


def test_a_run_can_be_submitted_as_of_a_version(worker):
    """``submit --catalog path@n`` runs the whole run — every step in it —
    against the catalog as it was when that version was published: other
    paths are seen as of then too."""
    targets = [
        workflow("test", "reader"),
        task("test", "child"),
        workflow("test", "writer"),
    ]

    with worker(targets, concurrency=3) as ctx:
        _publish(ctx, "models/m", "v1")
        _publish(ctx, "configs/c", "c1")
        _publish(ctx, "models/m", "v2")

        ctx.submit("test", "reader", catalog="models/m@1")
        reader = ctx.executor.next_execute()
        assert reader.conn.catalog_get(reader.execution_id, "models/m")["number"] == 1
        # configs/c@1 came after models/m@1, so it isn't there yet.
        assert reader.conn.catalog_get(reader.execution_id, "configs/c") is None

        reader.conn.submit_task(reader.execution_id, "test", "child", [])
        child = ctx.executor.next_execute()
        assert child.conn.catalog_get(child.execution_id, "models/m")["number"] == 1
        child.conn.complete(child.execution_id, value="done")
        reader.conn.complete(reader.execution_id, value="done")

        # The default is the catalog as of each execution's own start.
        ctx.submit("test", "reader")
        later = ctx.executor.next_execute()
        assert later.conn.catalog_get(later.execution_id, "models/m")["number"] == 2
        assert later.conn.catalog_get(later.execution_id, "configs/c")["number"] == 1
        later.conn.complete(later.execution_id, value="done")


def test_an_unknown_or_malformed_version_is_refused(worker):
    targets = [workflow("test", "reader")]

    with worker(targets) as ctx:
        for ref in ["models/m@1", "models/m", "models/m@0", "models/m@x", "@1"]:
            with pytest.raises(subprocess.CalledProcessError) as raised:
                ctx.submit("test", "reader", catalog=ref)
            assert "catalog" in raised.value.stderr, ref


def test_the_iteration_after_a_recurrence_takes_a_fresh_snapshot(worker):
    """A recurrent target's iterations are separate runs in all but name:
    each starts from the catalog as it is then."""
    targets = [
        workflow("test", "main"),
        task("test", "ticker"),
        workflow("test", "writer"),
    ]

    with worker(targets, concurrency=3) as ctx:
        _publish(ctx, "models/m", "v1")

        ctx.submit("test", "main")
        main = ctx.executor.next_execute()
        main.conn.submit_task(main.execution_id, "test", "ticker", [], recurrent=True)

        tick1 = ctx.executor.next_execute()
        assert tick1.conn.catalog_get(tick1.execution_id, "models/m")["number"] == 1
        _publish(ctx, "models/m", "v2")
        tick1.conn.complete(tick1.execution_id, value=None)

        tick2 = ctx.executor.next_execute()
        assert tick2.conn.catalog_get(tick2.execution_id, "models/m")["number"] == 2
        tick2.conn.complete(tick2.execution_id, value="final")
        main.conn.complete(main.execution_id, value="done")


# --- Waiting for the next version ----------------------------------------------


def test_next_polls_and_resolves_with_the_superseding_version(worker):
    targets = [workflow("test", "watcher"), workflow("test", "writer")]

    with worker(targets, concurrency=3) as ctx:
        ctx.submit("test", "watcher")
        watcher = ctx.executor.next_execute()
        # Nothing after position 0 yet: a poll says so with a null result.
        assert (
            watcher.conn.catalog_next(
                watcher.execution_id, "models/m", 0, timeout_ms=0, suspend=False
            )
            is None
        )

        ctx.submit("test", "writer")
        writer = ctx.executor.next_execute()
        writer.conn.catalog_publish(writer.execution_id, "models/m", "v1")
        writer.conn.complete(writer.execution_id, value="done")

        # The select's value is the number of the version that satisfied it.
        result = watcher.conn.catalog_next(
            watcher.execution_id, "models/m", 0, timeout_ms=0, suspend=False
        )
        assert result["status"] == "ok" and result["winner"] == 0
        assert result["value"]["value"] == 1
        # Beyond the snapshot, deliberately: the watcher's pin predates it.
        assert watcher.conn.catalog_get(watcher.execution_id, "models/m") is None
        # And nothing after @1.
        assert (
            watcher.conn.catalog_next(
                watcher.execution_id, "models/m", 1, timeout_ms=0, suspend=False
            )
            is None
        )
        watcher.conn.complete(watcher.execution_id, value="done")


def test_wait_without_a_position_is_relative_to_the_executions_view(worker):
    """A handle with no number waits past whatever the execution can see —
    its snapshot plus its own run's writes — so its own publish never
    wakes it, and a reader assigned after a publish isn't woken by it."""
    targets = [workflow("test", "writer"), workflow("test", "reader")]

    def poll(ex):
        return ex.conn.catalog_next(
            ex.execution_id, "models/m", timeout_ms=0, suspend=False
        )

    with worker(targets, concurrency=3) as ctx:
        ctx.submit("test", "writer")
        writer = ctx.executor.next_execute()
        assert writer.conn.catalog_publish(writer.execution_id, "models/m", "v1") == 1
        # Its own @1 is in its view.
        assert poll(writer) is None

        ctx.submit("test", "reader")
        reader = ctx.executor.next_execute()
        # Assigned after @1, so @1 is in the snapshot: nothing newer yet.
        assert poll(reader) is None

        assert writer.conn.catalog_publish(writer.execution_id, "models/m", "v2") == 2
        assert poll(writer) is None
        # @2 is newer than the reader's view.
        assert poll(reader)["value"]["value"] == 2
        writer.conn.complete(writer.execution_id, value="done")
        reader.conn.complete(reader.execution_id, value="done")


def test_waiting_execution_is_rerun_when_a_version_lands(worker):
    """Suspending on a catalog handle gates the successor on the path. When
    a version lands, the successor runs with a fresh snapshot and reads it
    as the latest."""
    targets = [workflow("test", "watcher"), workflow("test", "writer")]

    with worker(targets, concurrency=3) as ctx:
        resp = ctx.submit("test", "watcher")
        watcher = ctx.executor.next_execute()

        msg = protocol.select_request(
            None,
            watcher.execution_id,
            [protocol.catalog_handle("models/m", 0)],
            timeout_ms=0,
            suspend=True,
        )
        msg["id"] = 999
        watcher.conn.send(msg)
        time.sleep(0.5)

        # Suspended: nothing is scheduled until something is published.
        with pytest.raises(TimeoutError):
            ctx.executor.next_execute(timeout=1)

        run = ctx.inspect(resp["runId"])
        successor = run["steps"][resp["stepId"]]["executions"]["2"]
        assert successor["dependencies"]["models/m@0+"] == {
            "type": "catalog",
            "path": "models/m",
            "number": 0,
            "version": None,
            "pending": True,
        }

        ctx.submit("test", "writer")
        writer = ctx.executor.next_execute()
        writer.conn.catalog_publish(writer.execution_id, "models/m", "v1")
        writer.conn.complete(writer.execution_id, value="done")

        resumed = ctx.executor.next_execute()
        assert resumed.execution_id == f"{resp['stepId']}:2"
        assert resumed.conn.catalog_get(resumed.execution_id, "models/m")["number"] == 1
        resumed.conn.complete(resumed.execution_id, value="resumed")
        assert ctx.result(resp["runId"])["value"]["data"] == "resumed"


def test_suspending_with_a_catalog_wait_gates_the_successor(worker):
    """``next()`` in the adapter is a suspension carrying the path. The
    position is the execution's own view — nothing, here — so the
    successor is held until the first version, and runs with a snapshot
    that includes it."""
    targets = [workflow("test", "watcher"), workflow("test", "writer")]

    with worker(targets, concurrency=3) as ctx:
        resp = ctx.submit("test", "watcher")
        watcher = ctx.executor.next_execute()
        assert watcher.conn.catalog_get(watcher.execution_id, "models/m") is None
        watcher.conn.suspend(watcher.execution_id, catalog_wait="models/m")
        time.sleep(0.5)

        with pytest.raises(TimeoutError):
            ctx.executor.next_execute(timeout=1)
        run = ctx.inspect(resp["runId"])
        successor = run["steps"][resp["stepId"]]["executions"]["2"]
        assert successor["dependencies"]["models/m@0+"]["pending"] is True

        ctx.submit("test", "writer")
        writer = ctx.executor.next_execute()
        writer.conn.catalog_publish(writer.execution_id, "models/m", "v1")
        writer.conn.complete(writer.execution_id, value="done")

        resumed = ctx.executor.next_execute()
        assert resumed.execution_id == f"{resp['stepId']}:2"
        assert resumed.conn.catalog_get(resumed.execution_id, "models/m")["number"] == 1
        # Its own view now holds @1, so a wait from here is for @2.
        resumed.conn.suspend(resumed.execution_id, catalog_wait="models/m")
        time.sleep(0.5)
        with pytest.raises(TimeoutError):
            ctx.executor.next_execute(timeout=1)
        run = ctx.inspect(resp["runId"])
        third = run["steps"][resp["stepId"]]["executions"]["3"]
        assert third["dependencies"]["models/m@1+"]["pending"] is True


def test_a_catalog_wait_is_reported_on_the_queue(worker):
    """The queue has to name the gate itself, not just the executions being
    waited on. A successor held by a catalog wait is otherwise unassigned
    for no visible reason."""
    targets = [workflow("test", "watcher")]

    with worker(targets, concurrency=3) as ctx:
        resp = ctx.submit("test", "watcher")
        watcher = ctx.executor.next_execute()
        watcher.conn.suspend(watcher.execution_id, catalog_wait="models/m")
        time.sleep(0.5)

        entry = ctx.queue()[f"{resp['stepId']}:2"]
        assert entry["assignedAt"] is None
        assert entry["dependencies"] == [
            {"type": "catalog", "path": "models/m", "number": 0}
        ]


def test_republish_of_the_head_does_not_wake_a_waiter(worker):
    targets = [workflow("test", "watcher"), workflow("test", "writer")]

    with worker(targets, concurrency=3) as ctx:
        ctx.submit("test", "writer")
        writer = ctx.executor.next_execute()
        writer.conn.catalog_publish(writer.execution_id, "models/m", "v1")

        ctx.submit("test", "watcher")
        watcher = ctx.executor.next_execute()
        msg = protocol.select_request(
            None,
            watcher.execution_id,
            [protocol.catalog_handle("models/m", 1)],
            timeout_ms=0,
            suspend=True,
        )
        msg["id"] = 999
        watcher.conn.send(msg)
        time.sleep(0.5)

        # Same value: no new version, no wake.
        writer.conn.catalog_publish(writer.execution_id, "models/m", "v1")
        with pytest.raises(TimeoutError):
            ctx.executor.next_execute(timeout=1)

        writer.conn.catalog_publish(writer.execution_id, "models/m", "v2")
        writer.conn.complete(writer.execution_id, value="done")

        resumed = ctx.executor.next_execute()
        assert resumed.conn.catalog_get(resumed.execution_id, "models/m")["number"] == 2
        resumed.conn.complete(resumed.execution_id, value="done")


def test_waiter_in_derived_workspace_is_woken_by_base_publish(worker):
    targets = [workflow("test", "watcher"), workflow("test", "writer")]

    with worker(targets, workspace="base") as ctx_base:
        saved_host = ctx_base.host

    cli.workspaces_create("derived", base="base", host=saved_host, workspace="derived")

    with (
        worker(targets, workspace="derived") as ctx_derived,
        worker(targets, workspace="base") as ctx_base,
    ):
        resp = ctx_derived.submit("test", "watcher")
        watcher = ctx_derived.executor.next_execute()
        msg = protocol.select_request(
            None,
            watcher.execution_id,
            [protocol.catalog_handle("models/m", 0)],
            timeout_ms=0,
            suspend=True,
        )
        msg["id"] = 999
        watcher.conn.send(msg)
        time.sleep(0.5)

        ctx_base.submit("test", "writer")
        writer = ctx_base.executor.next_execute()
        writer.conn.catalog_publish(writer.execution_id, "models/m", "v1")
        writer.conn.complete(writer.execution_id, value="done")

        resumed = ctx_derived.executor.next_execute()
        assert resumed.execution_id == f"{resp['stepId']}:2"
        assert resumed.conn.catalog_get(resumed.execution_id, "models/m")["number"] == 1
        resumed.conn.complete(resumed.execution_id, value="done")


def test_select_on_two_paths_wakes_on_either(worker):
    targets = [workflow("test", "watcher"), workflow("test", "writer")]

    with worker(targets, concurrency=3) as ctx:
        resp = ctx.submit("test", "watcher")
        watcher = ctx.executor.next_execute()
        msg = protocol.select_request(
            None,
            watcher.execution_id,
            [
                protocol.catalog_handle("data/a", 0),
                protocol.catalog_handle("data/b", 0),
            ],
            timeout_ms=0,
            suspend=True,
        )
        msg["id"] = 999
        watcher.conn.send(msg)
        time.sleep(0.5)

        ctx.submit("test", "writer")
        writer = ctx.executor.next_execute()
        writer.conn.catalog_publish(writer.execution_id, "data/b", "b")
        writer.conn.complete(writer.execution_id, value="done")

        resumed = ctx.executor.next_execute()
        assert resumed.execution_id == f"{resp['stepId']}:2"
        assert resumed.conn.catalog_get(resumed.execution_id, "data/a") is None
        assert resumed.conn.catalog_get(resumed.execution_id, "data/b")["number"] == 1
        # The gate on the other path is gone with the group.
        run = ctx.inspect(resp["runId"])
        successor = run["steps"][resp["stepId"]]["executions"]["2"]
        assert successor["dependencies"]["data/a@0+"]["pending"] is False
        resumed.conn.complete(resumed.execution_id, value="done")


# --- Epochs ---------------------------------------------------------------------


def test_catalog_survives_epoch_rotation(isolated_server, tmp_path):
    """The log is copied wholesale at rotation, ids and all, so numbering
    continues and old versions — and the values behind them — stay
    readable."""
    server, host, project_id = isolated_server
    targets = [workflow("test", "main")]

    with managed_worker(targets, host, tmp_path) as executor:
        cli.submit("test/main", host=host)
        ex = executor.next_execute()
        a1 = _persist(ex, tmp_path, "a1", "one")
        assert ex.conn.catalog_publish_asset(ex.execution_id, "models/m", a1) == 1
        ex.conn.complete(ex.execution_id, value="done")

        api_post(server.port, project_id, "rotate_epoch")

        assert [v["number"] for v in cli.catalog_inspect("models/m", host=host)] == [1]

        cli.submit("test/main", host=host)
        ex = executor.next_execute()
        found = ex.conn.catalog_get(ex.execution_id, "models/m")
        assert found["number"] == 1
        assert found["value"]["references"] == [["asset", a1, "a1", 1, 3]]
        a2 = _persist(ex, tmp_path, "a2", "two")
        assert ex.conn.catalog_publish_asset(ex.execution_id, "models/m", a2) == 2
        ex.conn.complete(ex.execution_id, value="done")

        assert [v["number"] for v in cli.catalog_inspect("models/m", host=host)] == [
            2,
            1,
        ]
        download_dir = tmp_path / "downloaded"
        os.makedirs(download_dir)
        cli.catalog_download("models/m@1", str(download_dir), host=host)
        assert (download_dir / "a1.txt").read_text() == "one"


# --- CLI ------------------------------------------------------------------------


def test_cli_publish_list_inspect_download_an_asset(worker, tmp_path):
    targets = [workflow("test", "main")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "main")
        ex = ctx.executor.next_execute()
        asset_id = _persist(ex, tmp_path, "report", "hello")
        ex.conn.complete(ex.execution_id, value="done")
        ctx.result(resp["runId"])

        assert ctx.catalog_list() == []

        published = ctx.catalog_publish("reports/daily", asset_id=asset_id)
        assert published["created"] is True
        assert published["version"]["number"] == 1
        assert published["version"]["value"]["data"] == {"type": "ref", "index": 0}
        assert published["version"]["value"]["references"][0]["assetId"] == asset_id
        # Published through the API, so not by an execution.
        assert published["version"]["publishedBy"] is None

        again = ctx.catalog_publish("reports/daily", asset_id=asset_id)
        assert again["created"] is False

        entries = ctx.catalog_list("reports/")
        assert [(e["path"], e["number"]) for e in entries] == [("reports/daily", 1)]
        assert entries[0]["value"]["references"][0]["asset"]["totalCount"] == 1
        assert ctx.catalog_list("other/") == []

        versions = ctx.catalog_inspect("reports/daily")
        assert [v["number"] for v in versions] == [1]

        # A single asset restores flat.
        download_dir = tmp_path / "downloaded"
        os.makedirs(download_dir)
        ctx.catalog_download("reports/daily", str(download_dir))
        assert (download_dir / "report.txt").read_text() == "hello"


def test_cli_publish_and_get_a_json_value(worker):
    targets = [workflow("test", "main")]

    with worker(targets) as ctx:
        published = ctx.catalog_publish("configs/training", value={"threshold": 0.7})
        assert published["created"] is True
        assert published["version"]["value"] == {
            "type": "raw",
            "data": _dict(threshold=0.7),
            "references": [],
        }

        version = ctx.catalog_get("configs/training")
        assert version["number"] == 1
        assert version["value"]["data"] == _dict(threshold=0.7)
        assert ctx.catalog_get("configs/training@1")["number"] == 1

        # Nothing to download from a value that holds no assets.
        with pytest.raises(subprocess.CalledProcessError):
            ctx.catalog_download("configs/training", "unused")


def test_cli_download_lays_out_several_assets_by_key(worker, tmp_path):
    """A value holding more than one asset restores each into a
    subdirectory named by the keys leading to it."""
    targets = [workflow("test", "main")]

    with worker(targets) as ctx:
        resp = ctx.submit("test", "main")
        ex = ctx.executor.next_execute()
        train = _persist(ex, tmp_path, "train", "training rows")
        test = _persist(ex, tmp_path, "test", "test rows")
        number = ex.conn.catalog_publish(
            ex.execution_id,
            "datasets/split",
            _dict(
                train={"type": "ref", "index": 0},
                test={"type": "ref", "index": 1},
                ratio=0.8,
            ),
            references=[["asset", train], ["asset", test]],
        )
        assert number == 1
        ex.conn.complete(ex.execution_id, value="done")
        ctx.result(resp["runId"])

        download_dir = tmp_path / "downloaded"
        os.makedirs(download_dir)
        ctx.catalog_download("datasets/split", str(download_dir))
        assert (download_dir / "train" / "train.txt").read_text() == "training rows"
        assert (download_dir / "test" / "test.txt").read_text() == "test rows"


def test_cli_publish_of_unknown_asset_fails(worker):
    targets = [workflow("test", "main")]

    with worker(targets) as ctx:
        with pytest.raises(subprocess.CalledProcessError):
            ctx.catalog_publish("reports/daily", asset_id="Anope")


def test_cli_publish_null(worker):
    """`null` is a value like any other. It takes the same argument shape a
    submitted argument takes, so the value travels as a JSON string and an
    omission is a missing argument rather than a null one."""
    targets = [workflow("test", "main")]

    with worker(targets) as ctx:
        result = ctx.catalog_publish("configs/none", value=None)
        assert result["created"] is True
        assert result["version"]["value"]["data"] is None
