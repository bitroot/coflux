"""Tests for the run topics.

The run topic is a skeleton: the tree from the initial step with the latest
attempt of each step expanded, other attempts summarised, and each group
collapsed to its first member plus a summary of the rest. The execution
topic opens what's collapsed for one pinned execution, and the group topic
lists a group's members.
"""

import time

from support.manifest import task, workflow
from support.protocol import json_args, register_group_notification


def wait_for(fn, timeout=5, interval=0.1):
    deadline = time.time() + timeout
    while True:
        value = fn()
        if value:
            return value
        if time.time() > deadline:
            raise TimeoutError("condition not met")
        time.sleep(interval)


def step_number(execution_id):
    return int(execution_id.split(":")[1])


def counts(**by_status):
    return {
        "total": sum(by_status.values()),
        "byStatus": {
            **{
                s: 0
                for s in (
                    "assigning",
                    "running",
                    "completed",
                    "errored",
                    "aborted",
                    "suspended",
                )
            },
            **by_status,
        },
    }


def test_run_topics(worker):
    targets = [
        workflow("test", "main"),
        task("test", "item", parameters=["x"]),
        task("test", "sub"),
        task("test", "leaf"),
    ]

    with worker(targets, concurrency=8) as ctx:
        run_id = ctx.submit("test", "main")["runId"]
        root = ctx.executor.next_execute()

        # Steps are numbered in submission order: items are 2, 3, 4 and the
        # ungrouped leaf is 5.
        root.conn.send(
            register_group_notification(root.execution_id, group_id=0, name="batch")
        )
        for x in (1, 2, 3):
            root.conn.submit_task(
                root.execution_id, "test", "item", json_args(x), group_id=0
            )
        root.conn.submit_task(root.execution_id, "test", "leaf", json_args())

        by_step = {}
        for _ in range(4):
            ex = ctx.executor.next_execute(timeout=5)
            by_step[step_number(ex.execution_id)] = ex
        assert set(by_step) == {2, 3, 4, 5}

        # The second item has a child of its own (step 6).
        by_step[3].conn.submit_task(by_step[3].execution_id, "test", "sub", json_args())
        sub = ctx.executor.next_execute(timeout=5)
        assert step_number(sub.execution_id) == 6

        sub.conn.complete(sub.execution_id, value="sub")
        by_step[3].conn.complete(by_step[3].execution_id, value="b")
        by_step[4].conn.complete(by_step[4].execution_id, value="c")
        by_step[5].conn.complete(by_step[5].execution_id, value="leaf")
        by_step[2].conn.fail(by_step[2].execution_id, "ValueError", "boom")
        root.conn.complete(root.execution_id, value="done")
        assert ctx.result(run_id)["value"]["data"] == "done"

        def run_topic():
            return ctx.inspect(run_id)

        def execution_topic(execution_id):
            return ctx.inspect_execution(run_id, execution_id)

        def group_topic():
            return ctx.inspect_group(run_id, root.execution_id, 0)

        def key(step):
            return f"{run_id}:{step}"

        # --- The skeleton: root, the group's first member, and the leaf. ---
        steps = run_topic()["steps"]
        assert set(steps) == {key(1), key(2), key(5)}

        root_execution = steps[key(1)]["executions"]["1"]
        assert [c["stepId"] for c in root_execution["children"]] == [key(2), key(5)]
        assert root_execution["groups"]["0"]["name"] == "batch"
        assert root_execution["groups"]["0"]["members"] == counts(
            completed=2, errored=1
        )
        assert set(steps[key(2)]["attempts"]) == {"1"}
        assert steps[key(2)]["attempts"]["1"]["completion"]["kind"] == "errored"

        # --- A member other than the first is opened by the execution topic. ---
        opened = execution_topic(by_step[3].execution_id)
        assert opened["root"] == by_step[3].execution_id
        assert opened["parent"] == {"executionId": root.execution_id, "groupId": 0}
        assert set(opened["steps"]) == {key(3), key(6)}

        # --- Something already in the skeleton needs nothing. ---
        assert execution_topic(by_step[5].execution_id) == {
            "root": None,
            "parent": None,
            "steps": {},
        }

        # --- The group topic lists every member with its branch status. ---
        group = group_topic()
        assert group["name"] == "batch"
        assert [(c["stepId"], c["status"]) for c in group["children"]] == [
            (key(2), "errored"),
            (key(3), "completed"),
            (key(4), "completed"),
        ]

        # --- A re-run expands the new attempt and summarises the old one. ---
        assert ctx.rerun(key(2))["attempt"] == 2
        retry = ctx.executor.next_execute(timeout=5)
        assert retry.execution_id == f"{run_id}:2:2"
        retry.conn.complete(retry.execution_id, value="a")

        def retried(steps):
            attempt = steps[key(2)]["attempts"].get("2")
            return attempt and attempt["completion"]

        steps = wait_for(
            lambda: (lambda s: s if retried(s) else None)(run_topic()["steps"])
        )
        assert set(steps[key(2)]["attempts"]) == {"1", "2"}
        assert set(steps[key(2)]["executions"]) == {"2"}
        assert steps[key(2)]["attempts"]["2"]["completion"]["kind"] == "succeeded"
        assert steps[key(1)]["executions"]["1"]["groups"]["0"]["members"] == counts(
            completed=3
        )

        # The old attempt can still be opened, on its own.
        opened = execution_topic(f"{run_id}:2:1")
        assert opened["root"] == f"{run_id}:2:1"
        assert opened["parent"] == {"executionId": root.execution_id, "groupId": 0}
        assert set(opened["steps"]) == {key(2)}
        assert set(opened["steps"][key(2)]["executions"]) == {"1"}
        assert set(opened["steps"][key(2)]["attempts"]) == {"1", "2"}

        # And the group's member status follows the latest attempt.
        group = wait_for(
            lambda: (
                lambda g: g if g["children"][0]["status"] == "completed" else None
            )(group_topic())
        )
        assert [c["status"] for c in group["children"]] == ["completed"] * 3
