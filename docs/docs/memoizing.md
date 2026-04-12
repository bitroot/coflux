# Memoizing

Memoizing is similar to [caching](/caching), however it only applies to steps within a run, and serves a subtly different purpose. With caching, a cache hit still results in a new step entity, but the result will be shared. Memoizing is more lightweight because the existing execution is referenced directly, rather than creating a new step which references the existing result.

Memoizing can be used as a way of optimizing runs (by sharing a result), and also to make debugging runs easier.

Enable memoizing of a task with the `memo` option:

```python
@task(memo=True)
def fetch_user(user_id):
    ...
```

## Workflow-level memo

Setting `memo=True` on a `@workflow` applies it as a default for all tasks in the run:

```python
@workflow(memo=True)
def analyse():
    ...
```

Individual tasks can still override this — for example, a task with `memo=False` will not be memoized even if the workflow has `memo=True`.

Memoized steps are indicated in Studio with a pin icon.

As with caching, explicitly clicking the 're-run' button for a step will force the step to be re-run, even if it's memoized. Then subsequent memoizing will use the new step execution.

If a step is manually re-run in a child workspace, the memoized results will be used (but memoized results from the child workspace aren't available to the parent). This follows the same rules as caching.

## For debugging

Memoizing provides several benefits for debugging:

1. Memoizing a task with side effects (e.g., sending a notification email) means you can re-run the whole run (or part of it) without that side-effect happening.

2. Memoizing slow tasks allows you to fix bugs that are occurring elsewhere in the workflow.

This is particularly useful when re-running a workflow from a production workspace in a development workspace (assuming the production workspace is configured as an ancestor of the development workspace). By liberally memoizing tasks, specific steps can be re-run in the development workspace without re-running downstream steps.

## For optimization

Memoizing can also be used as an optimization for workflows. For example, if a resource needs to be used in multiple parts of a workflow, rather than passing around that resource, the task to fetch it can be memoized:

```python
@task(memo=True)
def fetch_user(user_id):
    ...

@task()
def send_email(user_id):
    user = fetch_user(user_id)
    ...

@task()
def send_notification(user_id):
    user = fetch_user(user_id)
    ...
```

In this case, the `fetch_user` task will only be executed once for the run, even if steps are re-run (provided the user ID doesn't change).

## Memo parameters

As with caching, by default the memoization considers all arguments. This can be changed by specifying the individual parameters:

```python
@task(memo=["machine_id"])
def apply_configuration(machine_id, config):
    ...
```

In this case, the function to apply configuration to a machine is only run once for the specified machine, regardless of whether the configuration itself changes. This can allow you to make changes to a workflow and re-run it with (some) confidence that a new configuration won't be applied.
