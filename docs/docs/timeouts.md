# Timeouts

Tasks and workflows can be configured with a timeout. If an execution exceeds its timeout, the process is killed.

## Configuring a timeout

Set the `timeout` parameter on a `@task` or `@workflow` decorator:

```python
import coflux as cf

@cf.task(timeout=30)
def call_external_api():
    ...

@cf.workflow(timeout=300)
def long_running_pipeline():
    ...
```

The value is in seconds. You can also pass a `timedelta`:

```python
from datetime import timedelta

@cf.task(timeout=timedelta(minutes=5))
def slow_task():
    ...
```

A value of `0` (the default) means no timeout.

## Behaviour

When an execution times out:

1. The worker process is killed.
2. Child executions are cancelled.
3. Any execution waiting on the result receives an `ExecutionTimeout` exception.
4. The execution transitions to the _Timed out_ state.

## Interaction with retries

Timeouts compose with [retries](./retries.md) — a timed-out execution counts as a failure and can trigger a retry (if configured). Each retry attempt gets its own timeout window.

```python
@cf.task(timeout=30, retries=3)
def unreliable_api_call():
    ...
```
