---
sidebar_label: Python adapter
---

# Python adapter reference

All exports are available from the `coflux` package (`import coflux as cf`).

## Decorators

### `@workflow`

Defines a workflow — the entry point for a run.

```python
@cf.workflow(
    name: str | None = None,
    wait: bool | Iterable[str] | str = False,
    cache: bool | float | timedelta | Cache = False,
    retries: int | bool | Retries = 0,
    recurrent: bool = False,
    defer: bool | Defer = False,
    delay: float | timedelta = 0,
    memo: bool = False,
    requires: dict[str, str | bool | list[str]] | None = None,
    timeout: float | timedelta = 0,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str \| None` | `None` | Custom target name (defaults to function name) |
| `wait` | `bool \| Iterable[str] \| str` | `False` | Wait for `Execution` arguments to resolve before starting |
| `cache` | `bool \| float \| timedelta \| Cache` | `False` | [Caching](./caching.md) configuration |
| `retries` | `int \| bool \| Retries` | `0` | [Retry](./retries.md) configuration |
| `recurrent` | `bool` | `False` | Re-execute after completion ([recurrent](./recurring.md)) |
| `defer` | `bool \| Defer` | `False` | [Deferring](./deferring.md) configuration |
| `delay` | `float \| timedelta` | `0` | Delay before execution (seconds) |
| `memo` | `bool` | `False` | Enable [memoisation](./memoizing.md) as default for all tasks in the run |
| `requires` | `dict \| None` | `None` | [Tag requirements](./pools.md#provides-accepts-and-requires) for worker routing (applied to entire run) |
| `timeout` | `float \| timedelta` | `0` | Execution [timeout](./timeouts.md) in seconds (0 = no timeout) |

### `@task`

Defines a task — an operation that can be called from a workflow or another task. Tasks are the building blocks of a workflow and each invocation becomes a step in the run.

```python
@cf.task(
    name: str | None = None,
    wait: bool | Iterable[str] | str = False,
    cache: bool | float | timedelta | Cache = False,
    retries: int | bool | Retries = 0,
    recurrent: bool = False,
    defer: bool | Defer = False,
    delay: float | timedelta = 0,
    memo: bool | Iterable[str] = False,
    requires: dict[str, str | bool | list[str]] | None = None,
    timeout: float | timedelta = 0,
)
```

Parameters are the same as `@workflow`, except:

| Parameter | Difference |
|-----------|------------|
| `memo` | Also accepts an iterable of parameter names to memo on (e.g., `memo=["user_id"]`) |

### `@stub`

References a target defined in another module, without importing it. This allows modules with different dependencies to be hosted on different workers. The stub's function body is used as a fallback when called outside of a Coflux context (e.g., in tests).

```python
@cf.stub(
    module: str,
    *,
    name: str | None = None,
    type: Literal["workflow", "task"] = "task",
    wait: bool | Iterable[str] | str = False,
    cache: bool | float | timedelta | Cache = False,
    retries: int | bool | Retries = 0,
    recurrent: bool = False,
    defer: bool | Defer = False,
    delay: float | timedelta = 0,
    memo: bool | Iterable[str] = False,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `module` | `str` | required | Module where the target is defined |
| `type` | `Literal["workflow", "task"]` | `"task"` | Whether referencing a workflow or task |

Other parameters behave the same as `@task`. `requires` and `timeout` are not available on stubs.

## Target

Decorating a function with `@workflow`, `@task`, or `@stub` returns a `Target` object. The target can be called directly (blocking) or submitted for asynchronous execution.

### `target(…) -> T`

Calls the target synchronously — submits for execution and blocks until the result is available. Equivalent to `target.submit(…).result()`.

### `target.submit(…) -> Execution[T]`

Submits the target for asynchronous execution and returns an `Execution` handle. The caller can continue other work and retrieve the result later.

## Execution

Returned by `target.submit()`. Represents a running or completed execution, and acts as a future for its result. Can be passed as an argument to other tasks, or returned from a task/workflow.

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `id` | `str` | Execution ID |
| `module` | `str` | Module name |
| `target` | `str` | Target name |

### `execution.result() -> T`

Blocks (suspends) until the execution completes and returns the result. If the execution failed, raises the corresponding exception.

**Raises:** `ExecutionError`, `ExecutionCancelled`, or `ExecutionTimeout`. If called inside a `cf.suspense(timeout=...)` scope and the timeout expires before the handle resolves, raises `TimeoutError`.

### `execution.poll(timeout=None, *, default=None) -> T | D`

Checks whether a result is ready without suspending the caller. Returns the result if available, otherwise returns `default`. Useful for coordination patterns where you want to check multiple executions or do other work while waiting.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `timeout` | `float \| None` | `None` | Seconds to wait before returning default |
| `default` | `D` | `None` | Value to return if result isn't ready |

### `execution.cancel() -> None`

Cancels the execution and its descendants.

## Configuration classes

These are used as values for decorator parameters when more control is needed than the shorthand forms allow.

### `Cache`

Advanced caching configuration. See [caching](./caching.md).

```python
cf.Cache(
    max_age: float | timedelta | None = None,
    params: Iterable[str] | str | None = None,
    namespace: str | None = None,
    version: str | None = None,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_age` | `float \| timedelta \| None` | `None` | Maximum age of cached result |
| `params` | `Iterable[str] \| str \| None` | `None` | Parameters to include in cache key (`None` = all) |
| `namespace` | `str \| None` | `None` | Cache namespace |
| `version` | `str \| None` | `None` | Cache version (change to invalidate) |

### `Defer`

Advanced deferring configuration. See [deferring](./deferring.md).

```python
cf.Defer(
    params: Iterable[str] | str | None = None,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `params` | `Iterable[str] \| str \| None` | `None` | Parameters to defer on (`None` = all) |

### `Retries`

Advanced retry configuration. See [retries](./retries.md).

```python
cf.Retries(
    limit: int | None = None,
    backoff: tuple[float | timedelta, float | timedelta] = (1, 60),
    when: type[BaseException] | tuple[type[BaseException], ...] | Callable[[BaseException], bool] | None = None,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | `int \| None` | `None` | Maximum retries (`None` = unlimited) |
| `backoff` | `tuple` | `(1, 60)` | Backoff range (min, max) in seconds |
| `when` | type, tuple, callable, or `None` | `None` | Exception filter (`None` = retry on any error) |

## Metrics

Record numeric values from executions, streamed in real-time and rendered as charts in Studio. See [metrics](./metrics.md).

### `Metric`

Defines a metric and provides a `record()` method for emitting data points. Metrics can be defined at module level or within a function.

```python
cf.Metric(
    key: str,
    *,
    group: str | MetricGroup | None = None,
    scale: str | MetricScale | None = None,
    throttle: float | None = 10,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `key` | `str` | required | Metric key name |
| `group` | `str \| MetricGroup \| None` | `None` | Group (metrics in the same group share a chart) |
| `scale` | `str \| MetricScale \| None` | `None` | Y-axis scale (metrics with the same scale name share a y-axis) |
| `throttle` | `float \| None` | `10` | Max data points per second (`None` = no limit) |

#### `metric.record(value, *, at=None)`

Records a data point. `at` specifies an explicit x-value (defaults to time since execution start).

### `MetricGroup`

Configures the x-axis for a group of metrics displayed on a shared chart.

```python
cf.MetricGroup(
    name: str,
    *,
    units: str | None = None,
    lower: float | None = None,
    upper: float | None = None,
)
```

### `MetricScale`

Configures the y-axis. Metrics sharing a scale name within a group share a y-axis.

```python
cf.MetricScale(
    name: str | None = None,
    *,
    units: str | None = None,
    progress: bool = False,
    lower: float | None = None,
    upper: float | None = None,
)
```

### `progress(iterable, key="progress", *, group=None)`

Wraps a sized iterable and automatically records a progress metric as items are consumed. Renders as a progress bar in Studio.

## Context functions

### `group(name=None)`

Context manager for visually grouping child executions in the graph view. Useful for organising steps in complex workflows.

```python
with cf.group("batch processing"):
    for item in items:
        process.submit(item)
```

### `suspense(timeout=None)`

Context manager that sets a timeout on `.result()` calls within its scope. If the result isn't ready within the timeout, the execution is suspended and automatically re-run later. See [suspense](./suspense.md).

### `suspend(delay=None)`

Explicitly suspends the current execution. It will be re-run after the specified delay. `delay` can be `float` (seconds), `timedelta`, or `datetime`.

### Logging

Structured logging functions that associate key-value pairs with a template string. Values are stored separately from the template, enabling filtering and search in Studio. See [logging](./logging.md).

```python
cf.log_debug(template=None, **kwargs)
cf.log_info(template=None, **kwargs)
cf.log_warning(template=None, **kwargs)
cf.log_error(template=None, **kwargs)
```

### `asset(entries=None, *, at=None, match=None, name=None)`

Creates and persists a collection of files as an asset, which can be inspected and downloaded from Studio or the CLI. See [assets](./assets.md).

## Exceptions

| Exception | Description |
|-----------|-------------|
| `ExecutionError` | Child execution failed. When the original exception type can be resolved, the raised exception subclasses both `ExecutionError` and the original type, so you can catch either. |
| `ExecutionCancelled` | Child execution was cancelled. |
| `ExecutionTimeout` | Child execution exceeded its configured timeout. |
