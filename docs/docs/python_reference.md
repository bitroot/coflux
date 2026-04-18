---
sidebar_label: Python adapter
---

# Python adapter reference

All exports are available from the `coflux` package (`import coflux as cf`).

## Decorators

Both `@workflow` and `@task` accept `async def` functions in addition to regular functions. The coroutine is run to completion by the executor, and the target's return type is the coroutine's resolved value.

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

### Per-call-site overrides

Each `with_*` method returns a new `Target` with the corresponding decorator-level option overridden, leaving the original target unchanged. This is useful for one-off variations without re-decorating, and the methods can be chained:

```python
my_task.with_retries(3).with_timeout(30).submit(x)
my_task.with_cache(False).submit(x)  # disable caching for this call

cached = my_task.with_cache(60)      # stash a configured variant
cached.submit(a)
cached.submit(b)
```

| Method | Description |
|--------|-------------|
| `with_cache(cache)` | Override [caching](./caching.md). Pass `False` to disable. |
| `with_retries(retries)` | Override [retries](./retries.md). Pass `0` or `False` to disable. |
| `with_defer(defer)` | Override [defer](./deferring.md) configuration. |
| `with_memo(memo)` | Override [memoisation](./memoizing.md) configuration. |
| `with_delay(delay)` | Override submission delay (seconds or `timedelta`). |
| `with_timeout(timeout)` | Override execution [timeout](./timeouts.md). |
| `with_requires(requires)` | Override worker routing tags. |

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

## Input

Returned by `prompt.submit(...)`. Represents a [requested input](./inputs.md), and acts as a future for the response. Like `Execution`, it can be passed to other tasks and only carries an ID across the wire.

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `id` | `str` | Input ID |

### `input.result() -> T`

Blocks (suspends) until the input is responded to and returns the value. If the prompt was created with a `model`, the value is parsed into that type.

**Raises:** `InputDismissed` if the prompt was dismissed; `ExecutionCancelled` if the input was cancelled. Raises `TimeoutError` if called inside a `cf.suspense(timeout=...)` scope and the timeout expires before a response arrives.

### `input.poll(timeout=None, *, default=None) -> T | D`

Non-suspending check for a response. Returns the value if available, or `default` otherwise.

### `input.cancel() -> None`

Cancels the input, transitioning it to a terminal _cancelled_ state (distinct from _dismissed_).

## Prompt

Defines a prompt for [requesting input](./inputs.md) from a user. Submit it to get an `Input` handle.

```python
cf.Prompt(
    template: str,
    model: type[T] | None = None,
    *,
    title: str | None = None,
    actions: tuple[str, str] | None = None,
    schema: str | dict | None = None,
    requires: dict[str, str | bool | list[str]] | None = None,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `template` | `str` | required | Message template. May contain `{placeholders}` substituted at submission. |
| `model` | `type \| None` | `None` | Pydantic model (or primitive type) used to render a form and validate the response. Without a model, the prompt is approval-only. |
| `title` | `str \| None` | `None` | Short title shown above the prompt. |
| `actions` | `tuple[str, str] \| None` | `None` | Custom labels for the respond/dismiss buttons, as `(respond, dismiss)`. |
| `schema` | `str \| dict \| None` | `None` | Raw JSON schema (alternative to `model`). |
| `requires` | `dict \| None` | `None` | Routing tags for matching to responders. |

### `prompt(**placeholders) -> T`

Submits the prompt and blocks until a response is available. Equivalent to `prompt.submit(...).result()`.

### `prompt.submit(**placeholders) -> Input[T]`

Submits the prompt and returns an `Input` handle without blocking.

### Per-submission overrides

Each `with_*` method returns a new `Prompt` with overrides applied:

| Method | Description |
|--------|-------------|
| `with_key(key)` | Use an explicit memoisation key (per-run). |
| `with_initial(value)` | Pre-populate the form with an initial value (e.g. a Pydantic instance). |
| `with_actions(respond, dismiss)` | Override button labels. |
| `with_requires(requires)` | Override routing tags. |

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

### `select(handles, *, cancel_remaining=False)`

Wait for the first of one or more handles (`Execution` and/or `Input`) to resolve. Returns `(winner, remaining)` — call `winner.result()` to get the value (or to raise the exception that resolved it). Picks up its timeout from any enclosing `cf.suspense(timeout=...)` scope; raises `TimeoutError` if the wait expires. See [select](./select.md).

```python
winner, remaining = cf.select([a.submit(), b.submit()])
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `handles` | `Sequence[Execution \| Input]` | required | Handles to wait on (must be non-empty). |
| `cancel_remaining` | `bool` | `False` | Atomically cancel non-winner `Execution` handles when one resolves. `Input` handles are left pending. |

### `cancel(handles)`

Atomically cancel one or more handles. `Execution` handles are cancelled recursively (descendants too); `Input` handles transition to a terminal _cancelled_ state (distinct from _dismissed_). Already-resolved handles are silently skipped.

```python
cf.cancel([execution_a, execution_b, input_c])
```

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
| `ExecutionCancelled` | Child execution (or input) was cancelled. |
| `ExecutionTimeout` | Child execution exceeded its configured `timeout`. |
| `InputDismissed` | A requested input was dismissed by the responder. |
| `TimeoutError` (built-in) | A `cf.suspense(timeout=...)` wait expired before a handle resolved. |
