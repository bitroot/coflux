## 0.12.1

No changes.

## 0.12.0

Enhancements:

- Adds streams. A task whose body is a generator (`def`/`async def` with `yield`) produces a stream, and its result is a `cf.Stream` handle that other tasks iterate with `for` or `async for`. `cf.stream(generator, buffer=..., timeout=...)` registers a generator explicitly, for returning several streams or a stream alongside other values.
- Adds `cf.Streams(buffer=, timeout=)` for a target's default stream configuration (`streams=` on `@task`/`@workflow`, or `target.with_streams(...)` per call site), and `Stream.slice`/`partition`/`stride` views for splitting a stream across consumers.
- A producer can `cf.suspend()` from inside its generator: the stream pauses, and the resumed execution continues it.
- Iterating a stream inside `cf.suspense(...)` suspends the consumer when the stream goes quiet, and resumes it from where it left off. The position is kept in an adapter-managed checkpoint, published together with any checkpoint writes made in the loop body.
- Adds `cf.Checkpoint` for state that survives across executions of a step — retries, suspensions, recurrences and re-runs. Supports `get`, `set`, `update` and `reset`, with a declared default.
- Adds `cf.flush` for synchronously flushing buffered state to the server.
- Adds `AssetEntry.read(offset, length)` for reading part of an asset entry without restoring the whole file.
- Adds `ExecutionTerminated` as the base class of `ExecutionCancelled` and `ExecutionTimeout`, with new `ExecutionAbandoned`, `ExecutionCrashed` and `StreamSuperseded` subclasses, raised when waiting on an execution (or iterating a stream) that ended that way.

Changes:

- Removes `ModelSchema` from the public API.
- `Prompt` templates are dedented, and leading/trailing blank lines stripped, before rendering — so a triple-quoted template indented to match its code renders as Markdown rather than as a code block.
- Discovery exits non-zero when a module fails to import, reporting the traceback on stderr.

## 0.11.0

Enhancements:

- Adds `cf.Prompt` and `cf.Input` for requesting structured input from users mid-execution (with optional Pydantic model for typed responses, per-run memoisation, and `requires` tags for routing).
- Adds `cf.select` for waiting on the first of multiple handles (executions and/or inputs) to resolve, with optional cancellation of the rest.
- Adds `cf.cancel` (and `.cancel()` on handles) for atomic cancellation of executions and inputs.
- Supports `async def` functions in `@task` and `@workflow` decorators.
- Adds fluent `with_*` methods to `Target` for overriding decorator options at a call site.

Changes:

- Wait-expiry from `cf.suspense(timeout=...)` now raises the standard `TimeoutError`; `ExecutionTimeout` is reserved for executions exceeding their configured `timeout`.

## 0.10.0

Enhancements:

- Adds support for writing metrics, and writing progress.
- Adds `timeout` parameter to `@task` and `@workflow` decorators.
- Adds `ExecutionCancelled` and `ExecutionTimeout` exceptions.
- Adds `.poll()` method to `Execution` for checking execution results without blocking (or suspending).
- Adds `memo` parameter to `@task`, `@workflow`, and `@stub` decorators.
- Adds `requires` parameter to `@task` and `@workflow` decorators.

## 0.9.0

Enhancements:

- Communicates with the new Go CLI over JSON Lines (replacing the previous all-in-one Python package).
- Adds support for conditional retries (`@task(retries=Retries(3, when=TransientError))`).
- Supports serialisation of additional types (datetime, UUID, Decimal, bytes, frozenset, etc.).
- Spawns a fresh process for each execution, improving isolation and memory management.
- Improved error reconstruction for remote exceptions.
- Reports protocol version during worker handshake for compatibility validation.

Changes:

- The `coflux` package is now a pure Python SDK — the CLI is a separate Go binary.
- Replaces 'sensors' and 'checkpoints' with recurrent targets.
- Renames 'spaces' back to 'workspaces'.
