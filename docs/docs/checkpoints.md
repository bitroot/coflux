# Checkpoints

A checkpoint is a named value that survives across executions of a step. When a step [suspends](./suspense.md), [retries](./retries.md), [recurs](./recurring.md), or is re-run from Studio, the code starts again from the top — a checkpoint is how it picks up where it left off.

```python
import coflux as cf

cursor = cf.Checkpoint("cursor", default=0)


@cf.workflow(recurrent=True, delay=60)
def poll_orders():
    since = cursor.get()
    orders, next_since = fetch_orders(since)
    for order in orders:
        process_order.submit(order)
    cursor.set(next_since)
```

Each iteration reads what the previous one wrote, so the poller only ever fetches what it hasn't seen.

## Checkpoints vs memoizing

These solve overlapping problems, and it's worth being deliberate about which you reach for.

[Memoizing](./memoizing.md) is the right tool when the state you want to keep is *the result of some work*. A memoized child task runs once and its result is reused by every subsequent attempt, so the work isn't repeated.

Checkpoints are for state that isn't naturally a task result — a cursor, a page token, a partially-built accumulator. There's no work to memoize, just a value to carry forward.

## Reading and writing

Create a `Checkpoint` with a name, and optionally a default:

```python
cursor = cf.Checkpoint("cursor", default=0)
```

The name identifies storage scoped to the step, so declaring the handle at module level is fine — it isn't module state.

```python
cursor.get()        # the current value, or the default if unset
cursor.set(value)   # replace the value
cursor.reset()      # clear it, so get() returns the default again
cursor.is_set()     # whether it has a value
```

Reads are served locally: the effective state arrives with the execution, and an execution always sees its own writes. Nothing round-trips to the server.

`set(None)` and `reset()` are different. `set(None)` stores `None`, and `get()` returns `None`. `reset()` removes the checkpoint, so `get()` falls back to the declared default.

A checkpoint is typed by what `get()` returns. That's inferred from the default when there is one, so `cursor` above is a `Checkpoint[int]`. Without a default, `get()` can return `None`, so declare that:

```python
cursor = cf.Checkpoint[int | None]("cursor")
```

Like argument and result types, this only informs type checkers — nothing is enforced at runtime.

A checkpoint handle can't be passed to another execution — it names storage belonging to a particular step, so declare it in the target that uses it.

## Durability

Writes are throttled and delivered in the background, so a crash can lose up to a fraction of a second of them. **Treat a checkpoint as at-least-once**: the code after a read has to be safe to run again from the value it read.

In the polling example above, that means a crash may cause some orders to be fetched twice — which is fine, because `process_order` is submitted with the same arguments and can be memoized.

Whatever has been written when an execution suspends, returns, or fails is always delivered before the next attempt starts. You only need to think about this for a side effect *within* an execution that must not be repeated. `cf.flush()` gives an explicit boundary:

```python
cursor.set(next_cursor)
cf.flush()
send_notification()
```

`cf.flush()` returns once the server has acknowledged the write.

## Scope

Checkpoints are scoped to a step within a [workspace](./concepts.md). Reads fall back through the workspace's bases, so re-running a step in a derived workspace reads the base's real state — useful for debugging against production values — while writes only ever land in the workspace doing the writing. A derived workspace can't corrupt the state its base is using, and once it has written its own value it reads that instead.

A checkpoint belongs to the step that actually executes. A step resolved from the [cache](./caching.md) or by [memoization](./memoizing.md) never runs, so it never sees one, and a checkpoint is never part of a cache, memo or defer key.

:::warning
Checkpoints are scoped to a step within a run, so a recurring workflow keeps its checkpoints for as long as its run is alive — across every recurrence, retry and suspension. But if recurrence stops (retries are exhausted, the task returns a non-`None` value, or the run is cancelled), submitting the workflow again creates a new run with a fresh step, which starts from the declared defaults.
:::

## Size

A checkpoint value is serialized like any other, so a large one is stored as a [blob](./blobs.md) and downloaded at the start of every execution of the step. That's cheap for a cursor and expensive for a large dataframe — prefer keeping checkpoints small, and use an [asset](./assets.md) for anything substantial.
