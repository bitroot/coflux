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
cursor.get()          # the current value, or the default if unset
cursor.set(value)     # replace the value
cursor.update(fn)     # set it to fn(current), and return that
cursor.reset()        # clear it, so get() returns the default again
cursor.is_set()       # whether it has a value
```

`update` is the read-modify-write most checkpoints do — advancing a cursor, accumulating a total — without naming the old value:

```python
n = count.update(lambda x: x + 1)
```

`fn` receives the declared default when the checkpoint isn't set, so an unset checkpoint needs no special case at the call site. It's a read then a write rather than an atomic swap, so if you share one checkpoint between a task body and a `cf.stream` generator, guard it yourself.

Reads are served locally: the effective state arrives with the execution, and an execution always sees its own writes. Nothing round-trips to the server.

`set(None)` and `reset()` are different. `set(None)` stores `None`, and `get()` returns `None`. `reset()` removes the checkpoint, so `get()` falls back to the declared default.

A checkpoint handle can't be passed to another execution — it names storage belonging to a particular step, so declare it in the target that uses it.

## Typed values

A checkpoint is typed by what `get()` returns. That's inferred from the default when there is one, so `cursor` above is a `Checkpoint[int]`. Without a default, `get()` can return `None`, so declare that:

```python
cursor = cf.Checkpoint[int | None]("cursor")
```

Spelling the type out does more than inform type checkers. With [Pydantic](https://docs.pydantic.dev/) installed, `set()` validates the value against it and stores plain data, and `get()` validates what it reads and returns it as that type. Any type Pydantic can validate works: a model, a dataclass, a `TypedDict`, or a plain annotation.

```python
from pydantic import BaseModel


class Position(BaseModel):
    since: int
    page: int


cursor = cf.Checkpoint[Position]("cursor", default=Position(since=0, page=0))
seen = cf.Checkpoint[set[str]]("seen", default=set())


@cf.workflow(recurrent=True, delay=60)
def poll_orders():
    position = cursor.get()                       # a Position
    orders, since, page = fetch_orders(position)
    for order in orders:
        process_order.submit(order)
    cursor.set(Position(since=since, page=page))  # stored as its fields
```

A model is stored as its fields, so what's in the checkpoint doesn't depend on the class: an attempt reads what an earlier attempt wrote whether or not the class has moved, and Studio shows the fields. The type that validates a read is the one declared in the code doing the reading, so an attempt running new code finds out at the read that what an older attempt stored doesn't fit — rather than somewhere downstream.

The declared default is validated where it's declared, and a checkpoint with no default reads as `None` until it's set, so the type has to admit that (`Checkpoint[int | None]`) or a default has to be given.

An asset, or another of Coflux's own types, is checked by instance: `cf.Checkpoint[cf.Asset | None]("weights")` needs nothing more. A model that holds one needs Pydantic told that an arbitrary type is fine there, with `model_config = ConfigDict(arbitrary_types_allowed=True)`.

Without Pydantic, the type only informs type checkers, as with task arguments and results: the value is stored and returned as it is.

## Durability

Writes are throttled and delivered in the background, so a crash can lose up to a fraction of a second of them. **Treat a checkpoint as at-least-once**: the code after a read has to be safe to run again from the value it read.

In the polling example above, that means a crash may cause some orders to be fetched twice — which is fine, because `process_order` is submitted with the same arguments and can be memoized.

Whatever has been written when an execution suspends, returns, or fails is delivered before the next attempt starts — with one deliberate exception, described under [consistency](#consistency) below. You only need to think about this for a side effect *within* an execution that must not be repeated. `cf.flush()` gives an explicit boundary:

```python
cursor.set(next_cursor)
cf.flush()
send_notification()
```

`cf.flush()` returns once the server has acknowledged the write.

## Consistency

A step's checkpoints are one snapshot of its progress rather than a set of independent cells: whatever has been written is delivered as a single delta and applied at once. Deltas are only cut where the execution could resume from — never part-way through consuming something a replay can't re-read.

That's what keeps state derived from a [stream](./streams.md) honest. A checkpoint written in the loop body is published in the same delta as the cursor advance that consumes the item, so a running total never counts an item the cursor says was never read. If the iteration doesn't finish — a `break` or `return`, an exception, a lost worker — the item stays unconsumed and the writes derived from it are dropped, because the next attempt reads that item again.

A replay therefore repeats whole items rather than fractions of one. That isn't exactly-once: the item *is* delivered again, so anything else the loop body did happens again too. [Memoize](./memoizing.md) what it calls.

## Scope

Checkpoints are scoped to a step within a [workspace](./concepts.md). Reads fall back through the workspace's bases, so re-running a step in a derived workspace reads the base's real state — useful for debugging against production values — while writes only ever land in the workspace doing the writing. A derived workspace can't corrupt the state its base is using, and once it has written its own value it reads that instead.

A checkpoint belongs to the step that actually executes. A step resolved from the [cache](./caching.md) or by [memoization](./memoizing.md) never runs, so it never sees one, and a checkpoint is never part of a cache, memo or defer key.

:::warning
Checkpoints are scoped to a step within a run, so a recurring workflow keeps its checkpoints for as long as its run is alive — across every recurrence, retry and suspension. But if recurrence stops (retries are exhausted, the task returns a non-`None` value, or the run is cancelled), submitting the workflow again creates a new run with a fresh step, which starts from the declared defaults.
:::

## Reserved names

Names starting with an underscore belong to the adapter, and `cf.Checkpoint("_...")` raises. They're currently used for the cursors behind [stream](./streams.md) suspension, which track how far a consumer has read; you'll see them alongside your own in Studio. The Python variable name is unrestricted — only the checkpoint's name matters, so `_cursor = cf.Checkpoint("cursor")` is fine.

## Size

A checkpoint value is serialized like any other, so a large one is stored as a [blob](./blobs.md) and downloaded at the start of every execution of the step. That's cheap for a cursor and expensive for a large dataframe — prefer keeping checkpoints small, and use an [asset](./assets.md) for anything substantial.
