# Streams

A stream is an ordered sequence of values that one task produces and others consume as it grows. Where a task result is a single value handed over at the end, a stream lets a consumer start on the first item while the producer is still working on the last.

## Producing

The simplest producer is a task whose body is a generator. Its result *is* the stream:

```python
import coflux as cf


@cf.task()
def fetch_pages(url: str):
    for page in paginate(url):
        yield page
```

Calling `fetch_pages(url)` returns a `cf.Stream` handle, and `fetch_pages.submit(url)` returns an execution whose result resolves to one. `async def` generators work the same way.

To return several streams, or a stream alongside other values, register a generator explicitly with `cf.stream()` and put the handle wherever you like in the return value:

```python
@cf.task()
def split(url: str):
    pages = paginate(url)
    return {
        "headers": cf.stream(h for h in headers_of(pages)),
        "bodies": cf.stream(b for b in bodies_of(pages)),
    }
```

`cf.stream()` accepts only generators. A list is already a value; return it as one.

Each item is delivered to consumers as soon as it's produced. The producing execution stays running until every stream it produced has finished, even after it has returned its result.

## Consuming

A `cf.Stream` is iterable. Iterating blocks until the next item arrives, and ends when the stream closes:

```python
@cf.task()
def index(pages: cf.Stream[dict]):
    for page in pages:
        add_to_index(page)
```

Use `async for` inside `async def` bodies. Each iteration of a handle starts from the beginning, so a stream can be read more than once, by more than one consumer, and by a consumer that starts long after the producer finished.

`stream.slice(start, stop)` restricts iteration to a range of positions, and `stream.partition(n, i)` delivers every `n`-th item starting at `i`, which spreads a stream across parallel consumers. Views compose, and a view can be passed to another task like any handle.

If the producer raises, iterating raises the same error. If the producer is cancelled or lost, iterating raises the corresponding `ExecutionTerminated` subclass.

## Backpressure

By default a producer runs in lockstep with its slowest consumer: it emits one item, waits for a consumer to finish with it, then emits the next. Configure how far ahead it may run with `streams=`:

```python
@cf.task(streams=cf.Streams(buffer=100))
def fetch_pages(url: str): ...
```

`buffer=N` lets the producer run up to `N` items ahead of the slowest consumer. `buffer=None` disables backpressure. Items are never lost to a slow consumer: they're stored as they're produced, and a consumer that falls behind catches up at its own pace.

Per-call overrides are available on `cf.stream(generator, buffer=...)`, and `target.with_streams(...)` overrides the configuration for one submission.

Note that with the default lockstep buffer, a producer whose stream never gains a consumer waits indefinitely, holding its worker slot. Set a `timeout` (below) to bound that.

## Suspending

A stream belongs to the *step* that produces it, not to any one execution. The difference shows when a producer [suspends](./suspense.md): its streams pause rather than close, and the execution that resumes the step continues them. Consumers see one unbroken stream.

This is how to write a producer that keeps going indefinitely without holding a worker slot while it waits:

```python
cursor = cf.Checkpoint("cursor", default=0)


@cf.task()
def tail_events():
    since = cursor.get()
    for event in fetch_events(since):
        yield event
        since = event.id
        cursor.set(since)
    cf.suspend(60)
```

Each resume runs the body from the top, reads the [checkpoint](./checkpoints.md), and carries on the same stream. Consumers just wait through the pause.

Streams are matched up by the order they're registered in, so code that registers several streams before suspending must register them in the same order when it resumes. That's the same determinism suspend already requires of the code before the suspend point, and it's automatic for a task whose body is a generator.

Studio shows a stream under its step, listing every attempt that produced into it, and labels each item with the attempt that produced it.

## When a stream ends

Only a suspend keeps a stream open across executions. Every other way an execution can end closes the step's open streams:

- The generator finishing closes its stream normally, and a task completing normally closes anything it left open.
- An exception in the generator closes the stream with that error. A [retry](./retries.md) opens a fresh stream.
- Cancellation, a crash, or a lost worker closes it with that reason. Cancelling a suspended step, which cancels its pending resumption, closes its paused streams too.
- A [recurrent](./recurring.md) task finishing an iteration closes its streams, and the next iteration opens its own. A recurrent task can't have a generator body for this reason: its result would be a stream, so it could never return `None` to recur. Use the suspend form above for a continuous stream.
- Re-running a step from Studio cancels the running attempt, closing its streams, and the new attempt starts fresh. Re-running a *suspended* step instead continues its paused streams, since nothing was producing into them.

Once closed, a stream is never reopened. A later attempt of the step produces a new stream.

## Timeouts

A stream can be given an idle timeout:

```python
@cf.task(streams=cf.Streams(timeout=30))
def fetch_pages(url: str): ...
```

If the producer doesn't append an item within that window, the stream is closed as timed out and the generator is stopped. The window counts time spent waiting for consumer demand too, so with a lockstep buffer a slow or absent consumer can time out the producer. The producing execution still completes with its value, but is excluded from [caching](./caching.md) and [memoizing](./memoizing.md).

The timeout is measured from the last item appended, per execution. A suspended step is not idle, so the pause between a suspend and its resumption doesn't count; the timer starts again when the resumed execution registers. There is no consumer-side timeout: iterating a stream waits as long as it takes.

## Workspaces

A stream is only written from the [workspace](./concepts.md) it was produced in. Re-running a producer in a derived workspace produces a new stream there rather than continuing the base's, while consumers in a derived workspace can still read a base workspace's stream, the same way they read its results.
