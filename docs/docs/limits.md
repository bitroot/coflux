# Concurrency limits

Some work can't safely run in parallel with itself: a rebuild that writes to a
shared index, a sync that would interleave badly with another copy of itself, an
API that rations requests. A task (or workflow) can declare how many of its
executions may run at once:

```python
@cf.task(concurrency=1)
def rebuild_index(): ...
```

The limit is enforced by the scheduler, before an execution is handed to a
worker. An execution beyond the limit stays in the queue — it doesn't occupy a
worker slot, and it doesn't cause a new worker to be launched.

:::info
This page is about limiting parallelism. For the mechanics of *achieving*
parallelism — submitting tasks and collecting results later — see
[concurrency](./concurrency.md).
:::

## What the limit applies to

By default the limit covers the target as a whole: one `rebuild_index` at a
time, whatever its arguments. Limits above `1` cost nothing extra, and are
written the same way:

```python
@cf.task(concurrency=4)
def call_model(prompt): ...
```

`concurrency=0` (the default) means no limit.

## Limiting per argument

To get one execution at a time *per account*, rather than one overall, name the
parameters the limit should key on with the `Concurrency` class:

```python
@cf.task(concurrency=cf.Concurrency(1, params=["account_id"]))
def sync_account(account_id, since): ...
```

Two calls with different `account_id` values run side by side; two with the same
one are serialised. `params` accepts an iterable of parameter names, a
comma-separated string, or `True` for every argument:

```python
@cf.task(concurrency=cf.Concurrency(2, params=True))
def fetch(url, headers): ...
```

This is the opposite default to [caching](./caching.md) and
[deferring](./deferring.md), where the argument tuple is the essence of the
feature. Here the common case is "one of these at a time", so `params` defaults
to `False`.

:::note
A `params=True` limit on a high-cardinality argument is harmless — the server
only tracks executions that are actually running — but it also won't limit much,
since each distinct argument tuple gets its own allowance.
:::

## Sharing a limit between targets

The limit is keyed by a namespace, which defaults to `"{module}:{target}"`. Give
several targets the same namespace and they draw on one pool:

```python
@cf.task(concurrency=cf.Concurrency(4, namespace="openai"))
def summarise(text): ...


@cf.task(concurrency=cf.Concurrency(4, namespace="openai"))
def classify(text): ...
```

At most four executions across both tasks run at once.

The limit itself is *not* part of the key: each execution is admitted against
the number it declared, not the number its neighbours declared. If `summarise`
says `4` and `classify` says `2`, then `classify` is admitted only while fewer
than two of the pool's permits are taken, while `summarise` is admitted up to
four. Mixed limits on one namespace are allowed, but they're easier to reason
about kept in step.

## Scope

Limits are counted per workspace. Workspaces are scheduled independently — each
has its own workers and pools, and [deferring](./deferring.md) is per workspace
too — and [inheritance](./concepts.md#workspace-inheritance) only shares
results, so a limit follows suit: an execution in a workspace derived from
`production` neither waits behind `production`'s executions nor holds them up.
Projects are fully separate, so a limit never crosses one either.

That does mean the limit doesn't protect an external resource that several
workspaces' workers all reach. If a derived workspace runs with production
credentials, its executions add to whatever `production` is already doing.

## When a permit is taken and released

An execution takes a permit when it's assigned to a worker, and releases it when
its completion is recorded. In practice:

- **Cache hits, deferred duplicates and memo hits never take one.** They're
  resolved by the server without an execution ever reaching a worker.
- **Suspending releases the permit.** A [suspended](./suspense.md) execution
  isn't running, so it doesn't hold the limit; its successor re-acquires when it
  next becomes due.
- **A failed attempt releases at completion.** A [retry](./retries.md) waiting
  out its backoff holds nothing, and re-acquires when it's admitted.
- **A [recurrent](./recurring.md) task releases at the end of each
  occurrence** — with `concurrency=1`, that gives you one occurrence at a time
  for free.
- **Cancellation, timeout, abandonment and crashes all write a completion**, so
  all of them release.
- **A task producing a [stream](./streams.md) holds its permit until the stream
  has drained**, since that's when the execution completes.

Held permits are derived from what's in the database, so a server restart
doesn't leak them.

## Workflows

`concurrency` on a `@workflow` limits executions of the workflow's own step. It
isn't inherited by the tasks the workflow submits (unlike `requires` and `memo`).

In the synchronous style — where the workflow blocks on each task it calls —
that amounts to "one run at a time", because the workflow's own execution is
alive for the whole run. If the workflow submits tasks and returns without
waiting for them, it doesn't: the workflow step finishes early and releases,
while its children carry on.

## Limiting fan-out from one caller

A task limit applies across the workspace: it protects a *resource*, and every
execution of the task in that workspace counts against it, wherever it was
submitted from. To bound how many of the children *one caller* has submitted run
at once, put the limit on the
[group](./groups.md#limiting-concurrency-within-a-group) instead:

```python
with cf.group("fetch", concurrency=2):
    executions = [fetch.submit(url) for url in urls]
```

That's local to the one `cf.group()` block, in the one execution of the caller.
A child can be under both kinds of limit, and is admitted only when both have
room.

## Composing with deferring

A limit and [deferring](./deferring.md) solve different halves of the same
problem, and compose well. Deferring collapses queued duplicates down to the
newest; the limit then serialises whatever survives:

```python
@cf.task(delay=60, defer=True, concurrency=1)
def rebuild_index(project_id): ...
```

## Deadlocks

A limit is held for as long as the execution runs, including while it waits on
something else. So an execution that holds the only permit and then
synchronously calls a task needing that same permit will wait forever:

```python
@cf.task(concurrency=cf.Concurrency(1, namespace="db"))
def child(): ...


@cf.task(concurrency=cf.Concurrency(1, namespace="db"))
def parent():
    return child()  # never admitted — parent holds the only permit
```

The same thing happens more subtly when every holder of a pool is waiting on
gated children.

Coflux doesn't detect this. Two things help: the queue page in Studio names the
executions holding a key, so a stuck execution says what it's stuck behind; and
setting a [timeout](./timeouts.md) bounds how long the damage lasts.

Avoid it by keeping the limited work at the leaves — put the limit on the task
that actually touches the constrained resource, not on the one that orchestrates
it.
