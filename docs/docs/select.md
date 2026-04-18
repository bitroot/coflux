# Select

`cf.select` waits for the _first_ of multiple handles to resolve. The handles can be [executions](./executions.md), [inputs](./inputs.md), or a mix of both. It's the building block for first-wins coordination patterns: racing alternative implementations, prompting a user with a fallback timeout, multi-channel approvals, or any case where you want to react to whichever result arrives soonest.

## Basic usage

```python
import coflux as cf

@cf.workflow()
def search(query: str):
    a = source_a.submit(query)
    b = source_b.submit(query)
    c = source_c.submit(query)

    winner, remaining = cf.select([a, b, c])
    return winner.result()
```

`select` returns a tuple `(winner, remaining)` where:

- `winner` is the handle that resolved first. Call `.result()` on it to get the value (or to raise the exception that caused it to resolve).
- `remaining` is the list of handles that did _not_ win, in the order they were passed in. They keep running and can be awaited later.

## Cancelling the losers

Pass `cancel_remaining=True` to atomically cancel the executions that didn't win as soon as one resolves. This is done in a single round-trip with the resolution itself, so there's no window where the losers continue to consume resources unnecessarily:

```python
winner, _ = cf.select([fast.submit(), slow.submit()], cancel_remaining=True)
return winner.result()
```

`Input` handles in the list are left pending — only `Execution` handles are cancelled.

## Mixing executions and inputs

The handles can be a mix of `Execution` and `Input`, so you can race a long-running task against a user prompt — for example, asking for input only if a default lookup takes too long:

```python
auto = lookup_value.submit(key)
manual = cf.Prompt("Enter value for {key}").submit(key=key)

winner, _ = cf.select([auto, manual], cancel_remaining=True)
value = winner.result()
```

Or wait for the first of several alternative approvers to respond:

```python
inputs = [approve.with_requires({"user": user}).submit() for user in approvers]
winner, _ = cf.select(inputs)
```

## Timeouts

`select` takes its timeout from any enclosing [`cf.suspense(timeout=...)`](./suspense.md) scope. If the wait expires before any handle resolves, `cf.select` raises `TimeoutError`:

```python
try:
    with cf.suspense(timeout=30):
        winner, _ = cf.select([a, b])
except TimeoutError:
    # neither resolved within 30s
    ...
```

Note that `TimeoutError` here means the _wait_ expired — distinct from `ExecutionTimeout`, which is raised when an individual execution exceeds its configured `timeout`.

## Resolving the same handle later

When a handle wins a `select`, its result is cached in the execution context. Calling `.result()` (or `.poll()`) on the winner afterwards returns immediately without another round-trip. Handles in `remaining` are unaffected — they can be awaited normally as they resolve.
