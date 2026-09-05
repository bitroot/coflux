# Recurring tasks

A task or workflow can be configured to automatically re-execute after it completes successfully, by setting `recurrent=True`:

```python
@cf.workflow(recurrent=True)
def poll_for_updates():
    updates = fetch_updates()
    for update in updates:
        process_update.submit(update)
```

The task recurs as long as it returns `None`. Returning any other value completes the cycle and stops recurrence. The run can also be stopped by cancelling it, or if an error occurs (without a successful retry).

Each iteration is a fresh execution starting from the top, so anything that needs to carry forward between them — a cursor, say — belongs in a [checkpoint](./checkpoints.md).

A recurrent task can't have a generator body: each iteration would produce a separate [stream](./streams.md), and the result would never be `None`. For a stream that continues across pauses, suspend from inside the generator instead.

## Delay

By default, recurring tasks restart immediately. Use `delay` to wait between executions:

```python
@cf.workflow(recurrent=True, delay=60)
def poll_every_minute(): ...
```

The delay is in seconds (or pass a `timedelta`).

## Retries

Recurring tasks can be combined with [retries](./retries.md). If a task fails, retries are attempted first. Only successful completions trigger the next recurrence.

```python
@cf.workflow(recurrent=True, delay=60, retries=3)
def resilient_polling(): ...
```
