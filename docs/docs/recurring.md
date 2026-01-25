# Recurring tasks

A task or workflow can be configured to automatically re-execute after it completes successfully, by setting `recurrent=True`:

```python
@cf.workflow(recurrent=True)
def poll_for_updates():
    updates = fetch_updates()
    for update in updates:
        process_update.submit(update)
```

This continues indefinitely until the run is cancelled.

## Delay

By default, recurring tasks restart immediately. Use `delay` to wait between executions:

```python
@cf.workflow(recurrent=True, delay=60)
def poll_every_minute():
    ...
```

The delay is in seconds (or pass a `timedelta`).

## Retries

Recurring tasks can be combined with [retries](./retries.md). If a task fails, retries are attempted first. Only successful completions trigger the next recurrence.

```python
@cf.workflow(recurrent=True, delay=60, retries=3)
def resilient_polling():
    ...
```
