# Deferring

## Delaying tasks

Execution of tasks can be delayed by a fixed duration, by configuring the `delay`. This can be specified as a `datetime.timedelta`, or as a numeric value, in seconds:

```python
import datetime as dt

@cf.task(delay=dt.timedelta(minutes=10))
def send_reminder(reminder_id):
    ...
```

## De-duplicating tasks

Delaying tasks is useful in combination with 'deferring' as a way to de-duplicate some operation. This concept is sometimes known (particularly in frontend development) as 'debouncing'.

For example, you might want to be able to send a notification to a user to notify of them of updates to a document. If there are lots of updates to the document within a short period of time, you wouldn't want to send notifications for every change. Instead, you can configure a delay, as above, and enable deferring. This is done by specifying the `defer` option on the task:

```python
@cf.task(delay=60, defer=True)
def send_notification(user_id, document_id):
    ...
```

With this configuration, the initial task call will be delayed by 60 seconds, and, in the meantime, if the task is called again with the same arguments, the new task will replace the original one. Any tasks waiting on the initial task will wait for the new task instead. This deferring process will continue until there's a break of 60 seconds between calls, at which point the latest task will execute.

Deferring can also be useful without specifying an explicit delay in the case where there's a backlog of tasks waiting to be executed.

## Defer parameters

Similarly to how caching is configured, specific parameters can be specified for use with deferring, if needed, using the `Defer` class:

```python
@cf.task(delay=60, defer=cf.Defer(params=["user_id", "document_id"]))
def send_notification(user_id, document_id, update):
    ...
```

In this case, subsequent calls for the same user and document would be de-duplicated, even though the update is different each time. Initial calls to `send_notification` would be discarded, in favor of the latest call.
