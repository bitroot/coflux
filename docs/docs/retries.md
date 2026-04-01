# Automatic retries

A step can fail for a number of reasons. One case is where an exception is raised in the code. Other cases might involve network issues, or a worker restarting while executing (causing the execution to be identified as 'abandoned').

By default, Coflux takes a cautious _at-most-once_ approach to execution, to avoid unintentionally executing a task that might have significant side-effects more than once.

The simplest way to enable retries is to specify the maximum number of attempts the task (or workflow) should be retried:

```python
@cf.task(retries=2)
def submit_reports():
    ...
```

This task will be executed at most three times (the initial attempt plus up to two retries). Each subsequent attempt will happen immediately after the previous failure.

Any tasks waiting for the initial task will continue waiting for retries. If all the retries fail then the error will be returned to the waiting task.

## Retry backoff

Failures can be caused by external environmental issues, so it's useful to be able to delay the retry. This can be done using the `Retries` class:

```python
@cf.task(retries=cf.Retries(limit=2, backoff=(300, 300)))
def send_notification():
    ...
```

In this case, the task will be retried up to twice, with a five minute delay (300 seconds) between each attempt.

## Backoff range

A range of backoff delays can be specified:

```python
@cf.task(retries=cf.Retries(limit=5, backoff=(300, 600)))
def send_notification():
    ...
```

In this case, the first retry will happen after 300 seconds, and the fifth (and final) retry will happen 600 seconds after the fourth, with an increasing gap between each retry. So (ignoring time taken for the execution and scheduling) attempts of this task would happen at `t+0`, `t+300`, `t+675`, `t+1125`, `t+1650`, `t+2250` (i.e., the final attempt happening over half an hour after the initial attempt).

## Unlimited retries

For tasks that should keep retrying indefinitely until they succeed, use `retries=True`:

```python
@cf.task(retries=True)
def critical_task():
    ...
```

This will retry with a random delay between 1 and 60 seconds (the defaults). For custom backoff:

```python
@cf.task(retries=cf.Retries(limit=None, backoff=(10, 300)))
def critical_task():
    ...
```

With unlimited retries, each retry uses a random delay between the min and max backoff seconds.

## Retry conditions

By default, retries apply to all errors. The `when` parameter allows you to control which errors trigger a retry — errors that don't match fail immediately without consuming retry attempts.

The condition is evaluated on the worker at the time the exception is raised, so it has access to the full exception object.

### Exception class

Specify a single exception type to only retry on that error:

```python
@cf.task(retries=cf.Retries(limit=3, when=ConnectionError))
def call_api():
    ...
```

### Tuple of exception classes

Specify multiple exception types as a tuple:

```python
@cf.task(retries=cf.Retries(limit=3, when=(ConnectionError, TimeoutError)))
def call_api():
    ...
```

### Callback function

For more complex logic, pass a function that receives the exception and returns whether to retry:

```python
@cf.task(
    retries=cf.Retries(
        limit=5,
        backoff=(1, 30),
        when=lambda e: getattr(e, "status_code", 0) >= 500,
    ),
)
def call_api():
    ...
```

This is useful for distinguishing between transient errors (e.g., 5xx server errors) that are worth retrying and permanent errors (e.g., 4xx client errors) that should fail immediately.
