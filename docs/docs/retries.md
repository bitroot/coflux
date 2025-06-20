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

## Retry delay

Failures can be caused by external environmental issues, so it's useful to be able to delay the retry. This can be done by passing a delay parameter:

```python
@cf.task(retries=(2, 300))
def send_notification():
    ...
```

In this case, the task will be retried up to twice, with a five minute delay (300 seconds) between each attempt.

## Backoff

Alternatively, a range of delays can be specified:

```python
@cf.task(retries=(5, 300, 600))
def send_notification():
    ...
```

In this case, the first retry will happen after 300 seconds, and the fifth (and final) retry will happen 600 seconds after the fourth, with an increasing gap between each retry. So (ignoring time taken for the execution and scheduling) attempts of this task would happen at `t+0`, `t+300`, `t+675`, `t+1125`, `t+1650`, `t+2250` (i.e., the final attempt happening over half an hour after the initial attempt).
