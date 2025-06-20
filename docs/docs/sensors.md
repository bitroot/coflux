# Sensors

Triggering runs on demand may suit some use cases, but often you'll want to be able to react to events occurring in your system. As well as tasks and steps, Coflux provides another target type: sensors. These can be used to monitor a database, watch a file system, or listen to a queue. They provide flexibility to subscribe to events, or poll a resource.

Sensors are defined in a module, along with workflows and tasks, and hosted by your worker:

```python
import coflux as cf

@cf.workflow():
def process_file():
    ...

@cf.sensor()
def new_files():
    ...
```

Once a sensor is activated, the orchestrator will do its best to ensure the function is always running. Once it terminates, it will be automatically restarted (subject to rate limiting). The sensor is responsible for initiating workflows as needed.

## Checkpoints

Typically a sensor needs to maintain some state. For example, to track a database cursor, or the name of the last file that was processed. Coflux supports this be allowing a sensor to 'checkpoint'. In the event that a sensor is restarted, its arguments will be replaced with those that were most recently passed to the `checkpoint` function.

## An example

Here's a sensor that periodically starts a workflow:

```python
@cf.sensor()
def ticker(interval: int = 300, last_tick: float | None = None):
    next_tick = last_tick + interval if last_tick else time.time()
    while True:
        remaining = max(0, next_tick - time.time())
        if remaining:
            time.sleep(remaining)
        my_workflow.submit()
        cf.checkpoint(interval, next_tick)
        next_tick += interval
```

This will call `my_workflow` every five minutes. The use of checkpointing means that if the worker gets restarted, the interval shouldn't get interrupted (subject to the time and duration of the restart).
