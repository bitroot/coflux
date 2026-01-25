# Execution states

The state transitions of an execution can be represented in a diagram:

```mermaid
graph TB;
  start((Start))-->queued
  queued[Queued]-->cached[Cached]
  queued-->assigned
  queued-->spawned[Spawned]
  queued-->deferred[Deferred]
  queued-->cancelled
  assigned[Assigned]-->succeeded[Succeeded]
  assigned-->failed[Failed]
  assigned-->abandoned[Abandoned]
  assigned-->suspended[Suspended]
  assigned-->cancelled[Cancelled]
```

When an execution is first scheduled, it starts in the _Queued_ state.

(unless caching is enabled and there is a cache hit, in which case it transitions straight to _Cached_).

From the _Queued_ state it will transition to _Assigned_ once it is due, and a suitable worker is available to run it, unless: caching is enabled and there's a cache hit; it's referring to a workflow, in which case that is 'spawned' as a separate run; it become 'deferred' to another execution in the meantime.

Once assigned, the worker will generally execute it until it succeeds (_Succeeded_) or raises an exception (_Failed_). If contact is lost with a worker for more than the timeout period, the task that are running will be marked as _Abandoned_ (we don't know whether it completed successfully). Executions may be _Cancelled_ while they're running (or before they've been assigned). An execution may choose to suspend itself (either explicitly, or from timing out while waiting for another execution to complete) - in this case it will be automatically re-run.

Steps maybe be configured to automatically retry (from a failed or abandoned state), or they may be re-run manually, in which case a new execution will be started.
