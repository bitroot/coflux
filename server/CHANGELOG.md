## 0.13.0

Enhancements:

- A worker launched on ECS that fails now carries the tail of its log stream, read from CloudWatch, the way the Docker, Kubernetes and process launchers already carry the container's own output. It needs the task definition to use the `awslogs` driver and the launcher's identity to have `logs:GetLogEvents`; without either, the worker reports what ECS said about the stop — now including the container's own reason and exit code rather than only the task-level message, which for a container exit says nothing but that it exited.
- The ECS launcher can assume an IAM role (`roleArn`, with `roleExternalId` where the trust policy asks for one) with whatever credentials it finds, so a server in one account can launch into another. The search of the server's surroundings now includes a web identity token (`AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN`, as EKS sets for a pod whose service account has a role). Credentials issued by STS are cached until shortly before they expire.

Changes:

- Durations carry their unit in their name, and are milliseconds everywhere the API and the database speak them. `delay` is `delayMs`, `timeout` is `timeoutMs`, `maxAge` is `maxAgeMs`, `backoffMin`/`backoffMax` are `backoffMinMs`/`backoffMaxMs` — on `submit_workflow`, on `register_manifests`, and in the topics that report them. The worker protocol matches, so `max_age`, `backoff_min` and `backoff_max` gain the `_ms` the adapter already sent them with. Database columns are renamed to match; no values change.
- The pool `idleTimeout` field, added earlier in this release and never published, is `idleTimeoutMs` on the API and is milliseconds rather than seconds. Its default is unchanged at 5 seconds. (The CLI still writes it as `idleTimeout`, taking a duration.)
- Workspace patterns mean one thing everywhere now. A pattern selects a workspace (`development`), the workspaces under it (`development/*`, at any depth, but not `development` itself), or all of them (`*`) — the rule tokens already used, now used for a secret's workspaces too, where a bare name previously selected everything beneath it as well. Patterns that name nothing are rejected.
- Setting a secret takes access containing every pattern given, whole, rather than access to any one workspace the pattern reaches — so a token for `staging` can no longer set a secret reaching `staging/feature-1`.
- Secrets are set for one or more workspace patterns (`workspaces` on `set_secret` and `delete_secret`, replacing `scope`), stored once per pattern. Where patterns overlap, the nearest wins: an exact workspace, then a longer prefix, then a shorter one, then `*`.

## 0.12.0

Enhancements:

- Adds streams — ordered sequences of values that one execution produces and others consume as they're produced. Items are stored as they arrive, so a stream can be read by several consumers, more than once, and after the producer has finished. Supports credit-based backpressure (`buffer`), idle timeouts, and strided views (`slice`/`partition`).
- Streams belong to the step that produces them: a producer that suspends leaves its streams paused, and the execution that resumes the step continues them.
- Supports consumers suspending while iterating a stream, holding the successor until the stream reaches the awaited item (or closes). The producer's idle countdown is paused while every consumer is waiting on it.
- Adds checkpoints — named values scoped to a step within a workspace, carried across retries, suspensions, recurrences and re-runs. Reads fall back through the workspace's bases; writes stay in the writing workspace. History is compacted to the effective state at epoch rotation.
- Exposes the dependencies that a queued or suspended execution is waiting on (executions, inputs, stream items) from the run and queue topics.
- Supports HTTP range requests on the blob endpoint.
- Adds an API endpoint for creating an asset from uploaded blobs (used by Studio's run dialog).
- Validates the shape of worker messages before they reach the orchestration process.

Fixes:

- Fixes a path traversal issue in the built-in blob store.
- Fixes request body handling for large bodies and malformed JSON in the API, logs and metrics endpoints.
- Fixes inputs staying active after their execution was retried, and the modules topic's active-run tracking.

## 0.11.0

Enhancements:

- Adds support for tasks requesting structured inputs from users.
- Adds a generalised `select` operation for waiting for results, with support for specifying multiple handles, and optional cancellation of the other items.

## 0.10.0

Enhancements:

- Adds support for storing metrics and their definitions.
- Adds support for execution timeouts on tasks and workflows.
- Adds `suspend` flag to `get_result` for non-suspending result polling.
- Adds experimental Kubernetes launcher for launching workers as Kubernetes jobs.
- Adds support for run-level memoisation (`memo` on workflows sets default for all steps).
- Adds `accepts` tags on pools/workers, restricting which executions can be assigned.
- Workflow `requires` tags now apply to the entire run (merged with step-level tags).
- Adds support for disabling and re-enabling pools (disabled pools drain workers).
- Prevents concurrent executions of the same step; re-running cancels in-progress execution.
- Recurrent targets now only recur when the result is `None`.
- Adds support for exporting and importing pool configurations.
- Adds support for overriding workflow options (`requires`, `memo`, `delay`, `retries`) at submission time.
- Adds support for specifying project via header (in addition to subdomain/server config).
- Tracks total execution count on sessions and workers.
- Updates modules topic to track in-progress workflow runs.

## 0.9.0

Enhancements:

- Adds authentication to worker connections and blob/log endpoints.
- Adds support for project-level tokens and Studio authentication.
- Introduces epochs for managing data retention.
- Supports conditional retries on tasks.
- Supports cancelling executions across workspaces.
- Returns API version in the discover endpoint for client compatibility validation.
- Automatically stops idle orchestration servers to reduce resource usage.
- Workspaces are auto-created on first worker connection.

Fixes:

- Fixes idempotency key handling for duplicate run submissions.
- Fixes manifest hashing for consistent change detection.
- Fixes result delivery for suspended executions.
- Makes argument waiting recursive for deeply nested dependencies.

Changes:

- Removes the bundled frontend (use Coflux Studio instead).
- Replaces sensors and checkpoints with recurrent targets.
- Renames 'spaces' back to 'workspaces'.
- Removes namespaces in favour of a simplified project model.
- Reworks ID generation for shorter, URL-friendly identifiers.

## 0.8.1

Enhancements:

- Improvements to asset dialog, including supporting markdown/PDF previews.
- Improvements to group selection UI.
- Added support for validating expected API version.

Changes:

- Reverses prioritisation of execution (oldest first).

## 0.8.0

Enhancements:

- Modernises the UI.
- Updates implementation of suspense to be managed by the server.
- Reworks assets so that listings are managed by the server, and improves UI integration.
- Removes the need to JSON-encode run arguments.

Fixes:

- Switching a workflow to a sensor and vice versa.

Changes:

- Renames 'agents' to 'workers'.
- Renamed 'workspaces' (previously 'environments') to 'spaces'.

## 0.7.0

Enhancements:

- Adds support for steps to be associated with 'groups'.

Changes:

- Renames 'environments' to 'spaces'.
- Renames 'repositories' to 'modules'.

## 0.6.1

Enhancements:

- Adds further support for the experimental (and undocumented) 'pools' functionality.

Fixes:

- Handling saving blobs when the data directory is on a different device to the temporary directory (e.g., when mounting the data directory as a Docker volume).

## 0.6.0

Enhancements:

- Introduces the concept of 'spawned' runs.
- Improved sensor observability.
- Adds a search box to the UI for jumping to a workflow/task/etc.
- Instructions for workflow/sensor specified during registration are shown in the 'run' dialog.
- Repositories can be 'archived' (hidden from the sidebar until they're re-registered).
- Sorts the list of targets in the sidebar alphabetically.
- Indicates when steps in the graph are 'stale'.
- Shows caching information in the step detail panel.

## 0.5.0

Enhancements:

- Displays assets as nodes in the graph view of the UI.
- Handles updated serialisation approach.
- Adds project settings dialog to UI (supports configuring blob stores).
- Supports fetching blobs from S3 blob store in UI.

## 0.4.0

Enhancements:

- Separates registration of manifests from initialisation of agent sessions.
- Adds support for pausing an environment (no new executions will be assigned until unpaused).
- Adds support for executions to 'suspend'.
- Adds experimental support for previewing the contents of directory assets in the UI.
- Adds an initial experimental implementation for 'pools'.

## 0.3.0

Enhancements:

- Re-works environments so that results can be shared across environments, based on a hierarchy.

## 0.2.5

Fixes:

- Upgrades and pins versions of the base images used in the Docker image.

## 0.2.4

Fixes:

- Handling (file-based) repositories containing slashes in the frontend.

## 0.2.3

Fixes:

- Creating Git tag as part of the release.

## 0.2.2

Fixes:

- Handling of 'wait' arguments that aren't present (e.g., because they have default values).

## 0.2.1

Enhancements:

- Updated graph rendering in web UI, using elkjs.

Fixes:

- Reliable cancellation of recurrent (i.e., sensor) runs.
- Rendering of sensor runs page in web UI.

## 0.2.0

Enhancements:

- Supports persisting and restorig assets (files or directories) within tasks, and previewing these in the web UI.
- Supports explicitly waiting for executions in specific parameters before starting a task.

## 0.1.1

Enhancements:

- Supports configuring the data directory from an environment variable.

## 0.1.0

First public release.
