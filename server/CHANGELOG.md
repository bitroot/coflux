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
