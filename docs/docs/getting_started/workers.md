# 4. Starting workers

Modules are hosted by _workers_. A worker is a process that connects to the server and executes the code required by your workflows. Each worker can have its own package dependencies, and be deployed within your infrastructure as needed — for example, one worker could run on an on-premise server with a GPU, while another runs as a Docker image on an auto-scaling cloud cluster.

A worker will:

1. Listen for commands from the server.
2. Execute operations in isolated sub-processes.
3. Report the status (including results, errors, etc.) of executions back to the server.

Importantly, workers can be run locally, automatically watching for code changes, restarting, and registering workflows as needed.

## Set up

Use the `setup` command to populate a configuration file (`coflux.toml`). A configuration file isn't necessary, but avoids having to specify configuration manually in the following commands. Run the following command:

```bash
coflux setup
```

You will be prompted to enter the host (`localhost:7777`), the _workspace_ name, and the adapter command for your Python environment. A workspace is an environment within a project (e.g., `production`, `development/joe`) — see [Concepts](/concepts#workspaces) for more detail. Use `--detect` to auto-detect your Python environment.

## Run

Now the worker can be started. Run the following command:

```bash
coflux worker --dev hello
```

The `--dev` flag (equivalent to specifying `--watch` and `--register`) enables development mode, which watches for code changes, automatically restarts the worker, and registers workflows with the server. Without it, modules need to be registered separately (e.g., using `coflux manifests register`), and the worker would need to be restarted after making code changes.

Next, let's submit a run...
