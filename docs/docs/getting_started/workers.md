# 4. Starting workers

Modules are hosted by _workers_ - each worker can have its own package dependencies, and be deployed within your infrastructure as needed - for example one worker could be deployed on an on-premise bare-metal server with a GPU, and another worker could be deployed as a Docker image on an auto-scaling cloud cluster.

An worker is a process that's responsible for executing the code required by your workflow - it will:

1. Listen for commands from the orchestrator.
2. Invoke and monitor executions of operations (in forked sub-processes).
3. Report the status (including results, errors, etc.) of executions back to the orchestrator.

Importantly, they can be run locally, automatically watching for code changes, restarting, and registering workflows as needed.

## Initialise

Use the `configure` command to populate a configuration file. A configuration file isn't necessary, but avoids having to specify configuration manually in the following commands. Run the following command:

```bash
coflux configure
```

You will be prompted to enter the host (`localhost:7777`), the project ID, and the workspace name.

## Run

Now the worker can be started. Run the following command:

```bash
coflux worker --dev hello.py
```

In the web UI you will be able to see your workflow appear in the sidebar.

Next, let's initiate a run...
