# 5. Submitting runs

We've defined our workflow, started the Coflux server, and started a worker. The final step is to submit a run of our workflow.

## Using the CLI

Submit a run using the CLI:

```bash
coflux submit hello/print_greeting '"Joe"'
```

The target is specified in the format `module/target`. Arguments are passed as JSON strings (note the need to quote the argument).

By default, the command waits for the workflow to complete, showing a live-updating tree of step statuses. Use `--no-wait` to submit and exit immediately.

You can also inspect runs and retrieve results:

```bash
coflux runs inspect <run-id>
coflux runs result <run-id>
coflux logs <run-id>
```

Next, we can connect from Studio to explore runs in a browser...
