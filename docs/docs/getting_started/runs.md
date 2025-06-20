# 5. Submitting runs

We've defined our workflow, started the Coflux server, and started a worker. The final step is to submit a run of our workflow.

## Using the web UI

We can do this in the web UI:

1. Select the `print_greeting` workflow in the sidebar.
2. Click the 'Run...' button.
3. Enter a name (it must be _JSON-encoded_ - e.g., `"Joe"`, in quotes).
4. Click 'Run'.

In the web UI, you'll see the run graph appear as the run executes.

### Exploring the run

From the graph you can see the relationship between steps. You can also switch to _timeline_ and _logs_ views. And select steps to see details related to the specific step. You should be able to find the result from the `build_greeting` step, and this result being logged by the `print_greeting` step.

## Using the CLI

You can also submit runs using the CLI:

```bash
coflux submit hello.py print_greeting '"Joe"'
```

(Note the need to triple-quote the argument.)

## Next steps

Congratulations on defining and starting your first run. Continue with the documentation or try defining another workflow.
