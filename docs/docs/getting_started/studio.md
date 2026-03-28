# 6. Connecting from Studio

Coflux Studio provides a web UI for monitoring and exploring workflow runs, managing access to projects, and sharing them within a team. It connects to your self-hosted Coflux server at runtime — your data stays in your infrastructure.

## Connecting

1. Visit [studio.coflux.com](https://studio.coflux.com).
2. Create a project, entering your server address (`localhost:7777`).
3. You should see your project and workspace, along with the workflow you registered.

## Exploring a run

Select the `print_greeting` workflow in the sidebar and find the run you submitted. From here you can:

- View the **run graph** showing the relationship between steps.
- Switch to **timeline** and **logs** views.
- Select individual steps to see their details, results, and logs.
- Start new runs.

You should be able to find the result from the `build_greeting` step, and this result being logged by the `print_greeting` step.

:::note
Studio can be used without creating an account. Creating an account allows you to use Studio for authentication and to share access to projects with your team.
:::

## Next steps

Now that you have a workflow running, here are some areas to explore:

- [Concurrency](/concurrency) — run tasks in parallel using `.submit()`.
- [Caching](/caching) — avoid re-computing results that haven't changed.
- [Automatic retries](/retries) — handle transient failures gracefully.
- [Assets](/assets) — persist and share files between tasks.
- [Concepts](/concepts) — a deeper look at projects, workspaces, and workspace inheritance.
