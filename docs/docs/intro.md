---
slug: '/'
---

# Introduction

Coflux is an open-source workflow engine for orchestrating and observing computational workflows defined in plain Python. You define workflows using simple decorators — no DSLs, no YAML, no DAG definitions — and Coflux handles scheduling, retries, caching, and observability.

You can use it to build data pipelines, coordinate background tasks, or orchestrate real-time workflows.

### Why Coflux?

- **Plain Python**: Workflows are regular Python functions with decorators. They remain callable outside of Coflux, making them easy to test and debug.
- **Self-hosted**: You run the server. Your data stays in your infrastructure.
- **Workspace inheritance**: Branch your production environment into development workspaces, re-run specific steps with real data, and experiment without affecting production.
- **Live observation**: Studio provides a web UI for exploring run graphs, timelines, logs, and results in real time.

## Getting started

The following guide walks you through defining and running your first workflow:

1. [Installing the CLI](./getting_started/install.md)
2. [Starting the server](./getting_started/server.md)
3. [Defining a workflow](./getting_started/workflows.md)
4. [Running a worker](./getting_started/workers.md)
5. [Submitting a run](./getting_started/runs.md)
6. [Connecting from Studio](./getting_started/studio.md) (optional)
