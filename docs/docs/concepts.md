# Concepts

This page outlines the main concepts in Coflux.

## Projects

A Coflux _server_ can host multiple _projects_. Data for each project is isolated from other projects, and orchestration is handled by a dedicated process for each project.

You should use a separate project when:

1. Data needs to be kept separate for reasons of security or privacy.
2. Throughput is
3. There's a logical separation of concerns.

## Spaces

A individual project can contain multiple spaces (workspaces). All spaces within a projects are controlled by the same orchestration process, and some level of separation is provided between spaces, but space inheritance allows controlled data sharing. Spaces might be mapped to deployment environments (e.g., production, staging, development), or separated further - for example a space per customer in a production environment, or a space per developer in a development environment. Or even more granular separation is possible - for example using temporary spaces which correspond with a Git branch, to work on fixing a bug or building a new feature.

### Space inheritance

By default there is isolation between spaces within a project - for example, workflows, runs, results are separated. But spaces can be arranged into a hierarchy. This allows:

1. Cached (or memoised) results to be inherited from parent spaces.
2. Steps to be _re-run_ in a 'child' spaces.

For example, a `development` space can inherit from a `production` space, allowing you to re-run whole workflows, or specific steps within a workflow, in a development space, experimenting with changes to the code without having to re-run the whole workflow from scratch. When working with a team on a shared project, you might choose to set up separate space for each engineer, or even create spaces temporarily to work on specific features.

This makes it easier to diagnose issues that arise in a production space by retrying individual steps locally, and trying out code changes safely.

## Workers

An _worker_ is a process that hosts _modules_ (collections workflows/tasks). An worker connects to the server and is associated with a specific project and space. The worker waits for commands from the server telling it to execute specific tasks, and the worker monitors and reports progress of these executions back to the server.

This model of having workers connect to the server provides flexibility over where and how workers are run. During development a worker can run locally on a laptop, restarting automatically as code changes are made. Or multiple workers can run in the cloud, or on dedicated machines - or a combination. An worker can be started with specific environment variables associated with the deployment environment (e.g., production access keys).

## Workflows

A _workflow_ is defined in a module, in code. Additionally, _tasks_ can be defined, and called from workflows (or other tasks).

Workflows and tasks are collectively referred to as _targets_, although workflows are really just special forms of tasks, from which runs can be started. You can think of the distinction between workflows and tasks a bit like the distinction between public and private functions in a module.

Workflows need to be registered with a project and space so that they appear in the UI. This can be done explicitly (e.g., for a production space as part of a build process), or automatically by a worker when it starts/restarts (using the `--register` or `--dev` flag).

## Runs

When a workflow is submitted, this initiates a _run_. A run is made up of _steps_, which each correspond to a target to be executed. The target (a workflow or task) can call other tasks, which cause those to scheduled as steps. Each step has at least one associated _execution_. Steps can be retried (manually or automatically), which will lead to multiple executions being associated with the step.

# Assets

Executions can persist _assets_ (a collection of files) which can be passed between executions and restored as needed, or viewed in the UI.
