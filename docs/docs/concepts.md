# Concepts

This page outlines the main concepts in Coflux.

## Projects

A Coflux _server_ can host multiple _projects_. Data for each project is isolated from other projects, and orchestration is handled by a dedicated process for each project.

Consider using separate projects when:

1. There's a logical separation of concerns.
2. Data needs to be kept separate for compliance reasons.
3. To improve throughput (i.e., by partitioning).

## Workspaces

An individual project can contain multiple workspaces. All workspaces within a project are controlled by the same orchestration process, and some level of separation is provided between workspaces, but workspace inheritance allows controlled sharing of data.

### Workspace inheritance

By default there is isolation between workspaces within a project — for example, workflows, runs, and results are separated. But workspaces can be arranged into a hierarchy. This allows:

1. Cached (or memoized) results can be inherited from parent workspaces.
2. Steps can be _re-run_ in child workspaces.

For example, a `development` workspace could inherit from a `production` workspace, allowing you to re-run whole workflows, or specific steps within a run, in a development workspace. This lets you experiment with changes to the code using real data without having to re-run the whole workflow from scratch. When working with a team on a shared project, separate workspaces can be used by each engineer. Or separate short-lived workspaces can be used to work on individual features, or investigate bugs.

## Workers

A _worker_ is a process that hosts _modules_ (collections of workflows/tasks). A worker connects to the server and is associated with a specific project and workspace. The worker waits for commands from the server telling it to execute specific tasks, and reports progress of these executions back to the server.

This model of having workers connect to the server provides flexibility over where and how workers are run. During development a worker can run locally on a laptop, restarting automatically as code changes are made. Or multiple workers can run in the cloud, or on dedicated machines — or a combination. A worker can be started with specific environment variables associated with the deployment environment (e.g., production access keys).

Each execution is run in an isolated process. The worker creates a number of 'warm' executor processes (with the module code loaded) ready to handle executions, so executions start in milliseconds.

## Workflows

A _workflow_ is defined in a module, in code. Additionally, _tasks_ can be defined, and called from workflows (or other tasks).

Workflows and tasks are collectively referred to as _targets_, although workflows are really just special forms of tasks, from which runs can be started. You can think of the distinction between workflows and tasks like the distinction between public and private functions in a module.

Workflows need to be registered with a project and workspace so that they appear in Studio. This can be done explicitly (e.g., for a production workspace as part of a build process), or automatically by a worker when it starts/restarts (using the `--register` or `--dev` flag).

## Runs

When a workflow is submitted, this initiates a _run_. A run is made up of _steps_, which each correspond to a target to be executed. The target (a workflow or task) can call other tasks, which cause those to be scheduled as steps. Each step has at least one associated _execution_. Steps can be retried (manually or automatically), which will lead to multiple executions being associated with the step.

## Inputs

Tasks can request _input_ from a user mid-execution - for example, an approval before a deployment, or a structured form to fill in. Prompts are responded to from Studio (or the CLI), and the execution resumes once a response is provided. See [inputs](./inputs.md).

## Assets

Executions can persist _assets_ (a collection of files) which can be passed between executions and restored as needed, or viewed in Studio.
