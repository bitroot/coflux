defmodule Coflux.Events do
  @moduledoc """
  One struct per fact the orchestration server records.

  Each event carries the fact's own fields plus its *identity*, resolved to
  external ids and never an internal id, so an event is valid across epoch
  rotation. The server emits each event once (`emit/2` in
  `Coflux.Orchestration.Server`), `Coflux.Orchestration.Routing` says which
  topic keys receive it, and each topic folds it into a model. A snapshot
  for a key (`Coflux.Orchestration.Snapshots`) is a list of the same
  structs, so a model has one code path for its initial load and its live
  updates.

  Identity fields, where an event carries them, always mean:

    * `execution` - `"<run>:<step>:<attempt>"`
    * `run` - the run's external id
    * `step` - the step number within the run
    * `attempt` - the attempt number within the step
    * `workspace` - the external id of the workspace the execution runs in
    * `module`, `target`, `type` - the step's
    * `root_module`, `root_target` - the run's initial step's, which the
      modules and workflow topics key on

  Every field is filled every time. A loader returns events exactly as the
  emit site would, so a model never needs to know where an event came from.

  Three kinds of event exist. *Row events* record an insert (or one of the
  few upserts). *State events* record a change to state that lives only in
  the server. *Derived events* record something the server computes that a
  topic could not reasonably recompute from row events it would otherwise
  need to subscribe workspace-wide for; each of those says so in its doc.
  """
end

defmodule Coflux.Events.ExecutionScheduled do
  @moduledoc "Row: `executions`. An execution was created and is waiting to run."
  defstruct [
    :execution,
    :run,
    :step,
    :attempt,
    :workspace,
    :module,
    :target,
    :type,
    :root_module,
    :root_target,
    :execute_after,
    :created_at,
    :created_by,
    :requires
  ]
end

defmodule Coflux.Events.ExecutionAssigned do
  @moduledoc "Row: `assignments`. A worker session picked the execution up."
  defstruct [
    :execution,
    :run,
    :step,
    :attempt,
    :workspace,
    :module,
    :target,
    :type,
    :root_module,
    :root_target,
    :assigned_at
  ]
end

defmodule Coflux.Events.CompletionRecorded do
  @moduledoc """
  Row: `completions`. The execution reached a terminal state. `kind` is the
  completion kind atom; `successor` describes the execution that takes over
  (a retry, a cached or deferred execution), or is nil.
  """
  defstruct [
    :execution,
    :run,
    :step,
    :attempt,
    :workspace,
    :module,
    :target,
    :type,
    :root_module,
    :root_target,
    :kind,
    :successor,
    :completed_at
  ]
end

defmodule Coflux.Events.ExecutionWaiting do
  @moduledoc """
  Derived. The gates a queued execution is still waiting on, in the form the
  queue renders: unresolved dependencies (executions, inputs, streams,
  catalog entries) and concurrency gates. Derived because the pending set
  lives only in the server's dependency ledger and the gates are decided
  by the scheduler on each tick; an empty list means the execution is free
  to run as far as gates are concerned.
  """
  defstruct [:execution, :run, :workspace, :gates]
end

defmodule Coflux.Events.ManifestRegistered do
  @moduledoc """
  Row: `workspace_manifests`. A module's manifest was registered in a
  workspace. `workflows` maps each workflow name to its definition in the
  shape the API parses, with `instruction` as content rather than an id.
  """
  defstruct [:workspace, :module, :workflows]
end

defmodule Coflux.Events.ModuleArchived do
  @moduledoc "Row: `workspace_manifests` with no manifest. The module was archived in the workspace."
  defstruct [:workspace, :module]
end

defmodule Coflux.Events.RunCreated do
  @moduledoc """
  Row: `runs`. `type` is the initial step's; `parent` is the execution that
  submitted the run as `{execution, module, target}`, or nil.
  """
  defstruct [
    :run,
    :workspace,
    :root_module,
    :root_target,
    :type,
    :created_at,
    :created_by,
    :parent,
    :requires
  ]
end

defmodule Coflux.Events.RunOutcome do
  @moduledoc """
  Derived. How the run turned out, as `Results.run_outcome/2` computes it
  from the initial execution and whatever it handed off to. Recomputed when
  the initial execution completes or is re-run, and whenever the run has
  nothing left in flight; nil while undecided.
  """
  defstruct [:run, :workspace, :root_module, :root_target, :outcome]
end

defmodule Coflux.Events.StepCreated do
  @moduledoc """
  Row: `steps`. `parent` is the external id of the execution that submitted
  the step, nil for a run's initial step. Arguments are detail, carried by
  `StepArguments`.
  """
  defstruct [
    :run,
    :step,
    :module,
    :target,
    :type,
    :parent,
    :cache_config,
    :cache_key,
    :memo_key,
    :concurrency_key,
    :concurrency_limit,
    :group_key,
    :group_limit,
    :retries,
    :recurrent,
    :timeout_ms,
    :created_at,
    :requires
  ]
end

defmodule Coflux.Events.StepArguments do
  @moduledoc "Row: `step_arguments`. The step's arguments, with values resolved."
  defstruct [:run, :step, :arguments]
end

defmodule Coflux.Events.ChildLinked do
  @moduledoc "Row: `children`. The execution `parent` submitted `step` (at `attempt`), in `group` if any."
  defstruct [:run, :parent, :step, :attempt, :group]
end

defmodule Coflux.Events.GroupCreated do
  @moduledoc "Row: `groups`."
  defstruct [:run, :execution, :group, :name, :concurrency]
end

defmodule Coflux.Events.ResultRecorded do
  @moduledoc """
  Row: `results`. `result` is the resolved result tuple. `final` says
  whether it stands (nothing retries or supersedes it), which is when an
  execution that handed off to this one shows it nested.
  """
  defstruct [:run, :execution, :result, :result_at, :created_by, :final]
end

defmodule Coflux.Events.MetricDefined do
  @moduledoc "Row: `metrics`. `definition` has the atom keys the run topic publishes."
  defstruct [:run, :execution, :key, :definition]
end

defmodule Coflux.Events.AssetPut do
  @moduledoc "Row: `execution_assets`. `summary` is `{name, total_count, total_size, entry}`."
  defstruct [:run, :execution, :asset, :summary]
end

defmodule Coflux.Events.DependenciesPending do
  @moduledoc """
  Derived. Which of the execution's dependencies it is still waiting on,
  as the set of their ids, sent whole rather than as a delta: a result that
  redirects clears a dependency keyed by the execution originally
  referenced, so there's no dependable one-to-one between what resolved
  and which entry it releases. Empty once the execution has completed.
  """
  defstruct [:run, :execution, :pending]
end

defmodule Coflux.Events.ResultDependencyRecorded do
  @moduledoc "Row: `result_dependencies` (or an argument reference). `dependency` is `{execution, module, target}`."
  defstruct [:run, :execution, :dependency, :pending]
end

defmodule Coflux.Events.StreamDependencyRecorded do
  @moduledoc "Row: `stream_dependencies`. `module` and `target` are the producing step's."
  defstruct [:run, :execution, :stream, :module, :target, :pending]
end

defmodule Coflux.Events.AssetDependencyRecorded do
  @moduledoc "Row: `asset_dependencies`. `summary` is the asset's `{name, total_count, total_size, entry}`."
  defstruct [:run, :execution, :asset, :summary, :pending]
end

defmodule Coflux.Events.CatalogPublished do
  @moduledoc "Row: `catalog_versions`. `version` is the built version, value resolved."
  defstruct [:run, :execution, :version]
end

defmodule Coflux.Events.CatalogRead do
  @moduledoc "Row: `catalog_reads`. The execution resolved `version`."
  defstruct [:run, :execution, :version]
end

defmodule Coflux.Events.CatalogWaitRecorded do
  @moduledoc "Row: `catalog_waits`. The execution waits for whatever follows `number` at `path`."
  defstruct [:run, :execution, :path, :number, :pending]
end

defmodule Coflux.Events.InputDependencyRecorded do
  @moduledoc "Row: `input_dependencies`. `title` is the input's; `response` its response type so far, or nil."
  defstruct [:run, :execution, :input, :title, :response, :pending]
end

defmodule Coflux.Events.InputSubmitted do
  @moduledoc "Row: `inputs`. The execution asked for an input."
  defstruct [:run, :execution, :input, :title]
end

defmodule Coflux.Events.InputResponded do
  @moduledoc """
  Row: `input_responses`. `response` is the built response: `type` (`:value`,
  `:dismissed` or `:cancelled`), `value`, `created_at`, `created_by`. `run`
  is the input's own run; routing also reaches the runs that depend on it.
  """
  defstruct [:run, :workspace, :input, :response]
end

defmodule Coflux.Events.CheckpointsInherited do
  @moduledoc "Derived from `checkpoints`: what the execution started from, fixed when it was created."
  defstruct [:run, :execution, :checkpoints]
end

defmodule Coflux.Events.CheckpointsSet do
  @moduledoc "Row: `checkpoints`. What the execution currently holds."
  defstruct [:run, :execution, :checkpoints]
end

defmodule Coflux.Events.StreamRegistered do
  @moduledoc """
  Row: `stream_registrations`. An execution (`attempt` of the stream's
  step) registered on the stream, opening it or resuming it after a
  suspend. `opened_at` is when the stream was first created.
  """
  defstruct [
    :run,
    :step,
    :index,
    :stream,
    :workspace,
    :module,
    :target,
    :position,
    :attempt,
    :buffer,
    :timeout_ms,
    :opened_at
  ]
end

defmodule Coflux.Events.StreamClosed do
  @moduledoc "Row: `stream_closures`. `reason` is the closure reason as a string; `error` only for `errored`."
  defstruct [:run, :step, :index, :stream, :reason, :error, :attempt, :closed_at]
end

defmodule Coflux.Events.TokenCreated do
  @moduledoc "Row: `tokens`. `token` is the external id."
  defstruct [:token, :id, :name, :workspaces, :created_at, :expires_at, :created_by]
end

defmodule Coflux.Events.TokenRevoked do
  @moduledoc "Row: `tokens`, `revoked_at` set: the one true update in the store."
  defstruct [:token]
end

defmodule Coflux.Events.SecretSet do
  @moduledoc """
  Row: `secrets` (admin store), created or replaced. Its value is not an
  event: nothing that carries this ever sees it.
  """
  defstruct [:workspaces, :name, :version, :created_at, :updated_at, :updated_by]
end

defmodule Coflux.Events.SecretDeleted do
  @moduledoc "Row: `secrets` (admin store), gone."
  defstruct [:workspaces, :name]
end

defmodule Coflux.Events.WorkspaceCreated do
  @moduledoc "Row: `workspaces`. `base` is the base workspace's external id, or nil."
  defstruct [:workspace, :name, :base, :state]
end

defmodule Coflux.Events.WorkspaceUpdated do
  @moduledoc "Rows: `workspace_names`, `workspace_bases`. The workspace's current name and base."
  defstruct [:workspace, :name, :base, :state]
end

defmodule Coflux.Events.WorkspaceStateChanged do
  @moduledoc "Row: `workspace_states`."
  defstruct [:workspace, :state]
end

defmodule Coflux.Events.SessionUpdated do
  @moduledoc """
  State. Everything the sessions topic shows about a worker session, sent
  whole when the session starts, reconnects, declares targets or drains.
  """
  defstruct [
    :workspace,
    :session,
    :connected,
    :executing,
    :concurrency,
    :pool,
    :targets,
    :provides,
    :accepts,
    :worker_state,
    :executions
  ]
end

defmodule Coflux.Events.SessionEnded do
  @moduledoc "Row: `session_expirations`. The session was removed."
  defstruct [:workspace, :session]
end

defmodule Coflux.Events.SessionConnected do
  @moduledoc "State. Whether the session's connection is up."
  defstruct [:workspace, :session, :connected]
end

defmodule Coflux.Events.SessionExecuting do
  @moduledoc "State. How many executions the session is starting or running."
  defstruct [:workspace, :session, :executing]
end

defmodule Coflux.Events.SessionExecutions do
  @moduledoc """
  State. How many executions the session has been given in total; `worker`
  and `pool` name the worker the session belongs to, if any.
  """
  defstruct [:workspace, :session, :worker, :pool, :executions]
end

defmodule Coflux.Events.PoolUpdated do
  @moduledoc "Row: `pool_definitions`. `definition` is nil when the pool was removed."
  defstruct [:workspace, :pool, :definition]
end

defmodule Coflux.Events.PoolStateChanged do
  @moduledoc "Row: `pool_states`."
  defstruct [:workspace, :pool, :state]
end

defmodule Coflux.Events.WorkerCreated do
  @moduledoc "Row: `workers`. `session` is the external id of the session created for it."
  defstruct [:workspace, :pool, :worker, :created_at, :session]
end

defmodule Coflux.Events.WorkerLaunchResult do
  @moduledoc "Row: `worker_launch_results`."
  defstruct [:workspace, :pool, :worker, :started_at, :error]
end

defmodule Coflux.Events.WorkerStopping do
  @moduledoc """
  Row: `worker_stops`. One attempt to stop the worker - over its own
  connection, or through its launcher. A worker can have several.
  """
  defstruct [:workspace, :pool, :worker, :stopping_at]
end

defmodule Coflux.Events.WorkerStopResult do
  @moduledoc """
  Row: `worker_stop_results`. How the attempt went: an error if the
  launcher refused, nil if the request was made. Neither means the worker
  has gone - `WorkerDeactivated` says that.
  """
  defstruct [:workspace, :pool, :worker, :completed_at, :error]
end

defmodule Coflux.Events.WorkerDeactivated do
  @moduledoc "Row: `worker_deactivations`."
  defstruct [:workspace, :pool, :worker, :deactivated_at, :error, :logs]
end

defmodule Coflux.Events.WorkerStateChanged do
  @moduledoc "Row: `worker_states`. Nil once the worker is no longer tracked."
  defstruct [:workspace, :pool, :worker, :state]
end

defmodule Coflux.Events.InputActivated do
  @moduledoc """
  Derived. An unanswered input has an execution waiting on it. `run` is the
  input's own run. Derived because activity follows the dependents'
  completions, which the orchestrator tracks for its own waiting logic.
  """
  defstruct [:workspace, :input, :run, :created_at, :title, :requires]
end

defmodule Coflux.Events.InputDeactivated do
  @moduledoc "Derived. No execution is waiting on the input any more."
  defstruct [:workspace, :input]
end

defmodule Coflux.Events.InputDetails do
  @moduledoc """
  Row: `inputs` (with its prompt and schema). Only a snapshot carries it:
  an input can't be subscribed to before it exists, so it is never emitted
  live.
  """
  defstruct [
    :workspace,
    :input,
    :key,
    :template,
    :placeholders,
    :schema,
    :initial,
    :title,
    :actions,
    :requires,
    :created_at
  ]
end

defmodule Coflux.Events.StreamItemAppended do
  @moduledoc "Row: `stream_items`. `value` is resolved."
  defstruct [:stream, :sequence, :value, :attempt, :created_at]
end
