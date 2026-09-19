defmodule Coflux.Orchestration.Routing do
  @moduledoc """
  Which topic keys receive each event. The only place that knows which
  topics care about which facts: an emit site records a fact, a topic folds
  what reaches its key, and this table joins the two.

  Routing may consult server state for reverse lookups (the runs whose
  executions depend on an input, the workspaces a catalog version is
  visible from, the workflow topics open on a module), and nothing else
  runs a subscriber's code in the server.
  """

  alias Coflux.Events.{
    AssetDependencyRecorded,
    AssetPut,
    CatalogPublished,
    CatalogRead,
    CatalogWaitRecorded,
    CheckpointsInherited,
    CheckpointsSet,
    ChildLinked,
    CompletionRecorded,
    DependenciesPending,
    ExecutionAssigned,
    ExecutionScheduled,
    ExecutionWaiting,
    GroupCreated,
    InputActivated,
    InputDeactivated,
    InputDependencyRecorded,
    InputDetails,
    InputResponded,
    InputSubmitted,
    ManifestRegistered,
    MetricDefined,
    ModuleArchived,
    PoolStateChanged,
    PoolUpdated,
    ResultDependencyRecorded,
    ResultRecorded,
    RunCreated,
    RunOutcome,
    SessionConnected,
    SessionEnded,
    SessionExecuting,
    SessionExecutions,
    SessionUpdated,
    StepArguments,
    StepCreated,
    StreamClosed,
    StreamDependencyRecorded,
    StreamItemAppended,
    StreamRegistered,
    TokenCreated,
    TokenRevoked,
    WorkerCreated,
    WorkerDeactivated,
    WorkerLaunchResult,
    WorkerStateChanged,
    WorkerStopping,
    WorkerStopResult,
    WorkspaceCreated,
    WorkspaceStateChanged,
    WorkspaceUpdated
  }

  alias Coflux.Orchestration.{Ids, Inputs, Runs}

  # ---------------------------------------------------------------------------
  # Executions and runs

  def route(%ExecutionScheduled{} = e, _state),
    do: [
      {:run, e.run},
      {:queue, e.workspace},
      {:modules, e.workspace},
      workflow(e),
      {:targets, e.workspace}
    ]

  def route(%ExecutionAssigned{} = e, _state),
    do: [{:run, e.run}, {:queue, e.workspace}, {:modules, e.workspace}, workflow(e)]

  def route(%CompletionRecorded{} = e, _state),
    do: [{:run, e.run}, {:queue, e.workspace}, {:modules, e.workspace}, workflow(e)]

  def route(%ExecutionWaiting{} = e, _state), do: [{:queue, e.workspace}]
  def route(%RunCreated{} = e, _state), do: [{:run, e.run}, workflow(e)]
  def route(%RunOutcome{} = e, _state), do: [workflow(e)]

  # A result is shown nested in every execution that handed off to this
  # one, transitively, and those may be in other runs. Resolved from `db`
  # rather than `state.execution_ids`: that cache holds only what this
  # server lifetime scheduled or assigned, so a queued execution another
  # run deferred onto would be missing from it after a restart.
  def route(%ResultRecorded{} = e, state) do
    runs =
      with {:ok, run, step, attempt} <- Ids.parse_execution(e.execution),
           {:ok, {execution_id}} when not is_nil(execution_id) <-
             Runs.get_execution_id(state.db, run, step, attempt),
           {:ok, rows} <- Runs.get_result_successors(state.db, execution_id) do
        Enum.map(rows, &elem(&1, 0))
      else
        _ -> []
      end

    Enum.uniq(Enum.map([e.run | runs], &{:run, &1}))
  end

  def route(%StepCreated{} = e, _state), do: [{:run, e.run}]
  def route(%StepArguments{} = e, _state), do: [{:run, e.run}]
  def route(%ChildLinked{} = e, _state), do: [{:run, e.run}]
  def route(%GroupCreated{} = e, _state), do: [{:run, e.run}]
  def route(%MetricDefined{} = e, _state), do: [{:run, e.run}]
  def route(%AssetPut{} = e, _state), do: [{:run, e.run}]
  def route(%DependenciesPending{} = e, _state), do: [{:run, e.run}]
  def route(%ResultDependencyRecorded{} = e, _state), do: [{:run, e.run}]
  def route(%StreamDependencyRecorded{} = e, _state), do: [{:run, e.run}]
  def route(%AssetDependencyRecorded{} = e, _state), do: [{:run, e.run}]
  def route(%CatalogRead{} = e, _state), do: [{:run, e.run}]
  def route(%CatalogWaitRecorded{} = e, _state), do: [{:run, e.run}]
  def route(%InputDependencyRecorded{} = e, _state), do: [{:run, e.run}]
  def route(%InputSubmitted{} = e, _state), do: [{:run, e.run}]
  def route(%CheckpointsInherited{} = e, _state), do: [{:run, e.run}]
  def route(%CheckpointsSet{} = e, _state), do: [{:run, e.run}]

  # ---------------------------------------------------------------------------
  # Streams

  def route(%StreamRegistered{} = e, _state), do: [{:run, e.run}, {:stream, e.stream}]
  def route(%StreamClosed{} = e, _state), do: [{:run, e.run}, {:stream, e.stream}]
  def route(%StreamItemAppended{} = e, _state), do: [{:stream, e.stream}]

  # ---------------------------------------------------------------------------
  # Inputs

  # The input's own run, every run with an execution depending on it, and
  # the input topics.
  def route(%InputResponded{} = e, state) do
    dependents =
      with {:ok, run, number} <- Ids.parse_input(e.input),
           {:ok, input_id} <- Inputs.get_input_id_by_run_and_number(state.db, run, number),
           {:ok, rows} <- Inputs.get_dependent_run_external_ids(state.db, input_id) do
        Enum.map(rows, &elem(&1, 0))
      else
        _ -> []
      end

    Enum.uniq(Enum.map([e.run | dependents], &{:run, &1})) ++
      [{:inputs, e.workspace}, {:input, e.input}]
  end

  def route(%InputActivated{} = e, _state), do: [{:inputs, e.workspace}, {:input, e.input}]
  def route(%InputDeactivated{} = e, _state), do: [{:inputs, e.workspace}, {:input, e.input}]
  def route(%InputDetails{} = e, _state), do: [{:input, e.input}]

  # ---------------------------------------------------------------------------
  # Catalog

  # The publishing run, and the catalog of every workspace the version is
  # visible from: the one it was published in and every workspace based on
  # it, transitively.
  def route(%CatalogPublished{} = e, state) do
    catalogs =
      case Map.fetch(state.workspace_external_ids, e.version.workspace_id) do
        {:ok, published_in} ->
          state.workspaces
          |> Enum.filter(fn {workspace_id, _} -> published_in in chain(state, workspace_id) end)
          |> Enum.map(fn {_, workspace} -> {:catalog, workspace.external_id} end)

        :error ->
          []
      end

    if(e.run, do: [{:run, e.run}], else: []) ++ catalogs
  end

  # ---------------------------------------------------------------------------
  # Manifests and modules

  def route(%ManifestRegistered{} = e, _state) do
    [
      {:modules, e.workspace},
      {:manifests, e.workspace},
      {:targets, e.workspace}
      | Enum.map(Map.keys(e.workflows), &{:workflow, e.module, &1, e.workspace})
    ]
  end

  # Every workflow topic open on the module loses its definition. The keys
  # come from the listener index, which is the only place they are known.
  def route(%ModuleArchived{} = e, state) do
    workflows =
      state.topics
      |> Map.keys()
      |> Enum.filter(
        &match?(
          {:workflow, module, _target, workspace}
          when module == e.module and workspace == e.workspace,
          &1
        )
      )

    [{:modules, e.workspace}, {:manifests, e.workspace}, {:targets, e.workspace} | workflows]
  end

  # ---------------------------------------------------------------------------
  # Sessions, pools and workers

  def route(%SessionUpdated{} = e, _state), do: [{:sessions, e.workspace}]
  def route(%SessionEnded{} = e, _state), do: [{:sessions, e.workspace}]
  def route(%SessionConnected{} = e, _state), do: [{:sessions, e.workspace}]
  def route(%SessionExecuting{} = e, _state), do: [{:sessions, e.workspace}]

  def route(%SessionExecutions{} = e, _state) do
    [{:sessions, e.workspace} | if(e.worker, do: [{:pool, e.workspace, e.pool}], else: [])]
  end

  def route(%PoolUpdated{} = e, _state), do: [{:pools, e.workspace}, {:pool, e.workspace, e.pool}]

  def route(%PoolStateChanged{} = e, _state),
    do: [{:pools, e.workspace}, {:pool, e.workspace, e.pool}]

  def route(%WorkerCreated{} = e, _state), do: [{:pool, e.workspace, e.pool}]
  def route(%WorkerLaunchResult{} = e, _state), do: [{:pool, e.workspace, e.pool}]
  def route(%WorkerStopping{} = e, _state), do: [{:pool, e.workspace, e.pool}]
  def route(%WorkerStopResult{} = e, _state), do: [{:pool, e.workspace, e.pool}]
  def route(%WorkerDeactivated{} = e, _state), do: [{:pool, e.workspace, e.pool}]
  def route(%WorkerStateChanged{} = e, _state), do: [{:pool, e.workspace, e.pool}]

  # ---------------------------------------------------------------------------
  # Workspaces and tokens

  def route(%WorkspaceCreated{}, _state), do: [:workspaces]
  def route(%WorkspaceUpdated{}, _state), do: [:workspaces]
  def route(%WorkspaceStateChanged{}, _state), do: [:workspaces]
  def route(%TokenCreated{}, _state), do: [:tokens]
  def route(%TokenRevoked{}, _state), do: [:tokens]

  # ---------------------------------------------------------------------------

  defp workflow(e), do: {:workflow, e.root_module, e.root_target, e.workspace}

  # The workspace and its bases, nearest first.
  defp chain(state, workspace_id) do
    case Map.fetch(state.workspaces, workspace_id) do
      {:ok, %{base_id: nil}} -> [workspace_id]
      {:ok, %{base_id: base_id}} -> [workspace_id | chain(state, base_id)]
      :error -> []
    end
  end
end
