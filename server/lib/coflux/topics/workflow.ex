defmodule Coflux.Topics.Workflow do
  @moduledoc """
  One workflow in a workspace: its latest definition and its most recent
  runs, each with its outcome and whether it is still in flight.
  """

  use Topical.Topic,
    route: ["workspaces", :workspace_id, "workflows", :module, :target]

  alias Coflux.Orchestration
  alias Coflux.Topics.Diff
  alias Coflux.Topics.Workflow.Model

  @max_runs 50

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    module = Map.fetch!(params, :module)
    target = Map.fetch!(params, :target)
    workspace_id = Map.fetch!(params, :workspace_id)
    key = {:workflow, module, target, workspace_id}

    case Orchestration.subscribe(project_id, key, self(), max_runs: @max_runs) do
      {:ok, events, ref} ->
        {model, _dirty} = Model.fold(Model.new(module, target, @max_runs), events)
        {:ok, Topic.new(Model.project(model), %{model: model, ref: ref})}

      {:error, :not_found} ->
        {:error, :not_found}

      {:error, :workspace_invalid} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, events}, topic) do
    {model, _dirty} = Model.fold(topic.state.model, events)
    topic = Diff.apply(topic, [], topic.value, Model.project(model))
    {:ok, %{topic | state: %{topic.state | model: model}}}
  end
end

defmodule Coflux.Topics.Workflow.Model do
  @moduledoc """
  The workflow topic as a fold over events: the workflow's definition from
  the module's latest manifest, its `max_runs` most recent runs, and the
  in-flight executions of each run.
  """

  import Kernel, except: [apply: 2]
  import Coflux.TopicUtils, only: [build_principal: 1]

  alias Coflux.Events.{
    CompletionRecorded,
    ExecutionAssigned,
    ExecutionScheduled,
    ManifestRegistered,
    ModuleArchived,
    RunCreated,
    RunOutcome
  }

  def new(module, target, max_runs) do
    %{module: module, target: target, max_runs: max_runs, workflow: nil, runs: %{}, active: %{}}
  end

  def fold(model, events) do
    Enum.reduce(events, {model, false}, fn event, {model, _} -> {apply(model, event), true} end)
  end

  # A manifest that no longer names the workflow leaves its last definition.
  def apply(model, %ManifestRegistered{} = e) do
    case Map.fetch(e.workflows, model.target) do
      {:ok, workflow} -> %{model | workflow: workflow}
      :error -> model
    end
  end

  def apply(model, %ModuleArchived{}), do: %{model | workflow: nil}

  def apply(model, %RunCreated{type: :workflow} = e) do
    runs =
      Map.put_new(model.runs, e.run, %{
        created_at: e.created_at,
        created_by: e.created_by,
        outcome: nil
      })

    runs =
      if map_size(runs) > model.max_runs do
        {oldest, _} = Enum.min_by(runs, fn {_id, run} -> run.created_at end)
        Map.delete(runs, oldest)
      else
        runs
      end

    %{model | runs: runs}
  end

  def apply(model, %RunCreated{}), do: model

  # Runs that have been evicted from the list still get their outcome, and
  # have nothing to update.
  def apply(model, %RunOutcome{} = e) do
    case Map.fetch(model.runs, e.run) do
      {:ok, run} -> put_in(model, [:runs, e.run], %{run | outcome: e.outcome})
      :error -> model
    end
  end

  def apply(model, %ExecutionScheduled{} = e) do
    update_active(model, e.run, &Map.put_new(&1, e.execution, false))
  end

  def apply(model, %ExecutionAssigned{} = e) do
    update_active(model, e.run, &Map.put(&1, e.execution, true))
  end

  def apply(model, %CompletionRecorded{} = e) do
    update_active(model, e.run, &Map.delete(&1, e.execution))
  end

  defp update_active(model, run, fun) do
    case fun.(Map.get(model.active, run, %{})) do
      executions when map_size(executions) == 0 ->
        %{model | active: Map.delete(model.active, run)}

      executions ->
        %{model | active: Map.put(model.active, run, executions)}
    end
  end

  def project(model) do
    workflow = model.workflow

    %{
      parameters: if(workflow, do: build_parameters(workflow.parameters)),
      instruction: if(workflow, do: workflow.instruction),
      configuration: build_configuration(workflow),
      runs:
        Map.new(model.runs, fn {id, run} ->
          {id,
           %{
             id: id,
             createdAt: run.created_at,
             createdBy: build_principal(run.created_by),
             outcome: build_outcome(run.outcome),
             active: build_active(Map.get(model.active, id))
           }}
        end)
    }
  end

  # A run is "running" once any of its in-flight executions has been
  # assigned to a worker, and "queued" while they're all still waiting for
  # one.
  defp build_active(nil), do: nil

  defp build_active(executions) do
    if Enum.any?(executions, fn {_, assigned?} -> assigned? end), do: "running", else: "queued"
  end

  defp build_outcome(nil), do: nil
  defp build_outcome(outcome), do: Atom.to_string(outcome)

  defp build_parameters(parameters) do
    Enum.map(parameters, fn {name, default, annotation} ->
      %{name: name, default: default, annotation: annotation}
    end)
  end

  defp build_cache_configuration(cache) do
    if cache do
      %{
        params: cache.params,
        maxAgeMs: cache.max_age_ms,
        namespace: cache.namespace,
        version: cache.version
      }
    end
  end

  defp build_defer_configuration(defer) do
    if defer do
      %{params: defer.params}
    end
  end

  defp build_retries_configuration(retries) do
    if retries do
      %{
        limit: retries.limit,
        backoffMinMs: retries.backoff_min_ms,
        backoffMaxMs: retries.backoff_max_ms
      }
    end
  end

  defp build_configuration(workflow) do
    if workflow do
      %{
        waitFor: workflow.wait_for,
        cache: build_cache_configuration(workflow.cache),
        defer: build_defer_configuration(workflow.defer),
        delayMs: workflow.delay_ms,
        retries: build_retries_configuration(workflow.retries),
        recurrent: workflow.recurrent,
        timeoutMs: workflow.timeout_ms,
        requires: workflow.requires,
        memo: workflow.memo,
        streams: build_streams_configuration(workflow[:streams]),
        concurrency: build_concurrency_configuration(workflow[:concurrency])
      }
    end
  end

  defp build_concurrency_configuration(nil), do: nil

  defp build_concurrency_configuration(concurrency) do
    %{
      limit: concurrency.limit,
      params: concurrency.params,
      namespace: concurrency.namespace
    }
  end

  defp build_streams_configuration(nil), do: nil

  defp build_streams_configuration(streams) do
    %{
      buffer: streams[:buffer],
      timeoutMs: streams[:timeout_ms]
    }
  end
end
