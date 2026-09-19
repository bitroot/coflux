defmodule Coflux.Topics.Modules do
  @moduledoc """
  The modules registered in a workspace, each with its workflows and the
  runs of each workflow that are still in flight.
  """

  use Topical.Topic, route: ["workspaces", :workspace_id, "modules"]

  alias Coflux.Orchestration
  alias Coflux.Topics.Diff
  alias Coflux.Topics.Modules.Model

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.subscribe(project_id, {:modules, workspace_id}, self()) do
      {:ok, events, ref} ->
        {model, _dirty} = Model.fold(Model.new(), events)
        {:ok, Topic.new(Model.project(model), %{model: model, ref: ref})}

      {:error, :workspace_invalid} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, events}, topic) do
    {model, dirty} = Model.fold(topic.state.model, events)

    topic =
      Enum.reduce(dirty, topic, fn module, topic ->
        Diff.apply(
          topic,
          [module],
          Map.get(topic.value, module),
          Model.project_module(model, module)
        )
      end)

    {:ok, %{topic | state: %{topic.state | model: model}}}
  end
end

defmodule Coflux.Topics.Modules.Model do
  @moduledoc """
  The modules topic as a fold over events: the workflow names of each
  module's latest manifest, and per workflow the executions of its runs
  that are in flight. A run is "running" once any of its in-flight
  executions has been assigned to a worker, and "queued" while they're all
  still waiting for one.

  Only the names are kept: nothing here shows a workflow's definition, and
  a manifest carries every parameter and instruction of every workflow.
  """

  import Kernel, except: [apply: 2]

  alias Coflux.Events.{
    CompletionRecorded,
    ExecutionAssigned,
    ExecutionScheduled,
    ManifestRegistered,
    ModuleArchived
  }

  def new, do: %{manifests: %{}, active: %{}}

  @doc "Folds events into the model, returning it with the modules whose entries may have changed."
  def fold(model, events) do
    Enum.reduce(events, {model, MapSet.new()}, fn event, {model, dirty} ->
      {model, modules} = apply(model, event)
      {model, Enum.into(modules, dirty)}
    end)
  end

  def apply(model, %ManifestRegistered{} = e) do
    {put_in(model, [:manifests, e.module], Map.keys(e.workflows)), [e.module]}
  end

  def apply(model, %ModuleArchived{} = e) do
    {update_in(model.manifests, &Map.delete(&1, e.module)), [e.module]}
  end

  def apply(model, %ExecutionScheduled{} = e) do
    # Put new, so that a scheduled fact can never undo an assigned one.
    model =
      update_active(model, e, fn runs ->
        Map.update(runs, e.run, %{e.execution => false}, &Map.put_new(&1, e.execution, false))
      end)

    {model, [e.root_module]}
  end

  def apply(model, %ExecutionAssigned{} = e) do
    model =
      update_active(model, e, fn runs ->
        Map.update(runs, e.run, %{e.execution => true}, &Map.put(&1, e.execution, true))
      end)

    {model, [e.root_module]}
  end

  def apply(model, %CompletionRecorded{} = e) do
    model =
      update_active(model, e, fn runs ->
        case Map.fetch(runs, e.run) do
          {:ok, executions} ->
            case Map.delete(executions, e.execution) do
              remaining when map_size(remaining) == 0 -> Map.delete(runs, e.run)
              remaining -> Map.put(runs, e.run, remaining)
            end

          :error ->
            runs
        end
      end)

    {model, [e.root_module]}
  end

  # A workflow with nothing in flight has no entry, as a snapshot would
  # give it none.
  defp update_active(model, e, fun) do
    key = {e.root_module, e.root_target}

    case fun.(Map.get(model.active, key, %{})) do
      runs when map_size(runs) == 0 -> %{model | active: Map.delete(model.active, key)}
      runs -> %{model | active: Map.put(model.active, key, runs)}
    end
  end

  def project(model) do
    Map.new(model.manifests, fn {module, _targets} ->
      {module, project_module(model, module)}
    end)
  end

  @doc "The wire shape of one module, or nil if no manifest is registered for it."
  def project_module(model, module) do
    case Map.fetch(model.manifests, module) do
      {:ok, targets} ->
        %{
          workflows:
            Map.new(targets, fn target ->
              {target, %{activeRuns: activity(Map.get(model.active, {module, target}, %{}))}}
            end)
        }

      :error ->
        nil
    end
  end

  defp activity(runs) do
    Map.new(runs, fn {run, executions} ->
      {run,
       if(Enum.any?(executions, fn {_, assigned?} -> assigned? end),
         do: "running",
         else: "queued"
       )}
    end)
  end
end
