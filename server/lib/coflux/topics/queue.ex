defmodule Coflux.Topics.Queue do
  @moduledoc """
  Every execution in a workspace that hasn't completed: waiting for a
  worker, gated, or running. Keyed by execution id.
  """

  use Topical.Topic, route: ["workspaces", :workspace_id, "queue"]

  alias Coflux.Orchestration
  alias Coflux.Topics.Diff
  alias Coflux.Topics.Queue.Model

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.subscribe(project_id, {:queue, workspace_id}, self()) do
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
      Enum.reduce(dirty, topic, fn id, topic ->
        Diff.apply(topic, [id], Map.get(topic.value, id), Model.project_entry(model, id))
      end)

    {:ok, %{topic | state: %{topic.state | model: model}}}
  end
end

defmodule Coflux.Topics.Queue.Model do
  @moduledoc """
  The queue as a fold over events: a map from execution id to what the
  queue knows about it. `apply/2` is idempotent for row events, since
  detail can reach a topic by pull as well as by event.
  """

  import Kernel, except: [apply: 2]

  alias Coflux.Orchestration.Ids

  alias Coflux.Events.{
    CompletionRecorded,
    ExecutionAssigned,
    ExecutionScheduled,
    ExecutionWaiting
  }

  def new, do: %{}

  @doc "Folds events into the model, returning it with the ids whose entries may have changed."
  def fold(model, events) do
    Enum.reduce(events, {model, MapSet.new()}, fn event, {model, dirty} ->
      {model, ids} = apply(model, event)
      {model, Enum.into(ids, dirty)}
    end)
  end

  def apply(model, %ExecutionScheduled{} = e) do
    entry = %{
      module: e.module,
      target: e.target,
      run: e.run,
      step: e.step,
      attempt: e.attempt,
      execute_after: e.execute_after,
      created_at: e.created_at,
      assigned_at: nil,
      gates: [],
      requires: e.requires
    }

    {Map.put_new(model, e.execution, entry), [e.execution]}
  end

  def apply(model, %ExecutionAssigned{} = e) do
    {update(model, e.execution, &%{&1 | assigned_at: e.assigned_at}), [e.execution]}
  end

  def apply(model, %ExecutionWaiting{} = e) do
    {update(model, e.execution, &%{&1 | gates: e.gates}), [e.execution]}
  end

  def apply(model, %CompletionRecorded{} = e) do
    {Map.delete(model, e.execution), [e.execution]}
  end

  def project(model) do
    Map.new(model, fn {id, _entry} -> {id, project_entry(model, id)} end)
  end

  @doc "The wire shape of one entry, or nil if the queue no longer holds it."
  def project_entry(model, id) do
    case Map.fetch(model, id) do
      {:ok, entry} ->
        %{
          module: entry.module,
          target: entry.target,
          runId: entry.run,
          stepId: Ids.step(entry.run, entry.step),
          stepNumber: entry.step,
          attempt: entry.attempt,
          executeAfter: entry.execute_after,
          createdAt: entry.created_at,
          assignedAt: entry.assigned_at,
          dependencies: entry.gates,
          requires: entry.requires
        }

      :error ->
        nil
    end
  end

  # An assignment or gate change for an execution the queue doesn't hold
  # (already completed, say) is nothing to it.
  defp update(model, id, fun) do
    case Map.fetch(model, id) do
      {:ok, entry} -> Map.put(model, id, fun.(entry))
      :error -> model
    end
  end
end
