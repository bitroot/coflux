defmodule Coflux.Topics.Inputs do
  @moduledoc "The inputs of a workspace that an execution is waiting on, keyed by input id."

  use Topical.Topic, route: ["workspaces", :workspace_id, "inputs"]

  alias Coflux.Orchestration
  alias Coflux.Topics.Diff
  alias Coflux.Topics.Inputs.Model

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.subscribe(project_id, {:inputs, workspace_id}, self()) do
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

defmodule Coflux.Topics.Inputs.Model do
  @moduledoc false

  import Kernel, except: [apply: 2]

  alias Coflux.Events.{InputActivated, InputDeactivated, InputResponded}

  def new, do: %{}

  def fold(model, events) do
    Enum.reduce(events, {model, MapSet.new()}, fn event, {model, dirty} ->
      {model, keys} = apply(model, event)
      {model, Enum.into(keys, dirty)}
    end)
  end

  def apply(model, %InputActivated{} = e) do
    input = %{run: e.run, created_at: e.created_at, title: e.title, requires: e.requires}
    {Map.put(model, e.input, input), [e.input]}
  end

  def apply(model, %InputDeactivated{} = e), do: {Map.delete(model, e.input), [e.input]}
  def apply(model, %InputResponded{} = e), do: {Map.delete(model, e.input), [e.input]}

  def project(model), do: Map.new(model, fn {id, _} -> {id, project_entry(model, id)} end)

  def project_entry(model, id) do
    case Map.fetch(model, id) do
      {:ok, input} ->
        %{
          runId: input.run,
          createdAt: input.created_at,
          title: input.title,
          requires: input.requires
        }

      :error ->
        nil
    end
  end
end
