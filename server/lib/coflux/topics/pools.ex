defmodule Coflux.Topics.Pools do
  @moduledoc "The pools defined in a workspace, keyed by name."

  use Topical.Topic, route: ["workspaces", :workspace_id, "pools"]

  alias Coflux.Orchestration
  alias Coflux.Topics.Diff
  alias Coflux.Topics.Pools.Model

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.subscribe(project_id, {:pools, workspace_id}, self()) do
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
      Enum.reduce(dirty, topic, fn name, topic ->
        Diff.apply(topic, [name], Map.get(topic.value, name), Model.project_entry(model, name))
      end)

    {:ok, %{topic | state: %{topic.state | model: model}}}
  end
end

defmodule Coflux.Topics.Pools.Model do
  @moduledoc false

  import Kernel, except: [apply: 2]

  alias Coflux.Events.{PoolStateChanged, PoolUpdated}
  alias Coflux.Topics.Pool

  def new, do: %{}

  def fold(model, events) do
    Enum.reduce(events, {model, MapSet.new()}, fn event, {model, dirty} ->
      {model, keys} = apply(model, event)
      {model, Enum.into(keys, dirty)}
    end)
  end

  def apply(model, %PoolUpdated{definition: nil} = e), do: {Map.delete(model, e.pool), [e.pool]}
  def apply(model, %PoolUpdated{} = e), do: {Map.put(model, e.pool, e.definition), [e.pool]}

  def apply(model, %PoolStateChanged{} = e) do
    case Map.fetch(model, e.pool) do
      {:ok, pool} -> {Map.put(model, e.pool, Map.put(pool, :state, e.state)), [e.pool]}
      :error -> {model, []}
    end
  end

  def project(model), do: Map.new(model, fn {name, _} -> {name, project_entry(model, name)} end)

  def project_entry(model, name) do
    case Map.fetch(model, name) do
      {:ok, pool} -> Pool.build_pool(pool)
      :error -> nil
    end
  end
end
