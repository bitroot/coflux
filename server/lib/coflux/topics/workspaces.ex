defmodule Coflux.Topics.Workspaces do
  @moduledoc "Every workspace in the project, keyed by external id."

  use Topical.Topic, route: ["workspaces"]

  alias Coflux.Orchestration
  alias Coflux.Topics.Diff
  alias Coflux.Topics.Workspaces.Model

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    {:ok, events, ref} = Orchestration.subscribe(project_id, :workspaces, self())
    {model, _dirty} = Model.fold(Model.new(), events)
    {:ok, Topic.new(Model.project(model), %{model: model, ref: ref})}
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

defmodule Coflux.Topics.Workspaces.Model do
  @moduledoc false

  import Kernel, except: [apply: 2]

  alias Coflux.Events.{WorkspaceCreated, WorkspaceStateChanged, WorkspaceUpdated}

  def new, do: %{}

  def fold(model, events) do
    Enum.reduce(events, {model, MapSet.new()}, fn event, {model, dirty} ->
      {model, keys} = apply(model, event)
      {model, Enum.into(keys, dirty)}
    end)
  end

  def apply(model, %WorkspaceCreated{} = e), do: put(model, e)
  def apply(model, %WorkspaceUpdated{} = e), do: put(model, e)

  def apply(model, %WorkspaceStateChanged{} = e) do
    case Map.fetch(model, e.workspace) do
      {:ok, workspace} ->
        {Map.put(model, e.workspace, %{workspace | state: e.state}), [e.workspace]}

      :error ->
        {model, []}
    end
  end

  defp put(model, e) do
    {Map.put(model, e.workspace, %{name: e.name, base: e.base, state: e.state}), [e.workspace]}
  end

  def project(model), do: Map.new(model, fn {id, _} -> {id, project_entry(model, id)} end)

  def project_entry(model, id) do
    case Map.fetch(model, id) do
      {:ok, workspace} ->
        %{name: workspace.name, baseId: workspace.base, state: Atom.to_string(workspace.state)}

      :error ->
        nil
    end
  end
end
