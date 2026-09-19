defmodule Coflux.Topics.Catalog do
  @moduledoc """
  The head of every catalog path visible from a workspace, keyed by path.
  A publish in the workspace or any of its bases updates the entry; the
  version listing behind a path is served by the API rather than a topic,
  since a path has slashes and topic routes are fixed-length.
  """

  use Topical.Topic, route: ["workspaces", :workspace_id, "catalog"]

  alias Coflux.Orchestration
  alias Coflux.Topics.Catalog.Model
  alias Coflux.Topics.Diff

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.subscribe(project_id, {:catalog, workspace_id}, self()) do
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
      Enum.reduce(dirty, topic, fn path, topic ->
        Diff.apply(topic, [path], Map.get(topic.value, path), Model.project_entry(model, path))
      end)

    {:ok, %{topic | state: %{topic.state | model: model}}}
  end
end

defmodule Coflux.Topics.Catalog.Model do
  @moduledoc false

  import Kernel, except: [apply: 2]
  import Coflux.TopicUtils, only: [build_catalog_version: 1]

  alias Coflux.Events.CatalogPublished

  def new, do: %{}

  def fold(model, events) do
    Enum.reduce(events, {model, MapSet.new()}, fn event, {model, dirty} ->
      {model, keys} = apply(model, event)
      {model, Enum.into(keys, dirty)}
    end)
  end

  # The newest version of a path is its head.
  def apply(model, %CatalogPublished{version: version}) do
    case Map.fetch(model, version.path) do
      {:ok, %{sequence: sequence}} when sequence > version.sequence -> {model, []}
      _ -> {Map.put(model, version.path, version), [version.path]}
    end
  end

  def project(model), do: Map.new(model, fn {path, _} -> {path, project_entry(model, path)} end)

  def project_entry(model, path) do
    case Map.fetch(model, path) do
      {:ok, version} -> build_catalog_version(version)
      :error -> nil
    end
  end
end
