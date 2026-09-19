defmodule Coflux.Topics.Catalog do
  @moduledoc """
  The head of every catalog path visible from a workspace, keyed by path.
  A publish in the workspace or any of its bases updates the entry; the
  version listing behind a path is served by the API rather than a topic,
  since a path has slashes and topic routes are fixed-length.
  """

  use Topical.Topic, route: ["workspaces", :workspace_id, "catalog"]

  alias Coflux.Orchestration

  import Coflux.TopicUtils

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.subscribe_catalog(project_id, workspace_id, self()) do
      {:ok, heads, ref} ->
        value = Map.new(heads, fn {path, version} -> {path, build_catalog_version(version)} end)
        {:ok, Topic.new(value, %{ref: ref})}

      {:error, :workspace_invalid} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification/2)
    {:ok, topic}
  end

  defp process_notification({:catalog_version, version}, topic) do
    Topic.set(topic, [version.path], build_catalog_version(version))
  end
end
