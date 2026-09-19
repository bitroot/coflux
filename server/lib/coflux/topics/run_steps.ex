defmodule Coflux.Topics.RunSteps do
  @moduledoc """
  The structure of a whole run: every step, each attempt summarised with
  its complete list of children. Nothing is collapsed and no detail is
  carried, so it stays small even for a large fan-out, and the pages that
  show the whole run (timeline, logs) can traverse it while the run topic
  keeps the graph's collapsed view.
  """

  use Topical.Topic, route: ["workspaces", :workspace_id, "runs", :run_id, "steps"]

  alias Coflux.RunView
  alias Coflux.RunView.{Loader, Sync}

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    run_id = Map.fetch!(params, :run_id)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Loader.load(project_id, run_id, workspace_id, self()) do
      {:error, :not_found} ->
        {:error, :not_found}

      {:ok, view, _fetch} ->
        {:ok, Topic.new(%{steps: RunView.project_structure(view)}, %{view: view})}
    end
  end

  def handle_info({:topic, _ref, events}, topic) do
    {view, effects} = RunView.apply_all(topic.state.view, events)
    topic = %{topic | state: %{topic.state | view: view}}
    {:ok, Sync.structure(topic, effects)}
  end
end
