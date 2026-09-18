defmodule Coflux.Topics.Run do
  @moduledoc """
  The skeleton of a run: the tree from its initial step, with the latest
  attempt of each step expanded, other attempts summarised, and each group
  collapsed to its first member plus a summary of the rest. Steps only
  reachable through a superseded attempt aren't included. See
  `Coflux.RunView` for the rule, and `Coflux.Topics.RunExecution` for
  opening what's collapsed.
  """

  use Topical.Topic, route: ["workspaces", :workspace_id, "runs", :run_id]

  import Coflux.TopicUtils

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

      {:ok, view, run, parent, fetch} ->
        visible = RunView.visible_steps(view)
        view = Sync.load(view, fetch, visible)

        value = %{
          createdAt: run.created_at,
          createdBy: build_principal(run.created_by),
          requires: run.requires,
          parent: if(parent, do: build_execution(parent)),
          steps: RunView.project(view)
        }

        {:ok, Topic.new(value, %{view: view, visible: visible, fetch: fetch})}
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    {view, effects} = RunView.apply_all(topic.state.view, notifications)
    topic = %{topic | state: %{topic.state | view: view}}
    {:ok, Sync.steps(topic, effects)}
  end
end
