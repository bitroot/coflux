defmodule Coflux.Topics.Workspaces do
  use Topical.Topic, route: ["workspaces"]

  alias Coflux.Orchestration

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    {:ok, workspaces, ref} = Orchestration.subscribe_workspaces(project_id, self())

    workspaces =
      Map.new(workspaces, fn {_workspace_id, workspace} ->
        {workspace.external_id, build_workspace(workspace)}
      end)

    {:ok, Topic.new(workspaces, %{ref: ref})}
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification(&2, &1))
    {:ok, topic}
  end

  defp process_notification(topic, {:workspace, _workspace_id, workspace}) do
    Topic.set(topic, [workspace.external_id], build_workspace(workspace))
  end

  defp process_notification(topic, {:state, workspace_external_id, state}) do
    Topic.set(topic, [workspace_external_id, :state], build_state(state))
  end

  defp build_workspace(workspace) do
    %{
      name: workspace.name,
      baseId: workspace[:base_external_id],
      state: build_state(workspace.state)
    }
  end

  defp build_state(state) do
    case state do
      :active -> "active"
      :paused -> "paused"
      :archived -> "archived"
    end
  end
end
