defmodule Coflux.Topics.Workspaces do
  use Topical.Topic, route: ["projects", :project_id, "workspaces"]

  alias Coflux.Orchestration

  import Coflux.TopicUtils, only: [validate_project_access: 2]

  def connect(params, context) do
    namespace = Map.get(context, :namespace)

    with :ok <- validate_project_access(params.project_id, namespace) do
      {:ok, params}
    end
  end

  def init(params) do
    project_id = Map.fetch!(params, :project_id)
    {:ok, workspaces, ref} = Orchestration.subscribe_workspaces(project_id, self())

    workspaces =
      Map.new(workspaces, fn {workspace_id, workspace} ->
        {Integer.to_string(workspace_id), build_workspace(workspace)}
      end)

    {:ok, Topic.new(workspaces, %{ref: ref})}
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification(&2, &1))
    {:ok, topic}
  end

  defp process_notification(topic, {:workspace, workspace_id, workspace}) do
    Topic.set(topic, [Integer.to_string(workspace_id)], build_workspace(workspace))
  end

  defp process_notification(topic, {:state, workspace_id, state}) do
    Topic.set(topic, [Integer.to_string(workspace_id), :state], build_state(state))
  end

  defp build_workspace(workspace) do
    %{
      name: workspace.name,
      baseId: workspace.base_id,
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
