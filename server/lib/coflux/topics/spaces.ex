defmodule Coflux.Topics.Spaces do
  use Topical.Topic, route: ["projects", :project_id, "spaces"]

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
    {:ok, spaces, ref} = Orchestration.subscribe_spaces(project_id, self())

    spaces =
      Map.new(spaces, fn {space_id, space} ->
        {Integer.to_string(space_id), build_space(space)}
      end)

    {:ok, Topic.new(spaces, %{ref: ref})}
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification(&2, &1))
    {:ok, topic}
  end

  defp process_notification(topic, {:space, space_id, space}) do
    Topic.set(topic, [Integer.to_string(space_id)], build_space(space))
  end

  defp process_notification(topic, {:state, space_id, state}) do
    Topic.set(topic, [Integer.to_string(space_id), :state], build_state(state))
  end

  defp build_space(space) do
    %{
      name: space.name,
      baseId: space.base_id,
      state: build_state(space.state)
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
