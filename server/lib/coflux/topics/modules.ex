defmodule Coflux.Topics.Modules do
  use Topical.Topic, route: ["modules", :workspace_id]

  alias Coflux.Orchestration

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = String.to_integer(Map.fetch!(params, :workspace_id))

    {:ok, manifests, executions, ref} =
      Orchestration.subscribe_modules(project_id, workspace_id, self())

    value =
      Map.new(manifests, fn {module, workflows} ->
        result = %{
          workflows: Map.keys(workflows),
          executing: 0,
          scheduled: 0,
          nextDueAt: nil
        }

        result =
          case Map.fetch(executions, module) do
            {:ok, {executing, scheduled}} ->
              next_due_at = scheduled |> Map.values() |> Enum.min(fn -> nil end)

              result
              |> Map.put(:executing, MapSet.size(executing))
              |> Map.put(:scheduled, map_size(scheduled))
              |> Map.put(:nextDueAt, next_due_at)

            :error ->
              result
          end

        {module, result}
      end)

    {:ok, Topic.new(value, %{ref: ref, executions: executions})}
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification(&2, &1))
    {:ok, topic}
  end

  defp process_notification(topic, {:manifests, manifests}) do
    Enum.reduce(manifests, topic, fn {module, manifest}, topic ->
      update_manifest(topic, module, manifest)
    end)
  end

  defp process_notification(topic, {:manifest, module, manifest}) do
    update_manifest(topic, module, manifest)
  end

  defp process_notification(topic, {:scheduled, module, execution_id, execute_at}) do
    update_executing(topic, module, fn {executing, scheduled} ->
      scheduled = Map.put(scheduled, execution_id, execute_at)
      {executing, scheduled}
    end)
  end

  defp process_notification(topic, {:assigned, executions}) do
    Enum.reduce(executions, topic, fn {module, execution_ids}, topic ->
      update_executing(topic, module, fn {executing, scheduled} ->
        executing = MapSet.union(executing, execution_ids)
        scheduled = Map.drop(scheduled, MapSet.to_list(execution_ids))
        {executing, scheduled}
      end)
    end)
  end

  defp process_notification(topic, {:completed, module, execution_id}) do
    update_executing(topic, module, fn {executing, scheduled} ->
      executing = MapSet.delete(executing, execution_id)
      scheduled = Map.delete(scheduled, execution_id)
      {executing, scheduled}
    end)
  end

  defp update_executing(topic, module, fun) do
    default = {MapSet.new(), %{}}

    topic =
      update_in(
        topic,
        [Access.key(:state), :executions, Access.key(module, default)],
        fun
      )

    {executing, scheduled} = topic.state.executions[module]
    next_due_at = scheduled |> Map.values() |> Enum.min(fn -> nil end)

    topic
    |> Topic.set([module, :executing], MapSet.size(executing))
    |> Topic.set([module, :scheduled], map_size(scheduled))
    |> Topic.set([module, :nextDueAt], next_due_at)
  end

  defp update_manifest(topic, module, workflows) do
    if workflows do
      Topic.set(topic, [module, :workflows], Map.keys(workflows))
    else
      Topic.unset(topic, [], module)
    end
  end
end
