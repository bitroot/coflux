defmodule Coflux.Topics.Modules do
  use Topical.Topic, route: ["projects", :project_id, "modules", :workspace_id]

  alias Coflux.Orchestration

  def init(params) do
    project_id = Keyword.fetch!(params, :project_id)
    workspace_id = String.to_integer(Keyword.fetch!(params, :workspace_id))

    {:ok, manifests, executions, ref} =
      Orchestration.subscribe_modules(project_id, workspace_id, self())

    value =
      Map.new(manifests, fn {module, manifest} ->
        result = %{
          workflows: Map.keys(manifest.workflows),
          sensors: Map.keys(manifest.sensors),
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

  defp update_manifest(topic, module, manifest) do
    if manifest do
      topic
      |> Topic.set([module, :workflows], Map.keys(manifest.workflows))
      |> Topic.set([module, :sensors], Map.keys(manifest.sensors))
    else
      Topic.unset(topic, [], module)
    end
  end
end
