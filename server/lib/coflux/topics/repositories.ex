defmodule Coflux.Topics.Repositories do
  use Topical.Topic, route: ["projects", :project_id, "repositories", :workspace_id]

  alias Coflux.Orchestration

  def init(params) do
    project_id = Keyword.fetch!(params, :project_id)
    workspace_id = String.to_integer(Keyword.fetch!(params, :workspace_id))

    {:ok, manifests, executions, ref} =
      Orchestration.subscribe_repositories(project_id, workspace_id, self())

    value =
      Map.new(manifests, fn {repository, manifest} ->
        result = %{
          workflows: Map.keys(manifest.workflows),
          sensors: Map.keys(manifest.sensors),
          executing: 0,
          scheduled: 0,
          nextDueAt: nil
        }

        result =
          case Map.fetch(executions, repository) do
            {:ok, {executing, scheduled}} ->
              next_due_at = scheduled |> Map.values() |> Enum.min(fn -> nil end)

              result
              |> Map.put(:executing, MapSet.size(executing))
              |> Map.put(:scheduled, map_size(scheduled))
              |> Map.put(:nextDueAt, next_due_at)

            :error ->
              result
          end

        {repository, result}
      end)

    {:ok, Topic.new(value, %{ref: ref, executions: executions})}
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification(&2, &1))
    {:ok, topic}
  end

  defp process_notification(topic, {:manifests, manifests}) do
    Enum.reduce(manifests, topic, fn {repository, manifest}, topic ->
      update_manifest(topic, repository, manifest)
    end)
  end

  defp process_notification(topic, {:manifest, repository, manifest}) do
    update_manifest(topic, repository, manifest)
  end

  defp process_notification(topic, {:scheduled, repository, execution_id, execute_at}) do
    update_executing(topic, repository, fn {executing, scheduled} ->
      scheduled = Map.put(scheduled, execution_id, execute_at)
      {executing, scheduled}
    end)
  end

  defp process_notification(topic, {:assigned, executions}) do
    Enum.reduce(executions, topic, fn {repository, execution_ids}, topic ->
      update_executing(topic, repository, fn {executing, scheduled} ->
        executing = MapSet.union(executing, execution_ids)
        scheduled = Map.drop(scheduled, MapSet.to_list(execution_ids))
        {executing, scheduled}
      end)
    end)
  end

  defp process_notification(topic, {:completed, repository, execution_id}) do
    update_executing(topic, repository, fn {executing, scheduled} ->
      executing = MapSet.delete(executing, execution_id)
      scheduled = Map.delete(scheduled, execution_id)
      {executing, scheduled}
    end)
  end

  defp update_executing(topic, repository, fun) do
    default = {MapSet.new(), %{}}

    topic =
      update_in(
        topic,
        [Access.key(:state), :executions, Access.key(repository, default)],
        fun
      )

    {executing, scheduled} = topic.state.executions[repository]
    next_due_at = scheduled |> Map.values() |> Enum.min(fn -> nil end)

    topic
    |> Topic.set([repository, :executing], MapSet.size(executing))
    |> Topic.set([repository, :scheduled], map_size(scheduled))
    |> Topic.set([repository, :nextDueAt], next_due_at)
  end

  defp update_manifest(topic, repository, manifest) do
    if manifest do
      topic
      |> Topic.set([repository, :workflows], Map.keys(manifest.workflows))
      |> Topic.set([repository, :sensors], Map.keys(manifest.sensors))
    else
      Topic.unset(topic, [], repository)
    end
  end
end
