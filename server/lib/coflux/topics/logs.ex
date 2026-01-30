defmodule Coflux.Topics.Logs do
  use Topical.Topic, route: ["runs", :run_id, "logs", :workspace_id]

  alias Coflux.Orchestration

  import Coflux.TopicUtils

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    run_id = Map.fetch!(params, :run_id)
    workspace_id = String.to_integer(Map.fetch!(params, :workspace_id))

    case Orchestration.subscribe_run(project_id, run_id, self()) do
      {:ok, _run, _parent, steps, _ref} ->
        case Orchestration.subscribe_logs(project_id, run_id, self()) do
          {:ok, _ref, messages} ->
            run_workspace_id =
              steps
              |> Map.values()
              |> Enum.reject(& &1.parent_id)
              |> Enum.min_by(& &1.created_at)
              |> Map.fetch!(:executions)
              |> Map.values()
              |> Enum.min_by(& &1.created_at)
              |> Map.fetch!(:workspace_id)

            workspace_ids = Enum.uniq([run_workspace_id, workspace_id])

            execution_ids =
              steps
              |> Map.values()
              |> Enum.flat_map(fn step ->
                step.executions
                |> Map.values()
                |> Enum.filter(&(&1.workspace_id in workspace_ids))
                |> Enum.map(& &1.execution_id)
              end)
              |> MapSet.new()

            topic =
              messages
              |> Enum.filter(&(elem(&1, 0) in execution_ids))
              |> Enum.map(&build_message/1)
              |> Topic.new(%{
                workspace_ids: workspace_ids,
                execution_ids: execution_ids
              })

            {:ok, topic}
        end
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification(&2, &1))
    {:ok, topic}
  end

  defp process_notification(topic, {:execution, _, _, execution_id, workspace_id, _, _, _}) do
    if workspace_id in topic.state.workspace_ids do
      update_in(topic.state.execution_ids, &MapSet.put(&1, execution_id))
    else
      topic
    end
  end

  defp process_notification(topic, {:messages, messages}) do
    messages =
      messages
      |> Enum.filter(&(elem(&1, 0) in topic.state.execution_ids))
      |> Enum.map(&build_message/1)

    Topic.insert(topic, [], messages)
  end

  defp process_notification(topic, {:step, _, _, _}), do: topic
  defp process_notification(topic, {:group, _, _, _}), do: topic
  defp process_notification(topic, {:asset, _, _, _}), do: topic
  defp process_notification(topic, {:assigned, _}), do: topic
  defp process_notification(topic, {:result_dependency, _, _, _}), do: topic
  defp process_notification(topic, {:child, _, _}), do: topic
  defp process_notification(topic, {:result, _, _, _}), do: topic
  defp process_notification(topic, {:result_result, _, _, _}), do: topic
  defp process_notification(topic, {:log_counts, _, _}), do: topic

  defp build_message({execution_id, timestamp, level, template, values}) do
    [
      Integer.to_string(execution_id),
      timestamp,
      encode_level(level),
      template,
      Map.new(values, fn {k, v} -> {k, build_value(v)} end)
    ]
  end

  defp encode_level(level) do
    case level do
      :debug -> 0
      :stdout -> 1
      :info -> 2
      :stderr -> 3
      :warning -> 4
      :error -> 5
    end
  end
end
