defmodule Coflux.Topics.Module do
  use Topical.Topic,
    route: ["modules", :module, :workspace_id]

  alias Coflux.Orchestration

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    module = Map.fetch!(params, :module)
    workspace_id = String.to_integer(Map.fetch!(params, :workspace_id))

    {:ok, executions, ref} =
      Orchestration.subscribe_module(project_id, module, workspace_id, self())

    value =
      Map.new(executions, fn {execution_id, target_name, external_run_id, external_step_id,
                              attempt, execute_after, created_at, assigned_at} ->
        {Integer.to_string(execution_id),
         %{
           target: target_name,
           runId: external_run_id,
           stepId: external_step_id,
           attempt: attempt,
           executeAfter: execute_after,
           createdAt: created_at,
           assignedAt: assigned_at
         }}
      end)

    topic = Topic.new(value, %{ref: ref})

    {:ok, topic}
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification(&2, &1))
    {:ok, topic}
  end

  defp process_notification(
         topic,
         {:scheduled, execution_id, target_name, external_run_id, external_step_id, attempt,
          execute_after, created_at}
       ) do
    Topic.set(topic, [Integer.to_string(execution_id)], %{
      target: target_name,
      runId: external_run_id,
      stepId: external_step_id,
      attempt: attempt,
      executeAfter: execute_after,
      createdAt: created_at,
      assignedAt: nil
    })
  end

  defp process_notification(topic, {:assigned, executions}) do
    Enum.reduce(executions, topic, fn {execution_id, assigned_at}, topic ->
      Topic.set(topic, [Integer.to_string(execution_id), :assignedAt], assigned_at)
    end)
  end

  defp process_notification(topic, {:completed, execution_id}) do
    Topic.unset(topic, [], Integer.to_string(execution_id))
  end
end
