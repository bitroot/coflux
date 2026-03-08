defmodule Coflux.Topics.Queue do
  use Topical.Topic, route: ["queue", :workspace_id]

  alias Coflux.Orchestration

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.subscribe_queue(project_id, workspace_id, self()) do
      {:ok, executions, ref} ->
        value =
          Map.new(executions, fn {module, target, run_external_id, step_number, attempt,
                                  execute_after, created_at, assigned_at} ->
            execution_id = "#{run_external_id}:#{step_number}:#{attempt}"

            {execution_id,
             %{
               module: module,
               target: target,
               runId: run_external_id,
               stepId: "#{run_external_id}:#{step_number}",
               stepNumber: step_number,
               attempt: attempt,
               executeAfter: execute_after,
               createdAt: created_at,
               assignedAt: assigned_at
             }}
          end)

        {:ok, Topic.new(value, %{ref: ref})}

      {:error, :workspace_invalid} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification/2)
    {:ok, topic}
  end

  defp process_notification(
         {:scheduled, execution_external_id, module, target, run_external_id, step_number,
          attempt, execute_after, created_at},
         topic
       ) do
    Topic.set(topic, [execution_external_id], %{
      module: module,
      target: target,
      runId: run_external_id,
      stepId: "#{run_external_id}:#{step_number}",
      stepNumber: step_number,
      attempt: attempt,
      executeAfter: execute_after,
      createdAt: created_at,
      assignedAt: nil
    })
  end

  defp process_notification({:assigned, executions}, topic) do
    Enum.reduce(executions, topic, fn {execution_external_id, assigned_at}, topic ->
      Topic.set(topic, [execution_external_id, :assignedAt], assigned_at)
    end)
  end

  defp process_notification({:completed, execution_external_id}, topic) do
    Topic.unset(topic, [], execution_external_id)
  end
end
