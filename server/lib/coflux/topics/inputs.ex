defmodule Coflux.Topics.Inputs do
  use Topical.Topic, route: ["workspaces", :workspace_id, "inputs"]

  alias Coflux.Orchestration

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.subscribe_inputs(project_id, workspace_id, self()) do
      {:ok, inputs, ref} ->
        {:ok, Topic.new(inputs, %{ref: ref})}

      {:error, :workspace_invalid} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification/2)
    {:ok, topic}
  end

  defp process_notification(
         {:input_dependency_active, external_id, run_id, created_at, title},
         topic
       ) do
    Topic.set(topic, [external_id], %{
      runId: run_id,
      createdAt: created_at,
      title: title
    })
  end

  defp process_notification({:input_dependency_inactive, external_id}, topic) do
    Topic.unset(topic, [], external_id)
  end

  defp process_notification({:input_responded, external_id, _responded_at}, topic) do
    Topic.unset(topic, [], external_id)
  end
end
