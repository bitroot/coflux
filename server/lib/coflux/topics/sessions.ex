defmodule Coflux.Topics.Sessions do
  use Topical.Topic, route: ["workspaces", :workspace_id, "sessions"]

  alias Coflux.Orchestration

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.subscribe_sessions(project_id, workspace_id, self()) do
      {:ok, sessions, ref} ->
        sessions =
          Map.new(sessions, fn {session_external_id, session} ->
            {session_external_id, build_session(session)}
          end)

        {:ok, Topic.new(sessions, %{ref: ref})}

      {:error, :workspace_invalid} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification(&2, &1))
    {:ok, topic}
  end

  defp process_notification(topic, {:session, session_external_id, session}) do
    if is_nil(session) do
      Topic.unset(topic, [], session_external_id)
    else
      Topic.set(topic, [session_external_id], build_session(session))
    end
  end

  defp process_notification(topic, {:connected, session_external_id, connected}) do
    Topic.set(topic, [session_external_id, :connected], connected)
  end

  defp process_notification(topic, {:executions, session_external_id, executions}) do
    Topic.set(topic, [session_external_id, :executions], executions)
  end

  defp build_session(session) do
    %{
      connected: session.connected,
      executions: session.executions,
      concurrency: session.concurrency,
      poolName: session.pool_name,
      targets: session.targets,
      provides: session.provides,
      accepts: session.accepts,
      workerState: session.worker_state
    }
  end
end
