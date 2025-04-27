defmodule Coflux.Topics.Pool do
  use Topical.Topic, route: ["projects", :project_id, "pools", :workspace_id, :pool_name]

  alias Coflux.Orchestration

  def init(params) do
    project_id = Keyword.fetch!(params, :project_id)
    workspace_id = String.to_integer(Keyword.fetch!(params, :workspace_id))
    pool_name = Keyword.fetch!(params, :pool_name)

    case Orchestration.subscribe_pool(project_id, workspace_id, pool_name, self()) do
      {:ok, pool, agents, ref} ->
        {:ok,
         Topic.new(
           %{
             pool: build_pool(pool),
             agents:
               Map.new(agents, fn {agent_id, agent} ->
                 {Integer.to_string(agent_id), build_agent(agent)}
               end)
           },
           %{ref: ref}
         )}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification(&2, &1))
    {:ok, topic}
  end

  defp process_notification(topic, {:updated, pool}) do
    Topic.set(topic, [:pool], build_pool(pool))
  end

  defp process_notification(topic, {:agent, agent_id, starting_at}) do
    Topic.set(topic, [:agents, Integer.to_string(agent_id)], %{
      startingAt: starting_at,
      startedAt: nil,
      startError: nil,
      stoppingAt: nil,
      stopError: nil,
      deactivatedAt: nil,
      state: :active,
      connected: nil
    })
  end

  defp process_notification(topic, {:launch_result, agent_id, started_at, error}) do
    topic
    |> Topic.set([:agents, Integer.to_string(agent_id), :startedAt], started_at)
    |> Topic.set([:agents, Integer.to_string(agent_id), :startError], error)
  end

  defp process_notification(topic, {:agent_stopping, agent_id, stopping_at}) do
    Topic.set(topic, [:agents, Integer.to_string(agent_id), :stoppingAt], stopping_at)
  end

  defp process_notification(topic, {:agent_stop_result, agent_id, stopped_at, error}) do
    # TODO: don't set 'stopped_at' if error?
    topic
    |> Topic.set([:agents, Integer.to_string(agent_id), :stoppedAt], stopped_at)
    |> Topic.set([:agents, Integer.to_string(agent_id), :stopError], error)
  end

  defp process_notification(topic, {:agent_deactivated, agent_id, deactivated_at}) do
    Topic.set(topic, [:agents, Integer.to_string(agent_id), :deactivatedAt], deactivated_at)
  end

  defp process_notification(topic, {:agent_state, agent_id, state}) do
    Topic.set(topic, [:agents, Integer.to_string(agent_id), :state], state)
  end

  defp process_notification(topic, {:agent_connected, agent_id, connected}) do
    Topic.set(topic, [:agents, Integer.to_string(agent_id), :connected], connected)
  end

  defp build_launcher(launcher) do
    case launcher.type do
      :docker ->
        %{
          type: "docker",
          image: launcher.image
        }
    end
  end

  defp build_pool(pool) do
    if pool do
      %{
        modules: pool.modules,
        provides: pool.provides,
        # TODO: include launcher ID?
        launcher: if(pool.launcher, do: build_launcher(pool.launcher))
      }
    end
  end

  defp build_agent(agent) do
    %{
      # TODO: launcher ID? (and/or launcher?)
      startingAt: agent.starting_at,
      startedAt: agent.started_at,
      startError: agent.start_error,
      stoppingAt: agent.stopping_at,
      stopError: agent.stop_error,
      deactivatedAt: agent.deactivated_at,
      state: agent.state,
      connected: agent.connected
    }
  end
end
