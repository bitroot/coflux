defmodule Coflux.Topics.Pool do
  use Topical.Topic, route: ["projects", :project_id, "pools", :space_id, :pool_name]

  alias Coflux.Orchestration

  def init(params) do
    project_id = Keyword.fetch!(params, :project_id)
    space_id = String.to_integer(Keyword.fetch!(params, :space_id))
    pool_name = Keyword.fetch!(params, :pool_name)

    case Orchestration.subscribe_pool(project_id, space_id, pool_name, self()) do
      {:ok, pool, workers, ref} ->
        {:ok,
         Topic.new(
           %{
             pool: build_pool(pool),
             workers:
               Map.new(workers, fn {worker_id, worker} ->
                 {Integer.to_string(worker_id), build_worker(worker)}
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

  defp process_notification(topic, {:worker, worker_id, starting_at}) do
    Topic.set(topic, [:workers, Integer.to_string(worker_id)], %{
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

  defp process_notification(topic, {:launch_result, worker_id, started_at, error}) do
    topic
    |> Topic.set([:workers, Integer.to_string(worker_id), :startedAt], started_at)
    |> Topic.set([:workers, Integer.to_string(worker_id), :startError], error)
  end

  defp process_notification(topic, {:worker_stopping, worker_id, stopping_at}) do
    Topic.set(topic, [:workers, Integer.to_string(worker_id), :stoppingAt], stopping_at)
  end

  defp process_notification(topic, {:worker_stop_result, worker_id, stopped_at, error}) do
    # TODO: don't set 'stopped_at' if error?
    topic
    |> Topic.set([:workers, Integer.to_string(worker_id), :stoppedAt], stopped_at)
    |> Topic.set([:workers, Integer.to_string(worker_id), :stopError], error)
  end

  defp process_notification(topic, {:worker_deactivated, worker_id, deactivated_at}) do
    Topic.set(topic, [:workers, Integer.to_string(worker_id), :deactivatedAt], deactivated_at)
  end

  defp process_notification(topic, {:worker_state, worker_id, state}) do
    Topic.set(topic, [:workers, Integer.to_string(worker_id), :state], state)
  end

  defp process_notification(topic, {:worker_connected, worker_id, connected}) do
    Topic.set(topic, [:workers, Integer.to_string(worker_id), :connected], connected)
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

  defp build_worker(worker) do
    %{
      # TODO: launcher ID? (and/or launcher?)
      startingAt: worker.starting_at,
      startedAt: worker.started_at,
      startError: worker.start_error,
      stoppingAt: worker.stopping_at,
      stopError: worker.stop_error,
      deactivatedAt: worker.deactivated_at,
      state: worker.state,
      connected: worker.connected
    }
  end
end
