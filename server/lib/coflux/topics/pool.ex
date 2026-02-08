defmodule Coflux.Topics.Pool do
  use Topical.Topic, route: ["pools", :workspace_id, :pool_name]

  alias Coflux.Orchestration

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)
    pool_name = Map.fetch!(params, :pool_name)

    case Orchestration.subscribe_pool(project_id, workspace_id, pool_name, self()) do
      {:ok, pool, workers, ref} ->
        {:ok,
         Topic.new(
           %{
             pool: build_pool(pool),
             workers:
               Map.new(workers, fn {worker_external_id, worker} ->
                 {worker_external_id, build_worker(worker)}
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

  defp process_notification(topic, {:worker, _worker_id, worker_external_id, starting_at}) do
    Topic.set(topic, [:workers, worker_external_id], %{
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

  defp process_notification(topic, {:launch_result, worker_external_id, started_at, error}) do
    topic
    |> Topic.set([:workers, worker_external_id, :startedAt], started_at)
    |> Topic.set([:workers, worker_external_id, :startError], error)
  end

  defp process_notification(topic, {:worker_stopping, worker_external_id, stopping_at}) do
    Topic.set(topic, [:workers, worker_external_id, :stoppingAt], stopping_at)
  end

  defp process_notification(topic, {:worker_stop_result, worker_external_id, stopped_at, error}) do
    # TODO: don't set 'stopped_at' if error?
    topic
    |> Topic.set([:workers, worker_external_id, :stoppedAt], stopped_at)
    |> Topic.set([:workers, worker_external_id, :stopError], error)
  end

  defp process_notification(topic, {:worker_deactivated, worker_external_id, deactivated_at, error}) do
    topic
    |> Topic.set([:workers, worker_external_id, :deactivatedAt], deactivated_at)
    |> Topic.set([:workers, worker_external_id, :error], error)
  end

  defp process_notification(topic, {:worker_state, worker_external_id, state}) do
    Topic.set(topic, [:workers, worker_external_id, :state], state)
  end

  defp process_notification(topic, {:worker_connected, worker_external_id, connected}) do
    Topic.set(topic, [:workers, worker_external_id, :connected], connected)
  end

  defp build_launcher(launcher) do
    case launcher.type do
      :docker ->
        base = %{type: "docker", image: launcher.image}

        base
        |> maybe_put(:dockerHost, Map.get(launcher, :docker_host))
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

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
      error: worker.error,
      state: worker.state,
      connected: worker.connected
    }
  end
end
