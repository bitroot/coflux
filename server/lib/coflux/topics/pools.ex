defmodule Coflux.Topics.Pools do
  use Topical.Topic, route: ["workspaces", :workspace_id, "pools"]

  alias Coflux.Orchestration

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.subscribe_pools(project_id, workspace_id, self()) do
      {:ok, pools, ref} ->
        {:ok, Topic.new(build_value(pools), %{ref: ref})}

      {:error, :workspace_invalid} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification(&2, &1))
    {:ok, topic}
  end

  defp process_notification(topic, {:pool, pool_name, pool}) do
    if pool do
      Topic.set(topic, [pool_name], build_pool(pool))
    else
      Topic.unset(topic, [], pool_name)
    end
  end

  defp build_value(pools) do
    Map.new(pools, fn {key, pool} ->
      {key, build_pool(pool)}
    end)
  end

  defp build_pool(pool) do
    %{
      modules: pool.modules,
      provides: pool.provides,
      launcher: pool.launcher && build_launcher(pool.launcher)
    }
  end

  defp build_launcher(launcher) do
    type_fields =
      case launcher.type do
        :docker ->
          %{type: "docker", image: launcher.image}
          |> maybe_put(:dockerHost, Map.get(launcher, :docker_host))

        :process ->
          %{type: "process", directory: launcher.directory}
      end

    type_fields
    |> maybe_put(:serverHost, Map.get(launcher, :server_host))
    |> maybe_put(:adapter, Map.get(launcher, :adapter))
    |> maybe_put(:concurrency, Map.get(launcher, :concurrency))
    |> maybe_put(:env, Map.get(launcher, :env))
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
