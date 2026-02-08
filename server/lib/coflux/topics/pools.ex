defmodule Coflux.Topics.Pools do
  use Topical.Topic, route: ["pools", :workspace_id]

  alias Coflux.Orchestration

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)

    {:ok, pools, ref} =
      Orchestration.subscribe_pools(project_id, workspace_id, self())

    value = build_value(pools)

    {:ok, Topic.new(value, %{ref: ref})}
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
    case launcher.type do
      :docker -> Map.take(launcher, [:type, :image])
    end
  end
end
