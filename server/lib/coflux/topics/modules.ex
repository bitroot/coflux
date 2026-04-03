defmodule Coflux.Topics.Modules do
  use Topical.Topic, route: ["workspaces", :workspace_id, "modules"]

  alias Coflux.Orchestration

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.subscribe_modules(project_id, workspace_id, self()) do
      {:ok, manifests, active_runs, ref} ->
        value =
          Map.new(manifests, fn {module, workflows} ->
            workflow_map =
              Map.new(Map.keys(workflows), fn target ->
                runs =
                  case Map.fetch(active_runs, {module, target}) do
                    {:ok, runs_map} -> runs_map |> Map.keys() |> Enum.sort()
                    :error -> []
                  end

                {target, %{activeRuns: runs}}
              end)

            {module, %{workflows: workflow_map}}
          end)

        # Internal state: {module, target} -> %{run_ext_id -> MapSet of execution_ext_ids}
        {:ok, Topic.new(value, %{ref: ref, active_runs: active_runs})}

      {:error, :workspace_invalid} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification(&2, &1))
    {:ok, topic}
  end

  defp process_notification(topic, {:manifests, manifests}) do
    Enum.reduce(manifests, topic, fn {module, manifest}, topic ->
      update_manifest(topic, module, manifest)
    end)
  end

  defp process_notification(topic, {:manifest, module, manifest}) do
    update_manifest(topic, module, manifest)
  end

  defp process_notification(
         topic,
         {:scheduled, {root_module, root_target}, run_ext_id, execution_ext_id, _execute_at}
       ) do
    update_active_runs(topic, root_module, root_target, fn runs_map ->
      Map.update(
        runs_map,
        run_ext_id,
        MapSet.new([execution_ext_id]),
        &MapSet.put(&1, execution_ext_id)
      )
    end)
  end

  defp process_notification(topic, {:assigned, workflow_executions}) do
    Enum.reduce(workflow_executions, topic, fn {{root_module, root_target}, executions}, topic ->
      update_active_runs(topic, root_module, root_target, fn runs_map ->
        Enum.reduce(executions, runs_map, fn {run_ext_id, execution_ext_id}, runs_map ->
          Map.update(
            runs_map,
            run_ext_id,
            MapSet.new([execution_ext_id]),
            &MapSet.put(&1, execution_ext_id)
          )
        end)
      end)
    end)
  end

  defp process_notification(
         topic,
         {:completed, {root_module, root_target}, run_ext_id, execution_ext_id}
       ) do
    update_active_runs(topic, root_module, root_target, fn runs_map ->
      case Map.fetch(runs_map, run_ext_id) do
        {:ok, execution_ids} ->
          remaining = MapSet.delete(execution_ids, execution_ext_id)

          if MapSet.size(remaining) == 0 do
            Map.delete(runs_map, run_ext_id)
          else
            Map.put(runs_map, run_ext_id, remaining)
          end

        :error ->
          runs_map
      end
    end)
  end

  defp update_active_runs(topic, root_module, root_target, fun) do
    key = {root_module, root_target}
    default = %{}

    topic =
      update_in(
        topic,
        [Access.key(:state), :active_runs, Access.key(key, default)],
        fun
      )

    # Only update topic value for modules/targets that have a manifest registered
    if get_in(topic.value, [root_module, :workflows]) |> is_map() &&
         Map.has_key?(topic.value[root_module][:workflows], root_target) do
      runs_map = topic.state.active_runs[key] || %{}
      Topic.set(topic, [root_module, :workflows, root_target, :activeRuns], runs_map |> Map.keys() |> Enum.sort())
    else
      topic
    end
  end

  defp update_manifest(topic, module, workflows) do
    if workflows do
      workflow_map =
        Map.new(Map.keys(workflows), fn target ->
          runs_map = Map.get(topic.state.active_runs, {module, target}, %{})
          {target, %{activeRuns: runs_map |> Map.keys() |> Enum.sort()}}
        end)

      Topic.set(topic, [module], %{workflows: workflow_map})
    else
      Topic.unset(topic, [], module)
    end
  end
end
