defmodule Coflux.Topics.Run do
  use Topical.Topic, route: ["projects", :project_id, "runs", :run_id, :workspace_id]

  alias Coflux.Orchestration

  import Coflux.TopicUtils

  def init(params) do
    project_id = Keyword.fetch!(params, :project_id)
    external_run_id = Keyword.fetch!(params, :run_id)
    workspace_id = String.to_integer(Keyword.fetch!(params, :workspace_id))

    case Orchestration.subscribe_run(
           project_id,
           external_run_id,
           self()
         ) do
      {:error, :not_found} ->
        {:error, :not_found}

      {:ok, run, parent, steps, _ref} ->
        run_workspace_id =
          steps
          |> Map.values()
          |> Enum.reject(& &1.parent_id)
          |> Enum.min_by(& &1.created_at)
          |> Map.fetch!(:executions)
          |> Map.values()
          |> Enum.min_by(& &1.created_at)
          |> Map.fetch!(:workspace_id)

        workspace_ids = Enum.uniq([run_workspace_id, workspace_id])

        {:ok,
         Topic.new(build_run(run, parent, steps, workspace_ids), %{
           project_id: project_id,
           external_run_id: external_run_id,
           workspace_ids: workspace_ids
         })}
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification(&2, &1))
    {:ok, topic}
  end

  defp process_notification(topic, {:step, external_step_id, step, workspace_id}) do
    if workspace_id in topic.state.workspace_ids do
      Topic.set(topic, [:steps, external_step_id], %{
        module: step.module,
        target: step.target,
        type: step.type,
        parentId: if(step.parent_id, do: Integer.to_string(step.parent_id)),
        cacheConfig: build_cache_config(step.cache_config),
        cacheKey: build_key(step.cache_key),
        memoKey: build_key(step.memo_key),
        createdAt: step.created_at,
        arguments: Enum.map(step.arguments, &build_value/1),
        requires: step.requires,
        executions: %{}
      })
    else
      topic
    end
  end

  defp process_notification(
         topic,
         {:execution, step_id, attempt, execution_id, workspace_id, created_at, execute_after,
          dependencies}
       ) do
    if workspace_id in topic.state.workspace_ids do
      Topic.set(
        topic,
        [:steps, step_id, :executions, Integer.to_string(attempt)],
        %{
          executionId: Integer.to_string(execution_id),
          workspaceId: Integer.to_string(workspace_id),
          createdAt: created_at,
          executeAfter: execute_after,
          assignedAt: nil,
          completedAt: nil,
          groups: %{},
          assets: %{},
          dependencies:
            Map.new(dependencies, fn {dependency_id, dependency} ->
              {Integer.to_string(dependency_id), build_dependency(dependency)}
            end),
          children: [],
          result: nil,
          logCount: 0
        }
      )
    else
      topic
    end
  end

  defp process_notification(topic, {:group, execution_id, group_id, name}) do
    update_execution(topic, execution_id, fn topic, base_path ->
      Topic.set(topic, base_path ++ [:groups, Integer.to_string(group_id)], name)
    end)
  end

  defp process_notification(topic, {:asset, execution_id, asset_id, asset}) do
    asset = build_asset(asset)

    update_execution(
      topic,
      execution_id,
      fn topic, base_path ->
        Topic.set(
          topic,
          base_path ++ [:assets, Integer.to_string(asset_id)],
          asset
        )
      end
    )
  end

  defp process_notification(topic, {:assigned, assigned}) do
    Enum.reduce(assigned, topic, fn {execution_id, assigned_at}, topic ->
      update_execution(topic, execution_id, fn topic, base_path ->
        Topic.set(topic, base_path ++ [:assignedAt], assigned_at)
      end)
    end)
  end

  defp process_notification(topic, {:result_dependency, execution_id, dependency_id, dependency}) do
    dependency = build_dependency(dependency)

    update_execution(
      topic,
      execution_id,
      fn topic, base_path ->
        Topic.merge(
          topic,
          base_path ++ [:dependencies, Integer.to_string(dependency_id)],
          dependency
        )
      end
    )
  end

  defp process_notification(topic, {:child, parent_id, child}) do
    child = build_child(child)

    update_execution(topic, parent_id, fn topic, base_path ->
      Topic.insert(topic, base_path ++ [:children], child)
    end)
  end

  defp process_notification(topic, {:result, execution_id, result, created_at}) do
    result = build_result(result)

    update_execution(topic, execution_id, fn topic, base_path ->
      topic
      |> Topic.set(base_path ++ [:result], result)
      |> Topic.set(base_path ++ [:completedAt], created_at)
    end)
  end

  defp process_notification(topic, {:result_result, execution_id, result, _created_at}) do
    result = build_result(result)

    update_execution(topic, execution_id, fn topic, base_path ->
      Topic.set(topic, base_path ++ [:result, :result], result)
    end)
  end

  defp process_notification(topic, {:log_counts, execution_id, delta}) do
    update_execution(topic, execution_id, fn topic, base_path ->
      path = base_path ++ [:logCount]
      count = get_in(topic.value, path) + delta
      Topic.set(topic, base_path ++ [:logCount], count)
    end)
  end

  defp build_run(run, parent, steps, workspace_ids) do
    %{
      createdAt: run.created_at,
      parent: if(parent, do: build_execution(parent)),
      steps:
        steps
        |> Enum.filter(fn {_, step} ->
          step.executions
          |> Map.values()
          |> Enum.any?(&(&1.workspace_id in workspace_ids))
        end)
        |> Map.new(fn {step_id, step} ->
          {step_id,
           %{
             module: step.module,
             target: step.target,
             type: step.type,
             parentId: if(step.parent_id, do: Integer.to_string(step.parent_id)),
             cacheConfig: build_cache_config(step.cache_config),
             cacheKey: build_key(step.cache_key),
             memoKey: build_key(step.memo_key),
             createdAt: step.created_at,
             arguments: Enum.map(step.arguments, &build_value/1),
             requires: step.requires,
             executions:
               step.executions
               |> Enum.filter(fn {_, execution} ->
                 execution.workspace_id in workspace_ids
               end)
               |> Map.new(fn {attempt, execution} ->
                 {Integer.to_string(attempt),
                  %{
                    executionId: Integer.to_string(execution.execution_id),
                    workspaceId: Integer.to_string(execution.workspace_id),
                    createdAt: execution.created_at,
                    executeAfter: execution.execute_after,
                    assignedAt: execution.assigned_at,
                    completedAt: execution.completed_at,
                    groups: execution.groups,
                    assets:
                      Map.new(execution.assets, fn {asset_id, asset} ->
                        {Integer.to_string(asset_id), build_asset(asset)}
                      end),
                    dependencies: build_dependencies(execution.dependencies),
                    children: Enum.map(execution.children, &build_child/1),
                    result: build_result(execution.result),
                    logCount: execution.log_count
                  }}
               end)
           }}
        end)
    }
  end

  defp build_dependencies(dependencies) do
    Map.new(dependencies, fn {execution_id, execution} ->
      {execution_id, build_dependency(execution)}
    end)
  end

  defp build_dependency(execution) do
    %{
      execution: build_execution(execution)
    }
  end

  defp build_frames(frames) do
    Enum.map(frames, fn {file, line, name, code} ->
      %{
        file: file,
        line: line,
        name: name,
        code: code
      }
    end)
  end

  defp build_result(result) do
    case result do
      {:error, type, message, frames, retry} ->
        %{
          type: "error",
          error: %{
            type: type,
            message: message,
            frames: build_frames(frames)
          },
          retry: if(retry, do: retry.attempt)
        }

      {:value, value} ->
        %{type: "value", value: build_value(value)}

      {:abandoned, retry} ->
        %{type: "abandoned", retry: if(retry, do: retry.attempt)}

      :cancelled ->
        %{type: "cancelled"}

      {:suspended, successor} ->
        %{type: "suspended", successor: if(successor, do: successor.attempt)}

      {:deferred, execution, result} ->
        %{type: "deferred", execution: build_execution(execution), result: build_result(result)}

      {:cached, execution, result} ->
        %{type: "cached", execution: build_execution(execution), result: build_result(result)}

      {:spawned, execution, result} ->
        %{type: "spawned", execution: build_execution(execution), result: build_result(result)}

      nil ->
        nil
    end
  end

  defp build_child({external_step_id, attempt, group_id}) do
    %{stepId: external_step_id, attempt: attempt, groupId: group_id}
  end

  defp build_cache_config(cache_config) do
    if cache_config do
      %{
        params: cache_config.params,
        maxAge: cache_config.max_age,
        namespace: cache_config.namespace,
        version: cache_config.version
      }
    end
  end

  defp build_key(key, length \\ 10) do
    if key do
      key
      |> Base.encode16(case: :lower)
      |> String.slice(0, length)
    end
  end

  defp update_execution(topic, execution_id, fun) do
    execution_id_s = Integer.to_string(execution_id)

    Enum.reduce(topic.value.steps, topic, fn {step_id, step}, topic ->
      Enum.reduce(step.executions, topic, fn {attempt, execution}, topic ->
        if execution.executionId == execution_id_s do
          fun.(topic, [:steps, step_id, :executions, attempt])
        else
          topic
        end
      end)
    end)
  end
end
