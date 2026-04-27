defmodule Coflux.Topics.Run do
  use Topical.Topic, route: ["workspaces", :workspace_id, "runs", :run_id]

  alias Coflux.Orchestration

  import Coflux.TopicUtils

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    external_run_id = Map.fetch!(params, :run_id)
    workspace_id = Map.fetch!(params, :workspace_id)

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
           project: project_id,
           external_run_id: external_run_id,
           workspace_ids: workspace_ids
         })}
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification(&2, &1))
    {:ok, topic}
  end

  defp process_notification(topic, {:step, step_number, step, workspace_external_id}) do
    if workspace_external_id in topic.state.workspace_ids do
      Topic.set(topic, [:steps, "#{topic.state.external_run_id}:#{step_number}"], %{
        stepNumber: step_number,
        module: step.module,
        target: step.target,
        parentId: step.parent_id,
        cacheConfig: build_cache_config(step.cache_config),
        cacheKey: build_key(step.cache_key),
        memoKey: build_key(step.memo_key),
        retries: build_retries(step.retries),
        recurrent: step.recurrent,
        timeout: step.timeout,
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
         {:execution, step_number, attempt, execution_external_id, workspace_external_id,
          created_at, execute_after, dependencies, created_by}
       ) do
    if workspace_external_id in topic.state.workspace_ids do
      Topic.set(
        topic,
        [
          :steps,
          "#{topic.state.external_run_id}:#{step_number}",
          :executions,
          Integer.to_string(attempt)
        ],
        %{
          executionId: execution_external_id,
          workspaceId: workspace_external_id,
          createdAt: created_at,
          createdBy: build_principal(created_by),
          executeAfter: execute_after,
          assignedAt: nil,
          completedAt: nil,
          completion: nil,
          groups: %{},
          assets: %{},
          dependencies:
            Map.new(dependencies, fn {dependency_id, dependency} ->
              {dependency_id, build_dependency(dependency)}
            end),
          children: [],
          inputs: %{},
          result: nil,
          metrics: %{},
          streams: %{}
        }
      )
    else
      topic
    end
  end

  defp process_notification(topic, {:metric_defined, execution_external_id, key, definition}) do
    update_execution(topic, execution_external_id, fn topic, base_path ->
      Topic.set(topic, base_path ++ [:metrics, key], %{
        group: Map.get(definition, "group"),
        groupUnits: Map.get(definition, "group_units"),
        groupLower: Map.get(definition, "group_lower"),
        groupUpper: Map.get(definition, "group_upper"),
        scale: Map.get(definition, "scale"),
        units: Map.get(definition, "units"),
        progress: Map.get(definition, "progress", false),
        lower: Map.get(definition, "lower"),
        upper: Map.get(definition, "upper")
      })
    end)
  end

  defp process_notification(topic, {:group, execution_external_id, group_id, name}) do
    update_execution(topic, execution_external_id, fn topic, base_path ->
      Topic.set(topic, base_path ++ [:groups, Integer.to_string(group_id)], name)
    end)
  end

  defp process_notification(topic, {:asset, execution_external_id, external_asset_id, asset}) do
    asset = build_asset(asset)

    update_execution(
      topic,
      execution_external_id,
      fn topic, base_path ->
        Topic.set(
          topic,
          base_path ++ [:assets, external_asset_id],
          asset
        )
      end
    )
  end

  defp process_notification(topic, {:assigned, assigned}) do
    Enum.reduce(assigned, topic, fn {execution_external_id, assigned_at}, topic ->
      update_execution(topic, execution_external_id, fn topic, base_path ->
        Topic.set(topic, base_path ++ [:assignedAt], assigned_at)
      end)
    end)
  end

  defp process_notification(
         topic,
         {:result_dependency, execution_external_id, dependency_id, dependency}
       ) do
    dependency = build_dependency(dependency)

    update_execution(
      topic,
      execution_external_id,
      fn topic, base_path ->
        Topic.merge(
          topic,
          base_path ++ [:dependencies, dependency_id],
          dependency
        )
      end
    )
  end

  defp process_notification(
         topic,
         {:stream_dependency, execution_external_id, producer_execution_id, index,
          producer_metadata}
       ) do
    dependency = %{
      type: "stream",
      execution: build_execution(producer_metadata),
      index: index
    }

    update_execution(
      topic,
      execution_external_id,
      fn topic, base_path ->
        Topic.merge(
          topic,
          base_path ++ [:dependencies, "#{producer_execution_id}:#{index}"],
          dependency
        )
      end
    )
  end

  defp process_notification(topic, {:child, parent_execution_external_id, child}) do
    child = build_child(child, topic.state.external_run_id)

    update_execution(topic, parent_execution_external_id, fn topic, base_path ->
      Topic.insert(topic, base_path ++ [:children], child)
    end)
  end

  defp process_notification(
         topic,
         {:result, execution_external_id, result, result_at, created_by}
       ) do
    result = build_result(result, created_by)

    update_execution(topic, execution_external_id, fn topic, base_path ->
      topic
      |> Topic.set(base_path ++ [:result], result)
      |> Topic.set(base_path ++ [:resultAt], result_at)
    end)
  end

  defp process_notification(
         topic,
         {:completion, execution_external_id, kind, successor, completion_at}
       ) do
    update_execution(topic, execution_external_id, fn topic, base_path ->
      topic
      |> Topic.set(base_path ++ [:completedAt], completion_at)
      |> Topic.set(base_path ++ [:completion], %{
        kind: Atom.to_string(kind),
        successor: successor
      })
    end)
  end

  defp process_notification(
         topic,
         {:stream_opened, execution_external_id, index, buffer, timeout_ms, created_at}
       ) do
    update_execution(topic, execution_external_id, fn topic, base_path ->
      Topic.set(topic, base_path ++ [:streams, Integer.to_string(index)], %{
        buffer: buffer,
        timeoutMs: timeout_ms,
        openedAt: created_at,
        closedAt: nil,
        reason: nil,
        error: nil
      })
    end)
  end

  defp process_notification(
         topic,
         {:stream_closed, execution_external_id, index, reason, error, closed_at}
       ) do
    index_key = Integer.to_string(index)

    update_execution(topic, execution_external_id, fn topic, base_path ->
      topic
      |> Topic.set(base_path ++ [:streams, index_key, :closedAt], closed_at)
      |> Topic.set(base_path ++ [:streams, index_key, :reason], reason)
      |> Topic.set(base_path ++ [:streams, index_key, :error], error)
    end)
  end

  defp process_notification(
         topic,
         {:result_result, execution_external_id, result, _created_at, created_by}
       ) do
    result = build_result(result, created_by)

    update_execution(topic, execution_external_id, fn topic, base_path ->
      Topic.set(topic, base_path ++ [:result, :result], result)
    end)
  end

  defp process_notification(
         topic,
         {:input_submitted, execution_external_id, input_external_id, title}
       ) do
    status = find_existing_input_status(topic, input_external_id)

    update_execution(topic, execution_external_id, fn topic, base_path ->
      Topic.set(topic, base_path ++ [:inputs, input_external_id], %{
        title: title,
        status: status
      })
    end)
  end

  defp process_notification(
         topic,
         {:input_dependency, execution_external_id, input_external_id, title, response_type}
       ) do
    update_execution(topic, execution_external_id, fn topic, base_path ->
      Topic.set(topic, base_path ++ [:dependencies, input_external_id], %{
        type: "input",
        inputId: input_external_id,
        title: title,
        status: response_type
      })
    end)
  end

  defp process_notification(
         topic,
         {:asset_dependency, execution_external_id, asset_external_id, asset}
       ) do
    update_execution(topic, execution_external_id, fn topic, base_path ->
      Topic.set(topic, base_path ++ [:dependencies, asset_external_id], %{
        type: "asset",
        assetId: asset_external_id,
        asset: build_asset(asset)
      })
    end)
  end

  defp process_notification(
         topic,
         {:input_response, input_external_id, response_type}
       ) do
    topic
    |> update_submitted_input_status(input_external_id, response_type)
    |> update_dependency_input_status(input_external_id, response_type)
  end

  defp find_existing_input_status(topic, input_external_id) do
    Enum.find_value(topic.value.steps, fn {_step_id, step} ->
      Enum.find_value(step.executions, fn {_attempt, execution} ->
        get_in(execution, [:inputs, input_external_id, :status])
      end)
    end)
  end

  defp update_submitted_input_status(topic, input_external_id, response_type) do
    Enum.reduce(topic.value.steps, topic, fn {step_id, step}, topic ->
      Enum.reduce(step.executions, topic, fn {attempt, execution}, topic ->
        inputs = Map.get(execution, :inputs, %{})

        if Map.has_key?(inputs, input_external_id) do
          Topic.set(
            topic,
            [:steps, step_id, :executions, attempt, :inputs, input_external_id, :status],
            response_type
          )
        else
          topic
        end
      end)
    end)
  end

  defp update_dependency_input_status(topic, input_external_id, response_type) do
    Enum.reduce(topic.value.steps, topic, fn {step_id, step}, topic ->
      Enum.reduce(step.executions, topic, fn {attempt, execution}, topic ->
        deps = Map.get(execution, :dependencies, %{})

        if Map.has_key?(deps, input_external_id) and
             Map.get(deps[input_external_id], :type) == "input" do
          Topic.set(
            topic,
            [:steps, step_id, :executions, attempt, :dependencies, input_external_id, :status],
            response_type
          )
        else
          topic
        end
      end)
    end)
  end

  defp build_run(run, parent, steps, workspace_ids) do
    %{
      createdAt: run.created_at,
      createdBy: build_principal(run.created_by),
      requires: run.requires,
      parent: if(parent, do: build_execution(parent)),
      steps:
        steps
        |> Enum.filter(fn {_, step} ->
          step.executions
          |> Map.values()
          |> Enum.any?(&(&1.workspace_id in workspace_ids))
        end)
        |> Map.new(fn {step_id, step} ->
          {"#{run.external_id}:#{step_id}",
           %{
             stepNumber: step_id,
             module: step.module,
             target: step.target,
             parentId: step.parent_id,
             cacheConfig: build_cache_config(step.cache_config),
             cacheKey: build_key(step.cache_key),
             memoKey: build_key(step.memo_key),
             retries: build_retries(step),
             recurrent: step.recurrent == 1,
             timeout: step.timeout,
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
                    executionId: execution.execution_id,
                    workspaceId: execution.workspace_id,
                    createdAt: execution.created_at,
                    createdBy: build_principal(execution.created_by),
                    executeAfter: execution.execute_after,
                    assignedAt: execution.assigned_at,
                    resultAt: execution.result_at,
                    completedAt: execution.completed_at,
                    completion: execution.completion,
                    groups: execution.groups,
                    assets:
                      Map.new(execution.assets, fn {external_asset_id, asset} ->
                        {external_asset_id, build_asset(asset)}
                      end),
                    dependencies: build_dependencies(execution.dependencies),
                    children: Enum.map(execution.children, &build_child(&1, run.external_id)),
                    inputs: Map.get(execution, :inputs, %{}),
                    result: build_result(execution.result, execution.result_created_by),
                    metrics:
                      Map.new(execution.metric_definitions, fn {key, def_data} ->
                        {key,
                         %{
                           group: def_data.group,
                           groupUnits: def_data.group_units,
                           groupLower: def_data.group_lower,
                           groupUpper: def_data.group_upper,
                           scale: def_data.scale,
                           units: def_data.units,
                           progress: def_data.progress,
                           lower: def_data.lower,
                           upper: def_data.upper
                         }}
                      end),
                    streams: build_streams(execution.streams)
                  }}
               end)
           }}
        end)
    }
  end

  defp build_dependencies(dependencies) do
    Map.new(dependencies, fn
      {id, {:result, execution}} ->
        {id, %{type: "result", execution: build_execution(execution)}}

      {id, {:input, title, status}} ->
        {id, %{type: "input", inputId: id, title: title, status: status}}

      {id, {:asset, asset}} ->
        {id, %{type: "asset", assetId: id, asset: build_asset(asset)}}

      {id, {:stream, index, execution}} ->
        {id,
         %{
           type: "stream",
           execution: build_execution(execution),
           index: index
         }}
    end)
  end

  defp build_dependency(execution) do
    %{
      type: "result",
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

  defp build_result(result, created_by \\ nil) do
    created_by = build_principal(created_by)

    case result do
      {:error, type, message, frames, retry, retryable} ->
        %{
          type: "error",
          createdBy: created_by,
          error: %{
            type: type,
            message: message,
            frames: build_frames(frames)
          },
          retry: if(retry, do: execution_attempt(retry)),
          retryable: retryable
        }

      {:error, type, message, frames, retry} ->
        %{
          type: "error",
          createdBy: created_by,
          error: %{
            type: type,
            message: message,
            frames: build_frames(frames)
          },
          retry: if(retry, do: execution_attempt(retry)),
          retryable: nil
        }

      {:value, value} ->
        %{type: "value", createdBy: created_by, value: build_value(value)}

      {:abandoned, retry} ->
        %{
          type: "abandoned",
          createdBy: created_by,
          retry: if(retry, do: execution_attempt(retry))
        }

      {:crashed, retry} ->
        %{
          type: "crashed",
          createdBy: created_by,
          retry: if(retry, do: execution_attempt(retry))
        }

      :cancelled ->
        %{type: "cancelled", createdBy: created_by}

      {:timeout, retry} ->
        %{
          type: "timeout",
          createdBy: created_by,
          retry: if(retry, do: execution_attempt(retry))
        }

      {:suspended, successor} ->
        %{
          type: "suspended",
          createdBy: created_by,
          successor: if(successor, do: execution_attempt(successor))
        }

      {:recurred, successor} ->
        %{
          type: "recurred",
          createdBy: created_by,
          successor: if(successor, do: execution_attempt(successor))
        }

      {:deferred, execution, result} ->
        %{
          type: "deferred",
          createdBy: created_by,
          execution: build_execution(execution),
          result: build_result(result)
        }

      {:cached, execution, result} ->
        %{
          type: "cached",
          createdBy: created_by,
          execution: build_execution(execution),
          result: build_result(result)
        }

      {:spawned, execution, result} ->
        %{
          type: "spawned",
          createdBy: created_by,
          execution: build_execution(execution),
          result: build_result(result)
        }

      nil ->
        nil
    end
  end

  defp build_streams(streams) do
    Map.new(streams, fn
      {index, buffer, timeout_ms, opened_at, nil, nil, nil} ->
        {Integer.to_string(index),
         %{
           buffer: buffer,
           timeoutMs: timeout_ms,
           openedAt: opened_at,
           closedAt: nil,
           reason: nil,
           error: nil
         }}

      {index, buffer, timeout_ms, opened_at, closed_at, reason, nil} ->
        {Integer.to_string(index),
         %{
           buffer: buffer,
           timeoutMs: timeout_ms,
           openedAt: opened_at,
           closedAt: closed_at,
           reason: Atom.to_string(reason),
           error: nil
         }}

      {index, buffer, timeout_ms, opened_at, closed_at, reason, {type, message, frames}} ->
        {Integer.to_string(index),
         %{
           buffer: buffer,
           timeoutMs: timeout_ms,
           openedAt: opened_at,
           closedAt: closed_at,
           reason: Atom.to_string(reason),
           error: %{type: type, message: message, frames: build_frames(frames)}
         }}
    end)
  end

  defp execution_attempt({ext_id, _module, _target}) do
    ext_id |> String.split(":") |> List.last() |> String.to_integer()
  end

  defp build_child({step_number, attempt, group_id}, external_run_id) do
    %{stepId: "#{external_run_id}:#{step_number}", attempt: attempt, groupId: group_id}
  end

  # From notification map (retries opt from worker)
  defp build_retries(nil), do: nil

  defp build_retries(%{limit: limit, backoff_min: backoff_min, backoff_max: backoff_max}) do
    %{limit: limit, backoffMin: backoff_min, backoffMax: backoff_max}
  end

  # From Step struct
  defp build_retries(%{retry_limit: 0}), do: nil

  defp build_retries(%{
         retry_limit: retry_limit,
         retry_backoff_min: retry_backoff_min,
         retry_backoff_max: retry_backoff_max
       }) do
    %{
      limit: if(retry_limit == -1, do: nil, else: retry_limit),
      backoffMin: retry_backoff_min,
      backoffMax: retry_backoff_max
    }
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

  defp update_execution(topic, execution_external_id, fun) do
    Enum.reduce(topic.value.steps, topic, fn {step_id, step}, topic ->
      Enum.reduce(step.executions, topic, fn {attempt, execution}, topic ->
        if execution.executionId == execution_external_id do
          fun.(topic, [:steps, step_id, :executions, attempt])
        else
          topic
        end
      end)
    end)
  end
end
