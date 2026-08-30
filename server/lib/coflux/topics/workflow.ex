defmodule Coflux.Topics.Workflow do
  use Topical.Topic,
    route: ["workspaces", :workspace_id, "workflows", :module, :target]

  import Coflux.TopicUtils

  alias Coflux.Orchestration

  @max_runs 50

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    module = Map.fetch!(params, :module)
    target_name = Map.fetch!(params, :target)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.subscribe_workflow(
           project_id,
           module,
           target_name,
           workspace_id,
           @max_runs,
           self()
         ) do
      {:ok, workflow, instruction, runs, active_runs, ref} ->
        value =
          %{
            parameters: if(workflow, do: build_parameters(workflow.parameters)),
            instruction: instruction,
            configuration: build_configuration(workflow),
            runs: build_runs(runs, active_runs)
          }

        # `active_runs` is %{run_id => %{execution_id => assigned?}} for
        # everything still in flight, kept in step with the scheduled /
        # assigned / completed notifications so that each run's activity can
        # be recomputed as they arrive
        {:ok, Topic.new(value, %{ref: ref, active_runs: active_runs})}

      {:error, :not_found} ->
        {:error, :not_found}

      {:error, :workspace_invalid} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification/2)
    {:ok, topic}
  end

  defp process_notification({:target, target}, topic) do
    topic
    |> Topic.set([:parameters], build_parameters(target.parameters))
    |> Topic.set([:instruction], target.instruction)
    |> Topic.set([:configuration], build_configuration(target))
  end

  defp process_notification({:run, external_run_id, created_at, created_by}, topic) do
    topic =
      Topic.set(
        topic,
        [:runs, external_run_id],
        %{
          id: external_run_id,
          createdAt: created_at,
          createdBy: build_principal(created_by),
          outcome: nil,
          active: build_active(topic.state.active_runs[external_run_id])
        }
      )

    runs = topic.value.runs

    if map_size(runs) > @max_runs do
      {oldest_id, _} = Enum.min_by(runs, fn {_id, run} -> run.createdAt end)
      Topic.unset(topic, [:runs], oldest_id)
    else
      topic
    end
  end

  defp process_notification({:scheduled, external_run_id, execution_id}, topic) do
    update_active(topic, external_run_id, &Map.put(&1, execution_id, false))
  end

  defp process_notification({:assigned, executions}, topic) do
    Enum.reduce(executions, topic, fn {external_run_id, execution_id}, topic ->
      update_active(topic, external_run_id, &Map.put(&1, execution_id, true))
    end)
  end

  defp process_notification({:completed, external_run_id, execution_id}, topic) do
    update_active(topic, external_run_id, &Map.delete(&1, execution_id))
  end

  defp process_notification({:outcome, external_run_id, outcome}, topic) do
    if Map.has_key?(topic.value.runs, external_run_id) do
      Topic.set(topic, [:runs, external_run_id, :outcome], build_outcome(outcome))
    else
      topic
    end
  end

  defp update_active(topic, external_run_id, fun) do
    topic =
      update_in(
        topic,
        [Access.key(:state), :active_runs, Access.key(external_run_id, %{})],
        fun
      )

    topic =
      if topic.state.active_runs[external_run_id] == %{} do
        update_in(topic, [Access.key(:state), :active_runs], &Map.delete(&1, external_run_id))
      else
        topic
      end

    # Runs that have been evicted from the list (or that belong to another
    # workspace) still get notifications, but have nothing to update
    if Map.has_key?(topic.value.runs, external_run_id) do
      Topic.set(
        topic,
        [:runs, external_run_id, :active],
        build_active(topic.state.active_runs[external_run_id])
      )
    else
      topic
    end
  end

  # A run is "running" once any of its in-flight executions has been assigned
  # to a worker, and "queued" while they're all still waiting for one
  defp build_active(nil), do: nil

  defp build_active(executions) when map_size(executions) == 0, do: nil

  defp build_active(executions) do
    if Enum.any?(executions, fn {_, assigned} -> assigned end), do: "running", else: "queued"
  end

  defp build_outcome(nil), do: nil
  defp build_outcome(outcome), do: Atom.to_string(outcome)

  defp build_parameters(parameters) do
    Enum.map(parameters, fn {name, default, annotation} ->
      %{name: name, default: default, annotation: annotation}
    end)
  end

  defp build_cache_configuration(cache) do
    if cache do
      %{
        params: cache.params,
        maxAge: cache.max_age,
        namespace: cache.namespace,
        version: cache.version
      }
    end
  end

  defp build_defer_configuration(defer) do
    if defer do
      %{params: defer.params}
    end
  end

  defp build_retries_configuration(retries) do
    if retries do
      %{
        limit: retries.limit,
        backoffMin: retries.backoff_min,
        backoffMax: retries.backoff_max
      }
    end
  end

  defp build_configuration(workflow) do
    if workflow do
      %{
        waitFor: workflow.wait_for,
        cache: build_cache_configuration(workflow.cache),
        defer: build_defer_configuration(workflow.defer),
        delay: workflow.delay,
        retries: build_retries_configuration(workflow.retries),
        recurrent: workflow.recurrent,
        timeout: workflow.timeout,
        requires: workflow.requires,
        memo: workflow.memo,
        streams: build_streams_configuration(workflow[:streams])
      }
    end
  end

  defp build_streams_configuration(nil), do: nil

  defp build_streams_configuration(streams) do
    %{
      buffer: streams[:buffer],
      timeoutMs: streams[:timeout_ms]
    }
  end

  defp build_runs(runs, active_runs) do
    Map.new(runs, fn
      {external_run_id, created_at, created_by_user_ext_id, created_by_token_ext_id, outcome} ->
        created_by =
          case {created_by_user_ext_id, created_by_token_ext_id} do
            {nil, nil} -> nil
            {user_ext_id, nil} -> %{type: "user", externalId: user_ext_id}
            {nil, token_ext_id} -> %{type: "token", externalId: token_ext_id}
          end

        active = build_active(Map.get(active_runs, external_run_id))

        {external_run_id,
         %{
           id: external_run_id,
           createdAt: created_at,
           createdBy: created_by,
           outcome: build_outcome(outcome),
           active: active
         }}
    end)
  end
end
