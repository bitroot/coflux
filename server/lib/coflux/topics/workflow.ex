defmodule Coflux.Topics.Workflow do
  use Topical.Topic,
    route: ["workflows", :module, :target, :workspace_id]

  import Coflux.TopicUtils

  alias Coflux.Orchestration

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    module = Map.fetch!(params, :module)
    target_name = Map.fetch!(params, :target)
    workspace_id = String.to_integer(Map.fetch!(params, :workspace_id))

    case Orchestration.subscribe_workflow(
           project_id,
           module,
           target_name,
           workspace_id,
           self()
         ) do
      {:ok, workflow, instruction, runs, ref} ->
        value =
          %{
            parameters: if(workflow, do: build_parameters(workflow.parameters)),
            instruction: instruction,
            configuration: build_configuration(workflow),
            runs: build_runs(runs)
          }

        {:ok, Topic.new(value, %{ref: ref})}

      {:error, :not_found} ->
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

  defp process_notification({:run, external_run_id, created_at}, topic) do
    # Legacy notification without created_by
    Topic.set(
      topic,
      [:runs, external_run_id],
      %{id: external_run_id, createdAt: created_at, createdBy: nil}
    )
  end

  defp process_notification({:run, external_run_id, created_at, created_by}, topic) do
    Topic.set(
      topic,
      [:runs, external_run_id],
      %{id: external_run_id, createdAt: created_at, createdBy: build_principal(created_by)}
    )
  end

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
        delayMin: retries.delay_min,
        delayMax: retries.delay_max
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
        requires: workflow.requires
      }
    end
  end

  defp build_runs(runs) do
    Map.new(runs, fn
      {external_run_id, created_at, created_by_user_ext_id, created_by_token_ext_id} ->
        created_by =
          case {created_by_user_ext_id, created_by_token_ext_id} do
            {nil, nil} -> nil
            {user_ext_id, nil} -> %{type: "user", externalId: user_ext_id}
            {nil, token_ext_id} -> %{type: "token", externalId: token_ext_id}
          end

        {external_run_id, %{id: external_run_id, createdAt: created_at, createdBy: created_by}}
    end)
  end
end
