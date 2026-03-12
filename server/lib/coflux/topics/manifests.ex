defmodule Coflux.Topics.Manifests do
  use Topical.Topic, route: ["workspaces", :workspace_id, "manifests"]

  alias Coflux.Orchestration

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.get_manifests(project_id, workspace_id) do
      {:ok, manifests} ->
        value =
          Map.new(manifests, fn {module, workflows} ->
            targets =
              Map.new(workflows, fn {name, workflow} ->
                {name, build_workflow(workflow)}
              end)

            {module, targets}
          end)

        {:ok, Topic.new(value, %{})}

      {:error, :workspace_invalid} ->
        {:error, :not_found}
    end
  end

  defp build_workflow(workflow) do
    %{
      parameters:
        Enum.map(workflow.parameters, fn {name, default, annotation} ->
          %{name: name, default: default, annotation: annotation}
        end),
      waitFor: workflow.wait_for,
      cache: build_cache(workflow.cache),
      defer: build_defer(workflow.defer),
      delay: workflow.delay,
      retries: build_retries(workflow.retries),
      requires: workflow.requires
    }
  end

  defp build_cache(nil), do: nil

  defp build_cache(cache) do
    %{
      params: cache.params,
      maxAge: cache.max_age,
      namespace: cache.namespace,
      version: cache.version
    }
  end

  defp build_defer(nil), do: nil
  defp build_defer(defer), do: %{params: defer.params}

  defp build_retries(nil), do: nil

  defp build_retries(retries) do
    %{
      limit: retries.limit,
      delayMin: retries.delay_min,
      delayMax: retries.delay_max
    }
  end
end
