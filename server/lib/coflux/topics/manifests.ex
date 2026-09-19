defmodule Coflux.Topics.Manifests do
  @moduledoc "The latest manifest of each module registered in a workspace."

  use Topical.Topic, route: ["workspaces", :workspace_id, "manifests"]

  alias Coflux.Orchestration
  alias Coflux.Topics.Diff
  alias Coflux.Topics.Manifests.Model

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.subscribe(project_id, {:manifests, workspace_id}, self()) do
      {:ok, events, ref} ->
        {model, _dirty} = Model.fold(Model.new(), events)
        {:ok, Topic.new(Model.project(model), %{model: model, ref: ref})}

      {:error, :workspace_invalid} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, events}, topic) do
    {model, dirty} = Model.fold(topic.state.model, events)

    topic =
      Enum.reduce(dirty, topic, fn module, topic ->
        Diff.apply(
          topic,
          [module],
          Map.get(topic.value, module),
          Model.project_entry(model, module)
        )
      end)

    {:ok, %{topic | state: %{topic.state | model: model}}}
  end
end

defmodule Coflux.Topics.Manifests.Model do
  @moduledoc false

  import Kernel, except: [apply: 2]

  alias Coflux.Events.{ManifestRegistered, ModuleArchived}

  def new, do: %{}

  def fold(model, events) do
    Enum.reduce(events, {model, MapSet.new()}, fn event, {model, dirty} ->
      {model, keys} = apply(model, event)
      {model, Enum.into(keys, dirty)}
    end)
  end

  def apply(model, %ManifestRegistered{} = e),
    do: {Map.put(model, e.module, e.workflows), [e.module]}

  def apply(model, %ModuleArchived{} = e), do: {Map.delete(model, e.module), [e.module]}

  def project(model),
    do: Map.new(model, fn {module, _} -> {module, project_entry(model, module)} end)

  def project_entry(model, module) do
    case Map.fetch(model, module) do
      {:ok, workflows} ->
        Map.new(workflows, fn {name, workflow} -> {name, build_workflow(workflow)} end)

      :error ->
        nil
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
      requires: workflow.requires,
      concurrency: build_concurrency(workflow[:concurrency])
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

  defp build_concurrency(nil), do: nil

  defp build_concurrency(concurrency) do
    %{
      limit: concurrency.limit,
      params: concurrency.params,
      namespace: concurrency.namespace
    }
  end

  defp build_retries(nil), do: nil

  defp build_retries(retries) do
    %{
      limit: retries.limit,
      backoffMin: retries.backoff_min,
      backoffMax: retries.backoff_max
    }
  end
end
