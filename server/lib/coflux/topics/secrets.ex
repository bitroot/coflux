defmodule Coflux.Topics.Secrets do
  @moduledoc """
  The project's secrets - what exists, for which workspaces, and when it
  last changed. Never a value.
  """

  use Topical.Topic, route: ["secrets"]

  alias Coflux.Orchestration
  alias Coflux.Topics.Diff
  alias Coflux.Topics.Secrets.Model

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    {:ok, events, ref} = Orchestration.subscribe(project_id, :secrets, self())
    {model, _dirty} = Model.fold(Model.new(), events)
    {:ok, Topic.new(Model.project(model), %{model: model, ref: ref})}
  end

  def handle_info({:topic, _ref, events}, topic) do
    {model, dirty} = Model.fold(topic.state.model, events)

    topic =
      Enum.reduce(dirty, topic, fn key, topic ->
        Diff.apply(topic, [key], Map.get(topic.value, key), Model.project_entry(model, key))
      end)

    {:ok, %{topic | state: %{topic.state | model: model}}}
  end
end

defmodule Coflux.Topics.Secrets.Model do
  @moduledoc false

  import Kernel, except: [apply: 2]
  import Coflux.TopicUtils, only: [build_principal: 1]

  alias Coflux.Events.{SecretDeleted, SecretSet}

  def new, do: %{}

  def fold(model, events) do
    Enum.reduce(events, {model, MapSet.new()}, fn event, {model, dirty} ->
      {model, keys} = apply(model, event)
      {model, Enum.into(keys, dirty)}
    end)
  end

  def apply(model, %SecretSet{} = e) do
    key = key(e.workspaces, e.name)

    secret = %{
      workspaces: e.workspaces,
      name: e.name,
      version: e.version,
      created_at: e.created_at,
      updated_at: e.updated_at,
      updated_by: e.updated_by
    }

    {Map.put(model, key, secret), [key]}
  end

  def apply(model, %SecretDeleted{} = e) do
    key = key(e.workspaces, e.name)
    {Map.delete(model, key), [key]}
  end

  # A name is unique within a workspace pattern.
  defp key(workspaces, name), do: "#{name}@#{workspaces}"

  def project(model), do: Map.new(model, fn {key, _} -> {key, project_entry(model, key)} end)

  def project_entry(model, key) do
    case Map.fetch(model, key) do
      {:ok, secret} ->
        %{
          workspaces: secret.workspaces,
          name: secret.name,
          version: secret.version,
          createdAt: secret.created_at,
          updatedAt: secret.updated_at,
          updatedBy: build_principal(secret.updated_by)
        }

      :error ->
        nil
    end
  end
end
