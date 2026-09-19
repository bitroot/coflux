defmodule Coflux.Topics.Tokens do
  @moduledoc "The project's active service tokens, keyed by external id."

  use Topical.Topic, route: ["tokens"]

  alias Coflux.Orchestration
  alias Coflux.Topics.Diff
  alias Coflux.Topics.Tokens.Model

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    {:ok, events, ref} = Orchestration.subscribe(project_id, :tokens, self())
    {model, _dirty} = Model.fold(Model.new(), events)
    {:ok, Topic.new(Model.project(model), %{model: model, ref: ref})}
  end

  def handle_info({:topic, _ref, events}, topic) do
    {model, dirty} = Model.fold(topic.state.model, events)

    topic =
      Enum.reduce(dirty, topic, fn token, topic ->
        Diff.apply(topic, [token], Map.get(topic.value, token), Model.project_entry(model, token))
      end)

    {:ok, %{topic | state: %{topic.state | model: model}}}
  end
end

defmodule Coflux.Topics.Tokens.Model do
  @moduledoc false

  import Kernel, except: [apply: 2]
  import Coflux.TopicUtils, only: [build_principal: 1]

  alias Coflux.Events.{TokenCreated, TokenRevoked}

  def new, do: %{}

  def fold(model, events) do
    Enum.reduce(events, {model, MapSet.new()}, fn event, {model, dirty} ->
      {model, keys} = apply(model, event)
      {model, Enum.into(keys, dirty)}
    end)
  end

  def apply(model, %TokenCreated{} = e) do
    token = %{
      id: e.id,
      name: e.name,
      workspaces: e.workspaces,
      created_at: e.created_at,
      expires_at: e.expires_at,
      created_by: e.created_by
    }

    {Map.put(model, e.token, token), [e.token]}
  end

  def apply(model, %TokenRevoked{} = e), do: {Map.delete(model, e.token), [e.token]}

  def project(model), do: Map.new(model, fn {id, _} -> {id, project_entry(model, id)} end)

  def project_entry(model, id) do
    case Map.fetch(model, id) do
      {:ok, token} ->
        %{
          id: token.id,
          externalId: id,
          name: token.name,
          workspaces: token.workspaces,
          createdAt: token.created_at,
          expiresAt: token.expires_at,
          createdBy: build_principal(token.created_by)
        }

      :error ->
        nil
    end
  end
end
