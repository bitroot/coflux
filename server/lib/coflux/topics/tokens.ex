defmodule Coflux.Topics.Tokens do
  use Topical.Topic, route: ["tokens"]

  import Coflux.TopicUtils, only: [build_principal: 1]

  alias Coflux.Orchestration

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)

    {:ok, tokens, ref} = Orchestration.subscribe_tokens(project_id, self())

    active_tokens =
      tokens
      |> Enum.filter(fn token -> is_nil(token.revoked_at) end)
      |> Map.new(fn token -> {token.external_id, build_token(token)} end)

    {:ok, Topic.new(active_tokens, %{ref: ref})}
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification(&2, &1))
    {:ok, topic}
  end

  defp process_notification(topic, {:token, external_id, nil}) do
    Topic.unset(topic, [], external_id)
  end

  defp process_notification(topic, {:token, external_id, token}) do
    Topic.set(topic, [external_id], build_token(token))
  end

  defp build_token(token) do
    %{
      id: token.id,
      externalId: token.external_id,
      name: token.name,
      workspaces: token.workspaces,
      createdAt: token.created_at,
      expiresAt: token.expires_at,
      createdBy: build_principal(token.created_by)
    }
  end
end
