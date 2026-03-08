defmodule Coflux.Topics.Tokens do
  use Topical.Topic, route: ["tokens"]

  import Coflux.TopicUtils, only: [build_principal: 1]

  alias Coflux.Orchestration

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)

    {:ok, tokens} = Orchestration.list_tokens(project_id)

    value = %{
      tokens: Enum.map(tokens, &build_token/1)
    }

    {:ok, Topic.new(value, %{})}
  end

  defp build_token(token) do
    %{
      id: token.id,
      externalId: token.external_id,
      name: token.name,
      workspaces: token.workspaces,
      createdAt: token.created_at,
      expiresAt: token.expires_at,
      revokedAt: token.revoked_at,
      createdBy: build_principal(token.created_by)
    }
  end
end
