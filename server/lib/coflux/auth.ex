defmodule Coflux.Auth do
  @moduledoc """
  Handles token authentication.

  Token access is configured in $COFLUX_DATA_DIR/tokens.json:
  ```json
  {
    "e3044207e9a26a59388c7224fc3f3c01...": {
      "projects": ["test1", "test2"],
      "workspaces": ["production"]
    },
    "479ccb088f9a5dce29dbff65996beac6...": {
      "projects": ["test1"],
      "workspaces": ["staging", "development/*"]
    }
  }
  ```

  Authentication mode is controlled by COFLUX_AUTH_MODE:
  - "token" (default): All requests require valid token
  - "none": No authentication required

  Workspace patterns control write access:
  - "*" matches all workspaces
  - "staging" matches exactly "staging"
  - "staging/*" matches "staging", "staging/feature1", etc.
  """

  alias Coflux.{Config, TokensStore}

  @type access :: %{workspaces: :all | [String.t()]}

  @doc """
  Checks if the given token is authorized for the project.

  Returns `{:ok, access}` with access details when allowed.
  Returns `{:error, :unauthorized}` otherwise.
  """
  def check(token, project_id) do
    case Config.auth_mode() do
      :none ->
        {:ok, %{workspaces: :all}}

      :token ->
        check_token(token, project_id)
    end
  end

  # Private functions

  defp check_token(nil, _project_id), do: {:error, :unauthorized}

  defp check_token(token, project_id) do
    token_hash = hash_token(token)

    case TokensStore.get_token_config(token_hash) do
      {:ok, config} ->
        if project_id in config.projects do
          {:ok, %{workspaces: config.workspaces}}
        else
          {:error, :unauthorized}
        end

      :error ->
        {:error, :unauthorized}
    end
  end

  defp hash_token(token) do
    :sha256
    |> :crypto.hash(token)
    |> Base.encode16(case: :lower)
  end
end
