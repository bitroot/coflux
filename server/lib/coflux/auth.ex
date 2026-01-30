defmodule Coflux.Auth do
  @moduledoc """
  Handles token authentication.

  Tokens are stored in $COFLUX_DATA_DIR/tokens.json with format:
  ```json
  {
    "<sha256-hash>": {
      "projects": ["acme", "demo"]
    }
  }
  ```

  - Key: SHA-256 hash of token (hex, lowercase)
  - projects: array of allowed project names (empty array = all projects)

  Auth mode is controlled by COFLUX_AUTH_MODE:
  - "none" (default): No authentication required
  - "token": Require valid token with project access
  """

  alias Coflux.Config
  alias Coflux.Auth.TokenStore

  @doc """
  Checks if the given token is authorized for the project.

  Returns `:ok` when auth is disabled or token is valid.
  Returns `{:error, :unauthorized}` otherwise.
  """
  def check(token, project_id) do
    case Config.auth_mode() do
      :none -> :ok
      :token -> validate_token(token, project_id)
    end
  end

  defp validate_token(nil, _project_id), do: {:error, :unauthorized}

  defp validate_token(token, project_id) do
    token_hash = hash_token(token)

    case TokenStore.lookup(token_hash) do
      {:ok, token_config} ->
        # Empty projects list means access to all projects
        if token_config.projects == [] or project_id in token_config.projects do
          :ok
        else
          {:error, :unauthorized}
        end

      :error ->
        {:error, :unauthorized}
    end
  end

  defp hash_token(token) do
    :crypto.hash(:sha256, token)
    |> Base.encode16(case: :lower)
  end
end
