defmodule Coflux.Auth do
  @moduledoc """
  Handles token authentication.

  Token access is configured per-project in $COFLUX_DATA_DIR/projects.json:
  ```json
  {
    "acme": {
      "tokens": ["<sha256-hash-1>", "<sha256-hash-2>"]
    },
    "demo": {}
  }
  ```

  The `tokens` field controls access:
  - missing/null: open access (no token required)
  - [] (empty array): locked (no valid tokens, access denied)
  - ["hash1", ...]: restricted to listed token hashes
  """

  alias Coflux.ProjectStore

  @doc """
  Checks if the given token is authorized for the project.

  Returns `:ok` when access is allowed.
  Returns `{:error, :unauthorized}` otherwise.
  """
  def check(token, project_id) do
    case ProjectStore.get_tokens(project_id) do
      {:ok, nil} ->
        # No token auth configured - open access
        :ok

      {:ok, []} ->
        # Empty tokens list - locked, no valid tokens
        {:error, :unauthorized}

      {:ok, tokens} ->
        # Token list configured - must provide valid token
        validate_token(token, tokens)

      :error ->
        # Project not found (shouldn't happen if validate_project passed)
        {:error, :unauthorized}
    end
  end

  defp validate_token(nil, _tokens), do: {:error, :unauthorized}

  defp validate_token(token, tokens) do
    token_hash = hash_token(token)

    if token_hash in tokens do
      :ok
    else
      {:error, :unauthorized}
    end
  end

  defp hash_token(token) do
    :crypto.hash(:sha256, token)
    |> Base.encode16(case: :lower)
  end
end
