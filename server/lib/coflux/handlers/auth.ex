defmodule Coflux.Handlers.Auth do
  @moduledoc """
  Handles token authentication for API and WebSocket requests.

  Tokens are stored in $COFLUX_DATA_DIR/tokens.json with format:
  ```json
  {
    "<sha256-hash>": {
      "namespaces": ["test", "acme", null]
    }
  }
  ```

  - Key: SHA-256 hash of token (hex, lowercase)
  - namespaces: array of allowed namespaces (null = default namespace)
  - If namespaces omitted, defaults to [null]

  Auth mode is controlled by COFLUX_AUTH_MODE:
  - "none" (default): No authentication required
  - "token": Require valid token with namespace access
  """

  alias Coflux.Config
  alias Coflux.Auth.TokenStore

  @doc """
  Checks if the request is authorized for the given namespace.
  Extracts token from Authorization header or query string.

  Returns `:ok` or `{:error, :unauthorized}`.
  """
  def check(req, namespace) do
    case Config.auth_mode() do
      :none ->
        :ok

      :token ->
        case get_token(req) do
          nil -> {:error, :unauthorized}
          token -> check_token(token, namespace)
        end
    end
  end

  @doc """
  Checks if the given token is authorized for the namespace.

  Returns `:ok` or `{:error, :unauthorized}`.
  """
  def check_token(token, namespace) do
    case Config.auth_mode() do
      :none ->
        :ok

      :token ->
        validate_token(token, namespace)
    end
  end

  defp validate_token(nil, _namespace), do: {:error, :unauthorized}

  defp validate_token(token, namespace) do
    token_hash = hash_token(token)

    case TokenStore.lookup(token_hash) do
      {:ok, token_config} ->
        if namespace in token_config.namespaces do
          :ok
        else
          {:error, :unauthorized}
        end

      :error ->
        {:error, :unauthorized}
    end
  end

  defp get_token(req) do
    get_bearer_token(req) || get_query_token(req)
  end

  defp get_bearer_token(req) do
    case :cowboy_req.header("authorization", req) do
      :undefined ->
        nil

      header ->
        case String.split(header, " ", parts: 2) do
          ["Bearer", token] -> String.trim(token)
          _ -> nil
        end
    end
  end

  defp get_query_token(req) do
    qs = :cowboy_req.parse_qs(req)

    case List.keyfind(qs, "token", 0) do
      {"token", token} when is_binary(token) and token != "" -> token
      _ -> nil
    end
  end

  defp hash_token(token) do
    :crypto.hash(:sha256, token)
    |> Base.encode16(case: :lower)
  end
end
