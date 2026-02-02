defmodule Coflux.Auth do
  @moduledoc """
  Handles token authentication.

  ## Token Mode (default)

  Token access is configured in $COFLUX_DATA_DIR/config/tokens.json:
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

  ## Studio Mode

  Validates JWTs issued by Coflux Studio. The JWT audience claim contains
  "{namespace}:{project}" where namespace is the team's external ID.

  Configure with:
  - COFLUX_AUTH_MODE=studio
  - COFLUX_NAMESPACES=team_id1,team_id2 (optional, restricts allowed namespaces)
  - COFLUX_STUDIO_URL=https://studio.coflux.com (default)

  ## Authentication modes (COFLUX_AUTH_MODE):
  - "token" (default): All requests require valid token from tokens.json
  - "studio": Validates JWTs issued by Studio
  - "none": No authentication required

  Workspace patterns control write access:
  - "*" matches all workspaces
  - "staging" matches exactly "staging"
  - "staging/*" matches "staging", "staging/feature1", etc.
  """

  alias Coflux.{Config, JwksStore, TokensStore}

  @type access :: %{workspaces: :all | [String.t()]}

  @expected_issuer "https://studio.coflux.com"

  @doc """
  Checks if the given token is authorized for the project.

  Args:
    - token: The authorization token (API token or JWT)
    - project_id: The project identifier (used for token auth mode)
    - host: The request host (used for studio/JWT auth mode)

  Returns `{:ok, access}` with access details when allowed.
  The access map contains:
    - workspaces: :all | [String.t()] - workspace patterns the token has access to
    - user_id: String.t() | nil - the external user ID (from JWT sub claim or nil)

  Returns `{:error, :unauthorized}` otherwise.
  """
  def check(token, project_id, host) do
    case Config.auth_mode() do
      :none ->
        {:ok, %{workspaces: :all, user_id: nil}}

      :token ->
        check_token(token, project_id)

      :studio ->
        check_jwt(token, host)
    end
  end

  @doc """
  Parses the audience claim from a JWT to extract namespace and host.

  Returns `{:ok, {namespace, host}}` or `{:error, reason}`.
  """
  def parse_audience(audience) when is_binary(audience) do
    case String.split(audience, ":", parts: 2) do
      [namespace, host] when namespace != "" and host != "" ->
        {:ok, {namespace, host}}

      _ ->
        {:error, :invalid_audience_format}
    end
  end

  def parse_audience(_), do: {:error, :invalid_audience}

  # Private functions

  defp check_token(nil, _project_id), do: {:error, :unauthorized}

  defp check_token(token, project_id) do
    token_hash = hash_token(token)

    case TokensStore.get_token_config(token_hash) do
      {:ok, config} ->
        if project_id in config.projects do
          {:ok, %{workspaces: config.workspaces, user_id: nil}}
        else
          {:error, :unauthorized}
        end

      :error ->
        {:error, :unauthorized}
    end
  end

  defp check_jwt(nil, _host), do: {:error, :unauthorized}

  defp check_jwt(token, host) do
    with {:ok, {_header, claims}} <- decode_and_verify_jwt(token),
         {:ok, {namespace, jwt_host}} <- parse_audience(claims["aud"]),
         :ok <- validate_namespace(namespace),
         :ok <- validate_host(jwt_host, host) do
      workspaces = claims["workspaces"] || ["*"]
      user_id = claims["sub"]
      {:ok, %{workspaces: normalize_workspaces(workspaces), user_id: user_id}}
    else
      {:error, _reason} ->
        {:error, :unauthorized}
    end
  end

  defp decode_and_verify_jwt(token) do
    with {:ok, header} <- peek_header(token),
         {:ok, kid} <- get_kid(header),
         {:ok, jwk} <- JwksStore.get_key(kid),
         {:ok, claims} <- verify_token(token, jwk) do
      {:ok, {header, claims}}
    end
  end

  defp peek_header(token) do
    # Joken.peek_header already returns {:ok, header_map} or {:error, reason}
    Joken.peek_header(token)
  end

  defp get_kid(%{"kid" => kid}) when is_binary(kid), do: {:ok, kid}
  defp get_kid(_), do: {:error, :missing_kid}

  defp verify_token(token, jwk) do
    case JOSE.JWT.verify_strict(jwk, ["EdDSA"], token) do
      {true, %JOSE.JWT{fields: claims}, _jws} ->
        validate_claims(claims)

      {false, _jwt, _jws} ->
        {:error, :signature_invalid}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_claims(claims) do
    now = DateTime.utc_now() |> DateTime.to_unix()

    cond do
      claims["iss"] != @expected_issuer ->
        {:error, :invalid_issuer}

      claims["exp"] && claims["exp"] < now ->
        {:error, :token_expired}

      claims["nbf"] && claims["nbf"] > now ->
        {:error, :token_not_yet_valid}

      is_nil(claims["aud"]) ->
        {:error, :missing_audience}

      true ->
        {:ok, claims}
    end
  end

  defp validate_namespace(namespace) do
    case Config.namespaces() do
      nil ->
        # No namespace restriction
        :ok

      namespaces ->
        if MapSet.member?(namespaces, namespace) do
          :ok
        else
          {:error, :namespace_not_allowed}
        end
    end
  end

  defp validate_host(jwt_host, host) do
    # The host in the JWT should match the request host
    if jwt_host == host do
      :ok
    else
      {:error, :host_mismatch}
    end
  end

  defp normalize_workspaces(workspaces) when is_list(workspaces) do
    if "*" in workspaces do
      :all
    else
      workspaces
    end
  end

  defp normalize_workspaces(_), do: :all

  defp hash_token(token) do
    :crypto.hash(:sha256, token) |> Base.encode16(case: :lower)
  end
end
