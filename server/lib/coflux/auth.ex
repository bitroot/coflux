defmodule Coflux.Auth do
  @moduledoc """
  Handles authentication for API requests.

  ## Super Token

  Set COFLUX_SUPER_TOKEN to enable a super token with full access. This token
  can be used to bootstrap the system and create other tokens.

  ## Signed API Tokens

  Tokens created via the API have the format: `cflx_<random><signature>`

  - random: 16 bytes (32 hex chars) - unique identifier, hash stored in DB
  - signature: 4 bytes (8 hex chars) - HMAC signature for quick validation

  The signature allows rejecting invalid tokens without starting the project's
  orchestration server. Valid signatures still require a database check for
  revocation and workspace restrictions.

  Signature computation:
  - project_key = HMAC-SHA256(COFLUX_SECRET, project_id)
  - signature = first 4 bytes of HMAC-SHA256(project_key, random_hex)

  Requires COFLUX_SECRET to be configured. Token creation and validation will
  fail if this is not set.

  ## Studio Authentication (JWT)

  Validates JWTs issued by Coflux Studio. The JWT audience claim contains
  "{namespace}:{host}" where namespace is the team's external ID.

  Studio auth is enabled when COFLUX_NAMESPACES is set:
  - COFLUX_NAMESPACES=team_id1,team_id2 (comma-separated list of allowed namespaces)
  - COFLUX_STUDIO_URL=https://studio.coflux.com (default, for JWKS)

  ## Authentication Flow

  Multiple auth methods can be enabled simultaneously. The auth method is
  determined by token format:
  - `cflx_...` (45 chars) → Signed API token
  - JWTs (containing dots) → Studio auth
  - Other tokens → Super token (legacy database tokens deprecated)

  Set COFLUX_REQUIRE_AUTH=false to allow anonymous requests (auth still works
  if credentials are provided).

  ## Workspace Patterns

  Workspace patterns control write access:
  - "*" matches all workspaces
  - "staging" matches exactly "staging"
  - "staging/*" matches "staging", "staging/feature1", etc.
  """

  alias Coflux.{Config, JwksStore, Orchestration}

  @type access :: %{workspaces: :all | [String.t()], principal_id: integer() | nil}

  @doc """
  Checks if the given token is authorized for the project.

  Args:
    - token: The authorization token (API token or JWT), or nil
    - project_id: The project identifier (used for token auth and principal lookup)
    - host: The request host (used for Studio/JWT auth)

  Returns `{:ok, access}` with access details when allowed.
  The access map contains:
    - workspaces: :all | [String.t()] - workspace patterns the token has access to
    - principal_id: integer() | nil - the database ID of the principal (nil for super token or anonymous)

  Returns `{:error, :unauthorized}` otherwise.

  Authentication flow:
  1. No token → allow if COFLUX_REQUIRE_AUTH=false, else reject
  2. JWT format (has dots) → try Studio auth, resolve user to principal_id
  3. Other format → try super token (principal_id=nil), then database tokens (returns principal_id)
  """
  def check(nil, _project_id, _host) do
    if Config.require_auth?() do
      {:error, :unauthorized}
    else
      {:ok, %{workspaces: :all, principal_id: nil}}
    end
  end

  def check(token, project_id, host) do
    cond do
      is_jwt?(token) ->
        # JWT format - try Studio auth
        if Config.namespaces() do
          check_jwt(token, project_id, host)
        else
          {:error, :unauthorized}
        end

      is_signed_token?(token) ->
        # Signed API token (cflx_...) - verify signature, then check database
        check_signed_token(token, project_id)

      true ->
        # Other format - try super token, then legacy database tokens
        check_api_token(token, project_id)
    end
  end

  # Check if token looks like a JWT (has two dots separating three parts)
  defp is_jwt?(token) do
    case String.split(token, ".") do
      [_, _, _] -> true
      _ -> false
    end
  end

  # Check if token is a signed API token (starts with cflx_)
  defp is_signed_token?(token) do
    String.starts_with?(token, "cflx_")
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

  @doc """
  Hashes a token using SHA-256.
  """
  def hash_token(token) do
    :crypto.hash(:sha256, token) |> Base.encode16(case: :lower)
  end

  # Private functions

  defp check_api_token(token, project_id) do
    # First check super token
    case check_super_token(token) do
      {:ok, access} ->
        {:ok, access}

      :error ->
        # Then check database tokens
        check_database_token(token, project_id)
    end
  end

  defp check_super_token(token) do
    case Config.super_token_hash() do
      nil ->
        :error

      expected_hash ->
        if hash_token(token) == expected_hash do
          # Super token has full access, no principal
          {:ok, %{workspaces: :all, principal_id: nil}}
        else
          :error
        end
    end
  end

  defp check_database_token(token, project_id) do
    token_hash = hash_token(token)

    case Orchestration.check_token(project_id, token_hash) do
      {:ok, %{workspaces: workspaces, principal_id: principal_id}} ->
        {:ok, %{workspaces: normalize_workspaces(workspaces), principal_id: principal_id}}

      {:error, _reason} ->
        {:error, :unauthorized}
    end
  end

  defp check_signed_token(token, project_id) do
    # Token format: cflx_<random:32hex><signature:8hex>
    # Total: 5 + 32 + 8 = 45 chars
    # Requires COFLUX_SECRET to be configured
    if Config.secret() == nil do
      {:error, :unauthorized}
    else
      case parse_signed_token(token) do
        {:ok, random_hex, signature_hex} ->
          if verify_token_signature(random_hex, signature_hex, project_id) do
            # Signature valid - check database for revocation/expiry
            check_database_token_by_random(random_hex, project_id)
          else
            {:error, :unauthorized}
          end

        :error ->
          {:error, :unauthorized}
      end
    end
  end

  defp parse_signed_token(<<"cflx_", rest::binary>>) when byte_size(rest) == 40 do
    <<random_hex::binary-size(32), signature_hex::binary-size(8)>> = rest
    {:ok, random_hex, signature_hex}
  end

  defp parse_signed_token(_), do: :error

  defp verify_token_signature(random_hex, signature_hex, project_id) do
    expected_signature = compute_token_signature(random_hex, project_id)
    # Constant-time comparison to prevent timing attacks
    :crypto.hash_equals(expected_signature, signature_hex)
  end

  @doc """
  Computes the signature for a token's random part.

  The signature is the first 4 bytes (8 hex chars) of:
  HMAC-SHA256(project_key, random_hex)

  Where project_key = HMAC-SHA256(server_secret, project_id)
  """
  def compute_token_signature(random_hex, project_id) do
    project_key = derive_project_key(project_id)

    :crypto.mac(:hmac, :sha256, project_key, random_hex)
    |> binary_part(0, 4)
    |> Base.encode16(case: :lower)
  end

  defp derive_project_key(project_id) do
    :crypto.mac(:hmac, :sha256, Config.secret(), project_id)
  end

  defp check_database_token_by_random(random_hex, project_id) do
    # Hash the random part for database lookup
    random_hash = hash_token(random_hex)

    case Orchestration.check_token(project_id, random_hash) do
      {:ok, %{workspaces: workspaces, principal_id: principal_id}} ->
        {:ok, %{workspaces: normalize_workspaces(workspaces), principal_id: principal_id}}

      {:error, _reason} ->
        {:error, :unauthorized}
    end
  end

  defp check_jwt(token, project_id, host) do
    with {:ok, {_header, claims}} <- decode_and_verify_jwt(token),
         {:ok, {namespace, jwt_host}} <- parse_audience(claims["aud"]),
         :ok <- validate_namespace(namespace),
         :ok <- validate_host(jwt_host, host) do
      workspaces = claims["workspaces"] || ["*"]
      external_id = claims["sub"]
      # Resolve external_id to principal_id
      {:ok, principal_id} = Orchestration.ensure_principal(project_id, external_id)
      {:ok, %{workspaces: normalize_workspaces(workspaces), principal_id: principal_id}}
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
      claims["iss"] != Config.studio_url() ->
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
end
