defmodule Coflux.Orchestration.Principals do
  @moduledoc """
  Manages principals (users and tokens) in the orchestration database.

  A principal represents an authenticated identity - either a user (from JWT)
  or a token (from API token auth).
  - Users have `user_external_id` set (from JWT sub claim)
  - Tokens have `token_id` set, with `external_id` on the tokens table
  """

  alias Coflux.Store

  @doc """
  Looks up or creates a user principal by external_id (JWT sub claim).
  Returns {:ok, principal_id}.
  """
  def ensure_user(db, external_id) do
    case Store.query_one(db, "SELECT id FROM principals WHERE user_external_id = ?1", {external_id}) do
      {:ok, {id}} -> {:ok, id}
      {:ok, nil} ->
        Store.insert_one(db, :principals, %{user_external_id: external_id})
    end
  end

  @doc """
  Gets the type and external_id for a principal.
  Returns {:ok, {type, external_id}} or {:ok, nil} if not found.
  """
  def get_principal(_db, nil), do: {:ok, nil}

  def get_principal(db, principal_id) do
    query = """
      SELECT p.user_external_id, t.external_id
      FROM principals p
      LEFT JOIN tokens t ON p.token_id = t.id
      WHERE p.id = ?1
    """

    case Store.query_one(db, query, {principal_id}) do
      {:ok, {user_external_id, nil}} ->
        {:ok, {"user", user_external_id}}

      {:ok, {nil, token_external_id}} ->
        {:ok, {"token", token_external_id}}

      {:ok, nil} ->
        {:ok, nil}
    end
  end

  @doc """
  Validates a token by hash and returns the principal_id and workspaces if valid.
  Returns {:ok, %{principal_id: id, workspaces: list | nil}} or {:error, :not_found}.
  Workspaces is nil for all-access, or a list of patterns.
  """
  def check_token(db, token_hash) do
    now = System.system_time(:second)
    query = """
      SELECT p.id, t.workspaces
      FROM tokens t
      JOIN principals p ON p.token_id = t.id
      WHERE t.token_hash = ?1
        AND t.revoked_at IS NULL
        AND (t.expires_at IS NULL OR t.expires_at > ?2)
    """

    case Store.query_one(db, query, {token_hash, now}) do
      {:ok, {principal_id, workspaces_json}} ->
        workspaces = decode_workspaces(workspaces_json)
        {:ok, %{principal_id: principal_id, workspaces: workspaces}}

      {:ok, nil} ->
        {:error, :not_found}
    end
  end

  defp decode_workspaces(nil), do: nil
  defp decode_workspaces(json), do: Jason.decode!(json)

  @doc """
  Creates a new token and its associated principal.
  Returns {:ok, %{token: token, token_id: token_id, principal_id: principal_id, external_id: external_id}}.

  Options:
    - workspaces: list of workspace patterns, or nil for all workspaces
  """
  def create_token(db, project_id, name, created_by, opts \\ []) do
    now = System.system_time(:second)
    workspaces = Keyword.get(opts, :workspaces)

    # Generate random bytes (16 bytes = 32 hex chars)
    random = :crypto.strong_rand_bytes(16)
    random_hex = Base.encode16(random, case: :lower)

    # Compute signature (first 4 bytes of HMAC)
    signature_hex = Coflux.Auth.compute_token_signature(random_hex, project_id)

    # Full token: cflx_<random><signature>
    token = "cflx_" <> random_hex <> signature_hex

    # Store hash of random part for lookup
    random_hash = :crypto.hash(:sha256, random_hex) |> Base.encode16(case: :lower)

    # Generate external_id for the token
    {:ok, external_id} = Store.generate_external_id(db, "tokens", 12)

    # Insert token with external_id
    {:ok, token_id} = Store.insert_one(db, :tokens, %{
      external_id: external_id,
      token_hash: random_hash,
      name: name,
      workspaces: encode_workspaces(workspaces),
      created_by: created_by,
      created_at: now
    })

    # Create principal for this token
    {:ok, principal_id} = Store.insert_one(db, :principals, %{
      token_id: token_id
    })

    {:ok, %{token: token, token_id: token_id, principal_id: principal_id, external_id: external_id}}
  end

  defp encode_workspaces(nil), do: nil
  defp encode_workspaces(workspaces), do: Jason.encode!(workspaces)

  @doc """
  Lists all tokens with their metadata.
  """
  def list_tokens(db) do
    query = """
      SELECT t.id, t.external_id, t.name, t.workspaces, t.created_at, t.expires_at, t.revoked_at,
             t.created_by, p_creator.user_external_id, t_creator.external_id
      FROM tokens t
      LEFT JOIN principals p_creator ON t.created_by = p_creator.id
      LEFT JOIN tokens t_creator ON p_creator.token_id = t_creator.id
      ORDER BY t.created_at DESC
    """

    {:ok, rows} = Store.query(db, query, {})

    tokens = Enum.map(rows, fn {id, external_id, name, workspaces_json, created_at, expires_at, revoked_at, created_by_principal_id, creator_user_ext_id, creator_token_ext_id} ->
      created_by =
        case {creator_user_ext_id, creator_token_ext_id} do
          {nil, nil} -> nil
          {user_ext_id, nil} -> %{type: "user", external_id: user_ext_id}
          {nil, token_ext_id} -> %{type: "token", external_id: token_ext_id}
        end

      %{
        id: id,
        external_id: external_id,
        name: name,
        workspaces: decode_workspaces(workspaces_json),
        created_at: created_at,
        expires_at: expires_at,
        revoked_at: revoked_at,
        created_by: created_by,
        created_by_principal_id: created_by_principal_id
      }
    end)

    {:ok, tokens}
  end

  @doc """
  Gets a token by its external_id.
  Returns {:ok, token} or {:ok, nil} if not found.
  """
  def get_token_by_external_id(db, external_id) do
    query = """
      SELECT t.id, t.external_id, t.name, t.created_at, t.expires_at, t.revoked_at, t.created_by
      FROM tokens t
      WHERE t.external_id = ?1
    """

    case Store.query_one(db, query, {external_id}) do
      {:ok, {id, ext_id, name, created_at, expires_at, revoked_at, created_by_principal_id}} ->
        {:ok, %{
          id: id,
          external_id: ext_id,
          name: name,
          created_at: created_at,
          expires_at: expires_at,
          revoked_at: revoked_at,
          created_by_principal_id: created_by_principal_id
        }}
      {:ok, nil} ->
        {:ok, nil}
    end
  end

  @doc """
  Revokes a token by ID.
  """
  def revoke_token(db, token_id) do
    now = System.system_time(:second)

    case Store.query_one(db, "SELECT id FROM tokens WHERE id = ?1", {token_id}) do
      {:ok, {^token_id}} ->
        {:ok, _} = Store.query(db, "UPDATE tokens SET revoked_at = ?1 WHERE id = ?2", {now, token_id})
        :ok

      {:ok, nil} ->
        {:error, :not_found}
    end
  end
end
