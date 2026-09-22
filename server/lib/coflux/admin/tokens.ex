defmodule Coflux.Admin.Tokens do
  @moduledoc """
  Service tokens, in the admin store.

  The admin store isn't rotated, so a token is created once and found for
  as long as it lives, whichever epoch is active. What a token *does* is
  attributed in the orchestration store, through a principal that names
  the token by external id (`Coflux.Orchestration.Principals`).

  Who created a token is kept here as an identity - a type and external
  id - rather than a principal id, since principal ids are local to an
  epoch.
  """

  alias Coflux.Store
  alias Exqlite.Sqlite3

  @doc """
  Validates a token by hash.

  Returns `{:ok, %{external_id: id, workspaces: patterns | nil}}`, with
  `nil` workspaces meaning all of them, or `{:error, :not_found}`.
  """
  def check_token(db, token_hash) do
    now = System.system_time(:second)

    query = """
      SELECT external_id, workspaces
      FROM tokens
      WHERE token_hash = ?1
        AND revoked_at IS NULL
        AND (expires_at IS NULL OR expires_at > ?2)
    """

    case Store.query_one(db, query, {token_hash, now}) do
      {:ok, {external_id, workspaces_json}} ->
        {:ok, %{external_id: external_id, workspaces: decode_workspaces(workspaces_json)}}

      {:ok, nil} ->
        {:error, :not_found}
    end
  end

  @doc """
  Creates a token. `created_by` is the creator's identity as
  `Principals.build/2` describes it, or nil.

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

    {:ok, external_id} = Store.generate_external_id(db, "tokens", 12)

    {:ok, token_id} =
      Store.insert_one(db, :tokens, %{
        external_id: external_id,
        token_hash: random_hash,
        name: name,
        workspaces: encode_workspaces(workspaces),
        created_by_type: created_by && created_by.type,
        created_by_external_id: created_by && created_by.external_id,
        created_at: now
      })

    {:ok,
     %{
       id: token_id,
       token: token,
       token_id: token_id,
       external_id: external_id,
       name: name,
       workspaces: workspaces,
       created_at: now,
       expires_at: nil,
       revoked_at: nil,
       created_by: created_by
     }}
  end

  @doc "Lists all tokens, revoked ones included, newest first."
  def list_tokens(db) do
    query = """
      SELECT id, external_id, name, workspaces, created_at, expires_at, revoked_at,
             created_by_type, created_by_external_id
      FROM tokens
      ORDER BY created_at DESC
    """

    {:ok, rows} = Store.query(db, query, {})

    {:ok, Enum.map(rows, &build_token/1)}
  end

  @doc """
  Gets a token by its external_id.
  Returns {:ok, token} or {:ok, nil} if not found.
  """
  def get_token_by_external_id(db, external_id) do
    query = """
      SELECT id, external_id, name, workspaces, created_at, expires_at, revoked_at,
             created_by_type, created_by_external_id
      FROM tokens
      WHERE external_id = ?1
    """

    case Store.query_one(db, query, {external_id}) do
      {:ok, nil} -> {:ok, nil}
      {:ok, row} -> {:ok, build_token(row)}
    end
  end

  @doc """
  Revokes a token by ID.
  """
  def revoke_token(db, token_id) do
    now = System.system_time(:second)

    case Store.query_one(db, "SELECT id, external_id FROM tokens WHERE id = ?1", {token_id}) do
      {:ok, {^token_id, external_id}} ->
        {:ok, _} =
          Store.query(db, "UPDATE tokens SET revoked_at = ?1 WHERE id = ?2", {now, token_id})

        {:ok, external_id}

      {:ok, nil} ->
        {:error, :not_found}
    end
  end

  @doc """
  Moves tokens out of an orchestration database that still has them - one
  from before the admin store existed - and drops the table. Nothing to do
  when the table is already gone. Safe to repeat: a token that is already
  in the admin store is left as it is.

  Runs after the orchestration migrations, which is what leaves the
  principals there naming tokens by external id.
  """
  def import_legacy(orchestration_db, admin_db) do
    case Store.query_one(
           orchestration_db,
           "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'tokens'",
           {}
         ) do
      {:ok, nil} ->
        :ok

      {:ok, _} ->
        {:ok, rows} =
          Store.query(
            orchestration_db,
            """
            SELECT t.id, t.external_id, t.token_hash, t.name, t.workspaces,
                   t.created_at, t.expires_at, t.revoked_at,
                   p.user_external_id, p.token_external_id
            FROM tokens AS t
            LEFT JOIN principals AS p ON p.id = t.created_by
            """,
            {}
          )

        values =
          Enum.map(rows, fn {id, external_id, token_hash, name, workspaces, created_at,
                             expires_at, revoked_at, creator_user_id, creator_token_id} ->
            {type, creator_id} =
              cond do
                creator_user_id -> {"user", creator_user_id}
                creator_token_id -> {"token", creator_token_id}
                true -> {nil, nil}
              end

            {id, external_id, token_hash, name, workspaces, type, creator_id, created_at,
             expires_at, revoked_at}
          end)

        {:ok, _} =
          Store.insert_many(
            admin_db,
            :tokens,
            {:id, :external_id, :token_hash, :name, :workspaces, :created_by_type,
             :created_by_external_id, :created_at, :expires_at, :revoked_at},
            values,
            on_conflict: "(external_id) DO NOTHING"
          )

        :ok = Sqlite3.execute(orchestration_db, "DROP TABLE tokens")
    end
  end

  defp build_token(
         {id, external_id, name, workspaces_json, created_at, expires_at, revoked_at,
          created_by_type, created_by_external_id}
       ) do
    %{
      id: id,
      external_id: external_id,
      name: name,
      workspaces: decode_workspaces(workspaces_json),
      created_at: created_at,
      expires_at: expires_at,
      revoked_at: revoked_at,
      created_by: build_created_by(created_by_type, created_by_external_id)
    }
  end

  defp build_created_by(nil, nil), do: nil
  defp build_created_by(type, external_id), do: %{type: type, external_id: external_id}

  defp decode_workspaces(nil), do: nil
  defp decode_workspaces(json), do: Jason.decode!(json)

  defp encode_workspaces(nil), do: nil
  defp encode_workspaces(workspaces), do: Jason.encode!(workspaces)
end
