defmodule Coflux.Orchestration.Sessions do
  alias Coflux.Orchestration.TagSets

  import Coflux.Store

  def create_session(db, workspace_id, worker_id, opts \\ []) do
    provides = Keyword.get(opts, :provides)
    concurrency = Keyword.get(opts, :concurrency, 0)
    activation_timeout = Keyword.get(opts, :activation_timeout)
    reconnection_timeout = Keyword.get(opts, :reconnection_timeout)
    created_by = Keyword.get(opts, :created_by)

    with_transaction(db, fn ->
      case generate_external_id(db, :sessions, 12) do
        {:ok, external_id} ->
          secret = generate_secret(24)
          secret_hash = hash_secret(secret)
          token = "#{external_id}.#{secret}"

          provides_tag_set_id =
            if provides && Enum.any?(provides) do
              case TagSets.get_or_create_tag_set_id(db, provides) do
                {:ok, tag_set_id} ->
                  tag_set_id
              end
            end

          now = current_timestamp()

          case insert_one(db, :sessions, %{
                 external_id: external_id,
                 workspace_id: workspace_id,
                 worker_id: worker_id,
                 provides_tag_set_id: provides_tag_set_id,
                 concurrency: concurrency,
                 activation_timeout: activation_timeout,
                 reconnection_timeout: reconnection_timeout,
                 secret_hash: {:blob, secret_hash},
                 created_at: now,
                 created_by: created_by
               }) do
            {:ok, session_id} ->
              {:ok, session_id, external_id, token, secret_hash, now}
          end
      end
    end)
  end

  def parse_token(token) do
    case String.split(token, ".", parts: 2) do
      [external_id, secret] -> {:ok, external_id, secret}
      _ -> :error
    end
  end

  def verify_secret(_secret, nil), do: false

  def verify_secret(secret, secret_hash) do
    hash_secret(secret) == secret_hash
  end

  defp generate_secret(length) do
    :crypto.strong_rand_bytes(length)
    |> Base.url_encode64(padding: false)
    |> binary_part(0, length)
  end

  defp hash_secret(secret) do
    :crypto.hash(:sha256, secret)
  end

  def activate_session(db, session_id) do
    now = current_timestamp()

    case insert_one(db, :session_activations, %{
           session_id: session_id,
           created_at: now
         }) do
      {:ok, _} -> {:ok, now}
    end
  end

  def expire_session(db, session_id) do
    now = current_timestamp()

    case insert_one(db, :session_expirations, %{
           session_id: session_id,
           created_at: now
         }) do
      {:ok, _} -> {:ok, now}
    end
  end

  def load_active_sessions(db) do
    query(
      db,
      """
      SELECT
        s.id,
        s.external_id,
        s.workspace_id,
        s.worker_id,
        s.provides_tag_set_id,
        s.concurrency,
        s.activation_timeout,
        s.reconnection_timeout,
        s.secret_hash,
        s.created_at,
        sa.created_at AS activated_at
      FROM sessions s
      LEFT JOIN session_activations sa ON sa.session_id = s.id
      WHERE NOT EXISTS (
        SELECT 1 FROM session_expirations se WHERE se.session_id = s.id
      )
      """,
      {}
    )
  end

  defp current_timestamp() do
    System.os_time(:millisecond)
  end
end
