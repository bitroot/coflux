defmodule Coflux.Orchestration.Sessions do
  alias Coflux.Orchestration.TagSets

  import Coflux.Store

  def create_session(db, space_id, provides, worker_id, concurrency) do
    with_transaction(db, fn ->
      case generate_external_id(db, :sessions, 30) do
        {:ok, external_id} ->
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
                 space_id: space_id,
                 worker_id: worker_id,
                 provides_tag_set_id: provides_tag_set_id,
                 concurrency: concurrency || 0,
                 created_at: now
               }) do
            {:ok, session_id} ->
              {:ok, session_id, external_id, now}
          end
      end
    end)
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
    query_all(
      db,
      """
      SELECT
        s.id,
        s.external_id,
        s.space_id,
        s.worker_id,
        s.provides_tag_set_id,
        s.concurrency,
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
