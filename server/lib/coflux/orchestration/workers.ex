defmodule Coflux.Orchestration.Workers do
  import Coflux.Store

  def create_worker(db, pool_id) do
    now = current_timestamp()

    case insert_one(db, :workers, %{
           pool_id: pool_id,
           created_at: now
         }) do
      {:ok, worker_id} ->
        {:ok, worker_id, now}
    end
  end

  def create_worker_launch_result(db, worker_id, data, error) do
    now = current_timestamp()

    case insert_one(db, :worker_launch_results, %{
           worker_id: worker_id,
           data: if(data, do: {:blob, :erlang.term_to_binary(data)}),
           error: if(error, do: Jason.encode!(error)),
           created_at: now
         }) do
      {:ok, _} ->
        {:ok, now}
    end
  end

  def create_worker_state(db, worker_id, state) do
    case insert_one(db, :worker_states, %{
           worker_id: worker_id,
           state: encode_state(state),
           created_at: current_timestamp()
         }) do
      {:ok, _} -> :ok
    end
  end

  def create_worker_stop(db, worker_id) do
    now = current_timestamp()

    case insert_one(db, :worker_stops, %{
           worker_id: worker_id,
           created_at: now
         }) do
      {:ok, worker_stop_id} -> {:ok, worker_stop_id, now}
    end
  end

  def create_worker_stop_result(db, worker_stop_id, error) do
    now = current_timestamp()

    case insert_one(db, :worker_stop_results, %{
           worker_stop_id: worker_stop_id,
           error: if(error, do: Jason.encode!(error)),
           created_at: now
         }) do
      {:ok, _} -> {:ok, now}
    end
  end

  def create_worker_deactivation(db, worker_id, error) do
    now = current_timestamp()

    case insert_one(db, :worker_deactivations, %{
           worker_id: worker_id,
           error: error,
           created_at: now
         }) do
      {:ok, _} ->
        {:ok, now}
    end
  end

  def get_active_workers(db) do
    case query(
           db,
           """
           SELECT
             w.id,
             w.created_at,
             p.id,
             p.name,
             p.workspace_id,
             (SELECT ws.state
               FROM worker_states AS ws
               WHERE ws.worker_id = w.id
               ORDER BY ws.created_at DESC
               LIMIT 1) AS state,
             r.data
           FROM workers AS w
           INNER JOIN pools AS p ON p.id = w.pool_id
           LEFT JOIN worker_launch_results AS r ON r.worker_id = w.id
           LEFT JOIN worker_stops AS s ON s.id = (
             SELECT id
             FROM worker_stops
             WHERE worker_id = w.id
             ORDER BY created_at DESC
             LIMIT 1
           )
           LEFT JOIN worker_stop_results AS sr ON sr.worker_stop_id = s.id
           LEFT JOIN worker_deactivations AS d ON d.worker_id = w.id
           WHERE d.created_at IS NULL
           ORDER BY w.created_at DESC
           """
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.map(
           rows,
           fn {worker_id, created_at, pool_id, pool_name, workspace_id, state, data} ->
             {worker_id, created_at, pool_id, pool_name, workspace_id, decode_state(state),
              if(data, do: :erlang.binary_to_term(data))}
           end
         )}
    end
  end

  def get_pool_workers(db, pool_name, limit \\ 100) do
    # TODO: decode errors?
    query(
      db,
      """
      SELECT w.id, w.created_at, r.created_at, r.error, s.created_at, sr.created_at, sr.error,
             d.created_at, d.error
      FROM workers AS w
      INNER JOIN pools AS p ON p.id = w.pool_id
      LEFT JOIN worker_launch_results AS r ON r.worker_id = w.id
      LEFT JOIN worker_stops AS s ON s.id = (
        SELECT id
        FROM worker_stops
        WHERE worker_id = w.id
        ORDER BY created_at DESC
        LIMIT 1
      )
      LEFT JOIN worker_stop_results AS sr ON sr.worker_stop_id = s.id
      LEFT JOIN worker_deactivations AS d ON d.worker_id = w.id
      WHERE p.name = ?1
      ORDER BY w.created_at DESC
      LIMIT ?2
      """,
      {pool_name, limit}
    )
  end

  defp current_timestamp() do
    System.os_time(:millisecond)
  end

  defp encode_state(state) do
    case state do
      :active -> 0
      :paused -> 1
      :draining -> 2
    end
  end

  defp decode_state(value) do
    case value do
      nil -> :active
      0 -> :active
      1 -> :paused
      2 -> :draining
    end
  end
end
