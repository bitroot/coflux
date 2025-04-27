defmodule Coflux.Orchestration.Agents do
  import Coflux.Store

  def create_agent(db, pool_id) do
    now = current_timestamp()

    case insert_one(db, :agents, %{
           pool_id: pool_id,
           created_at: now
         }) do
      {:ok, agent_id} ->
        {:ok, agent_id, now}
    end
  end

  def create_agent_launch_result(db, agent_id, data, error) do
    now = current_timestamp()

    case insert_one(db, :agent_launch_results, %{
           agent_id: agent_id,
           data: if(data, do: {:blob, :erlang.term_to_binary(data)}),
           error: if(error, do: Jason.encode!(error)),
           created_at: now
         }) do
      {:ok, _} ->
        {:ok, now}
    end
  end

  def create_agent_state(db, agent_id, state) do
    case insert_one(db, :agent_states, %{
           agent_id: agent_id,
           state: encode_state(state),
           created_at: current_timestamp()
         }) do
      {:ok, _} -> :ok
    end
  end

  def create_agent_stop(db, agent_id) do
    now = current_timestamp()

    case insert_one(db, :agent_stops, %{
           agent_id: agent_id,
           created_at: now
         }) do
      {:ok, agent_stop_id} -> {:ok, agent_stop_id, now}
    end
  end

  def create_agent_stop_result(db, agent_stop_id, error) do
    now = current_timestamp()

    case insert_one(db, :agent_stop_results, %{
           agent_stop_id: agent_stop_id,
           error: if(error, do: Jason.encode!(error)),
           created_at: now
         }) do
      {:ok, _} -> {:ok, now}
    end
  end

  def create_agent_deactivation(db, agent_id) do
    now = current_timestamp()

    case insert_one(db, :agent_deactivations, %{
           agent_id: agent_id,
           created_at: now
         }) do
      {:ok, _} ->
        {:ok, now}
    end
  end

  def get_active_agents(db) do
    case query(
           db,
           """
           SELECT
             l.id,
             l.created_at,
             p.id,
             p.name,
             p.workspace_id,
             (SELECT ls.state
               FROM agent_states AS ls
               WHERE ls.agent_id = l.id
               ORDER BY ls.created_at DESC
               LIMIT 1) AS state,
             r.data
           FROM agents AS l
           INNER JOIN pools AS p ON p.id = l.pool_id
           LEFT JOIN agent_launch_results AS r ON r.agent_id = l.id
           LEFT JOIN agent_stops AS s ON s.id = (
             SELECT id
             FROM agent_stops
             WHERE agent_id = l.id
             ORDER BY created_at DESC
             LIMIT 1
           )
           LEFT JOIN agent_stop_results AS sr ON sr.agent_stop_id = s.id
           LEFT JOIN agent_deactivations AS d ON d.agent_id = l.id
           WHERE d.created_at IS NULL
           ORDER BY l.created_at DESC
           """
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.map(
           rows,
           fn {agent_id, created_at, pool_id, pool_name, workspace_id, state, data} ->
             {agent_id, created_at, pool_id, pool_name, workspace_id, decode_state(state),
              if(data, do: :erlang.binary_to_term(data))}
           end
         )}
    end
  end

  def get_pool_agents(db, pool_name, limit \\ 100) do
    # TODO: decode errors?
    query(
      db,
      """
      SELECT l.id, l.created_at, r.created_at, r.error, s.created_at, sr.created_at, sr.error, d.created_at
      FROM agents AS l
      INNER JOIN pools AS p ON p.id = l.pool_id
      LEFT JOIN agent_launch_results AS r ON r.agent_id = l.id
      LEFT JOIN agent_stops AS s ON s.id = (
        SELECT id
        FROM agent_stops
        WHERE agent_id = l.id
        ORDER BY created_at DESC
        LIMIT 1
      )
      LEFT JOIN agent_stop_results AS sr ON sr.agent_stop_id = s.id
      LEFT JOIN agent_deactivations AS d ON d.agent_id = l.id
      WHERE p.name = ?1
      ORDER BY l.created_at DESC
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
