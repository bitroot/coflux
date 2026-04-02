defmodule Coflux.Metrics.Store do
  @moduledoc """
  SQLite database operations for metrics storage.

  Provides functions for batch insert of metric entries and querying
  metrics with filters.
  """

  alias Coflux.Store

  @doc """
  Insert a batch of metric entries.

  Each entry should be a map with:
  - run_id: string
  - execution_id: string
  - workspace_id: string
  - key: string
  - value: float
  - at: float
  """
  def insert_metrics(db, entries) when is_list(entries) do
    now = System.system_time(:millisecond)

    rows =
      Enum.map(entries, fn entry ->
        {
          entry.run_id,
          entry.execution_id,
          entry.workspace_id,
          entry.key,
          entry.value,
          entry.at,
          now
        }
      end)

    fields = {:run_id, :execution_id, :workspace_id, :key, :value, :at, :created_at}

    case Store.insert_many(db, "metrics", fields, rows) do
      {:ok, _ids} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Query metrics with optional filters.

  Options:
  - :run_id - filter by run ID (required unless :execution_id is given)
  - :execution_id - filter by execution ID
  - :workspace_ids - list of workspace IDs to include
  - :keys - list of metric key names to include
  - :after - cursor for pagination (at:id format)
  - :limit - max entries to return (default 1000)
  """
  def query_metrics(db, opts) do
    run_id = Keyword.get(opts, :run_id)
    execution_id = Keyword.get(opts, :execution_id)
    workspace_ids = Keyword.get(opts, :workspace_ids)
    keys = Keyword.get(opts, :keys)
    after_cursor = Keyword.get(opts, :after)
    limit = Keyword.get(opts, :limit, 1000)

    {where_clauses, args} =
      build_where_clauses(run_id, execution_id, workspace_ids, keys, after_cursor)

    sql = """
    SELECT m.id, m.run_id, m.execution_id, m.workspace_id, m.key, m.value, m.at, m.created_at
    FROM metrics m
    WHERE #{Enum.join(where_clauses, " AND ")}
    ORDER BY m.at ASC, m.id ASC
    LIMIT ?#{length(args) + 1}
    """

    args = List.to_tuple(args ++ [limit])

    case Store.query(db, sql, args) do
      {:ok, rows} ->
        entries = Enum.map(rows, &row_to_entry/1)
        cursor = build_cursor(List.last(entries))
        {:ok, entries, cursor}
    end
  end

  defp build_where_clauses(run_id, execution_id, workspace_ids, keys, after_cursor) do
    {clauses, args} =
      cond do
        execution_id ->
          {["m.execution_id = ?1"], [execution_id]}

        run_id ->
          {["m.run_id = ?1"], [run_id]}

        true ->
          {["1 = 0"], []}
      end

    {clauses, args} =
      if workspace_ids && workspace_ids != [] do
        start_idx = length(args) + 1

        placeholders =
          workspace_ids |> Enum.with_index(start_idx) |> Enum.map(fn {_, i} -> "?#{i}" end)

        clause = "m.workspace_id IN (#{Enum.join(placeholders, ", ")})"
        {clauses ++ [clause], args ++ workspace_ids}
      else
        {clauses, args}
      end

    {clauses, args} =
      if keys && keys != [] do
        start_idx = length(args) + 1

        placeholders =
          keys |> Enum.with_index(start_idx) |> Enum.map(fn {_, i} -> "?#{i}" end)

        clause = "m.key IN (#{Enum.join(placeholders, ", ")})"
        {clauses ++ [clause], args ++ keys}
      else
        {clauses, args}
      end

    {clauses, args} =
      if after_cursor do
        case parse_cursor(after_cursor) do
          {:ok, at, id} ->
            clause =
              "(m.at > ?#{length(args) + 1} OR (m.at = ?#{length(args) + 1} AND m.id > ?#{length(args) + 2}))"

            {clauses ++ [clause], args ++ [at, id]}

          :error ->
            {clauses, args}
        end
      else
        {clauses, args}
      end

    {clauses, args}
  end

  defp parse_cursor(cursor) when is_binary(cursor) do
    case String.split(cursor, ":") do
      [at_str, id_str] ->
        case {Float.parse(at_str), Integer.parse(id_str)} do
          {{at, ""}, {id, ""}} -> {:ok, at, id}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp parse_cursor(_), do: :error

  defp build_cursor(nil), do: nil

  defp build_cursor(entry) do
    "#{entry.at}:#{entry.id}"
  end

  defp row_to_entry({id, run_id, execution_id, workspace_id, key, value, at, _created_at}) do
    %{
      id: id,
      run_id: run_id,
      execution_id: execution_id,
      workspace_id: workspace_id,
      key: key,
      value: value,
      at: at
    }
  end
end
