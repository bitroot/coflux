defmodule Coflux.Logs.Store do
  @moduledoc """
  SQLite database operations for logs storage.

  Provides functions for:
  - Opening/creating logs databases
  - Template deduplication (for space efficiency)
  - Batch insert of log entries
  - Querying logs with filters
  """

  alias Coflux.Store

  @doc """
  Opens or creates a logs database for the given project.
  """
  def open(project_id) do
    Store.open(project_id, "logs")
  end

  @doc """
  Close the database connection.
  """
  def close(db) do
    Store.close(db)
  end

  @doc """
  Get or create a template, returning its ID.

  Uses SHA256 hash for fast lookup and deduplication.
  """
  def get_or_create_template(db, template) when is_binary(template) do
    hash = :crypto.hash(:sha256, template)

    # Try to find existing template
    case Store.query_one(db, "SELECT id FROM templates WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {id}} ->
        {:ok, id}

      {:ok, nil} ->
        # Insert new template
        Store.insert_one(db, "templates", hash: {:blob, hash}, template: template)
    end
  end

  def get_or_create_template(_db, nil), do: {:ok, nil}

  @doc """
  Insert a batch of log entries.

  Each entry should be a map with:
  - run_id: string
  - execution_id: integer
  - workspace_id: integer
  - timestamp: integer (unix ms)
  - level: integer (0-5)
  - template: string or nil
  - values: map of values (will be JSON encoded)

  Returns {:ok, template_cache} on success.
  """
  def insert_logs(db, entries, template_cache) when is_list(entries) do
    now = System.system_time(:millisecond)

    # Process entries and update template cache
    {rows, template_cache} =
      Enum.map_reduce(entries, template_cache, fn entry, t_cache ->
        {template_id, t_cache} = resolve_template(db, entry.template, t_cache)

        # Values are already validated JSON maps - store directly
        values_json =
          if entry.values && map_size(entry.values) > 0 do
            Jason.encode!(entry.values)
          else
            nil
          end

        row = {
          entry.run_id,
          entry.execution_id,
          entry.workspace_id,
          entry.timestamp,
          entry.level,
          template_id,
          values_json,
          now
        }

        {row, t_cache}
      end)

    fields = {:run_id, :execution_id, :workspace_id, :timestamp, :level, :template_id, :values_json, :created_at}

    case Store.insert_many(db, "messages", fields, rows) do
      {:ok, _ids} -> {:ok, template_cache}
      {:error, reason} -> {:error, reason}
    end
  end

  defp resolve_template(_db, nil, cache), do: {nil, cache}

  defp resolve_template(db, template, cache) do
    hash = :crypto.hash(:sha256, template)

    case Map.get(cache, hash) do
      nil ->
        {:ok, id} = get_or_create_template(db, template)
        {id, Map.put(cache, hash, id)}

      id ->
        {id, cache}
    end
  end

  @doc """
  Query logs for a run with optional filters.

  Options:
  - :run_id (required) - the run ID to query
  - :execution_id - filter by execution ID
  - :workspace_ids - list of workspace IDs to include
  - :after - cursor for pagination (timestamp:id format)
  - :limit - max entries to return (default 1000)
  """
  def query_logs(db, opts) do
    run_id = Keyword.fetch!(opts, :run_id)
    execution_id = Keyword.get(opts, :execution_id)
    workspace_ids = Keyword.get(opts, :workspace_ids)
    after_cursor = Keyword.get(opts, :after)
    limit = Keyword.get(opts, :limit, 1000)

    {where_clauses, args} = build_where_clauses(run_id, execution_id, workspace_ids, after_cursor)

    sql = """
    SELECT m.id, m.run_id, m.execution_id, m.workspace_id, m.timestamp, m.level, t.template, m.values_json, m.created_at
    FROM messages m
    LEFT JOIN templates t ON m.template_id = t.id
    WHERE #{Enum.join(where_clauses, " AND ")}
    ORDER BY m.timestamp ASC, m.id ASC
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

  defp build_where_clauses(run_id, execution_id, workspace_ids, after_cursor) do
    clauses = ["m.run_id = ?1"]
    args = [run_id]

    {clauses, args} =
      if execution_id do
        {clauses ++ ["m.execution_id = ?#{length(args) + 1}"], args ++ [execution_id]}
      else
        {clauses, args}
      end

    {clauses, args} =
      if workspace_ids && workspace_ids != [] do
        # Build IN clause with positional parameters
        start_idx = length(args) + 1
        placeholders = workspace_ids |> Enum.with_index(start_idx) |> Enum.map(fn {_, i} -> "?#{i}" end)
        clause = "m.workspace_id IN (#{Enum.join(placeholders, ", ")})"
        {clauses ++ [clause], args ++ workspace_ids}
      else
        {clauses, args}
      end

    {clauses, args} =
      if after_cursor do
        case parse_cursor(after_cursor) do
          {:ok, timestamp, id} ->
            clause = "(m.timestamp > ?#{length(args) + 1} OR (m.timestamp = ?#{length(args) + 1} AND m.id > ?#{length(args) + 2}))"
            {clauses ++ [clause], args ++ [timestamp, id]}

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
      [timestamp_str, id_str] ->
        case {Integer.parse(timestamp_str), Integer.parse(id_str)} do
          {{timestamp, ""}, {id, ""}} -> {:ok, timestamp, id}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp parse_cursor(_), do: :error

  defp build_cursor(nil), do: nil

  defp build_cursor(entry) do
    "#{entry.timestamp}:#{entry.id}"
  end

  defp row_to_entry({id, run_id, execution_id, workspace_id, timestamp, level, template, values_json, _created_at}) do
    # Values are stored as validated JSON - return directly
    values =
      if values_json do
        case Jason.decode(values_json) do
          {:ok, v} when is_map(v) -> v
          _ -> %{}
        end
      else
        %{}
      end

    %{
      id: id,
      run_id: run_id,
      execution_id: execution_id,
      workspace_id: workspace_id,
      timestamp: timestamp,
      level: level,
      template: template,
      values: values
    }
  end
end
