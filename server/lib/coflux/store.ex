defmodule Coflux.Store do
  alias Coflux.Store.Migrations
  alias Coflux.Utils
  alias Exqlite.Sqlite3

  def open(project_id, name) do
    path =
      ["projects", project_id, "#{name}.sqlite"]
      |> Path.join()
      |> Utils.data_path()

    {:ok, db} = Sqlite3.open(path)
    :ok = Migrations.run(db, name)
    {:ok, db}
  end

  def close(db) do
    Sqlite3.close(db)
  end

  def with_prepare(db, sql, fun) do
    {:ok, statement} = Sqlite3.prepare(db, sql)
    result = fun.(statement)
    :ok = Sqlite3.release(db, statement)
    result
  end

  def with_transaction(db, fun) do
    :ok = Sqlite3.execute(db, "BEGIN")

    try do
      fun.()
    rescue
      e ->
        :ok = Sqlite3.execute(db, "ROLLBACK")
        reraise e, __STACKTRACE__
    else
      result ->
        :ok = Sqlite3.execute(db, "COMMIT")
        result
    end
  end

  def with_snapshot(db, fun) do
    # SQLite's ROLLBACK TO reverts changes but leaves the savepoint on the
    # stack — a subsequent RELEASE is required to actually pop it (and
    # commit the implicit transaction when it's the outermost savepoint).
    # Missing the RELEASE leaks the implicit transaction, which breaks
    # any following BEGIN with "cannot start a transaction within a
    # transaction".
    name = "s#{:erlang.unique_integer([:positive])}"
    :ok = Sqlite3.execute(db, "SAVEPOINT #{name}")

    try do
      fun.()
    rescue
      e ->
        :ok = Sqlite3.execute(db, "ROLLBACK TO #{name}")
        :ok = Sqlite3.execute(db, "RELEASE #{name}")
        reraise e, __STACKTRACE__
    else
      {:ok, result} ->
        :ok = Sqlite3.execute(db, "RELEASE #{name}")
        {:ok, result}

      {:error, reason} ->
        :ok = Sqlite3.execute(db, "ROLLBACK TO #{name}")
        :ok = Sqlite3.execute(db, "RELEASE #{name}")
        {:error, reason}
    end
  end

  def insert_one(db, table, values, opts \\ nil) do
    {fields, values} = Enum.unzip(values)

    case insert_many(db, table, List.to_tuple(fields), [List.to_tuple(values)], opts) do
      {:ok, [id]} -> {:ok, id}
      {:error, reason} -> {:error, reason}
    end
  end

  def insert_many(db, table, fields, values, opts \\ nil) do
    if Enum.any?(values) do
      {fields, indexes} =
        fields
        |> Tuple.to_list()
        |> Enum.with_index(1)
        |> Enum.unzip()

      sql =
        if Enum.any?(fields) do
          fields = Enum.map_join(fields, ", ", &"`#{&1}`")
          placeholders = Enum.map_join(indexes, ", ", &"?#{&1}")

          sql_parts = [
            "INSERT INTO `#{table}` (#{fields})",
            "VALUES (#{placeholders})",
            if(opts[:on_conflict], do: "ON CONFLICT #{opts[:on_conflict]}")
          ]

          sql_parts |> Enum.reject(&is_nil/1) |> Enum.join(" ")
        else
          "INSERT INTO `#{table}` DEFAULT VALUES"
        end

      with_prepare(db, sql, fn statement ->
        with_snapshot(db, fn ->
          Enum.reduce_while(values, {:ok, []}, fn row, {:ok, ids} ->
            :ok = Sqlite3.bind(db, statement, Tuple.to_list(row))

            case Sqlite3.step(db, statement) do
              :done ->
                {:ok, changes} = Sqlite3.changes(db)

                {:ok, id} =
                  case changes do
                    0 -> {:ok, nil}
                    1 -> Sqlite3.last_insert_rowid(db)
                  end

                {:cont, {:ok, ids ++ [id]}}

              {:error, reason} ->
                {:halt, {:error, reason}}
            end
          end)
        end)
      end)
    else
      {:ok, []}
    end
  end

  defp build(row, builder, columns) do
    cond do
      is_function(builder) ->
        columns
        |> Enum.map(&String.to_atom/1)
        |> Enum.zip(row)
        |> Map.new()
        |> builder.()

      builder ->
        Code.ensure_loaded!(builder)

        prepare =
          if function_exported?(builder, :prepare, 1),
            do: &builder.prepare/1,
            else: &Function.identity/1

        columns
        |> Enum.map(&String.to_atom/1)
        |> Enum.zip(row)
        |> prepare.()
        |> then(&struct(builder, &1))

      true ->
        List.to_tuple(row)
    end
  end

  def query(db, sql, args \\ {}, builder \\ nil) do
    with_prepare(db, sql, fn statement ->
      :ok = Sqlite3.bind(db, statement, Tuple.to_list(args))
      {:ok, columns} = Sqlite3.columns(db, statement)
      {:ok, rows} = Sqlite3.fetch_all(db, statement)
      {:ok, Enum.map(rows, &build(&1, builder, columns))}
    end)
  end

  def query_one(db, sql, args, builder \\ nil) do
    case query(db, sql, args, builder) do
      {:ok, [row]} ->
        {:ok, row}

      {:ok, []} ->
        {:ok, nil}
    end
  end

  def query_one!(db, sql, args, builder \\ nil) do
    case query(db, sql, args, builder) do
      {:ok, [row]} ->
        {:ok, row}
    end
  end

  def generate_external_id(db, table, length, prefix \\ "") do
    id = Utils.generate_id(length, prefix)

    case query(db, "SELECT id FROM #{table} WHERE external_id = ?1", {id}) do
      {:ok, []} -> {:ok, id}
      {:ok, _} -> generate_external_id(db, table, length + 1, prefix)
    end
  end
end
