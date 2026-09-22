defmodule Coflux.Store.Migrations do
  alias Exqlite.Sqlite3

  @otp_app Mix.Project.config()[:app]

  @doc """
  Brings a database up to date with the migrations for `name`.

  Options:
  - `:up_to` - stop after this version (for tests that need an older shape)
  """
  def run(db, name, opts \\ []) do
    migrations_dir =
      @otp_app
      |> Application.app_dir("priv/migrations")
      |> Path.join(name)

    setup_migrations_table(db)

    available = get_available_versions(migrations_dir)

    available =
      case Keyword.get(opts, :up_to) do
        nil -> available
        up_to -> MapSet.filter(available, &(&1 <= up_to))
      end

    available
    |> MapSet.difference(get_migrated_versions(db))
    |> Enum.sort()
    |> Enum.each(&run_migration(db, migrations_dir, &1))

    :ok
  end

  defp get_available_versions(migrations_dir) do
    migrations_dir
    |> File.ls!()
    |> Enum.filter(&(Path.extname(&1) == ".sql"))
    |> Enum.map(&Path.basename(&1, ".sql"))
    |> Enum.map(&String.to_integer/1)
    |> MapSet.new()
  end

  defp setup_migrations_table(db) do
    :ok =
      Sqlite3.execute(
        db,
        "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY) STRICT"
      )
  end

  defp get_migrated_versions(db) do
    {:ok, statement} = Sqlite3.prepare(db, "SELECT version FROM schema_migrations")
    {:ok, rows} = Sqlite3.fetch_all(db, statement)
    :ok = Sqlite3.release(db, statement)

    rows
    |> Enum.map(fn [version] -> version end)
    |> MapSet.new()
  end

  defp run_migration(db, migrations_dir, version) do
    sql =
      migrations_dir
      |> Path.join("#{version}.sql")
      |> File.read!()

    # Take the write lock before looking again: two handles on one file -
    # an archive opened for a query while its index is being built, say -
    # can both find the same migration pending, and only one may apply it.
    :ok = Sqlite3.execute(db, "BEGIN IMMEDIATE")

    if MapSet.member?(get_migrated_versions(db), version) do
      :ok = Sqlite3.execute(db, "COMMIT")
    else
      :ok = Sqlite3.execute(db, sql)
      :ok = insert_schema_migration(db, version)
      :ok = Sqlite3.execute(db, "COMMIT")
    end
  end

  defp insert_schema_migration(db, version) do
    {:ok, statement} = Sqlite3.prepare(db, "INSERT INTO schema_migrations (version) VALUES (?1)")
    :ok = Sqlite3.bind(db, statement, [version])
    :done = Sqlite3.step(db, statement)
    Sqlite3.release(db, statement)
  end
end
