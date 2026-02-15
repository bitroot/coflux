defmodule Coflux.Store.Epoch do
  @moduledoc """
  Manages epoch databases for a project using a three-tier model:
  - **active**: open, read-write — always at `{dir}/{name}.sqlite`
  - **unindexed**: open, read-only (awaiting Bloom filter indexing)
  - **indexed**: closed, opened on demand (tracked in index)

  The active (mutable) file has a fixed, known name (e.g. `orchestration.sqlite`,
  `logs.sqlite`). Archived (immutable) files use date-based `YYYYMMDD_XXXX.sqlite`
  naming. This makes the active file easy to target with streaming backup tools
  (e.g. Litestream) and emphasises the mutable vs immutable distinction.
  """

  alias Coflux.Store.Migrations
  alias Coflux.Utils
  alias Exqlite.Sqlite3

  defstruct project_id: nil,
            name: nil,
            dir: nil,
            active_db: nil,
            active_epoch_id: nil,
            unindexed: []

  @doc """
  Open the epoch store for a project. If no active file exists, creates one.

  The active file is `{dir}/{name}.sqlite`. Archived files matching
  `YYYYMMDD_XXXX.sqlite` are treated as immutable.

  If no active file exists but archived files do (migration from the old
  naming scheme where all files were date-based), the latest dated file
  is promoted to the active name.

  Options:
  - `:dir` - subdirectory under the project for epoch files (default: same as `name`)
  - `:unindexed_epoch_ids` - which archived epochs should be kept open (default: `[]`)

  Returns {:ok, epoch_state}.
  """
  def open(project_id, name, opts \\ []) do
    dir = Keyword.get(opts, :dir, name)
    unindexed_epoch_ids = Keyword.get(opts, :unindexed_epoch_ids, [])
    dir_path = epochs_dir(project_id, dir)
    File.mkdir_p!(dir_path)

    active_path = Path.join(dir_path, "#{name}.sqlite")
    archived_files = list_archived_files(dir_path)

    # Migration: if no active file but dated files exist,
    # promote the latest dated file to active
    archived_files =
      if not File.exists?(active_path) and archived_files != [] do
        latest = List.last(archived_files)
        :ok = File.rename(Path.join(dir_path, latest), active_path)
        Enum.drop(archived_files, -1)
      else
        archived_files
      end

    {:ok, active_db} = Sqlite3.open(active_path)
    :ok = Migrations.run(active_db, name)

    # Open DB handles for unindexed archived epochs
    unindexed_set = MapSet.new(unindexed_epoch_ids)

    unindexed =
      archived_files
      |> Enum.filter(fn file ->
        epoch_id = Path.basename(file, ".sqlite")
        MapSet.member?(unindexed_set, epoch_id)
      end)
      |> Enum.map(fn file ->
        epoch_id = Path.basename(file, ".sqlite")
        {:ok, db} = Sqlite3.open(Path.join(dir_path, file))
        {epoch_id, db}
      end)

    {:ok,
     %__MODULE__{
       project_id: project_id,
       name: name,
       dir: dir,
       active_db: active_db,
       active_epoch_id: name,
       unindexed: unindexed
     }}
  end

  @doc """
  Create a new epoch, making it the active one.

  The current active file (`{name}.sqlite`) is renamed to an archive name
  (`YYYYMMDD_XXXX.sqlite`) and a fresh active file is created. The old DB
  handle remains valid (Linux fd semantics) and moves to the unindexed list.

  Returns {:ok, new_epoch_state, archive_epoch_id}.
  """
  def rotate(%__MODULE__{} = epoch_state) do
    dir_path = epochs_dir(epoch_state.project_id, epoch_state.dir)
    archive_epoch_id = generate_epoch_id(dir_path)

    active_path = Path.join(dir_path, "#{epoch_state.name}.sqlite")
    archive_path = Path.join(dir_path, "#{archive_epoch_id}.sqlite")

    # Rename current active → archive (old fd remains valid)
    :ok = File.rename(active_path, archive_path)

    # Create fresh active file
    {:ok, new_db} = Sqlite3.open(active_path)
    :ok = Migrations.run(new_db, epoch_state.name)

    old_db = epoch_state.active_db

    new_state = %{
      epoch_state
      | active_db: new_db,
        unindexed: epoch_state.unindexed ++ [{archive_epoch_id, old_db}]
    }

    {:ok, new_state, archive_epoch_id}
  end

  @doc """
  Get the active epoch's db handle (for writes).
  """
  def active_db(%__MODULE__{active_db: db}), do: db

  @doc """
  Get unindexed epoch entries as [{epoch_id, db}] in reverse chronological order (newest first).
  """
  def unindexed_dbs(%__MODULE__{unindexed: unindexed}) do
    Enum.reverse(unindexed)
  end

  @doc """
  Remove an epoch from the unindexed list and close its DB handle.
  Called after Bloom filters have been built and the epoch is now indexed.
  """
  def promote_to_indexed(%__MODULE__{} = state, epoch_id) do
    {to_close, remaining} =
      Enum.split_with(state.unindexed, fn {id, _db} -> id == epoch_id end)

    Enum.each(to_close, fn {_id, db} -> Sqlite3.close(db) end)

    %{state | unindexed: remaining}
  end

  @doc """
  Get the file size of the active epoch database in bytes.
  """
  def active_db_size(%__MODULE__{} = epoch_state) do
    dir_path = epochs_dir(epoch_state.project_id, epoch_state.dir)
    path = Path.join(dir_path, "#{epoch_state.name}.sqlite")

    case File.stat(path) do
      {:ok, %{size: size}} -> size
      {:error, _} -> 0
    end
  end

  @doc """
  Close all open epoch databases (active + unindexed).
  Indexed epochs have no open handles.
  """
  def close(%__MODULE__{active_db: active_db, unindexed: unindexed}) do
    Sqlite3.close(active_db)

    Enum.each(unindexed, fn {_epoch_id, db} ->
      Sqlite3.close(db)
    end)

    :ok
  end

  @doc """
  Returns the epochs directory path for a project (default: "epochs" subdirectory).
  """
  def epochs_dir(project_id) do
    epochs_dir(project_id, "epochs")
  end

  @doc """
  Returns the directory path for a project's epoch files under the given subdirectory.
  """
  def epochs_dir(project_id, dir) do
    ["projects", project_id, dir]
    |> Path.join()
    |> Utils.data_path()
  end

  # Private helpers

  @archive_pattern ~r/^\d{8}_\d{4}\.sqlite$/

  defp list_archived_files(dir_path) do
    dir_path
    |> File.ls!()
    |> Enum.filter(&Regex.match?(@archive_pattern, &1))
    |> Enum.sort()
  end

  defp generate_epoch_id(dir_path) do
    date = Date.utc_today() |> Date.to_iso8601(:basic)

    existing =
      dir_path
      |> File.ls!()
      |> Enum.filter(&String.starts_with?(&1, date))
      |> Enum.map(fn filename ->
        case String.split(Path.basename(filename, ".sqlite"), "_") do
          [^date, counter] -> String.to_integer(counter)
          _ -> 0
        end
      end)

    next_counter =
      case existing do
        [] -> 1
        counters -> Enum.max(counters) + 1
      end

    "#{date}_#{String.pad_leading(Integer.to_string(next_counter), 4, "0")}"
  end
end
