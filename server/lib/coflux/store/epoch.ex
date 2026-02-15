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

  All archived epoch IDs are tracked via the index file — no filesystem listing
  is needed. The index is the source of truth for which archived partitions exist.
  """

  alias Coflux.Store.Migrations
  alias Coflux.Utils
  alias Exqlite.Sqlite3

  defstruct project_id: nil,
            name: nil,
            dir: nil,
            active_db: nil,
            active_epoch_id: nil,
            unindexed: [],
            archived_ids: []

  @doc """
  Open the epoch store for a project. If no active file exists, creates one.

  The active file is `{dir}/{name}.sqlite`. Archived files use date-based
  `YYYYMMDD_XXXX.sqlite` naming and are identified by the index, not by
  filesystem listing.

  Options:
  - `:dir` - subdirectory under the project for epoch files (default: same as `name`)
  - `:unindexed_epoch_ids` - which archived epochs should be kept open (default: `[]`)
  - `:archived_epoch_ids` - all known archived epoch IDs from the index (default: `[]`)

  Returns {:ok, epoch_state}.
  """
  def open(project_id, name, opts \\ []) do
    dir = Keyword.get(opts, :dir, name)
    unindexed_epoch_ids = Keyword.get(opts, :unindexed_epoch_ids, [])
    archived_epoch_ids = Keyword.get(opts, :archived_epoch_ids, [])
    dir_path = epochs_dir(project_id, dir)
    File.mkdir_p!(dir_path)

    active_path = Path.join(dir_path, "#{name}.sqlite")
    {:ok, active_db} = Sqlite3.open(active_path)
    :ok = Migrations.run(active_db, name)

    # Open DB handles for unindexed archived epochs (by known ID, no listing)
    unindexed =
      Enum.flat_map(unindexed_epoch_ids, fn epoch_id ->
        path = Path.join(dir_path, "#{epoch_id}.sqlite")

        if File.exists?(path) do
          {:ok, db} = Sqlite3.open(path)
          [{epoch_id, db}]
        else
          []
        end
      end)

    {:ok,
     %__MODULE__{
       project_id: project_id,
       name: name,
       dir: dir,
       active_db: active_db,
       active_epoch_id: name,
       unindexed: unindexed,
       archived_ids: archived_epoch_ids
     }}
  end

  @doc """
  Create a new epoch, making it the active one.

  The current active file (`{name}.sqlite`) is renamed to an archive name
  (`YYYYMMDD_XXXX.sqlite`) and a fresh active file is created. The old DB
  handle remains valid (Linux fd semantics) and moves to the unindexed list.

  The archive ID is derived from existing known IDs (no filesystem listing).

  Returns {:ok, new_epoch_state, archive_epoch_id}.
  """
  def rotate(%__MODULE__{} = epoch_state) do
    dir_path = epochs_dir(epoch_state.project_id, epoch_state.dir)
    archive_epoch_id = generate_epoch_id(epoch_state.archived_ids)

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
        unindexed: epoch_state.unindexed ++ [{archive_epoch_id, old_db}],
        archived_ids: epoch_state.archived_ids ++ [archive_epoch_id]
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

  defp generate_epoch_id(existing_ids) do
    date = Date.utc_today() |> Date.to_iso8601(:basic)

    counters =
      existing_ids
      |> Enum.filter(&String.starts_with?(&1, date))
      |> Enum.map(fn id ->
        case String.split(id, "_") do
          [^date, counter] -> String.to_integer(counter)
          _ -> 0
        end
      end)

    next_counter =
      case counters do
        [] -> 1
        cs -> Enum.max(cs) + 1
      end

    "#{date}_#{String.pad_leading(Integer.to_string(next_counter), 4, "0")}"
  end
end
