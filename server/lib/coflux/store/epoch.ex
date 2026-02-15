defmodule Coflux.Store.Epoch do
  @moduledoc """
  Manages epoch databases for a project using a three-tier model:
  - **active**: open, read-write (one at a time)
  - **unindexed**: open, read-only (awaiting Bloom filter indexing)
  - **indexed**: closed, opened on demand (tracked in EpochIndex)

  Epoch files live at:
    data_dir/projects/{project_id}/{dir}/YYYYMMDD_XXXX.sqlite

  Where YYYYMMDD is the UTC date and XXXX is a zero-padded per-date counter.
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
  Open the epoch store for a project. If no epochs exist, creates the first one.

  Options:
  - `:dir` - subdirectory under the project for epoch files (default: same as `name`)
  - `:unindexed_epoch_ids` - which archived epochs should be kept open (default: `[]`)

  Returns {:ok, epoch_state}.
  """
  def open(project_id, name, opts \\ []) do
    dir = Keyword.get(opts, :dir, name)
    unindexed_epoch_ids = Keyword.get(opts, :unindexed_epoch_ids, [])
    epochs_dir = epochs_dir(project_id, dir)
    File.mkdir_p!(epochs_dir)

    epoch_files =
      epochs_dir
      |> File.ls!()
      |> Enum.filter(&String.ends_with?(&1, ".sqlite"))
      |> Enum.sort()

    case epoch_files do
      [] ->
        # No epochs exist - create the first one
        epoch_id = generate_epoch_id(epochs_dir)
        path = Path.join(epochs_dir, "#{epoch_id}.sqlite")
        {:ok, db} = Sqlite3.open(path)
        :ok = Migrations.run(db, name)

        {:ok,
         %__MODULE__{
           project_id: project_id,
           name: name,
           dir: dir,
           active_db: db,
           active_epoch_id: epoch_id,
           unindexed: []
         }}

      files ->
        # Last file is the active epoch
        active_file = List.last(files)
        active_epoch_id = Path.basename(active_file, ".sqlite")
        active_path = Path.join(epochs_dir, active_file)

        {:ok, active_db} = Sqlite3.open(active_path)
        :ok = Migrations.run(active_db, name)

        # Only open DB handles for unindexed epochs
        unindexed_set = MapSet.new(unindexed_epoch_ids)
        archived_files = Enum.drop(files, -1)

        unindexed =
          archived_files
          |> Enum.filter(fn file ->
            epoch_id = Path.basename(file, ".sqlite")
            MapSet.member?(unindexed_set, epoch_id)
          end)
          |> Enum.map(fn file ->
            epoch_id = Path.basename(file, ".sqlite")
            path = Path.join(epochs_dir, file)
            {:ok, db} = Sqlite3.open(path)
            {epoch_id, db}
          end)

        {:ok,
         %__MODULE__{
           project_id: project_id,
           name: name,
           dir: dir,
           active_db: active_db,
           active_epoch_id: active_epoch_id,
           unindexed: unindexed
         }}
    end
  end

  @doc """
  Create a new epoch, making it the active one.
  The old active epoch moves to the unindexed list.
  Returns {:ok, new_epoch_state, old_epoch_id}.
  """
  def rotate(%__MODULE__{} = epoch_state) do
    epochs_dir = epochs_dir(epoch_state.project_id, epoch_state.dir)
    new_epoch_id = generate_epoch_id(epochs_dir)
    new_path = Path.join(epochs_dir, "#{new_epoch_id}.sqlite")

    {:ok, new_db} = Sqlite3.open(new_path)
    :ok = Migrations.run(new_db, epoch_state.name)

    old_db = epoch_state.active_db
    old_epoch_id = epoch_state.active_epoch_id

    new_state = %{
      epoch_state
      | active_db: new_db,
        active_epoch_id: new_epoch_id,
        unindexed: epoch_state.unindexed ++ [{old_epoch_id, old_db}]
    }

    {:ok, new_state, old_epoch_id}
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
    epochs_dir = epochs_dir(epoch_state.project_id, epoch_state.dir)
    path = Path.join(epochs_dir, "#{epoch_state.active_epoch_id}.sqlite")

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

  defp generate_epoch_id(epochs_dir) do
    date = Date.utc_today() |> Date.to_iso8601(:basic)

    existing =
      epochs_dir
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
