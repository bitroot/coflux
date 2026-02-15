defmodule Coflux.Store.Epoch do
  @moduledoc """
  Manages epoch databases for a project using a three-tier model:
  - **active**: open, read-write — at `projects/{id}/{name}.sqlite`
  - **unindexed**: open, read-only (awaiting Bloom filter indexing)
  - **indexed**: closed, opened on demand (tracked in index)

  The active (mutable) file lives at the project root with a fixed name
  (e.g. `orchestration.sqlite`, `logs.sqlite`). Archived (immutable) files
  live in a subdirectory (`projects/{id}/{name}/YYYYMMDD_XXXX.sqlite`).

  All archived epoch IDs are tracked via the index file — no filesystem listing
  is needed. The index is the source of truth for which archived partitions exist.
  """

  alias Coflux.Store.Migrations
  alias Coflux.Utils
  alias Exqlite.Sqlite3

  defstruct project_id: nil,
            name: nil,
            active_db: nil,
            unindexed: [],
            archived_ids: []

  @doc """
  Open the epoch store for a project. If no active file exists, creates one.

  The active file is `projects/{id}/{name}.sqlite`. Archived files are in
  `projects/{id}/{name}/YYYYMMDD_XXXX.sqlite` and are identified by the
  index, not by filesystem listing.

  Options:
  - `:unindexed_epoch_ids` - which archived epochs should be kept open (default: `[]`)
  - `:archived_epoch_ids` - all known archived epoch IDs from the index (default: `[]`)

  Returns {:ok, epoch_state}.
  """
  def open(project_id, name, opts \\ []) do
    unindexed_epoch_ids = Keyword.get(opts, :unindexed_epoch_ids, [])
    archived_epoch_ids = Keyword.get(opts, :archived_epoch_ids, [])

    active = active_path(project_id, name)
    File.mkdir_p!(Path.dirname(active))
    {:ok, active_db} = Sqlite3.open(active)
    :ok = Migrations.run(active_db, name)

    # Open DB handles for unindexed archived epochs (by known ID, no listing)
    arch_dir = archive_dir(project_id, name)
    File.mkdir_p!(arch_dir)

    unindexed =
      Enum.flat_map(unindexed_epoch_ids, fn epoch_id ->
        path = Path.join(arch_dir, "#{epoch_id}.sqlite")

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
       active_db: active_db,
       unindexed: unindexed,
       archived_ids: archived_epoch_ids
     }}
  end

  @doc """
  Generate the next epoch ID based on today's date and existing archived IDs.
  """
  def next_epoch_id(%__MODULE__{archived_ids: ids}) do
    generate_epoch_id(ids)
  end

  @doc """
  Rotate the active epoch, archiving it under the given epoch ID.

  The caller is responsible for updating the index *before* calling this
  function, so that the index always knows about all archived epochs.

  The current active file is renamed to the archive path and a fresh
  active file is created. The old DB handle remains valid (Linux fd
  semantics) and moves to the unindexed list.

  Returns {:ok, new_epoch_state, old_db}.
  """
  def rotate(%__MODULE__{} = state, epoch_id) do
    active = active_path(state.project_id, state.name)
    archive = Path.join(archive_dir(state.project_id, state.name), "#{epoch_id}.sqlite")

    # Rename current active → archive (old fd remains valid)
    :ok = File.rename(active, archive)

    # Create fresh active file
    {:ok, new_db} = Sqlite3.open(active)
    :ok = Migrations.run(new_db, state.name)

    old_db = state.active_db

    new_state = %{
      state
      | active_db: new_db,
        unindexed: state.unindexed ++ [{epoch_id, old_db}],
        archived_ids: state.archived_ids ++ [epoch_id]
    }

    {:ok, new_state, old_db}
  end

  @doc """
  Get the active epoch's db handle (for writes).
  """
  def active_db(%__MODULE__{active_db: db}), do: db

  @doc """
  Get the file path for an archived epoch.
  """
  def archive_path(%__MODULE__{} = state, epoch_id) do
    Path.join(archive_dir(state.project_id, state.name), "#{epoch_id}.sqlite")
  end

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
  def active_db_size(%__MODULE__{} = state) do
    path = active_path(state.project_id, state.name)

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

  # Private helpers

  defp project_dir(project_id) do
    ["projects", project_id]
    |> Path.join()
    |> Utils.data_path()
  end

  defp active_path(project_id, name) do
    Path.join(project_dir(project_id), "#{name}.sqlite")
  end

  defp archive_dir(project_id, name) do
    Path.join(project_dir(project_id), to_string(name))
  end

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
