defmodule Coflux.Store.Epoch do
  @moduledoc """
  Manages epoch databases for a project. Each project has a series of epoch databases:
  one active (receiving writes), the rest archived and immutable.

  Epoch files live at:
    data_dir/projects/{project_id}/epochs/YYYYMMDD_XXXX.sqlite

  Where YYYYMMDD is the UTC date and XXXX is a zero-padded per-date counter.
  """

  alias Coflux.Store.Migrations
  alias Coflux.Utils
  alias Exqlite.Sqlite3

  defstruct project_id: nil,
            name: nil,
            active_db: nil,
            active_epoch_id: nil,
            archived: []

  @doc """
  Open the epoch store for a project. If no epochs exist, creates the first one.
  Returns {:ok, epoch_state}.
  """
  def open(project_id, name) do
    epochs_dir = epochs_dir(project_id)
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
           active_db: db,
           active_epoch_id: epoch_id,
           archived: []
         }}

      files ->
        # Last file is the active epoch
        active_file = List.last(files)
        active_epoch_id = Path.basename(active_file, ".sqlite")
        active_path = Path.join(epochs_dir, active_file)

        {:ok, active_db} = Sqlite3.open(active_path)
        :ok = Migrations.run(active_db, name)

        # Open archived epochs (read-only)
        archived_files = Enum.drop(files, -1)

        archived =
          Enum.map(archived_files, fn file ->
            epoch_id = Path.basename(file, ".sqlite")
            path = Path.join(epochs_dir, file)
            {:ok, db} = Sqlite3.open(path)
            {epoch_id, db}
          end)

        {:ok,
         %__MODULE__{
           project_id: project_id,
           name: name,
           active_db: active_db,
           active_epoch_id: active_epoch_id,
           archived: archived
         }}
    end
  end

  @doc """
  Create a new epoch, making it the active one.
  Returns {:ok, new_epoch_state, old_epoch_db}.
  """
  def rotate(%__MODULE__{} = epoch_state) do
    epochs_dir = epochs_dir(epoch_state.project_id)
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
        archived: epoch_state.archived ++ [{old_epoch_id, old_db}]
    }

    {:ok, new_state, old_db}
  end

  @doc """
  Get the active epoch's db handle (for writes).
  """
  def active_db(%__MODULE__{active_db: db}), do: db

  @doc """
  Get all epoch db handles (active first, then reverse chronological).
  """
  def all_dbs(%__MODULE__{active_db: active_db, archived: archived}) do
    [active_db | archived |> Enum.reverse() |> Enum.map(&elem(&1, 1))]
  end

  @doc """
  Get archived epoch entries as [{epoch_id, db}] in reverse chronological order (newest first).
  """
  def archived_dbs(%__MODULE__{archived: archived}) do
    Enum.reverse(archived)
  end

  @doc """
  Get the file size of the active epoch database in bytes.
  """
  def active_db_size(%__MODULE__{} = epoch_state) do
    epochs_dir = epochs_dir(epoch_state.project_id)
    path = Path.join(epochs_dir, "#{epoch_state.active_epoch_id}.sqlite")

    case File.stat(path) do
      {:ok, %{size: size}} -> size
      {:error, _} -> 0
    end
  end

  @doc """
  Close all epoch databases.
  """
  def close(%__MODULE__{active_db: active_db, archived: archived}) do
    Sqlite3.close(active_db)

    Enum.each(archived, fn {_epoch_id, db} ->
      Sqlite3.close(db)
    end)

    :ok
  end

  # Private helpers

  defp epochs_dir(project_id) do
    ["projects", project_id, "epochs"]
    |> Path.join()
    |> Utils.data_path()
  end

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
