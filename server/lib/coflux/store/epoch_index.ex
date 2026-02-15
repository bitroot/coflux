defmodule Coflux.Store.EpochIndex do
  @moduledoc """
  Index file for efficient cross-epoch lookups using Bloom filters.

  Stored at: data_dir/projects/{project_id}/orchestration/index.json

  Each entry contains Bloom filters for run external IDs and cache keys,
  allowing quick elimination of epochs that definitely don't contain a target.

  Unindexed entries (awaiting Bloom filter build) have a `null` JSON value.
  """

  alias Coflux.Store.Bloom
  alias Coflux.Utils

  defstruct entries: []

  defmodule Entry do
    defstruct epoch_id: nil,
              runs: nil,
              cache_keys: nil
  end

  @doc """
  Load the epoch index from disk. Returns an empty index if the file doesn't exist.
  """
  def load(project_id) do
    path = index_path(project_id)

    case File.read(path) do
      {:ok, data} ->
        {:ok, deserialize(data)}

      {:error, :enoent} ->
        {:ok, %__MODULE__{entries: []}}
    end
  end

  @doc """
  Save the epoch index to disk.
  """
  def save(project_id, %__MODULE__{} = index) do
    path = index_path(project_id)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, serialize(index))
    :ok
  end

  @doc """
  Add a new epoch entry to the index with null Bloom filters (unindexed).
  """
  def add_epoch(%__MODULE__{} = index, epoch_id) do
    entry = %Entry{epoch_id: epoch_id}
    %{index | entries: [entry | index.entries]}
  end

  @doc """
  Replace nil Bloom filters with real ones for a given epoch_id entry.
  """
  def update_blooms(%__MODULE__{} = index, epoch_id, runs, cache_keys) do
    entries =
      Enum.map(index.entries, fn entry ->
        if entry.epoch_id == epoch_id do
          %{entry | runs: runs, cache_keys: cache_keys}
        else
          entry
        end
      end)

    %{index | entries: entries}
  end

  @doc """
  Returns epoch_ids of entries where Bloom filters are nil (not yet indexed).
  """
  def unindexed_epoch_ids(%__MODULE__{} = index) do
    index.entries
    |> Enum.filter(fn entry -> entry.runs == nil end)
    |> Enum.map(& &1.epoch_id)
  end

  @doc """
  Returns all archived epoch IDs tracked by the index.
  """
  def all_epoch_ids(%__MODULE__{} = index) do
    Enum.map(index.entries, & &1.epoch_id)
  end

  @doc """
  Find epoch IDs that may contain a run with the given external ID.
  Returns epoch IDs in reverse chronological order (newest first).
  """
  def find_epochs_for_run(%__MODULE__{} = index, run_external_id) do
    index.entries
    |> Enum.filter(fn entry ->
      entry.runs != nil and Bloom.member?(entry.runs, run_external_id)
    end)
    |> Enum.map(& &1.epoch_id)
  end

  @doc """
  Find epoch IDs that may contain a cached execution with the given cache key.
  Returns epoch IDs in reverse chronological order (newest first).
  """
  def find_epochs_for_cache_key(%__MODULE__{} = index, cache_key) do
    index.entries
    |> Enum.filter(fn entry ->
      entry.cache_keys != nil and Bloom.member?(entry.cache_keys, cache_key)
    end)
    |> Enum.map(& &1.epoch_id)
  end

  @doc """
  Build Bloom filters for an epoch database by scanning its runs and cache keys.
  """
  def build_blooms_for_epoch(db) do
    import Coflux.Store

    # Count runs for sizing
    {:ok, [{run_count}]} = query(db, "SELECT COUNT(*) FROM runs")
    runs = Bloom.new(max(100, run_count))

    {:ok, run_rows} = query(db, "SELECT external_id FROM runs")
    runs = Enum.reduce(run_rows, runs, fn {ext_id}, bloom -> Bloom.add(bloom, ext_id) end)

    # Count cache keys for sizing
    {:ok, [{cache_count}]} =
      query(db, "SELECT COUNT(DISTINCT cache_key) FROM steps WHERE cache_key IS NOT NULL")

    cache_keys = Bloom.new(max(100, cache_count))

    {:ok, cache_rows} =
      query(db, "SELECT DISTINCT cache_key FROM steps WHERE cache_key IS NOT NULL")

    cache_keys =
      Enum.reduce(cache_rows, cache_keys, fn {key}, bloom -> Bloom.add(bloom, key) end)

    {runs, cache_keys}
  end

  defp serialize(%__MODULE__{entries: entries}) do
    map =
      Map.new(entries, fn entry ->
        value =
          if entry.runs && entry.cache_keys do
            %{
              "runs" => Base.encode64(Bloom.serialize(entry.runs)),
              "cache_keys" => Base.encode64(Bloom.serialize(entry.cache_keys))
            }
          else
            nil
          end

        {entry.epoch_id, value}
      end)

    Jason.encode!(map)
  end

  defp deserialize(data) do
    map = Jason.decode!(data)

    entries =
      map
      |> Enum.map(fn {epoch_id, value} ->
        {runs, cache_keys} =
          case value do
            nil ->
              {nil, nil}

            %{} ->
              {
                value |> Map.get("runs") |> decode_bloom(),
                value |> Map.get("cache_keys") |> decode_bloom()
              }
          end

        %Entry{epoch_id: epoch_id, runs: runs, cache_keys: cache_keys}
      end)
      |> Enum.sort_by(& &1.epoch_id, :desc)

    %__MODULE__{entries: entries}
  end

  defp decode_bloom(nil), do: nil
  defp decode_bloom(b64), do: Bloom.deserialize(Base.decode64!(b64))

  defp index_path(project_id) do
    ["projects", project_id, "orchestration", "index.json"]
    |> Path.join()
    |> Utils.data_path()
  end
end
