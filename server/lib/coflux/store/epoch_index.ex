defmodule Coflux.Store.EpochIndex do
  @moduledoc """
  Index file for efficient cross-epoch lookups using Bloom filters.

  Stored at: data_dir/projects/{project_id}/epoch_index.bin

  Each entry contains Bloom filters for run external IDs and cache keys,
  allowing quick elimination of epochs that definitely don't contain a target.
  """

  alias Coflux.Store.Bloom
  alias Coflux.Utils

  defstruct entries: []

  defmodule Entry do
    defstruct epoch_id: nil,
              created_at: nil,
              run_bloom: nil,
              cache_bloom: nil
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
  Add a new epoch entry to the index.
  """
  def add_epoch(%__MODULE__{} = index, epoch_id, created_at, run_bloom, cache_bloom) do
    entry = %Entry{
      epoch_id: epoch_id,
      created_at: created_at,
      run_bloom: run_bloom,
      cache_bloom: cache_bloom
    }

    %{index | entries: index.entries ++ [entry]}
  end

  @doc """
  Replace nil blooms with real ones for a given epoch_id entry.
  """
  def update_blooms(%__MODULE__{} = index, epoch_id, run_bloom, cache_bloom) do
    entries =
      Enum.map(index.entries, fn entry ->
        if entry.epoch_id == epoch_id do
          %{entry | run_bloom: run_bloom, cache_bloom: cache_bloom}
        else
          entry
        end
      end)

    %{index | entries: entries}
  end

  @doc """
  Returns epoch_ids of entries where blooms are nil (not yet indexed).
  """
  def unindexed_epoch_ids(%__MODULE__{} = index) do
    index.entries
    |> Enum.filter(fn entry -> entry.run_bloom == nil end)
    |> Enum.map(& &1.epoch_id)
  end

  @doc """
  Find epoch IDs that may contain a run with the given external ID.
  Returns epoch IDs in reverse chronological order (newest first).
  """
  def find_epochs_for_run(%__MODULE__{} = index, run_external_id) do
    index.entries
    |> Enum.filter(fn entry ->
      entry.run_bloom != nil and Bloom.member?(entry.run_bloom, run_external_id)
    end)
    |> Enum.reverse()
    |> Enum.map(& &1.epoch_id)
  end

  @doc """
  Find epoch IDs that may contain a cached execution with the given cache key.
  Returns epoch IDs in reverse chronological order (newest first).
  """
  def find_epochs_for_cache_key(%__MODULE__{} = index, cache_key) do
    index.entries
    |> Enum.filter(fn entry ->
      entry.cache_bloom != nil and Bloom.member?(entry.cache_bloom, cache_key)
    end)
    |> Enum.reverse()
    |> Enum.map(& &1.epoch_id)
  end

  @doc """
  Build Bloom filters for an epoch database by scanning its runs and cache keys.
  """
  def build_blooms_for_epoch(db) do
    import Coflux.Store

    # Count runs for sizing
    {:ok, [{run_count}]} = query(db, "SELECT COUNT(*) FROM runs")
    run_bloom = Bloom.new(max(100, run_count))

    {:ok, runs} = query(db, "SELECT external_id FROM runs")
    run_bloom = Enum.reduce(runs, run_bloom, fn {ext_id}, bloom -> Bloom.add(bloom, ext_id) end)

    # Count cache keys for sizing
    {:ok, [{cache_count}]} =
      query(db, "SELECT COUNT(DISTINCT cache_key) FROM steps WHERE cache_key IS NOT NULL")

    cache_bloom = Bloom.new(max(100, cache_count))

    {:ok, cache_keys} =
      query(db, "SELECT DISTINCT cache_key FROM steps WHERE cache_key IS NOT NULL")

    cache_bloom =
      Enum.reduce(cache_keys, cache_bloom, fn {key}, bloom -> Bloom.add(bloom, key) end)

    {run_bloom, cache_bloom}
  end

  # Serialization format:
  # <<entry_count::32, entries::binary>>
  # Each entry: <<epoch_id_len::16, epoch_id::binary, created_at::64,
  #               run_bloom_len::32, run_bloom::binary,
  #               cache_bloom_len::32, cache_bloom::binary>>
  defp serialize(%__MODULE__{entries: entries}) do
    entry_data =
      Enum.map(entries, fn entry ->
        {run_bloom_bin, cache_bloom_bin} =
          if entry.run_bloom && entry.cache_bloom do
            {Bloom.serialize(entry.run_bloom), Bloom.serialize(entry.cache_bloom)}
          else
            {<<>>, <<>>}
          end

        <<
          byte_size(entry.epoch_id)::unsigned-16,
          entry.epoch_id::binary,
          entry.created_at::signed-64,
          byte_size(run_bloom_bin)::unsigned-32,
          run_bloom_bin::binary,
          byte_size(cache_bloom_bin)::unsigned-32,
          cache_bloom_bin::binary
        >>
      end)

    <<length(entries)::unsigned-32, IO.iodata_to_binary(entry_data)::binary>>
  end

  defp deserialize(<<entry_count::unsigned-32, rest::binary>>) do
    {entries, <<>>} =
      Enum.reduce(1..entry_count//1, {[], rest}, fn _, {entries, data} ->
        <<epoch_id_len::unsigned-16, epoch_id::binary-size(epoch_id_len), created_at::signed-64,
          run_bloom_len::unsigned-32, run_bloom_bin::binary-size(run_bloom_len),
          cache_bloom_len::unsigned-32, cache_bloom_bin::binary-size(cache_bloom_len),
          rest::binary>> = data

        run_bloom = if run_bloom_len > 0, do: Bloom.deserialize(run_bloom_bin)
        cache_bloom = if cache_bloom_len > 0, do: Bloom.deserialize(cache_bloom_bin)

        entry = %Entry{
          epoch_id: epoch_id,
          created_at: created_at,
          run_bloom: run_bloom,
          cache_bloom: cache_bloom
        }

        {entries ++ [entry], rest}
      end)

    %__MODULE__{entries: entries}
  end

  defp index_path(project_id) do
    ["projects", project_id, "epoch_index.bin"]
    |> Path.join()
    |> Utils.data_path()
  end
end
