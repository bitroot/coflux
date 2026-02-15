defmodule Coflux.Logs.Index do
  @moduledoc """
  Index file for efficient cross-partition log lookups using Bloom filters.

  Stored at: data_dir/projects/{project_id}/logs/index.bin

  Each entry contains a Bloom filter for run IDs, allowing quick elimination
  of partitions that definitely don't contain logs for a target run.
  """

  alias Coflux.Store.Bloom
  alias Coflux.Utils

  defstruct entries: []

  defmodule Entry do
    defstruct epoch_id: nil, created_at: nil, run_bloom: nil
  end

  @version 1

  @doc """
  Load the log index from disk. Returns an empty index if the file doesn't exist.
  """
  def load(project_id) do
    path = index_path(project_id)

    case File.read(path) do
      {:ok, <<@version, data::binary>>} ->
        {:ok, deserialize(data)}

      {:ok, _} ->
        # Unknown version — start fresh
        {:ok, %__MODULE__{entries: []}}

      {:error, :enoent} ->
        {:ok, %__MODULE__{entries: []}}
    end
  end

  @doc """
  Save the log index to disk.
  """
  def save(project_id, %__MODULE__{} = index) do
    path = index_path(project_id)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, <<@version, serialize(index)::binary>>)
    :ok
  end

  @doc """
  Add a new partition entry with nil bloom (unindexed).
  """
  def add_partition(%__MODULE__{} = index, epoch_id, created_at) do
    entry = %Entry{epoch_id: epoch_id, created_at: created_at, run_bloom: nil}
    %{index | entries: [entry | index.entries]}
  end

  @doc """
  Replace nil bloom with a real one for a given epoch_id.
  """
  def update_bloom(%__MODULE__{} = index, epoch_id, run_bloom) do
    entries =
      Enum.map(index.entries, fn entry ->
        if entry.epoch_id == epoch_id do
          %{entry | run_bloom: run_bloom}
        else
          entry
        end
      end)

    %{index | entries: entries}
  end

  @doc """
  Returns epoch_ids of entries where bloom is nil (not yet indexed).
  """
  def unindexed_epoch_ids(%__MODULE__{} = index) do
    index.entries
    |> Enum.filter(fn entry -> entry.run_bloom == nil end)
    |> Enum.map(& &1.epoch_id)
  end

  @doc """
  Returns all archived epoch IDs tracked by the index.
  """
  def all_epoch_ids(%__MODULE__{} = index) do
    Enum.map(index.entries, & &1.epoch_id)
  end

  @doc """
  Find partition epoch_ids that may contain logs for the given run_id.
  Returns epoch_ids in reverse chronological order (newest first).

  Options:
  - `:from` - unix ms timestamp; skip partitions created before this time
  """
  def find_partitions_for_run(%__MODULE__{} = index, run_id, opts \\ []) do
    from = Keyword.get(opts, :from)

    index.entries
    |> Enum.filter(fn entry ->
      after_from? = is_nil(from) or is_nil(entry.created_at) or entry.created_at >= from
      bloom_match? = is_nil(entry.run_bloom) or Bloom.member?(entry.run_bloom, run_id)
      after_from? and bloom_match?
    end)
    |> Enum.map(& &1.epoch_id)
  end

  @doc """
  Build a Bloom filter for a log partition by scanning its messages table.
  """
  def build_bloom_for_partition(db) do
    import Coflux.Store

    {:ok, [{count}]} = query(db, "SELECT COUNT(DISTINCT run_id) FROM messages")
    bloom = Bloom.new(max(100, count))

    {:ok, run_ids} = query(db, "SELECT DISTINCT run_id FROM messages")

    Enum.reduce(run_ids, bloom, fn {id}, b -> Bloom.add(b, id) end)
  end

  # Serialization format:
  # <<entry_count::32, entries::binary>>
  # Each entry: <<epoch_id_len::16, epoch_id::binary, created_at::64,
  #               run_bloom_len::32, run_bloom::binary>>
  defp serialize(%__MODULE__{entries: entries}) do
    entry_data =
      Enum.map(entries, fn entry ->
        run_bloom_bin =
          if entry.run_bloom do
            Bloom.serialize(entry.run_bloom)
          else
            <<>>
          end

        <<
          byte_size(entry.epoch_id)::unsigned-16,
          entry.epoch_id::binary,
          entry.created_at || 0::signed-64,
          byte_size(run_bloom_bin)::unsigned-32,
          run_bloom_bin::binary
        >>
      end)

    <<length(entries)::unsigned-32, IO.iodata_to_binary(entry_data)::binary>>
  end

  defp deserialize(<<entry_count::unsigned-32, rest::binary>>) do
    {entries, <<>>} =
      Enum.reduce(1..entry_count//1, {[], rest}, fn _, {entries, data} ->
        <<epoch_id_len::unsigned-16, epoch_id::binary-size(epoch_id_len), created_at::signed-64,
          run_bloom_len::unsigned-32, run_bloom_bin::binary-size(run_bloom_len),
          rest::binary>> = data

        run_bloom = if run_bloom_len > 0, do: Bloom.deserialize(run_bloom_bin)

        entry = %Entry{
          epoch_id: epoch_id,
          created_at: if(created_at == 0, do: nil, else: created_at),
          run_bloom: run_bloom
        }

        {[entry | entries], rest}
      end)

    %__MODULE__{entries: Enum.reverse(entries)}
  end

  defp index_path(project_id) do
    ["projects", project_id, "logs", "index.bin"]
    |> Path.join()
    |> Utils.data_path()
  end
end
