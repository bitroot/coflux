defmodule Coflux.Logs.Index do
  @moduledoc """
  Index file for efficient cross-partition log lookups using Bloom filters.

  Stored at: data_dir/projects/{project_id}/logs/index.json

  Each entry contains a Bloom filter for run IDs, allowing quick elimination
  of partitions that definitely don't contain logs for a target run.

  Unindexed entries (awaiting Bloom filter build) have a `null` JSON value.
  """

  alias Coflux.Store.Bloom
  alias Coflux.Utils

  defstruct entries: []

  defmodule Entry do
    defstruct epoch_id: nil, runs: nil
  end

  @doc """
  Load the log index from disk. Returns an empty index if the file doesn't exist.
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
  Save the log index to disk.
  """
  def save(project_id, %__MODULE__{} = index) do
    path = index_path(project_id)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, serialize(index))
    :ok
  end

  @doc """
  Add a new partition entry with null Bloom filter (unindexed).
  """
  def add_partition(%__MODULE__{} = index, epoch_id) do
    entry = %Entry{epoch_id: epoch_id}
    %{index | entries: [entry | index.entries]}
  end

  @doc """
  Replace nil Bloom filter with a real one for a given epoch_id.
  """
  def update_bloom(%__MODULE__{} = index, epoch_id, runs) do
    entries =
      Enum.map(index.entries, fn entry ->
        if entry.epoch_id == epoch_id do
          %{entry | runs: runs}
        else
          entry
        end
      end)

    %{index | entries: entries}
  end

  @doc """
  Returns epoch_ids of entries where Bloom filter is nil (not yet indexed).
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
  Find partition epoch_ids that may contain logs for the given run_id.
  Returns epoch_ids in reverse chronological order (newest first).

  Options:
  - `:from` - unix ms timestamp; skip partitions archived before this date
  """
  def find_partitions_for_run(%__MODULE__{} = index, run_id, opts \\ []) do
    from = Keyword.get(opts, :from)

    index.entries
    |> Enum.filter(fn entry ->
      after_from? = is_nil(from) or not archived_before?(entry.epoch_id, from)
      bloom_match? = is_nil(entry.runs) or Bloom.member?(entry.runs, run_id)
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

  # Returns true if the epoch was definitely archived before the given timestamp.
  # Parses the date from the epoch_id (YYYYMMDD_XXXX format) and compares at
  # day granularity. Conservative: a partition archived on the same day as `from`
  # is included (not skipped).
  defp archived_before?(epoch_id, from_ms) do
    case epoch_id_date(epoch_id) do
      nil ->
        false

      date ->
        from_date = from_ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_date()
        Date.compare(date, from_date) == :lt
    end
  end

  defp epoch_id_date(epoch_id) do
    case String.split(epoch_id, "_") do
      [date_str, _counter] ->
        case Date.from_iso8601(date_str, :basic) do
          {:ok, date} -> date
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp serialize(%__MODULE__{entries: entries}) do
    map =
      Map.new(entries, fn entry ->
        value =
          if entry.runs do
            %{"runs" => Base.encode64(Bloom.serialize(entry.runs))}
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
        runs =
          case value do
            nil -> nil
            %{"runs" => b64} -> Bloom.deserialize(Base.decode64!(b64))
          end

        %Entry{epoch_id: epoch_id, runs: runs}
      end)
      |> Enum.sort_by(& &1.epoch_id, :desc)

    %__MODULE__{entries: entries}
  end

  defp index_path(project_id) do
    ["projects", project_id, "logs", "index.json"]
    |> Path.join()
    |> Utils.data_path()
  end
end
