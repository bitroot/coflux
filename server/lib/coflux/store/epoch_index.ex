defmodule Coflux.Store.EpochIndex do
  @moduledoc """
  Index file for efficient cross-epoch lookups using Bloom filters.

  Each entry is an `{epoch_id, filters}` tuple where `filters` is a map of
  `%{field_name => Bloom.t() | nil}`. An entry with nil filters is unindexed
  (awaiting background build).

  Stored as JSON: `{epoch_id: {field: base64_bloom, ...} | null, ...}`.
  """

  alias Coflux.Store.Bloom
  alias Coflux.Utils

  defstruct path: nil, fields: [], entries: []

  @doc """
  Load an epoch index from disk. Returns an empty index if the file doesn't exist.

  - `path` — relative path segments under the data dir (e.g. `["projects", id, "orchestration", "index.json"]`)
  - `fields` — list of field name strings (e.g. `["runs", "cache_keys"]`)
  """
  def load(path, fields) do
    full_path = resolve_path(path)

    case File.read(full_path) do
      {:ok, data} ->
        {:ok, %{deserialize(data) | path: path, fields: fields}}

      {:error, :enoent} ->
        {:ok, %__MODULE__{path: path, fields: fields, entries: []}}
    end
  end

  @doc """
  Save the epoch index to disk.
  """
  def save(%__MODULE__{} = index) do
    full_path = resolve_path(index.path)
    File.mkdir_p!(Path.dirname(full_path))
    File.write!(full_path, serialize(index))
    :ok
  end

  @doc """
  Add a new epoch entry with nil filters (unindexed).
  """
  def add_epoch(%__MODULE__{} = index, epoch_id) do
    filters = Map.new(index.fields, &{&1, nil})
    %{index | entries: [{epoch_id, filters} | index.entries]}
  end

  @doc """
  Replace nil filters with real ones for a given epoch_id.

  `filters` is a map of `%{field_name => Bloom.t()}`.
  """
  def update_filters(%__MODULE__{} = index, epoch_id, filters) when is_map(filters) do
    entries =
      Enum.map(index.entries, fn
        {^epoch_id, _} -> {epoch_id, filters}
        entry -> entry
      end)

    %{index | entries: entries}
  end

  @doc """
  Returns epoch_ids of entries where any filter is nil (not yet indexed).
  """
  def unindexed_epoch_ids(%__MODULE__{} = index) do
    index.entries
    |> Enum.filter(fn {_epoch_id, filters} ->
      Enum.any?(filters, fn {_field, value} -> value == nil end)
    end)
    |> Enum.map(&elem(&1, 0))
  end

  @doc """
  Returns all archived epoch IDs tracked by the index.
  """
  def all_epoch_ids(%__MODULE__{} = index) do
    Enum.map(index.entries, &elem(&1, 0))
  end

  @doc """
  Returns epoch IDs where the named field has a non-nil filter (fully indexed).
  """
  def indexed_epoch_ids(%__MODULE__{} = index, field) do
    index.entries
    |> Enum.filter(fn {_epoch_id, filters} -> Map.get(filters, field) != nil end)
    |> Enum.map(&elem(&1, 0))
  end

  @doc """
  Find epoch IDs where the given key may be present in the named field.
  Returns epoch IDs in reverse chronological order (newest first).

  Entries with a nil filter for the field are included (can't be ruled out).
  """
  def find_epochs(%__MODULE__{} = index, field, key) do
    index.entries
    |> Enum.filter(fn {_epoch_id, filters} ->
      case Map.get(filters, field) do
        nil -> true
        bloom -> Bloom.member?(bloom, key)
      end
    end)
    |> Enum.map(&elem(&1, 0))
  end

  # Serialization

  defp serialize(%__MODULE__{entries: entries}) do
    map =
      Map.new(entries, fn {epoch_id, filters} ->
        value =
          if Enum.all?(filters, fn {_f, v} -> v != nil end) do
            Map.new(filters, fn {field, bloom} ->
              {field, Base.encode64(Bloom.serialize(bloom))}
            end)
          else
            nil
          end

        {epoch_id, value}
      end)

    Jason.encode!(map)
  end

  defp deserialize(data) do
    map = Jason.decode!(data)

    entries =
      map
      |> Enum.map(fn {epoch_id, value} ->
        filters =
          case value do
            nil -> %{}
            %{} -> Map.new(value, fn {field, b64} -> {field, decode_bloom(b64)} end)
          end

        {epoch_id, filters}
      end)
      |> Enum.sort_by(&elem(&1, 0), :desc)

    %__MODULE__{entries: entries}
  end

  defp decode_bloom(nil), do: nil
  defp decode_bloom(b64), do: Bloom.deserialize(Base.decode64!(b64))

  defp resolve_path(segments) do
    segments
    |> Path.join()
    |> Utils.data_path()
  end
end
