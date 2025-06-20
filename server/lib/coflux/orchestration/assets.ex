defmodule Coflux.Orchestration.Assets do
  import Coflux.Store

  alias Coflux.Orchestration.Values

  defp hash_asset(name, entries) do
    data =
      [name || ""]
      |> Enum.concat(
        Enum.flat_map(entries, fn {path, blob_key, _size, metadata} ->
          Enum.concat(
            [path, blob_key, Enum.count(metadata)],
            Enum.flat_map(metadata, fn {key, value} ->
              [key, Jason.encode!(value)]
            end)
          )
        end)
      )
      |> Enum.intersperse(0)

    :crypto.hash(:sha256, data)
  end

  def get_or_create_asset(db, name, entries) do
    with_transaction(db, fn ->
      hash = hash_asset(name, entries)

      case query_one(db, "SELECT id FROM assets WHERE hash = ?1", {{:blob, hash}}) do
        {:ok, {asset_id}} ->
          {:ok, asset_id}

        {:ok, nil} ->
          {:ok, external_id} = generate_external_id(db, :assets, 2, "A")

          {:ok, asset_id} =
            insert_one(db, :assets, %{external_id: external_id, name: name, hash: {:blob, hash}})

          Enum.each(entries, fn {path, blob_key, size, metadata} ->
            {:ok, blob_id} = Values.get_or_create_blob(db, blob_key, size)

            {:ok, asset_entry_id} =
              insert_one(db, :asset_entries, %{
                asset_id: asset_id,
                path: path,
                blob_id: blob_id
              })

            {:ok, _} =
              insert_many(
                db,
                :asset_entry_metadata,
                {:asset_entry_id, :key, :value},
                Enum.map(metadata, fn {key, value} ->
                  {asset_entry_id, key, Jason.encode!(value)}
                end)
              )
          end)

          {:ok, asset_id}
      end
    end)
  end

  defp get_asset_entries(db, asset_id) do
    case query(
           db,
           """
           SELECT ae.entry_id, ae.path, b.key, b.size
           FROM asset_entries AS ae
           INNER JOIN blobs AS b ON b.id = ae.blob_id
           WHERE ae.asset_id = ?1
           """,
           {asset_id}
         ) do
      {:ok, rows} ->
        entries =
          Enum.map(rows, fn {asset_entry_id, path, blob_key, size} ->
            # TODO: load all metadata in single query
            metadata =
              case query(
                     db,
                     "SELECT key, value FROM asset_entry_metadata WHERE asset_entry_id = ?1",
                     {asset_entry_id}
                   ) do
                {:ok, rows} ->
                  Map.new(rows, fn {key, value} -> {key, Jason.decode!(value)} end)
              end

            {path, blob_key, size, metadata}
          end)

        {:ok, entries}
    end
  end

  def get_asset_summary(db, asset_id) do
    case query_one(db, "SELECT external_id, name FROM assets WHERE id = ?1", {asset_id}) do
      {:ok, {external_id, name}} ->
        case query_one(
               db,
               """
               SELECT count(*) AS total_count, total(b.size) AS total_size
               FROM asset_entries AS ae
               INNER JOIN blobs AS b ON ae.blob_id = b.id
               WHERE ae.asset_id = ?1
               """,
               {asset_id}
             ) do
          {:ok, {total_count, total_size}} ->
            entry =
              if total_count == 1 do
                case get_asset_entries(db, asset_id) do
                  {:ok, [entry]} -> entry
                end
              end

            {:ok, external_id, name, total_count, total_size, entry}
        end

      {:ok, nil} ->
        {:error, :not_found}
    end
  end

  def get_asset_by_id(db, asset_id) do
    case query_one(db, "SELECT external_id, name FROM assets WHERE id = ?1", {asset_id}) do
      {:ok, {external_id, name}} ->
        {:ok, entries} = get_asset_entries(db, asset_id)
        {:ok, external_id, name, entries}

      {:ok, nil} ->
        {:error, :not_found}
    end
  end

  def get_asset_by_external_id(db, external_id) do
    case query_one(db, "SELECT id, name FROM assets WHERE external_id = ?1", {external_id}) do
      {:ok, {asset_id, name}} ->
        {:ok, entries} = get_asset_entries(db, asset_id)
        {:ok, name, entries}

      {:ok, nil} ->
        {:error, :not_found}
    end
  end
end
