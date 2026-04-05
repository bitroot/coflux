defmodule Coflux.Orchestration.TagSets do
  import Coflux.Store

  def get_or_create_tag_set_id(db, tags) do
    hash = hash_tag_set(tags)

    case query_one(db, "SELECT id FROM tag_sets WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {tag_set_id}} ->
        {:ok, tag_set_id}

      {:ok, nil} ->
        case insert_one(db, :tag_sets, %{hash: {:blob, hash}}) do
          {:ok, tag_set_id} ->
            {:ok, _} =
              insert_many(
                db,
                :tag_set_items,
                {:tag_set_id, :key, :value},
                Enum.flat_map(tags, fn
                  {key, []} -> [{tag_set_id, key, ""}]
                  {key, values} -> Enum.map(values, &{tag_set_id, key, &1})
                end)
              )

            {:ok, tag_set_id}
        end
    end
  end

  def get_tag_set(db, tag_set_id) do
    case query(
           db,
           "SELECT key, value FROM tag_set_items WHERE tag_set_id = ?1",
           {tag_set_id}
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.reduce(rows, %{}, fn
           {key, ""}, result -> Map.put_new(result, key, [])
           {key, value}, result -> Map.update(result, key, [value], &[value | &1])
         end)}
    end
  end

  defp hash_tag_set(tags) do
    data =
      tags
      |> Enum.sort()
      |> Enum.map_join(";", fn {key, values} ->
        "#{key}=#{values |> Enum.sort() |> Enum.join(",")}"
      end)

    :crypto.hash(:sha256, data)
  end
end
