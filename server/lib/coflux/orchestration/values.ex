defmodule Coflux.Orchestration.Values do
  import Coflux.Store

  # TODO: move, or make private?
  def get_or_create_blob(db, blob_key, size) do
    case query_one(db, "SELECT id FROM blobs WHERE key = ?1", {blob_key}) do
      {:ok, {id}} ->
        {:ok, id}

      {:ok, nil} ->
        insert_one(db, :blobs, %{
          key: blob_key,
          size: size
        })
    end
  end

  def get_value_by_id(db, value_id) do
    case query_one!(
           db,
           """
           SELECT v.content, b.key, b.size
           FROM values_ AS v
           LEFT JOIN blobs AS b ON b.id = v.blob_id
           WHERE v.id = ?1
           """,
           {value_id}
         ) do
      {:ok, {content, blob_key, size}} ->
        references =
          case query(
                 db,
                 """
                 SELECT fragment_id, execution_ref_id, asset_id
                 FROM value_references
                 WHERE value_id = ?1
                 ORDER BY position
                 """,
                 {value_id}
               ) do
            {:ok, rows} ->
              Enum.map(rows, fn
                {fragment_id, nil, nil} ->
                  load_fragment(db, fragment_id)

                {nil, execution_ref_id, nil} ->
                  {:ok, {run_ext, step_num, attempt}} =
                    query_one!(
                      db,
                      "SELECT run_external_id, step_number, attempt FROM execution_refs WHERE id = ?1",
                      {execution_ref_id}
                    )

                  {:execution, run_ext, step_num, attempt}

                {nil, nil, asset_id} ->
                  {:ok, {external_id}} =
                    query_one!(db, "SELECT external_id FROM assets WHERE id = ?1", {asset_id})

                  {:asset, external_id}
              end)
          end

        value =
          case {content, blob_key} do
            {content, nil} ->
              {:raw, Jason.decode!(content), references}

            {nil, blob_key} ->
              {:blob, blob_key, size, references}
          end

        {:ok, value}
    end
  end

  defp load_fragment(db, fragment_id) do
    case query_one(
           db,
           """
           SELECT ff.name, b.key, b.size
           FROM fragments AS f
           INNER JOIN fragment_formats AS ff ON ff.id = f.format_id
           INNER JOIN blobs AS b ON b.id = f.blob_id
           WHERE f.id = ?1
           """,
           {fragment_id}
         ) do
      {:ok, {format, blob_key, size}} ->
        metadata =
          case query(
                 db,
                 "SELECT key, value FROM fragment_metadata WHERE fragment_id = ?1",
                 {fragment_id}
               ) do
            {:ok, rows} ->
              Map.new(rows, fn {key, value} -> {key, Jason.decode!(value)} end)
          end

        {:fragment, format, blob_key, size, metadata}
    end
  end

  defp hash_value(value) do
    {data, blob_key, references} =
      case value do
        {:raw, data, references} -> {data, nil, references}
        {:blob, blob_key, _size, references} -> {nil, blob_key, references}
      end

    reference_parts =
      Enum.flat_map(references, fn
        {:fragment, format, blob_key, _size, metadata} ->
          Enum.concat(
            [1, format, blob_key],
            Enum.flat_map(metadata, fn {key, value} -> [key, Jason.encode!(value)] end)
          )

        {:execution, run_ext, step_num, attempt} ->
          [2, "#{run_ext}:#{step_num}:#{attempt}"]

        {:asset, external_id} ->
          [3, external_id]
      end)

    data =
      [
        if(!is_nil(data), do: Jason.encode!(data), else: 0),
        if(blob_key, do: blob_key, else: 0)
      ]
      |> Enum.concat(reference_parts)
      |> Enum.intersperse(0)

    :crypto.hash(:sha256, data)
  end

  def get_or_create_value(db, value) do
    {data, blob_id, references} =
      case value do
        {:raw, data, references} ->
          {data, nil, references}

        {:blob, blob_key, size, references} ->
          {:ok, blob_id} = get_or_create_blob(db, blob_key, size)
          {nil, blob_id, references}
      end

    hash = hash_value(value)

    case query_one(db, "SELECT id FROM values_ WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {id}} ->
        {:ok, id}

      {:ok, nil} ->
        {:ok, value_id} =
          insert_one(db, :values_, %{
            hash: {:blob, hash},
            content: unless(blob_id, do: {:blob, Jason.encode!(data)}),
            blob_id: blob_id
          })

        {:ok, _} =
          insert_many(
            db,
            :value_references,
            {:value_id, :position, :fragment_id, :execution_ref_id, :asset_id},
            references
            |> Enum.with_index()
            |> Enum.map(fn {reference, position} ->
              case reference do
                {:fragment, format, blob_key, size, metadata} ->
                  {:ok, fragment_id} =
                    get_or_create_fragment(db, format, blob_key, size, metadata)

                  {value_id, position, fragment_id, nil, nil}

                {:execution, run_ext, step_num, attempt} ->
                  {:ok, ref_id} = ensure_execution_ref(db, run_ext, step_num, attempt)
                  {value_id, position, nil, ref_id, nil}

                {:asset, external_id} ->
                  {:ok, {asset_id}} =
                    query_one!(db, "SELECT id FROM assets WHERE external_id = ?1", {external_id})

                  {value_id, position, nil, nil, asset_id}
              end
            end)
          )

        {:ok, value_id}
    end
  end

  defp ensure_execution_ref(db, run_external_id, step_number, attempt) do
    {:ok, _} =
      insert_one(
        db,
        :execution_refs,
        %{
          run_external_id: run_external_id,
          step_number: step_number,
          attempt: attempt
        },
        on_conflict: "DO NOTHING"
      )

    case query_one(
           db,
           "SELECT id FROM execution_refs WHERE run_external_id = ?1 AND step_number = ?2 AND attempt = ?3",
           {run_external_id, step_number, attempt}
         ) do
      {:ok, {id}} -> {:ok, id}
    end
  end

  defp get_or_create_format(db, name) do
    case query_one(db, "SELECT id FROM fragment_formats WHERE name = ?1", {name}) do
      {:ok, {id}} -> {:ok, id}
      {:ok, nil} -> insert_one(db, :fragment_formats, %{name: name})
    end
  end

  defp hash_fragment(format, blob_key, metadata) do
    metadata_parts =
      Enum.flat_map(metadata, fn {key, value} -> [key, Jason.encode!(value)] end)

    data =
      [format, blob_key]
      |> Enum.concat(metadata_parts)
      |> Enum.intersperse(0)

    :crypto.hash(:sha256, data)
  end

  defp get_or_create_fragment(db, format, blob_key, size, metadata) do
    hash = hash_fragment(format, blob_key, metadata)

    case query_one(db, "SELECT id FROM fragments WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {id}} ->
        {:ok, id}

      {:ok, nil} ->
        {:ok, format_id} = get_or_create_format(db, format)
        {:ok, blob_id} = get_or_create_blob(db, blob_key, size)

        {:ok, fragment_id} =
          insert_one(db, :fragments, %{
            hash: {:blob, hash},
            format_id: format_id,
            blob_id: blob_id
          })

        {:ok, _} =
          insert_many(
            db,
            :fragment_metadata,
            {:fragment_id, :key, :value},
            Enum.map(metadata, fn {key, value} ->
              {fragment_id, key, Jason.encode!(value)}
            end)
          )

        {:ok, fragment_id}
    end
  end
end
