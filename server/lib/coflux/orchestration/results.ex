defmodule Coflux.Orchestration.Results do
  import Coflux.Store

  alias Coflux.Orchestration.Values

  def record_result(db, execution_id, result, created_by \\ nil) do
    with_transaction(db, fn ->
      now = current_timestamp()

      {type, error_id, value_id, successor_id} =
        case result do
          {:error, type, message, frames, retry_id} ->
            {:ok, error_id} = get_or_create_error(db, type, message, frames)
            {0, error_id, nil, retry_id}

          {:value, value} ->
            {:ok, value_id} = Values.get_or_create_value(db, value)
            {1, nil, value_id, nil}

          {:abandoned, retry_id} ->
            {2, nil, nil, retry_id}

          :cancelled ->
            {3, nil, nil, nil}

          {:deferred, defer_id} ->
            {4, nil, nil, defer_id}

          {:cached, cached_id} ->
            {5, nil, nil, cached_id}

          {:suspended, successor_id} ->
            {6, nil, nil, successor_id}

          {:spawned, execution_id} ->
            {7, nil, nil, execution_id}
        end

      case insert_result(
             db,
             execution_id,
             type,
             error_id,
             value_id,
             successor_id,
             now,
             created_by
           ) do
        {:ok, _} ->
          {:ok, now}

        {:error, "UNIQUE constraint failed: " <> _field} ->
          {:error, :already_recorded}
      end
    end)
  end

  def has_result?(db, execution_id) do
    case query_one(db, "SELECT count(*) FROM results WHERE execution_id = ?1", {execution_id}) do
      {:ok, {0}} -> {:ok, false}
      {:ok, {1}} -> {:ok, true}
    end
  end

  def get_result(db, execution_id) do
    case query_one(
           db,
           """
           SELECT r.type, r.error_id, r.value_id, r.successor_id, r.created_at,
                  p.user_external_id AS created_by_user_external_id,
                  t.external_id AS created_by_token_external_id
           FROM results AS r
           LEFT JOIN principals AS p ON r.created_by = p.id
           LEFT JOIN tokens AS t ON p.token_id = t.id
           WHERE r.execution_id = ?1
           """,
           {execution_id}
         ) do
      {:ok,
       {type, error_id, value_id, successor_id, created_at, created_by_user_ext_id,
        created_by_token_ext_id}} ->
        created_by =
          case {created_by_user_ext_id, created_by_token_ext_id} do
            {nil, nil} -> nil
            {user_ext_id, nil} -> %{type: "user", external_id: user_ext_id}
            {nil, token_ext_id} -> %{type: "token", external_id: token_ext_id}
          end

        result =
          case {type, error_id, value_id, successor_id} do
            {0, error_id, nil, retry_id} ->
              case get_error_by_id(db, error_id) do
                {:ok, {type, message, frames}} ->
                  {:error, type, message, frames, retry_id}
              end

            {1, nil, value_id, nil} ->
              case Values.get_value_by_id(db, value_id) do
                {:ok, value} -> {:value, value}
              end

            {2, nil, nil, retry_id} ->
              {:abandoned, retry_id}

            {3, nil, nil, nil} ->
              :cancelled

            {4, nil, nil, defer_id} ->
              {:deferred, defer_id}

            {5, nil, nil, cached_id} ->
              {:cached, cached_id}

            {6, nil, nil, successor_id} ->
              {:suspended, successor_id}

            {7, nil, nil, execution_id} ->
              {:spawned, execution_id}
          end

        {:ok, {result, created_at, created_by}}

      {:ok, nil} ->
        {:ok, nil}
    end
  end

  defp get_error_by_id(db, error_id) do
    {:ok, {type, message}} =
      query_one!(db, "SELECT type, message FROM errors WHERE id = ?1", {error_id})

    {:ok, frames} =
      query(
        db,
        "SELECT file, line, name, code FROM error_frames WHERE error_id = ?1 ORDER BY depth",
        {error_id}
      )

    {:ok, {type, message, frames}}
  end

  defp hash_error(type, message, frames) do
    frame_parts =
      Enum.flat_map(frames, fn {file, line, name, code} ->
        [file, Integer.to_string(line), name || 0, code || 0]
      end)

    parts = Enum.concat([type, message], frame_parts)
    :crypto.hash(:sha256, Enum.intersperse(parts, 0))
  end

  defp get_or_create_error(db, type, message, frames) do
    hash = hash_error(type, message, frames)

    case query_one(db, "SELECT id FROM errors WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {id}} ->
        {:ok, id}

      {:ok, nil} ->
        {:ok, error_id} =
          insert_one(db, :errors, %{
            hash: {:blob, hash},
            type: type,
            message: message
          })

        {:ok, _} =
          insert_many(
            db,
            :error_frames,
            {:error_id, :depth, :file, :line, :name, :code},
            frames
            |> Enum.with_index()
            |> Enum.map(fn {{file, line, name, code}, index} ->
              {error_id, index, file, line, name, code}
            end)
          )

        {:ok, error_id}
    end
  end

  def put_execution_asset(db, execution_id, asset_id) do
    now = current_timestamp()

    {:ok, _} =
      insert_one(
        db,
        :execution_assets,
        %{
          execution_id: execution_id,
          asset_id: asset_id,
          created_at: now
        },
        on_conflict: "DO NOTHING"
      )

    :ok
  end

  # TODO: get all assets for run?
  def get_assets_for_execution(db, execution_id) do
    case query(
           db,
           "SELECT asset_id FROM execution_assets WHERE execution_id = ?1",
           {execution_id}
         ) do
      {:ok, rows} ->
        {:ok, Enum.map(rows, fn {asset_id} -> asset_id end)}
    end
  end

  defp insert_result(
         db,
         execution_id,
         type,
         error_id,
         value_id,
         successor_id,
         created_at,
         created_by
       ) do
    insert_one(db, :results, %{
      execution_id: execution_id,
      type: type,
      error_id: error_id,
      value_id: value_id,
      successor_id: successor_id,
      created_at: created_at,
      created_by: created_by
    })
  end

  defp current_timestamp() do
    System.os_time(:millisecond)
  end
end
