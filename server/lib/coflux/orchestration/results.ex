defmodule Coflux.Orchestration.Results do
  import Coflux.Store

  alias Coflux.Orchestration.{Errors, Values}

  # Writes the results row capturing the disposition (value/error/retryable)
  # and any server-decided successor. Written at the time the disposition is
  # known — for worker-reported results that's put_result/put_error/etc.;
  # for server-initiated dispositions (abandonment, defer/cache/spawn) it's
  # when the server makes the decision.
  #
  # A matching completion row is written separately via record_completion,
  # typically when the worker's process confirms it has terminated (via
  # notify_terminated). For server-initiated cases that never involve a
  # worker, the caller writes both in sequence.
  def record_result(db, execution_id, result, created_by \\ nil) do
    with_snapshot(db, fn ->
      now = current_timestamp()

      {type, error_id, value_id, successor_id, successor_ref_id, retryable} =
        case result do
          {:error, type, message, frames, retry_id, retryable} ->
            error_id = Errors.get_or_create(db, type, message, frames)
            {0, error_id, nil, retry_id, nil, retryable}

          {:error, type, message, frames, retry_id} ->
            error_id = Errors.get_or_create(db, type, message, frames)
            {0, error_id, nil, retry_id, nil, nil}

          {:value, value} ->
            {:ok, value_id} = Values.get_or_create_value(db, value)
            {1, nil, value_id, nil, nil, nil}

          {:abandoned, retry_id} ->
            {2, nil, nil, retry_id, nil, nil}

          :cancelled ->
            {3, nil, nil, nil, nil, nil}

          {:timeout, retry_id} ->
            {8, nil, nil, retry_id, nil, nil}

          # In-flight deferred (successor still executing)
          {:deferred, defer_id} ->
            {4, nil, nil, defer_id, nil, nil}

          # Resolved deferred (successor resolved to a value — from epoch copy
          # or runtime cache hit)
          {:deferred, ref_id, value} ->
            {:ok, value_id} = Values.get_or_create_value(db, value)
            {4, nil, value_id, nil, ref_id, nil}

          # In-flight cached
          {:cached, cached_id} ->
            {5, nil, nil, cached_id, nil, nil}

          # Resolved cached
          {:cached, ref_id, value} ->
            {:ok, value_id} = Values.get_or_create_value(db, value)
            {5, nil, value_id, nil, ref_id, nil}

          {:suspended, successor_id} ->
            {6, nil, nil, successor_id, nil, nil}

          {:recurred, successor_id} ->
            {9, nil, nil, successor_id, nil, nil}

          # In-flight spawned
          {:spawned, execution_id} ->
            {7, nil, nil, execution_id, nil, nil}

          # Resolved spawned
          {:spawned, ref_id, value} ->
            {:ok, value_id} = Values.get_or_create_value(db, value)
            {7, nil, value_id, nil, ref_id, nil}
        end

      case insert_result(
             db,
             execution_id,
             type,
             error_id,
             value_id,
             successor_id,
             successor_ref_id,
             retryable,
             now,
             created_by
           ) do
        {:ok, _} -> {:ok, now}
        {:error, "UNIQUE constraint failed: " <> _field} -> {:error, :already_recorded}
      end
    end)
  end

  # Writes the completion row — a simple timestamp marker recording that the
  # execution's process has fully terminated. For worker-involved cases this
  # is triggered by notify_terminated; for server-initiated dispositions
  # (abandonment, cache-hit scheduling) the caller writes this right after
  # record_result.
  def record_completion(db, execution_id) do
    with_transaction(db, fn ->
      now = current_timestamp()

      case insert_one(db, :completions, %{
             execution_id: execution_id,
             created_at: now
           }) do
        {:ok, _} -> {:ok, now}
        {:error, "UNIQUE constraint failed: " <> _field} -> {:error, :already_completed}
      end
    end)
  end

  def has_result?(db, execution_id) do
    case query_one(db, "SELECT count(*) FROM results WHERE execution_id = ?1", {execution_id}) do
      {:ok, {0}} -> {:ok, false}
      {:ok, {1}} -> {:ok, true}
    end
  end

  def has_completion?(db, execution_id) do
    case query_one(
           db,
           "SELECT count(*) FROM completions WHERE execution_id = ?1",
           {execution_id}
         ) do
      {:ok, {0}} -> {:ok, false}
      {:ok, {1}} -> {:ok, true}
    end
  end

  def get_result(db, execution_id) do
    case query_one(
           db,
           """
           SELECT r.type, r.error_id, r.value_id, r.successor_id, r.successor_ref_id,
                  r.retryable, r.created_at, c.created_at AS completion_created_at,
                  p.user_external_id AS created_by_user_external_id,
                  t.external_id AS created_by_token_external_id
           FROM results AS r
           LEFT JOIN completions AS c ON c.execution_id = r.execution_id
           LEFT JOIN principals AS p ON r.created_by = p.id
           LEFT JOIN tokens AS t ON p.token_id = t.id
           WHERE r.execution_id = ?1
           """,
           {execution_id}
         ) do
      {:ok, nil} ->
        # No results row. If the execution has a completion row anyway,
        # the worker terminated without ever reporting — treat as crashed.
        case query_one(
               db,
               "SELECT created_at FROM completions WHERE execution_id = ?1",
               {execution_id}
             ) do
          {:ok, {completion_created_at}} ->
            {:ok, {{:crashed, nil}, nil, completion_created_at, nil}}

          {:ok, nil} ->
            {:ok, nil}
        end

      {:ok,
       {type, error_id, value_id, successor_id, successor_ref_id, retryable, created_at,
        completion_created_at, created_by_user_ext_id, created_by_token_ext_id}} ->
        created_by =
          case {created_by_user_ext_id, created_by_token_ext_id} do
            {nil, nil} -> nil
            {user_ext_id, nil} -> %{type: "user", external_id: user_ext_id}
            {nil, token_ext_id} -> %{type: "token", external_id: token_ext_id}
          end

        retryable =
          case retryable do
            1 -> true
            0 -> false
            nil -> nil
          end

        result =
          case {type, error_id, value_id, successor_id, successor_ref_id} do
            {0, error_id, nil, retry_id, nil} ->
              case Errors.get_by_id(db, error_id) do
                {:ok, {type, message, frames}} ->
                  {:error, type, message, frames, retry_id, retryable}
              end

            {1, nil, value_id, nil, nil} ->
              case Values.get_value_by_id(db, value_id) do
                {:ok, value} -> {:value, value}
              end

            {2, nil, nil, retry_id, nil} ->
              {:abandoned, retry_id}

            {3, nil, nil, nil, nil} ->
              :cancelled

            {8, nil, nil, retry_id, nil} ->
              {:timeout, retry_id}

            {4, nil, nil, defer_id, nil} ->
              {:deferred, defer_id}

            {4, nil, value_id, nil, ref_id} when not is_nil(ref_id) ->
              case Values.get_value_by_id(db, value_id) do
                {:ok, value} -> {:deferred, ref_id, value}
              end

            {5, nil, nil, cached_id, nil} ->
              {:cached, cached_id}

            {5, nil, value_id, nil, ref_id} when not is_nil(ref_id) ->
              case Values.get_value_by_id(db, value_id) do
                {:ok, value} -> {:cached, ref_id, value}
              end

            {6, nil, nil, successor_id, nil} ->
              {:suspended, successor_id}

            {7, nil, nil, execution_id, nil} ->
              {:spawned, execution_id}

            {7, nil, value_id, nil, ref_id} when not is_nil(ref_id) ->
              case Values.get_value_by_id(db, value_id) do
                {:ok, value} -> {:spawned, ref_id, value}
              end

            {9, nil, nil, successor_id, nil} ->
              {:recurred, successor_id}
          end

        {:ok, {result, created_at, completion_created_at, created_by}}
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
         successor_ref_id,
         retryable,
         created_at,
         created_by
       ) do
    insert_one(db, :results, %{
      execution_id: execution_id,
      type: type,
      error_id: error_id,
      value_id: value_id,
      successor_id: successor_id,
      successor_ref_id: successor_ref_id,
      retryable:
        case retryable do
          nil -> nil
          true -> 1
          false -> 0
        end,
      created_at: created_at,
      created_by: created_by
    })
  end

  defp current_timestamp() do
    System.os_time(:millisecond)
  end
end
