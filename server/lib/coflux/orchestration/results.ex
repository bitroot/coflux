defmodule Coflux.Orchestration.Results do
  import Coflux.Store

  alias Coflux.Orchestration.{Errors, Values}

  # --- Completion kinds ---
  #
  # Kept in sync with the `completions.kind` column. See migration 4 for the
  # authoritative list and descriptions.

  @kind_succeeded 0
  @kind_errored 1
  @kind_abandoned 2
  @kind_crashed 3
  @kind_timeout 4
  @kind_cancelled 5
  @kind_suspended 6
  @kind_recurred 7
  @kind_deferred 8
  @kind_cached 9
  @kind_spawned 10

  @failure_kinds [@kind_errored, @kind_abandoned, @kind_crashed, @kind_timeout]

  def kind_atom(0), do: :succeeded
  def kind_atom(1), do: :errored
  def kind_atom(2), do: :abandoned
  def kind_atom(3), do: :crashed
  def kind_atom(4), do: :timeout
  def kind_atom(5), do: :cancelled
  def kind_atom(6), do: :suspended
  def kind_atom(7), do: :recurred
  def kind_atom(8), do: :deferred
  def kind_atom(9), do: :cached
  def kind_atom(10), do: :spawned

  def atom_kind(:succeeded), do: @kind_succeeded
  def atom_kind(:errored), do: @kind_errored
  def atom_kind(:abandoned), do: @kind_abandoned
  def atom_kind(:crashed), do: @kind_crashed
  def atom_kind(:timeout), do: @kind_timeout
  def atom_kind(:cancelled), do: @kind_cancelled
  def atom_kind(:suspended), do: @kind_suspended
  def atom_kind(:recurred), do: @kind_recurred
  def atom_kind(:deferred), do: @kind_deferred
  def atom_kind(:cached), do: @kind_cached
  def atom_kind(:spawned), do: @kind_spawned

  def failure_kinds, do: @failure_kinds

  # --- Writing results (payload only) ---
  #
  # Written when a worker's process reports a value or an error for the task
  # body. Does not write a completion row — the completion is written later
  # (via notify_terminated for worker-involved cases, or directly by the
  # server for server-initiated dispositions).

  # Writes a value payload. Returns the created timestamp on success.
  def record_value_result(db, execution_id, value) do
    with_snapshot(db, fn ->
      {:ok, value_id} = Values.get_or_create_value(db, value)
      insert_result_row(db, execution_id, value_id: value_id)
    end)
  end

  # Writes an error payload. `retryable` is the optional `when`-callback
  # result from the worker: `nil` = no callback configured, `true` = callback
  # allows retry, `false` = callback blocks retry.
  def record_error_result(db, execution_id, type, message, frames, retryable \\ nil) do
    with_snapshot(db, fn ->
      error_id = Errors.get_or_create(db, type, message, frames)
      insert_result_row(db, execution_id, error_id: error_id, retryable: retryable)
    end)
  end

  defp insert_result_row(db, execution_id, fields) do
    now = current_timestamp()

    row =
      Map.merge(
        %{execution_id: execution_id, created_at: now},
        Map.new(fields, fn
          {:retryable, nil} -> {:retryable, nil}
          {:retryable, true} -> {:retryable, 1}
          {:retryable, false} -> {:retryable, 0}
          {k, v} -> {k, v}
        end)
      )

    case insert_one(db, :results, row) do
      {:ok, _} -> {:ok, now}
      {:error, "UNIQUE constraint failed: " <> _} -> {:error, :already_recorded}
    end
  end

  # Compatibility shim. Dispatches legacy-shaped result tuples to the
  # appropriate split-API writes (result payload and/or completion). For
  # value/error the caller must still invoke record_completion later (via
  # complete_execution). For all other tagged tuples this writes the
  # completion directly — no separate record_completion call needed.
  #
  # Returns `{:ok, timestamp}` on success. The timestamp is the results row
  # for value/error (result arrival time) and the completions row
  # otherwise (terminal state time).
  def record_result(db, execution_id, result, created_by \\ nil) do
    case result do
      {:value, value} ->
        record_value_result(db, execution_id, value)

      {:error, type, message, frames, _retry_id, retryable} ->
        # Retry successor is recorded on the completion now — so we drop it
        # from the results row. The caller passes it to record_completion.
        record_error_result(db, execution_id, type, message, frames, retryable)

      {:error, type, message, frames, _retry_id} ->
        record_error_result(db, execution_id, type, message, frames, nil)

      :cancelled ->
        record_completion(db, execution_id, :cancelled, created_by: created_by)

      {:abandoned, retry_id} ->
        record_completion(db, execution_id, :abandoned,
          successor_id: retry_id,
          created_by: created_by
        )

      {:crashed, retry_id} ->
        record_completion(db, execution_id, :crashed,
          successor_id: retry_id,
          created_by: created_by
        )

      {:timeout, retry_id} ->
        record_completion(db, execution_id, :timeout,
          successor_id: retry_id,
          created_by: created_by
        )

      {:suspended, successor_id} ->
        record_completion(db, execution_id, :suspended,
          successor_id: successor_id,
          created_by: created_by
        )

      {:recurred, successor_id} ->
        record_completion(db, execution_id, :recurred,
          successor_id: successor_id,
          created_by: created_by
        )

      {:deferred, successor_id} ->
        record_completion(db, execution_id, :deferred,
          successor_id: successor_id,
          created_by: created_by
        )

      {:deferred, ref_id, value} ->
        with {:ok, _} <- record_value_result(db, execution_id, value) do
          record_completion(db, execution_id, :deferred,
            successor_ref_id: ref_id,
            created_by: created_by
          )
        end

      {:cached, successor_id} ->
        record_completion(db, execution_id, :cached,
          successor_id: successor_id,
          created_by: created_by
        )

      {:cached, ref_id, value} ->
        with {:ok, _} <- record_value_result(db, execution_id, value) do
          record_completion(db, execution_id, :cached,
            successor_ref_id: ref_id,
            created_by: created_by
          )
        end

      {:spawned, successor_id} ->
        record_completion(db, execution_id, :spawned,
          successor_id: successor_id,
          created_by: created_by
        )

      {:spawned, ref_id, value} ->
        with {:ok, _} <- record_value_result(db, execution_id, value) do
          record_completion(db, execution_id, :spawned,
            successor_ref_id: ref_id,
            created_by: created_by
          )
        end
    end
  end

  # --- Writing completions ---
  #
  # Written at the point the execution's terminal state becomes known:
  #   * Worker-involved cases: notify_terminated arrives. Kind is derived
  #     from whether a result row exists (succeeded / errored) or not
  #     (crashed). Caller supplies the retry successor, if any.
  #   * Server-initiated cases (abandon / cancel / cache-hit / defer /
  #     spawn / suspend / recur / timeout): the server calls this directly
  #     with the appropriate kind and successor.

  # `kind` is an atom from the enum above. `opts` accepts:
  #   * `:successor_id` — integer FK into executions (same-epoch pointer).
  #   * `:successor_ref_id` — integer FK into execution_refs (post-rotation).
  #   * `:created_by` — principal id.
  def record_completion(db, execution_id, kind, opts \\ []) when is_atom(kind) do
    with_transaction(db, fn ->
      now = current_timestamp()

      row = %{
        execution_id: execution_id,
        kind: atom_kind(kind),
        successor_id: Keyword.get(opts, :successor_id),
        successor_ref_id: Keyword.get(opts, :successor_ref_id),
        created_at: now,
        created_by: Keyword.get(opts, :created_by)
      }

      case insert_one(db, :completions, row) do
        {:ok, _} -> {:ok, now}
        {:error, "UNIQUE constraint failed: " <> _} -> {:error, :already_completed}
      end
    end)
  end

  # --- Existence checks ---

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

  # --- Reading ---

  # Returns the logical result for a consumer, derived by joining results and
  # completions. Shape:
  #   `{:ok, {logical_result, result_at, completion_at, created_by}}`
  #   `{:ok, nil}` — execution has no result and no completion yet
  #
  # `logical_result` is a tagged tuple in the same shape the old
  # single-table version returned — kept compatible so existing callers
  # don't have to change their pattern matching:
  #   * `{:value, value}`
  #   * `{:error, type, message, frames, retry_id, retryable}`
  #   * `:cancelled`
  #   * `{:abandoned, retry_id}`
  #   * `{:crashed, retry_id}`
  #   * `{:timeout, retry_id}`
  #   * `{:suspended, successor_id}`
  #   * `{:recurred, successor_id}`
  #   * `{:deferred, successor_id}`                       — in-flight
  #   * `{:deferred, successor_ref_id, value}`            — resolved
  #   * `{:cached, successor_id}`                         — in-flight
  #   * `{:cached, successor_ref_id, value}`              — resolved
  #   * `{:spawned, successor_id}`                        — in-flight
  #   * `{:spawned, successor_ref_id, value}`             — resolved
  #
  # `result_at` is the results row's created_at (nil if none).
  # `completion_at` is the completions row's created_at (nil if none).
  # `created_by` is the completion's creator principal (nil if none).
  def get_result(db, execution_id) do
    result_row =
      query_one(
        db,
        """
        SELECT value_id, error_id, retryable, created_at
        FROM results
        WHERE execution_id = ?1
        """,
        {execution_id}
      )

    completion_row =
      query_one(
        db,
        """
        SELECT c.kind, c.successor_id, c.successor_ref_id, c.created_at,
               p.user_external_id, t.external_id
        FROM completions AS c
        LEFT JOIN principals AS p ON c.created_by = p.id
        LEFT JOIN tokens AS t ON p.token_id = t.id
        WHERE c.execution_id = ?1
        """,
        {execution_id}
      )

    {:ok, result_at, value_id, error_id, retryable} = decode_result_row(result_row)

    {:ok, completion_at, kind, successor_id, successor_ref_id, created_by} =
      decode_completion_row(completion_row)

    case resolve_logical(
           db,
           kind,
           value_id,
           error_id,
           retryable,
           successor_id,
           successor_ref_id
         ) do
      nil -> {:ok, nil}
      logical -> {:ok, {logical, result_at, completion_at, created_by}}
    end
  end

  # Returns the raw result payload (not joined with completion). Used by
  # the completion-writing path to decide kind + retry from the worker's
  # recorded outcome without needing any transient in-memory state.
  # Returns:
  #   `{:ok, {:value, value}}`
  #   `{:ok, {:error, type, message, frames, retryable}}`
  #   `{:ok, nil}` — no result payload recorded
  def get_result_payload(db, execution_id) do
    case query_one(
           db,
           """
           SELECT value_id, error_id, retryable
           FROM results
           WHERE execution_id = ?1
           """,
           {execution_id}
         ) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, {value_id, nil, _}} when not is_nil(value_id) ->
        {:ok, value} = Values.get_value_by_id(db, value_id)
        {:ok, {:value, value}}

      {:ok, {nil, error_id, retryable}} when not is_nil(error_id) ->
        {:ok, {type, message, frames}} = Errors.get_by_id(db, error_id)
        {:ok, {:error, type, message, frames, decode_retryable(retryable)}}
    end
  end

  # Returns the raw completion row for an execution. Shape:
  #   `{:ok, {kind_atom, successor_id, successor_ref_id, created_at, created_by}}`
  #   `{:ok, nil}` — no completion yet
  def get_completion(db, execution_id) do
    case query_one(
           db,
           """
           SELECT c.kind, c.successor_id, c.successor_ref_id, c.created_at,
                  p.user_external_id, t.external_id
           FROM completions AS c
           LEFT JOIN principals AS p ON c.created_by = p.id
           LEFT JOIN tokens AS t ON p.token_id = t.id
           WHERE c.execution_id = ?1
           """,
           {execution_id}
         ) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, {kind, successor_id, successor_ref_id, created_at, user_ext, token_ext}} ->
        created_by = decode_principal(user_ext, token_ext)
        {:ok, {kind_atom(kind), successor_id, successor_ref_id, created_at, created_by}}
    end
  end

  # Returns the execution's status as a single atom, derived from the
  # split result/completion tables. Used by UI and lifecycle checks that
  # used to key off `result != nil` — in the new model the execution's
  # lifecycle is completion-driven.
  #
  # Status values:
  #   * `:pending`    — nothing recorded yet (still queued/running)
  #   * `:draining`   — result recorded, completion not yet (streams etc.)
  #   * a completion kind atom (`:succeeded` / `:errored` / ...)
  def execution_status(db, execution_id) do
    has_completion = has_completion?(db, execution_id)
    has_result = has_result?(db, execution_id)

    case {has_completion, has_result} do
      {{:ok, true}, _} ->
        {:ok, {kind, _, _, _, _}} = get_completion(db, execution_id)
        kind

      {{:ok, false}, {:ok, true}} ->
        :draining

      {{:ok, false}, {:ok, false}} ->
        :pending
    end
  end

  # True when the execution is in a state that could resolve to a useful
  # value — either still running (pending/draining) or cleanly completed.
  # Used for cache candidacy and memoisation: once a negative signal
  # appears (error / abandoned / crashed / timeout) this flips to false.
  def cache_candidate?(db, execution_id) do
    case execution_status(db, execution_id) do
      :pending -> true
      :draining -> true
      :succeeded -> true
      :suspended -> true
      :recurred -> true
      :deferred -> true
      :cached -> true
      :spawned -> true
      _ -> false
    end
  end

  # --- Helpers ---

  defp decode_result_row({:ok, nil}), do: {:ok, nil, nil, nil, nil}

  defp decode_result_row({:ok, {value_id, error_id, retryable, created_at}}),
    do: {:ok, created_at, value_id, error_id, retryable}

  defp decode_completion_row({:ok, nil}), do: {:ok, nil, nil, nil, nil, nil}

  defp decode_completion_row(
         {:ok, {kind, successor_id, successor_ref_id, created_at, user_ext, token_ext}}
       ) do
    {:ok, created_at, kind, successor_id, successor_ref_id,
     decode_principal(user_ext, token_ext)}
  end

  defp decode_principal(nil, nil), do: nil
  defp decode_principal(user_ext, nil), do: %{type: "user", external_id: user_ext}
  defp decode_principal(nil, token_ext), do: %{type: "token", external_id: token_ext}

  defp decode_retryable(nil), do: nil
  defp decode_retryable(1), do: true
  defp decode_retryable(0), do: false

  # Builds the legacy "logical result" tuple from the split tables. Used
  # by most callers (UI, topic state, consumer resolution). Returns `nil`
  # only when nothing has been recorded yet.
  #
  # `resolve_result` in server.ex is responsible for deciding when it's
  # safe to follow a successor — error results without a completion carry
  # `nil` as their successor here, and the server treats that as "still
  # pending".
  defp resolve_logical(_db, nil, nil, nil, _, _, _), do: nil

  # Value payload present. Returns the appropriate tagged tuple, picking
  # the successor-flavoured form when the completion says this was a
  # deferred/cached/spawned resolution.
  defp resolve_logical(db, kind, value_id, nil, _retryable, _successor_id, successor_ref_id)
       when not is_nil(value_id) do
    {:ok, value} = Values.get_value_by_id(db, value_id)

    case kind && kind_atom(kind) do
      :deferred when not is_nil(successor_ref_id) -> {:deferred, successor_ref_id, value}
      :cached when not is_nil(successor_ref_id) -> {:cached, successor_ref_id, value}
      :spawned when not is_nil(successor_ref_id) -> {:spawned, successor_ref_id, value}
      _ -> {:value, value}
    end
  end

  # Error payload without a completion yet. We return the error so UI can
  # display it; the successor slot is nil so consumer resolution treats
  # it as still pending (the retry decision happens at completion time).
  defp resolve_logical(db, nil, nil, error_id, retryable, _successor_id, _successor_ref_id)
       when not is_nil(error_id) do
    {:ok, {type, message, frames}} = Errors.get_by_id(db, error_id)
    {:error, type, message, frames, nil, decode_retryable(retryable)}
  end

  # Completion present (possibly with no results row).
  defp resolve_logical(db, kind, _value_id, error_id, retryable, successor_id, successor_ref_id) do
    case kind_atom(kind) do
      :succeeded ->
        nil

      :errored when not is_nil(error_id) ->
        {:ok, {type, message, frames}} = Errors.get_by_id(db, error_id)
        {:error, type, message, frames, successor_id, decode_retryable(retryable)}

      :abandoned ->
        {:abandoned, successor_id}

      :crashed ->
        {:crashed, successor_id}

      :timeout ->
        {:timeout, successor_id}

      :cancelled ->
        :cancelled

      :suspended ->
        {:suspended, successor_id}

      :recurred ->
        {:recurred, successor_id}

      :deferred ->
        build_successor_tuple(db, :deferred, nil, successor_id, successor_ref_id)

      :cached ->
        build_successor_tuple(db, :cached, nil, successor_id, successor_ref_id)

      :spawned ->
        build_successor_tuple(db, :spawned, nil, successor_id, successor_ref_id)
    end
  end

  defp build_successor_tuple(_db, tag, nil, successor_id, nil) when not is_nil(successor_id),
    do: {tag, successor_id}

  defp build_successor_tuple(db, tag, value_id, nil, successor_ref_id)
       when not is_nil(value_id) and not is_nil(successor_ref_id) do
    {:ok, value} = Values.get_value_by_id(db, value_id)
    {tag, successor_ref_id, value}
  end

  # --- Assets (unchanged) ---

  def put_execution_asset(db, execution_id, asset_id) do
    now = current_timestamp()

    {:ok, _} =
      insert_one(
        db,
        :execution_assets,
        %{execution_id: execution_id, asset_id: asset_id, created_at: now},
        on_conflict: "DO NOTHING"
      )

    :ok
  end

  def get_assets_for_execution(db, execution_id) do
    case query(
           db,
           "SELECT asset_id FROM execution_assets WHERE execution_id = ?1",
           {execution_id}
         ) do
      {:ok, rows} -> {:ok, Enum.map(rows, fn {asset_id} -> asset_id end)}
    end
  end

  defp current_timestamp() do
    System.os_time(:millisecond)
  end
end
