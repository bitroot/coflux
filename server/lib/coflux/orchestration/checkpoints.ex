defmodule Coflux.Orchestration.Checkpoints do
  @moduledoc """
  Storage for step-scoped durable state ("checkpoints").

  A checkpoint is a named value that survives across executions of a step —
  retries, suspends, recurrences and manual re-runs. It exists for the state
  an execution needs to carry forward but that isn't naturally a task result
  (a cursor, a page token, an accumulator); anything that *is* a task result
  should be memoised instead.

  Scope is `(step, workspace)`. Reads walk the workspace chain nearest-first
  and take the latest attempt that touched checkpoints in the *nearest*
  workspace that has any — a workspace that has written its own state never
  falls back to its base. Writes always land on the writing execution's own
  attempt, so inheritance is read-only and one-directional: a re-run in a
  descendant workspace reads the base's state without being able to corrupt
  it.

  Invariants:

    * Each execution's row-set is a complete snapshot of the effective
      checkpoint, not a delta. `apply_delta/7` materialises the previous
      snapshot on an execution's first write, then applies the delta, in one
      transaction.
    * A `NULL` `value_id` is a tombstone — the name was explicitly reset at
      that attempt. Distinct from a stored null *value*, which is an ordinary
      `values_` row.
    * `reset` always writes a tombstone, even for a name that was never set.
      That keeps an execution's row-set non-empty whenever it touched
      checkpoints, so "reset everything" can't be mistaken for "wrote
      nothing" and resurrect the pre-reset state from an earlier attempt.
    * Rows are updated in place within an execution — a deliberate exception
      to the append-only convention, documented in `5.sql`. Nothing can
      observe an intra-execution overwrite: the execution reads its own
      writes from memory, and the worker-side throttle discards intermediate
      values before they reach the server.
    * Attempts are globally ordered per step (`executions` has
      `UNIQUE (step_id, attempt)`), so ordering by attempt is well-defined
      even across workspaces. Ordering by attempt rather than wall-clock is
      what makes a stale worker harmless: its writes land on its own, older
      attempt and are never read.

  History across attempts is preserved by the `(execution_id, name)` key and
  compacted to the effective snapshot at epoch rotation.
  """

  import Coflux.Store

  alias Coflux.Orchestration.Values

  @doc """
  Resolves the effective checkpoint for a step.

  `workspace_chain` is a list of workspace ids ordered nearest-first (the
  execution's own workspace, then its bases). `before_attempt` bounds the
  lookup to attempts strictly below it — pass the reading execution's own
  attempt so it sees what it started with, or `nil` for the current state.

  Returns `{:ok, %{name => value}}` with tombstones dropped and values
  resolved.
  """
  def get_effective(db, step_id, workspace_chain, before_attempt \\ nil) do
    {:ok, rows} = get_snapshot_rows(db, step_id, workspace_chain, before_attempt)

    entries =
      rows
      |> Enum.reject(fn {_name, value_id} -> is_nil(value_id) end)
      |> Map.new(fn {name, value_id} ->
        {:ok, value} = Values.get_value_by_id(db, value_id)
        {name, value}
      end)

    {:ok, entries}
  end

  @doc """
  Applies a checkpoint delta recorded by `execution_id`.

  `set` is a map of `name => value` (values in the same `{:raw, ...}` /
  `{:blob, ...}` form as step arguments); `reset` is a list of names. Both are
  applied in a single transaction, preceded by carry-forward of the previous
  snapshot if this is the execution's first checkpoint write. Resets are
  applied before sets, so a name appearing in both ends up set.

  Returns `{:ok, timestamp}`.
  """
  def apply_delta(db, execution_id, step_id, workspace_chain, attempt, set, reset) do
    with_transaction(db, fn ->
      now = current_timestamp()

      :ok = ensure_snapshot(db, execution_id, step_id, workspace_chain, attempt, now)

      Enum.each(reset, fn name ->
        :ok = put(db, execution_id, name, nil, now)
      end)

      Enum.each(set, fn {name, value} ->
        {:ok, value_id} = Values.get_or_create_value(db, value)
        :ok = put(db, execution_id, name, value_id, now)
      end)

      {:ok, now}
    end)
  end

  @doc """
  Every checkpoint row recorded by `execution_id`, tombstones included.

  Returns `{:ok, [{name, value_id | nil, updated_at}]}` in name order. Used by
  epoch rotation and by the run topic.
  """
  def get_rows_for_execution(db, execution_id) do
    query(
      db,
      """
      SELECT name, value_id, updated_at
      FROM checkpoints
      WHERE execution_id = ?1
      ORDER BY name
      """,
      {execution_id}
    )
  end

  @doc """
  The checkpoint an execution ended up holding, with values resolved and
  tombstones dropped.

  Since each execution's row-set is a complete snapshot, this is the whole
  effective checkpoint as of that attempt — not just what it changed. An
  execution that never wrote has no rows and returns an empty map.

  Returns `{:ok, %{name => value}}`.
  """
  def get_effective_for_execution(db, execution_id) do
    {:ok, rows} = get_rows_for_execution(db, execution_id)
    {:ok, resolve_execution_rows(db, rows)}
  end

  @doc """
  What an execution started from, and what it ended up holding.

  The first element is the effective checkpoint as of the execution's own
  attempt — what it was handed when it started. The second is its own
  snapshot, or the first again when it has no rows: an execution that never
  wrote left the state exactly as it found it, which isn't the same as
  holding nothing.

  Returns `{:ok, {before, after}}`, both `%{name => value}`.
  """
  def get_execution_snapshots(db, execution_id, step_id, workspace_chain, attempt) do
    {:ok, before} = get_effective(db, step_id, workspace_chain, attempt)
    {:ok, rows} = get_rows_for_execution(db, execution_id)

    # Only an empty row-set means "didn't write" — a row-set that resolves to
    # nothing is an execution that reset everything, which is a real change.
    after_ = if Enum.empty?(rows), do: before, else: resolve_execution_rows(db, rows)

    {:ok, {before, after_}}
  end

  defp resolve_execution_rows(db, rows) do
    rows
    |> Enum.reject(fn {_name, value_id, _updated_at} -> is_nil(value_id) end)
    |> Map.new(fn {name, value_id, _updated_at} ->
      {:ok, value} = Values.get_value_by_id(db, value_id)
      {name, value}
    end)
  end

  @doc """
  The execution ids holding the effective snapshot for `step_id` — one per
  workspace that has any checkpoint rows.

  Epoch rotation copies only these, discarding the rest of the history.
  """
  def get_snapshot_execution_ids(db, step_id) do
    case query(
           db,
           """
           SELECT e.id
           FROM executions AS e
           WHERE e.step_id = ?1
             AND EXISTS (SELECT 1 FROM checkpoints AS c WHERE c.execution_id = e.id)
             AND e.attempt = (
               SELECT MAX(e2.attempt)
               FROM executions AS e2
               WHERE e2.step_id = e.step_id
                 AND e2.workspace_id = e.workspace_id
                 AND EXISTS (SELECT 1 FROM checkpoints AS c2 WHERE c2.execution_id = e2.id)
             )
           """,
           {step_id}
         ) do
      {:ok, rows} ->
        {:ok, Enum.map(rows, fn {id} -> id end)}
    end
  end

  # The raw row-set of the effective snapshot — tombstones included, values
  # unresolved. Carry-forward needs the tombstones (so a reset stays reset),
  # `get_effective/4` drops them.
  defp get_snapshot_rows(db, step_id, workspace_chain, before_attempt) do
    location =
      Enum.find_value(workspace_chain, fn workspace_id ->
        case get_latest_attempt(db, step_id, workspace_id, before_attempt) do
          {:ok, nil} -> nil
          {:ok, attempt} -> {workspace_id, attempt}
        end
      end)

    case location do
      nil -> {:ok, []}
      {workspace_id, attempt} -> get_attempt_rows(db, step_id, workspace_id, attempt)
    end
  end

  # The highest attempt of `step_id` in `workspace_id` that recorded any
  # checkpoint rows, below `before_attempt` if given.
  #
  # Written as ORDER BY ... LIMIT 1 rather than MAX(...) so SQLite walks
  # idx_executions_step_workspace descending and stops at the first hit,
  # rather than scanning every attempt of the step.
  defp get_latest_attempt(db, step_id, workspace_id, before_attempt) do
    {sql, args} =
      if before_attempt do
        {"""
         SELECT e.attempt
         FROM executions AS e
         WHERE e.step_id = ?1 AND e.workspace_id = ?2 AND e.attempt < ?3
           AND EXISTS (SELECT 1 FROM checkpoints AS c WHERE c.execution_id = e.id)
         ORDER BY e.attempt DESC
         LIMIT 1
         """, {step_id, workspace_id, before_attempt}}
      else
        {"""
         SELECT e.attempt
         FROM executions AS e
         WHERE e.step_id = ?1 AND e.workspace_id = ?2
           AND EXISTS (SELECT 1 FROM checkpoints AS c WHERE c.execution_id = e.id)
         ORDER BY e.attempt DESC
         LIMIT 1
         """, {step_id, workspace_id}}
      end

    case query_one(db, sql, args) do
      {:ok, nil} -> {:ok, nil}
      {:ok, {attempt}} -> {:ok, attempt}
    end
  end

  defp get_attempt_rows(db, step_id, workspace_id, attempt) do
    query(
      db,
      """
      SELECT c.name, c.value_id
      FROM checkpoints AS c
      INNER JOIN executions AS e ON e.id = c.execution_id
      WHERE e.step_id = ?1 AND e.workspace_id = ?2 AND e.attempt = ?3
      """,
      {step_id, workspace_id, attempt}
    )
  end

  # Materialise the previous effective snapshot into this execution, if it
  # hasn't written anything yet. Bounded by the execution's own attempt so the
  # stored snapshot matches what the execution actually started with.
  defp ensure_snapshot(db, execution_id, step_id, workspace_chain, attempt, now) do
    case has_rows?(db, execution_id) do
      {:ok, true} ->
        :ok

      {:ok, false} ->
        {:ok, rows} = get_snapshot_rows(db, step_id, workspace_chain, attempt)

        Enum.each(rows, fn {name, value_id} ->
          :ok = put(db, execution_id, name, value_id, now)
        end)

        :ok
    end
  end

  defp has_rows?(db, execution_id) do
    case query_one(
           db,
           "SELECT 1 FROM checkpoints WHERE execution_id = ?1 LIMIT 1",
           {execution_id}
         ) do
      {:ok, nil} -> {:ok, false}
      {:ok, {1}} -> {:ok, true}
    end
  end

  defp put(db, execution_id, name, value_id, now) do
    {:ok, _} =
      insert_one(
        db,
        :checkpoints,
        %{
          execution_id: execution_id,
          name: name,
          value_id: value_id,
          updated_at: now
        },
        on_conflict:
          "(execution_id, name) DO UPDATE SET value_id = excluded.value_id, updated_at = excluded.updated_at"
      )

    :ok
  end

  defp current_timestamp() do
    System.os_time(:millisecond)
  end
end
