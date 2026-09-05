defmodule Coflux.Orchestration.Streams do
  @moduledoc """
  Storage for step-produced streams.

  A stream is an ordered, append-only sequence of values produced by a
  step within a workspace. It is identified externally by
  `<run>:<step>_<index>`, where `index` is allocated here, per step, so
  the id is unique across attempts and across workspaces.

  Streams outlive the execution that opens them in exactly one case: a
  suspend. An execution that suspends leaves its open streams *paused*,
  and the execution that resumes the step continues them. Registration
  matches a resuming execution's k-th stream onto the paused stream whose
  opener registered it k-th (`position`). Every other way an execution can
  end closes the step's open streams — that lifecycle logic lives in
  `Server`, not here.

  Items within a stream are identified by `sequence` — a 0-based,
  monotonically increasing per-item counter that continues across a
  suspension. Each item records the execution that appended it.

  Invariants enforced here (and by schema FKs):

    * Items are append-only with monotonic `sequence` starting at 0.
    * A closure is terminal — no items may be appended after one is
      recorded. The closure records the execution that closed the stream.
    * Only an execution registered on a stream may append to it.
    * The latest registrant is the stream's producer, and its registration
      carries the config (buffer, timeout) in force.
    * Consumer cursors are kept in-memory only; re-run consumers subscribe
      fresh from sequence 0.

  The SQL column is quoted with backticks (``` `index` ```) throughout
  because `INDEX` is a SQLite keyword; at the Elixir level we pass `:index`
  as a map key — the Store helper handles quoting for inserts.
  """

  import Coflux.Store

  alias Coflux.Orchestration.{Errors, Results, Values}

  # --- Registration ---

  # Registers `execution_id` as a producer of the stream it opened at
  # `position` (its k-th registration). Either continues the step's paused
  # stream at that position, or opens a new one with the next index for
  # the step.
  #
  # A stream is paused when it is open and its latest registrant has
  # completed as suspended. An open stream whose producer is still live is
  # never matched (a recurrent step's successor can be dispatched while
  # its predecessor is still draining an explicit stream), and a closed
  # stream is never continued.
  #
  # Returns `{:ok, %{id, index, head, continued, created_at}}`. `head` is
  # the highest sequence already in the stream (`-1` when empty), so the
  # producer knows where to resume numbering. Registering the same
  # position twice for one execution is idempotent and returns the
  # existing stream.
  def register(db, step_id, workspace_id, execution_id, position, buffer, timeout_ms) do
    with_transaction(db, fn ->
      now = current_timestamp()

      case get_registered_stream(db, execution_id, position) do
        {:ok, {stream_id, index}} ->
          {:ok, head} = get_stream_head(db, stream_id)
          {:ok, %{id: stream_id, index: index, head: head, continued: false, created_at: nil}}

        {:ok, nil} ->
          case find_paused_stream(db, step_id, workspace_id, position) do
            {:ok, {stream_id, index}} ->
              insert_registration(db, stream_id, execution_id, buffer, timeout_ms, now)
              {:ok, head} = get_stream_head(db, stream_id)
              {:ok, %{id: stream_id, index: index, head: head, continued: true, created_at: now}}

            {:ok, nil} ->
              {:ok, index} = next_index(db, step_id)

              {:ok, stream_id} =
                insert_one(db, :streams, %{
                  step_id: step_id,
                  workspace_id: workspace_id,
                  index: index,
                  position: position,
                  created_at: now
                })

              insert_registration(db, stream_id, execution_id, buffer, timeout_ms, now)
              {:ok, %{id: stream_id, index: index, head: -1, continued: false, created_at: now}}
          end
      end
    end)
  end

  defp insert_registration(db, stream_id, execution_id, buffer, timeout_ms, now) do
    {:ok, _} =
      insert_one(db, :stream_registrations, %{
        stream_id: stream_id,
        execution_id: execution_id,
        buffer: buffer,
        timeout_ms: timeout_ms,
        created_at: now
      })
  end

  defp get_registered_stream(db, execution_id, position) do
    query_one(
      db,
      """
      SELECT s.id, s.`index`
      FROM stream_registrations AS r
      INNER JOIN streams AS s ON s.id = r.stream_id
      WHERE r.execution_id = ?1 AND s.position = ?2
      """,
      {execution_id, position}
    )
  end

  defp find_paused_stream(db, step_id, workspace_id, position) do
    case query_one(
           db,
           """
           SELECT s.id, s.`index`
           FROM streams AS s
           LEFT JOIN stream_closures AS c ON c.stream_id = s.id
           WHERE s.step_id = ?1 AND s.workspace_id = ?2 AND s.position = ?3
             AND c.stream_id IS NULL
           ORDER BY s.`index` DESC
           LIMIT 1
           """,
           {step_id, workspace_id, position}
         ) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, {stream_id, index}} ->
        if paused?(db, stream_id) do
          {:ok, {stream_id, index}}
        else
          {:ok, nil}
        end
    end
  end

  # An open stream is paused when its latest registrant completed as
  # suspended. No completion means the producer is live.
  defp paused?(db, stream_id) do
    suspended = Results.atom_kind(:suspended)

    case query_one(
           db,
           """
           SELECT c.kind
           FROM stream_registrations AS r
           INNER JOIN executions AS e ON e.id = r.execution_id
           LEFT JOIN completions AS c ON c.execution_id = r.execution_id
           WHERE r.stream_id = ?1
           ORDER BY e.attempt DESC
           LIMIT 1
           """,
           {stream_id}
         ) do
      {:ok, {^suspended}} -> true
      {:ok, _} -> false
    end
  end

  defp next_index(db, step_id) do
    case query_one(
           db,
           "SELECT COALESCE(MAX(`index`) + 1, 0) FROM streams WHERE step_id = ?1",
           {step_id}
         ) do
      {:ok, {index}} -> {:ok, index}
    end
  end

  # --- Lookup ---

  def get_stream_by_step_index(db, step_id, index) do
    case query_one(
           db,
           "SELECT id FROM streams WHERE step_id = ?1 AND `index` = ?2",
           {step_id, index}
         ) do
      {:ok, {id}} -> {:ok, id}
      {:ok, nil} -> {:error, :not_found}
    end
  end

  def get_stream_id_by_key(db, run_external_id, step_number, index) do
    case query_one(
           db,
           """
           SELECT st.id
           FROM streams AS st
           INNER JOIN steps AS s ON s.id = st.step_id
           INNER JOIN runs AS r ON r.id = s.run_id
           WHERE r.external_id = ?1 AND s.number = ?2 AND st.`index` = ?3
           """,
           {run_external_id, step_number, index}
         ) do
      {:ok, {id}} -> {:ok, id}
      {:ok, nil} -> {:error, :not_found}
    end
  end

  # Everything needed to describe a stream: its identity, where it lives,
  # and the step it belongs to.
  def get_stream(db, stream_id) do
    case query_one(
           db,
           """
           SELECT st.step_id, st.workspace_id, st.`index`, st.position, st.created_at,
                  r.external_id, s.number, s.module, s.target
           FROM streams AS st
           INNER JOIN steps AS s ON s.id = st.step_id
           INNER JOIN runs AS r ON r.id = s.run_id
           WHERE st.id = ?1
           """,
           {stream_id}
         ) do
      {:ok, nil} ->
        {:error, :not_found}

      {:ok,
       {step_id, workspace_id, index, position, created_at, run_external_id, step_number, module,
        target}} ->
        {:ok,
         %{
           id: stream_id,
           step_id: step_id,
           workspace_id: workspace_id,
           index: index,
           position: position,
           created_at: created_at,
           run_external_id: run_external_id,
           step_number: step_number,
           module: module,
           target: target
         }}
    end
  end

  def exists?(db, stream_id) do
    case query_one(db, "SELECT 1 FROM streams WHERE id = ?1", {stream_id}) do
      {:ok, nil} -> {:ok, false}
      {:ok, {1}} -> {:ok, true}
    end
  end

  # The config in force: the latest registration's. `{:ok, {buffer,
  # timeout_ms}}`, or `{:error, :not_found}` for an unknown stream.
  def get_config(db, stream_id) do
    case query_one(
           db,
           """
           SELECT r.buffer, r.timeout_ms
           FROM stream_registrations AS r
           INNER JOIN executions AS e ON e.id = r.execution_id
           WHERE r.stream_id = ?1
           ORDER BY e.attempt DESC
           LIMIT 1
           """,
           {stream_id}
         ) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, {buffer, timeout_ms}} -> {:ok, {buffer, timeout_ms}}
    end
  end

  # The stream's producer: the execution behind the latest registration.
  def get_producer(db, stream_id) do
    case query_one(
           db,
           """
           SELECT r.execution_id
           FROM stream_registrations AS r
           INNER JOIN executions AS e ON e.id = r.execution_id
           WHERE r.stream_id = ?1
           ORDER BY e.attempt DESC
           LIMIT 1
           """,
           {stream_id}
         ) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, {execution_id}} -> {:ok, execution_id}
    end
  end

  def registered?(db, stream_id, execution_id) do
    case query_one(
           db,
           "SELECT 1 FROM stream_registrations WHERE stream_id = ?1 AND execution_id = ?2",
           {stream_id, execution_id}
         ) do
      {:ok, nil} -> {:ok, false}
      {:ok, {1}} -> {:ok, true}
    end
  end

  # Every execution that has produced into the stream, in attempt order:
  # `[{execution_id, attempt, buffer, timeout_ms, created_at}, ...]`.
  def get_registrations(db, stream_id) do
    query(
      db,
      """
      SELECT r.execution_id, e.attempt, r.buffer, r.timeout_ms, r.created_at
      FROM stream_registrations AS r
      INNER JOIN executions AS e ON e.id = r.execution_id
      WHERE r.stream_id = ?1
      ORDER BY e.attempt
      """,
      {stream_id}
    )
  end

  # --- Items ---

  # Appends an item at `sequence`. The caller supplies the sequence
  # (worker-assigned, monotonic, continuing from the head a resuming
  # producer was told about). Returns:
  #   * `{:error, :not_registered}` if the stream doesn't exist
  #   * `{:error, :closed}` if the stream has a closure row
  #   * `{:error, :not_producer}` if `execution_id` isn't registered on it
  #   * `{:error, :already_appended}` if the sequence collides
  def append_item(db, stream_id, execution_id, sequence, value) do
    with_transaction(db, fn ->
      with {:ok, true} <- exists_or(db, stream_id),
           {:ok, false} <- closed_or(db, stream_id),
           {:ok, true} <- registered_or(db, stream_id, execution_id) do
        {:ok, value_id} = Values.get_or_create_value(db, value)
        now = current_timestamp()

        case insert_one(db, :stream_items, %{
               stream_id: stream_id,
               sequence: sequence,
               value_id: value_id,
               execution_id: execution_id,
               created_at: now
             }) do
          {:ok, _} -> {:ok, now}
          {:error, "UNIQUE constraint failed: " <> _} -> {:error, :already_appended}
        end
      end
    end)
  end

  defp exists_or(db, stream_id) do
    case exists?(db, stream_id) do
      {:ok, true} -> {:ok, true}
      {:ok, false} -> {:error, :not_registered}
    end
  end

  defp closed_or(db, stream_id) do
    case has_closure?(db, stream_id) do
      {:ok, false} -> {:ok, false}
      {:ok, true} -> {:error, :closed}
    end
  end

  defp registered_or(db, stream_id, execution_id) do
    case registered?(db, stream_id, execution_id) do
      {:ok, true} -> {:ok, true}
      {:ok, false} -> {:error, :not_producer}
    end
  end

  # Fetches up to `max_items` items starting at `from_sequence`, as
  # `[{sequence, value, created_at}, ...]` in sequence order. The caller
  # (Server) layers stride logic on top.
  def get_stream_items(db, stream_id, from_sequence, max_items) do
    case query(
           db,
           """
           SELECT sequence, value_id, created_at
           FROM stream_items
           WHERE stream_id = ?1 AND sequence >= ?2
           ORDER BY sequence
           LIMIT ?3
           """,
           {stream_id, from_sequence, max_items}
         ) do
      {:ok, rows} ->
        items =
          Enum.map(rows, fn {sequence, value_id, created_at} ->
            {:ok, value} = Values.get_value_by_id(db, value_id)
            {sequence, value, created_at}
          end)

        {:ok, items}
    end
  end

  # Highest sequence recorded for the stream, or `-1` if empty.
  def get_stream_head(db, stream_id) do
    case query_one(
           db,
           "SELECT MAX(sequence) FROM stream_items WHERE stream_id = ?1",
           {stream_id}
         ) do
      {:ok, {nil}} -> {:ok, -1}
      {:ok, {sequence}} -> {:ok, sequence}
    end
  end

  # The last `max_items` items in sequence order, each with the attempt
  # that appended it, alongside the total item count. Used by the
  # inspection topic to bootstrap its bounded tail without materialising
  # the full log.
  def get_stream_tail(db, stream_id, max_items) do
    {:ok, {total_count}} =
      query_one(
        db,
        "SELECT COUNT(*) FROM stream_items WHERE stream_id = ?1",
        {stream_id}
      )

    case query(
           db,
           """
           SELECT i.sequence, i.value_id, e.attempt, i.created_at
           FROM stream_items AS i
           INNER JOIN executions AS e ON e.id = i.execution_id
           WHERE i.stream_id = ?1
           ORDER BY i.sequence DESC
           LIMIT ?2
           """,
           {stream_id, max_items}
         ) do
      {:ok, rows} ->
        items =
          rows
          |> Enum.reverse()
          |> Enum.map(fn {sequence, value_id, attempt, created_at} ->
            {:ok, value} = Values.get_value_by_id(db, value_id)
            {sequence, value, attempt, created_at}
          end)

        {:ok, {items, total_count}}
    end
  end

  # --- Closure ---

  # Closure reason codes — kept in sync with the CHECK constraint in 4.sql.
  @reason_complete 0
  @reason_errored 1
  @reason_lifecycle 2
  @reason_timeout 3

  # Closes the stream on behalf of `execution_id`. `spec` describes *why*:
  #
  #   * `:complete` — the producer finished normally, or the step
  #     completed successfully with the stream still open
  #   * `{:errored, type, message, frames}` — the producer raised; the
  #     error is stored via the errors table, same as Results
  #   * `:lifecycle` — closed implicitly because an execution of the step
  #     ended (cancel/crash/abandon/error/recur). No error is recorded
  #     here — callers derive one from the closing execution's completion.
  #   * `:timeout` — the worker closed the stream because its idle
  #     timeout elapsed without a new item being appended.
  def close_stream(db, stream_id, execution_id, spec) do
    with_transaction(db, fn ->
      case exists?(db, stream_id) do
        {:ok, false} ->
          {:error, :not_registered}

        {:ok, true} ->
          now = current_timestamp()
          {reason, error_id} = resolve_close_spec(db, spec)

          case insert_one(db, :stream_closures, %{
                 stream_id: stream_id,
                 reason: reason,
                 error_id: error_id,
                 execution_id: execution_id,
                 created_at: now
               }) do
            {:ok, _} -> {:ok, now}
            {:error, "UNIQUE constraint failed: " <> _} -> {:error, :already_closed}
          end
      end
    end)
  end

  defp resolve_close_spec(_db, :complete), do: {@reason_complete, nil}
  defp resolve_close_spec(_db, :lifecycle), do: {@reason_lifecycle, nil}
  defp resolve_close_spec(_db, :timeout), do: {@reason_timeout, nil}

  defp resolve_close_spec(db, {:errored, type, message, frames}) do
    error_id = Errors.get_or_create(db, type, message, frames)
    {@reason_errored, error_id}
  end

  def reason_from_int(@reason_complete), do: :complete
  def reason_from_int(@reason_errored), do: :errored
  def reason_from_int(@reason_lifecycle), do: :lifecycle
  def reason_from_int(@reason_timeout), do: :timeout

  def has_closure?(db, stream_id) do
    case query_one(db, "SELECT 1 FROM stream_closures WHERE stream_id = ?1", {stream_id}) do
      {:ok, nil} -> {:ok, false}
      {:ok, {1}} -> {:ok, true}
    end
  end

  # Closure info, or `{:ok, nil}` if the stream is still open. Closure
  # info is `{reason, error | nil, closing_execution_id, created_at}`,
  # where `reason` is :complete | :errored | :lifecycle | :timeout and
  # `error` is the `{type, message, frames}` triple for :errored only —
  # for :lifecycle, callers derive it from the closing execution's
  # completion.
  def get_stream_closure(db, stream_id) do
    case query_one(
           db,
           "SELECT reason, error_id, execution_id, created_at FROM stream_closures WHERE stream_id = ?1",
           {stream_id}
         ) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, {reason_int, nil, execution_id, created_at}} ->
        {:ok, {reason_from_int(reason_int), nil, execution_id, created_at}}

      {:ok, {reason_int, error_id, execution_id, created_at}} ->
        {:ok, error} = Errors.get_by_id(db, error_id)
        {:ok, {reason_from_int(reason_int), error, execution_id, created_at}}
    end
  end

  # Ids of the step's open streams in `workspace_id`, in index order. The
  # lifecycle code closes these when an execution of the step ends any
  # way other than suspending.
  def get_open_stream_ids_for_step(db, step_id, workspace_id) do
    case query(
           db,
           """
           SELECT s.id
           FROM streams AS s
           LEFT JOIN stream_closures AS c ON c.stream_id = s.id
           WHERE s.step_id = ?1 AND s.workspace_id = ?2 AND c.stream_id IS NULL
           ORDER BY s.`index`
           """,
           {step_id, workspace_id}
         ) do
      {:ok, rows} -> {:ok, Enum.map(rows, fn {id} -> id end)}
    end
  end

  # Ids of the open streams `execution_id` is registered on, in index
  # order. Narrower than the step's open streams: a pending execution that
  # never registered anything has none.
  def get_open_stream_ids_for_execution(db, execution_id) do
    case query(
           db,
           """
           SELECT s.id
           FROM stream_registrations AS r
           INNER JOIN streams AS s ON s.id = r.stream_id
           LEFT JOIN stream_closures AS c ON c.stream_id = s.id
           WHERE r.execution_id = ?1 AND c.stream_id IS NULL
           ORDER BY s.`index`
           """,
           {execution_id}
         ) do
      {:ok, rows} -> {:ok, Enum.map(rows, fn {id} -> id end)}
    end
  end

  # Summary of the closures `execution_id` wrote. Used by
  # `complete_execution` to decide whether to promote a value result to
  # `:stream_errored` / `:stream_timeout`.
  #
  # Shape: `{:ok, %{errored: integer | nil, timed_out: boolean}}`
  #   * `errored` — the `errors.id` for the *first* errored closure (in
  #     stream-index order), or `nil` if none errored
  #   * `timed_out` — true if any stream closed via idle timeout
  #
  # Lifecycle / complete closures are ignored: the former inherit the
  # execution's eventual outcome, the latter are the success case.
  def get_closure_summary_for_execution(db, execution_id) do
    case query(
           db,
           """
           SELECT c.reason, c.error_id
           FROM stream_closures AS c
           INNER JOIN streams AS s ON s.id = c.stream_id
           WHERE c.execution_id = ?1
           ORDER BY s.`index`
           """,
           {execution_id}
         ) do
      {:ok, rows} ->
        summary =
          Enum.reduce(rows, %{errored: nil, timed_out: false}, fn {reason, error_id}, acc ->
            case reason_from_int(reason) do
              :errored -> if acc.errored, do: acc, else: %{acc | errored: error_id}
              :timeout -> %{acc | timed_out: true}
              _ -> acc
            end
          end)

        {:ok, summary}
    end
  end

  # --- Run/topic views ---

  # One map per stream belonging to any step of the run, for the run
  # topic's initial state. Closure reasons are returned raw (`:lifecycle`
  # unresolved) — the server resolves them against the closing execution.
  def get_streams_for_run(db, run_id) do
    case query(
           db,
           """
           SELECT st.id, st.step_id, s.number, r.external_id, st.workspace_id, st.`index`,
                  st.position, st.created_at, c.created_at, c.reason, c.error_id, c.execution_id
           FROM streams AS st
           INNER JOIN steps AS s ON s.id = st.step_id
           INNER JOIN runs AS r ON r.id = s.run_id
           LEFT JOIN stream_closures AS c ON c.stream_id = st.id
           WHERE s.run_id = ?1
           ORDER BY s.number, st.`index`
           """,
           {run_id}
         ) do
      {:ok, rows} ->
        streams =
          Enum.map(rows, fn {id, step_id, step_number, run_external_id, workspace_id, index,
                             position, created_at, closed_at, reason_int, error_id, closed_by} ->
            error = if error_id, do: get_error(db, error_id)

            %{
              id: id,
              step_id: step_id,
              step_number: step_number,
              run_external_id: run_external_id,
              workspace_id: workspace_id,
              index: index,
              position: position,
              created_at: created_at,
              closed_at: closed_at,
              reason: if(reason_int, do: reason_from_int(reason_int)),
              error: error,
              closed_by: closed_by
            }
          end)

        {:ok, streams}
    end
  end

  defp get_error(db, error_id) do
    {:ok, error} = Errors.get_by_id(db, error_id)
    error
  end

  # --- Refs and lineage ---

  def get_or_create_stream_ref(db, run_external_id, step_number, index, module, target) do
    {:ok, _} =
      insert_one(
        db,
        :stream_refs,
        %{
          run_external_id: run_external_id,
          step_number: step_number,
          index: index,
          module: module,
          target: target
        },
        on_conflict: "DO NOTHING"
      )

    case query_one(
           db,
           """
           SELECT id
           FROM stream_refs
           WHERE run_external_id = ?1 AND step_number = ?2 AND `index` = ?3
           """,
           {run_external_id, step_number, index}
         ) do
      {:ok, {id}} -> {:ok, id}
    end
  end

  def get_stream_ref(db, ref_id) do
    case query_one(
           db,
           "SELECT run_external_id, step_number, `index`, module, target FROM stream_refs WHERE id = ?1",
           {ref_id}
         ) do
      {:ok, {run_external_id, step_number, index, module, target}} ->
        {:ok, {run_external_id, step_number, index, module, target}}

      {:ok, nil} ->
        {:error, :not_found}
    end
  end

  def create_stream_ref_for(db, stream_id) do
    case get_stream(db, stream_id) do
      {:ok, stream} ->
        get_or_create_stream_ref(
          db,
          stream.run_external_id,
          stream.step_number,
          stream.index,
          stream.module,
          stream.target
        )

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  # Records that `execution_id` subscribed to the stream. Returns
  # `{:ok, id}` for a new edge, `{:ok, nil}` if it already existed.
  def record_dependency(db, execution_id, stream_ref_id) do
    with_transaction(db, fn ->
      insert_one(
        db,
        :stream_dependencies,
        %{
          execution_id: execution_id,
          stream_ref_id: stream_ref_id,
          created_at: current_timestamp()
        },
        on_conflict: "DO NOTHING"
      )
    end)
  end

  # `%{execution_id => [stream_ref_id, ...]}` for every consumer execution
  # in the run.
  def get_run_dependencies(db, run_id) do
    case query(
           db,
           """
           SELECT d.execution_id, d.stream_ref_id
           FROM stream_dependencies AS d
           INNER JOIN executions AS e ON e.id = d.execution_id
           INNER JOIN steps AS s ON s.id = e.step_id
           WHERE s.run_id = ?1
           """,
           {run_id}
         ) do
      {:ok, rows} ->
        {:ok, Enum.group_by(rows, &elem(&1, 0), &elem(&1, 1))}
    end
  end

  defp current_timestamp() do
    System.os_time(:millisecond)
  end
end
