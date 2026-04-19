defmodule Coflux.Orchestration.Streams do
  @moduledoc """
  Storage for execution-produced streams.

  A stream is an ordered, append-only sequence of values produced by an
  execution. Each stream is identified by `(execution_id, index)` where
  `index` is assigned monotonically by the worker during return-value
  serialisation — the worker mints ids locally, no server round-trip.

  Items within a stream are identified by `sequence` — a 0-based,
  monotonically increasing per-item counter.

  The SQL column is quoted with backticks (``` `index` ```) throughout
  queries because `INDEX` is a SQLite keyword; at the Elixir level we
  just pass `:index` as a map key — the Store helper handles quoting
  for inserts.

  Invariants enforced here (and by schema FKs):

    * A stream is owned by exactly one execution (its producer).
    * Items are append-only with monotonic `sequence` starting at 0.
    * A closure is terminal — no items may be appended after one is recorded.
    * On execution completion / cancel / crash, every owned stream that lacks
      a closure receives one (clean, cancelled, or crashed). Enforced by the
      lifecycle code in `Server`, not by this module.
    * Re-running a producer execution creates fresh streams (new attempt ⇒
      new execution_id ⇒ new rows). Consumer refs pin to the original streams.
    * Consumer cursors are kept in-memory only; re-run consumers subscribe
      fresh from sequence 0.
  """

  import Coflux.Store

  alias Coflux.Orchestration.{Errors, Values}

  # Registers a new stream owned by `execution_id` at `index` (monotonic
  # per-execution, worker-assigned). Returns `{:error, :already_registered}`
  # if the index was already used.
  def register_stream(db, execution_id, index) do
    now = current_timestamp()

    case insert_one(db, :streams, %{
           execution_id: execution_id,
           index: index,
           created_at: now
         }) do
      {:ok, _} -> {:ok, now}
      {:error, "UNIQUE constraint failed: " <> _} -> {:error, :already_registered}
    end
  end

  # Appends an item at `sequence` to the stream. Caller supplies the sequence
  # (worker-assigned, monotonic). Returns:
  #   * `{:error, :not_registered}` if the stream doesn't exist
  #   * `{:error, :closed}` if the stream has a closure row
  #   * `{:error, :already_appended}` if sequence collides with an existing item
  def append_item(db, execution_id, index, sequence, value) do
    with_transaction(db, fn ->
      case has_closure?(db, execution_id, index) do
        {:ok, true} ->
          {:error, :closed}

        {:ok, false} ->
          case exists?(db, execution_id, index) do
            {:ok, false} ->
              {:error, :not_registered}

            {:ok, true} ->
              {:ok, value_id} = Values.get_or_create_value(db, value)
              now = current_timestamp()

              case insert_one(db, :stream_items, %{
                     execution_id: execution_id,
                     index: index,
                     sequence: sequence,
                     value_id: value_id,
                     created_at: now
                   }) do
                {:ok, _} -> {:ok, now}
                {:error, "UNIQUE constraint failed: " <> _} -> {:error, :already_appended}
              end
          end
      end
    end)
  end

  # Closes the stream. `spec` describes *why* it closed:
  #
  #   * `:complete` — producer finished normally
  #   * `{:errored, type, message, frames}` — producer raised an error; the
  #     error is stored via the errors table, same as Results
  #   * `:lifecycle` — closed implicitly because the producer execution
  #     ended (cancel/crash/abandon/error). No error is recorded here —
  #     callers that need to surface an error derive it from the
  #     execution's recorded result at read time.
  def close_stream(db, execution_id, index, spec \\ :complete) do
    with_transaction(db, fn ->
      case exists?(db, execution_id, index) do
        {:ok, false} ->
          {:error, :not_registered}

        {:ok, true} ->
          now = current_timestamp()
          {reason, error_id} = resolve_close_spec(db, spec)

          case insert_one(db, :stream_closures, %{
                 execution_id: execution_id,
                 index: index,
                 reason: reason,
                 error_id: error_id,
                 created_at: now
               }) do
            {:ok, _} -> {:ok, now}
            {:error, "UNIQUE constraint failed: " <> _} -> {:error, :already_closed}
          end
      end
    end)
  end

  # Closure reason codes — kept in sync with the CHECK constraint in 4.sql.
  @reason_complete 0
  @reason_errored 1
  @reason_lifecycle 2

  defp resolve_close_spec(_db, :complete), do: {@reason_complete, nil}
  defp resolve_close_spec(_db, :lifecycle), do: {@reason_lifecycle, nil}

  defp resolve_close_spec(db, {:errored, type, message, frames}) do
    error_id = Errors.get_or_create(db, type, message, frames)
    {@reason_errored, error_id}
  end

  # Atom form of the reason integer — used by callers that want to decide
  # whether to derive an error from the execution's result (:lifecycle)
  # or use the stored one (:errored / :complete).
  def reason_from_int(@reason_complete), do: :complete
  def reason_from_int(@reason_errored), do: :errored
  def reason_from_int(@reason_lifecycle), do: :lifecycle

  def exists?(db, execution_id, index) do
    case query_one(
           db,
           "SELECT 1 FROM streams WHERE execution_id = ?1 AND `index` = ?2",
           {execution_id, index}
         ) do
      {:ok, nil} -> {:ok, false}
      {:ok, {1}} -> {:ok, true}
    end
  end

  # Returns the stream's registration timestamp, or `{:error, :not_found}`.
  def get_opened_at(db, execution_id, index) do
    case query_one(
           db,
           "SELECT created_at FROM streams WHERE execution_id = ?1 AND `index` = ?2",
           {execution_id, index}
         ) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, {created_at}} -> {:ok, created_at}
    end
  end

  def has_closure?(db, execution_id, index) do
    case query_one(
           db,
           "SELECT 1 FROM stream_closures WHERE execution_id = ?1 AND `index` = ?2",
           {execution_id, index}
         ) do
      {:ok, nil} -> {:ok, false}
      {:ok, {1}} -> {:ok, true}
    end
  end

  # Returns `{:ok, [index, ...]}` for every stream owned by `execution_id`,
  # in index order.
  def get_streams_for_execution(db, execution_id) do
    case query(
           db,
           "SELECT `index` FROM streams WHERE execution_id = ?1 ORDER BY `index`",
           {execution_id}
         ) do
      {:ok, rows} ->
        {:ok, Enum.map(rows, fn {index} -> index end)}
    end
  end

  # Returns indexes of streams owned by `execution_id` that don't yet have
  # a closure row. Used by the lifecycle code to discover which streams to
  # close on completion / cancel / crash.
  def get_open_streams_for_execution(db, execution_id) do
    case query(
           db,
           """
           SELECT s.`index`
           FROM streams AS s
           LEFT JOIN stream_closures AS c
             ON c.execution_id = s.execution_id AND c.`index` = s.`index`
           WHERE s.execution_id = ?1 AND c.execution_id IS NULL
           ORDER BY s.`index`
           """,
           {execution_id}
         ) do
      {:ok, rows} ->
        {:ok, Enum.map(rows, fn {index} -> index end)}
    end
  end

  # Returns closure info or `{:ok, nil}` if the stream is still open.
  # Closure info: `{reason, error | nil, created_at}` where
  #   * reason is :complete | :errored | :lifecycle
  #   * error is the `{type, message, frames}` triple for :errored, nil
  #     otherwise (callers derive it from the execution's result on
  #     :lifecycle)
  def get_stream_closure(db, execution_id, index) do
    case query_one(
           db,
           "SELECT reason, error_id, created_at FROM stream_closures WHERE execution_id = ?1 AND `index` = ?2",
           {execution_id, index}
         ) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, {reason_int, nil, created_at}} ->
        {:ok, {reason_from_int(reason_int), nil, created_at}}

      {:ok, {reason_int, error_id, created_at}} ->
        {:ok, error} = Errors.get_by_id(db, error_id)
        {:ok, {reason_from_int(reason_int), error, created_at}}
    end
  end

  # Fetches up to `max_items` items from the stream starting at `from_sequence`.
  # Returns `{:ok, [{sequence, value, created_at}, ...]}` in sequence order.
  # The caller (Server) layers filter logic (slice / partition) on top of this.
  def get_stream_items(db, execution_id, index, from_sequence, max_items) do
    case query(
           db,
           """
           SELECT sequence, value_id, created_at
           FROM stream_items
           WHERE execution_id = ?1 AND `index` = ?2 AND sequence >= ?3
           ORDER BY sequence
           LIMIT ?4
           """,
           {execution_id, index, from_sequence, max_items}
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

  # Returns one row per stream owned by `execution_id`:
  # `{index, created_at, closed_at | nil, reason | nil, error | nil}`.
  #   * reason is :complete | :errored | :lifecycle when closed, nil when open
  #   * error is the stored `{type, message, frames}` triple for :errored
  #     closures only — callers that need to surface an error for a
  #     :lifecycle closure derive it from the execution's result.
  # Used when populating the topic state for a run.
  def get_streams_with_closures_for_execution(db, execution_id) do
    case query(
           db,
           """
           SELECT s.`index`, s.created_at, c.created_at, c.reason, c.error_id
           FROM streams AS s
           LEFT JOIN stream_closures AS c
             ON c.execution_id = s.execution_id AND c.`index` = s.`index`
           WHERE s.execution_id = ?1
           ORDER BY s.`index`
           """,
           {execution_id}
         ) do
      {:ok, rows} ->
        streams =
          Enum.map(rows, fn
            {index, created_at, nil, nil, nil} ->
              {index, created_at, nil, nil, nil}

            {index, created_at, closed_at, reason_int, nil} ->
              {index, created_at, closed_at, reason_from_int(reason_int), nil}

            {index, created_at, closed_at, reason_int, error_id} ->
              {:ok, error} = Errors.get_by_id(db, error_id)
              {index, created_at, closed_at, reason_from_int(reason_int), error}
          end)

        {:ok, streams}
    end
  end

  # Returns the highest sequence recorded for the stream, or `-1` if empty.
  # Used by the worker protocol to report "head" for flow control without
  # requiring the caller to scan all items.
  def get_stream_head(db, execution_id, index) do
    case query_one(
           db,
           "SELECT MAX(sequence) FROM stream_items WHERE execution_id = ?1 AND `index` = ?2",
           {execution_id, index}
         ) do
      {:ok, {nil}} -> {:ok, -1}
      {:ok, {sequence}} -> {:ok, sequence}
    end
  end

  # Returns the last `max_items` items of the stream, in sequence order,
  # alongside the total item count. Used by the inspection topic to
  # bootstrap its bounded tail buffer without materialising the full log.
  def get_stream_tail(db, execution_id, index, max_items) do
    {:ok, {total_count}} =
      query_one(
        db,
        "SELECT COUNT(*) FROM stream_items WHERE execution_id = ?1 AND `index` = ?2",
        {execution_id, index}
      )

    case query(
           db,
           """
           SELECT sequence, value_id, created_at
           FROM stream_items
           WHERE execution_id = ?1 AND `index` = ?2
           ORDER BY sequence DESC
           LIMIT ?3
           """,
           {execution_id, index, max_items}
         ) do
      {:ok, rows} ->
        items =
          rows
          |> Enum.reverse()
          |> Enum.map(fn {sequence, value_id, created_at} ->
            {:ok, value} = Values.get_value_by_id(db, value_id)
            {sequence, value, created_at}
          end)

        {:ok, {items, total_count}}
    end
  end

  defp current_timestamp() do
    System.os_time(:millisecond)
  end
end
