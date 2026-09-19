defmodule Coflux.Orchestration.Server.State do
  @moduledoc """
  Everything one project's orchestration server holds in memory, and the
  lookups that are pure projections of it.

  The fields fall into three kinds, and the difference matters:

    * **Handles** - `db`, `epochs`, `epoch_index`: the store this server
      reads and writes.

    * **Accelerations** - indexes over what the store already knows
      (`workspaces`, `execution_ids`, `pending_dependencies`,
      `concurrency_permits`, ...). Each is rebuilt at boot and after an
      epoch rotation, so none is authoritative: if one disagrees with the
      database, the database is right. Anything derived from another
      index says so where it is declared.

    * **Genuinely in-memory** - `connections`, `waiting`, `topics`,
      `stream_subscriptions`, `launcher_tasks` and the timers: state with
      no row behind it, correctly lost when the server stops.

  Only the last kind is irrecoverable, which is why rotation captures and
  restores it rather than rebuilding it.
  """

  alias Coflux.Orchestration.{Ids, Runs}

  alias Coflux.Orchestration.Server.{Lifecycle}

  # The project this server orchestrates. One server per project,
  # registered under this id, and it names the data directory everything
  # below is read from and written to.
  defstruct project_id: nil,

            # The active epoch's database handle - where every write goes
            # and where most reads are answered. Swapped for a fresh one
            # by `Rotation`, so nothing may hold onto it across a call.
            db: nil,

            # The admin store: tokens, and whatever else outlives an
            # epoch. Not rotated, so unlike `db` it never changes hands.
            admin_db: nil,

            # `Coflux.Store.Epochs`: the active database plus the ids of
            # the archived epochs behind it. `Archives` searches backwards
            # through those for rows the active epoch no longer holds.
            epochs: nil,

            # Pending `:tick`, or nil when none is scheduled. Set for
            # whichever comes sooner: the next queued execution's
            # `execute_after`, or the sweep interval while any worker
            # exists. Owned by `Scheduler`.
            tick_timer: nil,

            # Pending `:expire_waiters`, set for the earliest deadline
            # among parked calls, or nil if none has one. Owned by
            # `Waiters`.
            expire_waiters_timer: nil,

            # Pending `:expire_sessions`, set for the earliest session
            # expiry. Owned by `Fleet`.
            expire_sessions_timer: nil,

            # `{timer, ref}` while the server is counting down to stopping
            # itself, else nil. The `ref` identifies the current countdown,
            # so a message from a cancelled one is recognised as stale.
            # Owned by `Listeners`.
            idle_timer: nil,

            # id -> %{name, base_id, state}
            #
            # Every workspace in the project. `base_id` is what it
            # inherits from, and following it gives the chain a lookup
            # walks (`workspace_chain/2`). Read at boot and kept current
            # by the workspace operations.
            workspaces: %{},

            # workspace_id -> %{pool_name -> pool}
            #
            # The pools declared for each workspace: what shape of worker
            # should exist, and how many. `Scheduler` launches against the
            # shortfall.
            pools: %{},

            # name -> id
            #
            # Reverse index of `workspaces`, for resolving a workspace
            # named on the wire.
            workspace_names: %{},

            # worker_id -> %{created_at, pool_id, pool_name, workspace_id, state, data, session_id, stop_id, stop_retry_at, last_poll_at, polling, poll_failures, first_poll_failure_at}
            #
            # Workers this server launched and hasn't deactivated.
            # `session_id` links to the session that connected for it, if
            # one has. Rebuilt at boot by `Fleet.load/1`, which deactivates
            # any that were launched but never got a session - nothing is
            # going to connect to those.
            #
            # `polling` is set while a poll is in flight, so a slow
            # launcher can't accumulate overlapping polls. The poll failure
            # counters track *transient* launcher errors only: a launcher
            # that cannot answer is not evidence the worker is gone, so a
            # worker is only given up on once both counters pass their
            # thresholds (see `Scheduler`).
            workers: %{},

            # ref -> {pid, session_id}
            #
            # The monitor reference for each live worker connection. Purely
            # in-memory: a connection cannot survive a restart, which is
            # why a session is separate from it and outlives it.
            connections: %{},

            # session_id -> %{external_id, connection, targets, queue, starting, executing, concurrency, workspace_id, provides, accepts, worker_id, last_idle_at, activated_at, declared_at, activation_timeout, reconnection_timeout}
            #
            # One entry per worker session. A session outlives its
            # connection - a worker that drops reconnects into the same one
            # and picks up its executions - so `connection` is nil while it
            # is away and `queue` collects the commands recorded for it
            # meanwhile. Restored at boot by `Fleet.load/1` (with an empty
            # queue: commands buffered for a session do not survive a
            # restart); owned by `Fleet` thereafter.
            #
            # `activated_at` is when the worker first connected,
            # `declared_at` when it first said what it can run. Both are
            # needed before the session counts as ready: until then it has
            # never been able to take work, so its idleness means nothing
            # (see `Fleet.session_ready?/1`).
            sessions: %{},

            # external_id -> session_id
            #
            # Reverse index of `sessions`, for resolving the session a
            # worker names when it reconnects.
            session_ids: %{},

            # session_id -> expiry_timestamp_ms
            #
            # When each disconnected session gives up. A session that never
            # connected expires on its activation timeout, one that
            # connected and went away on its reconnection timeout.
            session_expiries: %{},

            # {module, target} -> %{type, session_ids}
            #
            # Which sessions declare which targets - the first thing
            # `Scheduler` consults when matching an execution to a worker.
            # Declared on connect, so it is correctly empty at boot and
            # refilled as workers arrive.
            targets: %{},

            # external_id -> workspace_id
            #
            # Reverse index of `workspaces`, for resolving the workspace an
            # API or worker call names.
            workspace_external_ids: %{},

            # external_id -> worker_id
            #
            # Reverse index of `workers`.
            worker_external_ids: %{},

            # execution_external_id -> execution_id (internal)
            #
            # A cache, and the one index here that is expected to miss. It
            # holds what this server lifetime scheduled or assigned, plus
            # the pending assignments recovered at boot - so an execution
            # that was merely queued when the server last stopped is absent
            # until something touches it. Anything that must be right for
            # every execution resolves through the database instead
            # (`Runs.get_execution_id/4`); this is for the hot worker calls,
            # which only ever name executions they are running.
            execution_ids: %{},

            # ref -> topic
            #
            # What each subscribed process is watching, so one that dies
            # can be unsubscribed from everything at once.
            listeners: %{},

            # topic -> %{ref -> pid}
            #
            # The other direction: who to notify for a topic key. `Effects`
            # only buffers an event if the key appears here, so a key
            # nobody watches costs nothing.
            topics: %{},

            # topic key -> [event], newest first, buffered until the
            # operation flushes
            notifications: %{},

            # [{session_id, command}], newest first, buffered until the
            # operation flushes. An operation records what it wants a
            # worker told; nothing leaves the server until it finishes,
            # so a command for a session that operation goes on to drop
            # is simply never delivered.
            commands: [],

            # Pending RPCs blocked on a dependency, keyed by what they wait for.
            # Key is a tagged tuple:
            #   {:execution, execution_external_id} — woken by notify_waiting
            #   {:input, input_external_id}         — woken by respond_input / dismiss_input
            #   {:catalog, workspace_external_id, path, number}
            #                                       — woken by wake_catalog_waiters
            # Keys name things by external id only: this map is carried across
            # an epoch rotation as it is, and rotation reassigns internal ids.
            # A new kind MUST be handled in: notify_waiting, respond_input /
            # dismiss_input, expire_waiters, cleanup_execution, and
            # cancel_other_execution_keys.
            # Value is a list of {from_execution_external_id, request_id, expire_at, suspend?}.
            waiting: %{},

            # task_ref -> callback
            #
            # Launches in flight. Launching a worker is asynchronous, so
            # the continuation is parked here and applied when the task
            # reports back. In-memory only: a launch that was running when
            # the server stopped is lost, and the worker it started (if
            # any) is deactivated at boot for having no session.
            launcher_tasks: %{},

            # pool_id -> %{failures, last_attempt_at}
            #
            # Consecutive failed launches per pool, and when the last one
            # was attempted, so a pool whose launches keep failing backs
            # off instead of retrying every pass. Keyed by pool id rather
            # than name: pool rows are immutable, so editing a pool mints a
            # new id and the backoff clears itself. Cleared when a worker
            # from the pool becomes ready. In-memory only - a restart is
            # worth one free attempt.
            pool_failures: %{},

            # `Coflux.Store.Index`: the Bloom filter per archived epoch,
            # so a lookup for a row that cannot be in an epoch never opens
            # it. Persisted, unlike everything else here.
            epoch_index: nil,

            # The reference of the background task building a Bloom filter,
            # or nil when none is running. Only one runs at a time.
            index_task: nil,

            # [epoch_id] awaiting Bloom filter build (FIFO)
            #
            # Epochs archived but not yet indexed. Until an epoch is
            # indexed it has no filter, so `Archives` must open it to find
            # out whether it holds a row.
            index_queue: [],

            # run_external_id -> {root_module, root_target, MapSet of execution_ids}
            #
            # The runs with something still in flight. A completion finds
            # the workflow to report against here, and a run whose set
            # empties is how the server knows it has nothing left running
            # and can settle its outcome.
            run_workflows: %{},

            # execution_id -> MapSet of execution_ids that this execution is waiting on
            #
            # The assignment gate: an execution with a non-empty set is not
            # eligible to run. Derivable from the database and rebuilt by
            # `Dependencies.rebuild/1`. The three indexes below are derived
            # from this one and rebuilt with it.
            pending_dependencies: %{},
            # Stream waits indexed by stream, so an append can check for
            # waiters with one lookup instead of scanning every pending
            # dependency. Derived from pending_dependencies; rebuilt with it.
            stream_dependency_keys: %{},

            # execution_id -> MapSet of execution_ids that are waiting on this execution
            #
            # The reverse of `pending_dependencies`, so a result that lands
            # can find who was waiting on it without scanning. Derived from
            # pending_dependencies; rebuilt with it.
            dependency_waiters: %{},

            # execution_id -> MapSet of the dependency keys that were
            # recorded on it by a suspended select (a result, input,
            # stream or catalog wait). Select is first-wins, so these
            # form an any-of group: when one clears, the execution is
            # done waiting on all of them. Argument dependencies
            # (`wait_for`) are never in a group — they must all resolve —
            # but they are also always resolved by the time a step has
            # run once, so the two never coexist in practice. Derived
            # from pending_dependencies; rebuilt with it.
            dependency_groups: %{},

            # Concurrency permits currently held: execution_id ->
            # %{workspace_id, key}. An execution holds a permit from the
            # moment its assignment is written until its completion is. The
            # same set is derivable from the database (assignment without
            # completion), so this is rebuilt at boot and after an epoch
            # rotation rather than being authoritative — a permit can't be
            # leaked by a restart.
            concurrency_permits: %{},

            # Executions the last tick left gated on a permit:
            # execution_id -> the queue-topic dependency map that was
            # emitted for it. Keeping the map (rather than just the id)
            # means a queue subscriber joining mid-gate is shown the same
            # entry existing subscribers already have, without recomputing
            # holders that may since have changed.
            #
            # Unlike the permits above this is *not* derivable: it is the
            # last scheduler pass's decision, not a fact about the
            # database, so a rebuild clears it and the next pass fills it.
            concurrency_gated: %{},

            # Active stream subscriptions — in-memory, session-scoped.
            # A consumer adapter opens a subscription by sending stream_subscribe
            # with a subscription_id unique within that consumer's adapter
            # process; we push items (stream_items command) as they arrive on
            # the producer side, and a terminal stream_closed command when the
            # stream ends. Dropped when the session disconnects, when the
            # consumer unsubscribes, when the consumer execution terminates,
            # or when the stream closes.
            #
            # The key includes consumer_execution_id so concurrent consumer
            # adapters (each starting subscription counters from 0) can't
            # collide.
            #
            # Delivery is credit-gated: the consumer declares a `prefetch`
            # window on subscribe and reports progress with `stream_ack`.
            # We only push while `delivered - acked_count < prefetch`, which
            # is what bounds the consumer's in-memory queue. Undelivered
            # items aren't lost — they're durable, and the next ack pumps
            # them from the DB.
            #
            # Nothing can rebuild these from the database, which is why a
            # rotation captures and restores them rather than re-deriving
            # them (`StreamDelivery.capture/1`, `restore/2`).
            #
            # stream_subscriptions: {consumer_execution_id, subscription_id} ->
            #     %{consumer_execution_external_id, stream_id, cursor,
            #       stride, prefetch, delivered, acked_count, acked_seq,
            #       pending_close}
            # stream_subscribers: stream_id -> MapSet of
            #     {consumer_execution_id, subscription_id}
            stream_subscriptions: %{},
            stream_subscribers: %{},

            # Per-stream producer state for backpressure. Only present
            # when the producer opted in by registering with a non-nil
            # buffer. Keyed by stream_id (the `streams` row).
            #
            #   %{buffer, demand_granted, session_id, execution_external_id,
            #     index}
            #
            # * buffer                — configured backpressure budget
            # * demand_granted        — cumulative credits sent so far, in
            #                           sequence space (so a producer
            #                           resuming a paused stream starts at
            #                           head + 1)
            # * session_id            — where to route stream_demand
            # * execution_external_id — the current producer, for the wire
            # * index                 — the stream's step index, for the wire
            #
            # A stream that spans a suspension changes producer: the
            # resuming execution's registration replaces this entry.
            #
            # The watermark the budget is measured against is the
            # *slowest* subscriber's acknowledged position, recomputed
            # from stream_subscribers on demand rather than cached here,
            # since it changes often and is cheap to derive.
            stream_producers: %{}

  @doc "A workspace's external id."
  def workspace_external_id(state, workspace_id) do
    state.workspaces[workspace_id].external_id
  end

  @doc "A workspace and its bases, furthest ancestor first."
  def workspace_chain(state, workspace_id, ids \\ []) do
    workspace = Map.fetch!(state.workspaces, workspace_id)
    ids = [workspace_id | ids]

    if workspace.base_id do
      workspace_chain(state, workspace.base_id, ids)
    else
      Enum.reverse(ids)
    end
  end

  @doc "The session an execution is starting or running on, if it is on one."
  def session_for_execution(state, execution_external_id) do
    state.sessions
    |> Map.keys()
    |> Enum.find(fn session_id ->
      session = Map.fetch!(state.sessions, session_id)

      MapSet.member?(session.starting, execution_external_id) or
        MapSet.member?(session.executing, execution_external_id)
    end)
    |> case do
      nil -> :error
      session_id -> {:ok, session_id}
    end
  end

  @doc """
  Records that `execution_id` is in flight for a run, along with the run's
  root target. `run_workflows` is how a completion finds the workflow to
  report against, and how the server knows a run has nothing left running.
  """
  def track_run_execution(state, run_ext_id, execution_id, root_module, root_target) do
    Map.update!(state, :run_workflows, fn rw ->
      Map.update(rw, run_ext_id, {root_module, root_target, MapSet.new([execution_id])}, fn {m, t,
                                                                                             ids} ->
        {m, t, MapSet.put(ids, execution_id)}
      end)
    end)
  end

  def untrack_run_execution(state, run_ext_id, execution_id) do
    case Map.fetch(state.run_workflows, run_ext_id) do
      {:ok, {m, t, ids}} ->
        remaining = MapSet.delete(ids, execution_id)

        state =
          if MapSet.size(remaining) == 0 do
            Map.update!(state, :run_workflows, &Map.delete(&1, run_ext_id))
          else
            put_in(state, [Access.key(:run_workflows), run_ext_id], {m, t, remaining})
          end

        {{m, t}, state}

      :error ->
        {nil, state}
    end
  end

  def get_run_workflow(state, run_ext_id) do
    case Map.fetch(state.run_workflows, run_ext_id) do
      {:ok, {m, t, _}} -> {m, t}
      :error -> nil
    end
  end

  @doc """
  Rebuilds the two id indexes at boot.

  Executions that were assigned when the server stopped are put back into
  their session's `executing` set rather than abandoned: the worker will
  reconnect and say which it is still running, and the heartbeat handler
  abandons the rest. An execution whose session is gone has nobody to
  report it, so it is abandoned here.
  """
  def load_indexes(state) do
    # Restore pending assignments into session state instead of abandoning them.
    # Workers will reconnect and report via heartbeats which executions they're
    # still running. Any that aren't reported will be abandoned by the heartbeat
    # handler. If the worker doesn't reconnect at all, session expiry handles it.
    {:ok, pending} = Runs.get_pending_assignments(state.db)

    # Group by session and collect all execution IDs
    {by_session, all_execution_ids} =
      Enum.reduce(pending, {%{}, []}, fn {session_id, execution_id}, {by_session, all} ->
        by_session = Map.update(by_session, session_id, [execution_id], &[execution_id | &1])
        {by_session, [execution_id | all]}
      end)

    # Populate execution_ids cache (external -> internal) so heartbeats can resolve them
    {:ok, key_map} = Runs.get_execution_keys(state.db, all_execution_ids)

    # Build internal->external mapping for this batch
    internal_to_external =
      Map.new(key_map, fn {execution_id, {run_ext_id, step_num, attempt}} ->
        {execution_id, Ids.execution(run_ext_id, step_num, attempt)}
      end)

    state =
      Enum.reduce(internal_to_external, state, fn {execution_id, ext_id}, state ->
        put_in(state, [Access.key(:execution_ids), ext_id], execution_id)
      end)

    # Add pending executions to each session's executing set (using external IDs)
    state =
      Enum.reduce(by_session, state, fn {session_id, execution_ids}, state ->
        if Map.has_key?(state.sessions, session_id) do
          update_in(
            state.sessions[session_id].executing,
            &Enum.reduce(execution_ids, &1, fn id, set ->
              case Map.fetch(internal_to_external, id) do
                {:ok, ext_id} -> MapSet.put(set, ext_id)
                :error -> set
              end
            end)
          )
        else
          # Session no longer active - abandon these executions. Server-initiated
          # so we write both the results row (via process_result) and the
          # completion row (via complete_execution) here — no worker is
          # going to send notify_terminated for this execution.
          Enum.reduce(execution_ids, state, fn execution_id, state ->
            {:ok, state} = Lifecycle.process_result(state, execution_id, :abandoned)
            Lifecycle.complete_execution(state, execution_id)
          end)
        end
      end)

    # Populate run_workflows lookup for all active runs
    {:ok, active_run_workflows} = Runs.get_active_run_workflows(state.db)

    state =
      Enum.reduce(active_run_workflows, state, fn {run_ext_id, module, target, _step_number,
                                                   _attempt, execution_id, _assigned},
                                                  state ->
        track_run_execution(state, run_ext_id, execution_id, module, target)
      end)

    state
  end
end
