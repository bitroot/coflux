defmodule Coflux.Orchestration.Server.Scheduler do
  @moduledoc """
  One pass over everything queued, deciding what runs next.

  A pass is a pull, not a push: it reads every unassigned execution and
  works out, for each, whether it can start now. That makes it idempotent
  - running it twice changes nothing running it once did not - which is
  why anything that might unblock work simply asks for another pass
  rather than reasoning about what it unblocked.

  Each execution falls into one of four outcomes:

    * **Deferred** onto an identical execution already queued, and
      completed as such rather than run twice.
    * **Future**, if its `execute_after` has not arrived. The next pass is
      scheduled for the earliest of those, so a delayed execution costs a
      single timer rather than repeated polling.
    * **Blocked**, on a dependency or a concurrency permit. The queue is
      told what it is waiting on, so "why isn't this running?" has an
      answer without asking again.
    * **Assigned** to a session that declares the target, has a free slot,
      and whose tags match what the execution requires.

  The pass also sweeps the fleet: sessions that have gone quiet are
  polled, workers past their idle timeout are stopped, and pools short of
  their declared size get another worker launched.
  """

  require Logger

  alias Coflux.Events.{
    ExecutionAssigned,
    SessionExecuting,
    SessionExecutions,
    WorkerCreated,
    WorkerLaunchResult,
    WorkerStopResult,
    WorkerStopping
  }

  alias Coflux.Orchestration.{CacheConfigs, Checkpoints, Ids, Runs, Sessions, Workers, Workspaces}

  alias Coflux.Orchestration.Server.{
    Archives,
    Commands,
    Dependencies,
    Effects,
    Fleet,
    Lifecycle,
    Listeners,
    Permissions,
    Resolve,
    Scheduling,
    State
  }

  # How long a worker may go unheard from before it is polled.
  @connected_worker_poll_interval_ms 30_000
  @disconnected_worker_poll_interval_ms 5_000

  # How long a *ready* worker sits idle before it is stopped, unless its
  # pool says otherwise (see `worker_idle_timeout_ms/2`). A worker that
  # has never declared targets is not idle, it is still starting - see
  # `Fleet.session_ready?/1`.
  @default_worker_idle_timeout_ms 5_000

  # How often to sweep while any worker exists, for the deadlines above.
  @sweep_interval_ms 5_000

  # The shortest gap between launches for one pool.
  @pool_launch_interval_ms 10_000

  # A pool whose launches keep failing backs off, so a bad image costs one
  # attempt every few minutes rather than one every pass. Cleared when a
  # worker from the pool becomes ready.
  @pool_backoff_base_ms 5_000
  @pool_backoff_max_ms 300_000

  # A launcher that cannot answer a poll is not evidence that the worker
  # is gone, and giving up on a worker abandons whatever it is running -
  # so a transient failure has to persist for this many consecutive polls
  # *and* this long before the worker is deactivated. Erring late is
  # cheap: the stale worker is polled again every pass and recovers as
  # soon as the launcher does.
  @poll_failure_threshold 3
  @poll_failure_grace_ms 300_000

  # How long before asking the launcher again after a stop fails.
  @stop_retry_interval_ms 30_000

  @default_activation_timeout_ms 600_000
  @default_reconnection_timeout_ms 30_000

  @doc """
  Runs one pass, returning the state it leaves behind with the next pass
  scheduled if anything is waiting on a clock.
  """
  def tick(state) do
    state =
      if state.tick_timer do
        Process.cancel_timer(state.tick_timer)
        Map.put(state, :tick_timer, nil)
      else
        state
      end

    {:ok, executions} = Runs.get_unassigned_executions(state.db)

    executions =
      Enum.filter(executions, fn execution ->
        state.workspaces[execution.workspace_id].state == :active
      end)

    now = System.os_time(:millisecond)

    {executions_due, executions_future, executions_defer} =
      Scheduling.split_executions(executions, now)

    state =
      executions_defer
      |> Enum.reverse()
      |> Enum.reduce(state, fn {execution_id, defer_id, _run_id, module}, state ->
        case Lifecycle.record_and_notify_result(
               state,
               execution_id,
               {:deferred, defer_id},
               module
             ) do
          {:ok, state} -> state
          {:error, :already_recorded} -> state
        end
      end)

    tag_sets =
      executions_due
      |> Enum.flat_map(&[&1.requires_tag_set_id, &1.run_requires_tag_set_id])
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()
      |> Map.new(&{&1, Resolve.tag_set(state.db, &1)})

    cache_configs =
      executions_due
      |> Enum.map(& &1.cache_config_id)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()
      |> Enum.reduce(%{}, fn cache_config_id, cache_configs ->
        case CacheConfigs.get_cache_config(state.db, cache_config_id) do
          {:ok, cache_config} -> Map.put(cache_configs, cache_config_id, cache_config)
        end
      end)

    {state, assigned, unassigned, _counts, gated_now} =
      Enum.reduce(
        executions_due,
        {state, [], [], Dependencies.concurrency_held_counts(state), %{}},
        fn
          execution, {state, assigned, unassigned, counts, gated} ->
            # TODO: support caching for other attempts?
            cached_result =
              if execution.attempt == 1 && execution.cache_config_id do
                cache_workspace_ids =
                  Permissions.get_cache_workspace_ids(state, execution.workspace_id)

                cache = Map.fetch!(cache_configs, execution.cache_config_id)
                recorded_after = if cache.max_age, do: now - cache.max_age, else: 0

                Archives.find_cached_execution_across_epochs(
                  state,
                  cache_workspace_ids,
                  execution.step_id,
                  execution.cache_key,
                  recorded_after
                )
              end

            if cached_result do
              result =
                case cached_result do
                  {:in_epoch, cached_execution_id} ->
                    {:cached, cached_execution_id}

                  {:resolved, ref_id, value} ->
                    {:cached, ref_id, value}
                end

              # Cache hit during scheduling — server-only, no worker runs this
              # execution. Write results + completion together.
              {:ok, state} = Lifecycle.process_result(state, execution.execution_id, result)
              state = Lifecycle.complete_execution(state, execution.execution_id)

              {state, assigned, unassigned, counts, gated}
            else
              # Skip executions whose dependencies haven't resolved yet
              has_pending = Map.has_key?(state.pending_dependencies, execution.execution_id)

              if has_pending do
                {state, assigned, unassigned, counts, gated}
              else
                requires =
                  Fleet.effective_requires(
                    tag_sets,
                    execution.run_requires_tag_set_id,
                    execution.requires_tag_set_id
                  )

                if execution.type == :task || !execution.parent_id do
                  # Checked before a session is chosen, so a gated execution
                  # never occupies a worker slot — and, by staying out of
                  # `unassigned`, never triggers a pool launch either.
                  case Dependencies.concurrency_gate(state, counts, execution) do
                    gate when gate != [] ->
                      {state, assigned, unassigned, counts,
                       Map.put(gated, execution.execution_id, gate)}

                    [] ->
                      case Fleet.choose_session(state, execution, requires) do
                        nil ->
                          {state, assigned, [execution | unassigned], counts, gated}

                        session_id ->
                          {:ok, assigned_at} =
                            Runs.assign_execution(state.db, execution.execution_id, session_id)

                          {:ok, arguments} = Runs.get_step_arguments(state.db, execution.step_id)

                          # Enrich arguments with resolved references (asset/execution metadata)
                          enriched_arguments = Enum.map(arguments, &Resolve.value(state.db, &1))

                          # Checkpoints travel with the execute message in the same
                          # wire form as arguments, and are handled the same way
                          # end-to-end — including the worker downloading any
                          # blob-backed value before the adapter starts. Bounded by
                          # this execution's own attempt, so it sees what it started
                          # with rather than anything a stale writer lands later.
                          {:ok, checkpoints} =
                            Checkpoints.get_effective(
                              state.db,
                              execution.step_id,
                              State.workspace_chain(state, execution.workspace_id),
                              execution.attempt
                            )

                          enriched_checkpoints = Resolve.checkpoints(state.db, checkpoints)

                          workspace_external_id =
                            state.workspaces[execution.workspace_id].external_id

                          execution_external_id =
                            Ids.execution(
                              execution.run_external_id,
                              execution.step_number,
                              execution.attempt
                            )

                          state =
                            state
                            |> put_in(
                              [Access.key(:execution_ids), execution_external_id],
                              execution.execution_id
                            )
                            |> update_in(
                              [Access.key(:sessions), session_id, :starting],
                              &MapSet.put(&1, execution_external_id)
                            )
                            |> update_in(
                              [Access.key(:sessions), session_id, :total_executions],
                              &(&1 + 1)
                            )
                            |> Effects.command(
                              session_id,
                              Commands.execute(
                                execution_external_id,
                                execution.module,
                                execution.target,
                                enriched_arguments,
                                execution.run_external_id,
                                workspace_external_id,
                                execution.timeout,
                                Scheduling.build_streams_config(
                                  execution.streams_buffer,
                                  execution.streams_timeout_ms
                                ),
                                enriched_checkpoints
                              )
                            )

                          session = Map.fetch!(state.sessions, session_id)
                          worker = session.worker_id && Map.get(state.workers, session.worker_id)

                          state =
                            state
                            |> Effects.emit(%SessionExecuting{
                              workspace: workspace_external_id,
                              session: session.external_id,
                              executing:
                                session.starting
                                |> MapSet.union(session.executing)
                                |> Enum.count()
                            })
                            |> Effects.emit(%SessionExecutions{
                              workspace: workspace_external_id,
                              session: session.external_id,
                              worker: worker && worker.external_id,
                              pool: worker && worker.pool_name,
                              executions: session.total_executions
                            })

                          state = Dependencies.grant_concurrency_permit(state, execution)
                          counts = Dependencies.increment_concurrency_count(counts, execution)

                          {state, [{execution, assigned_at} | assigned], unassigned, counts,
                           gated}
                      end
                  end
                else
                  {:ok, arguments} = Runs.get_step_arguments(state.db, execution.step_id)

                  state =
                    case Scheduling.schedule_run(
                           state,
                           execution.module,
                           execution.target,
                           execution.type,
                           arguments,
                           execution.workspace_id,
                           parent_id: execution.execution_id,
                           cache:
                             if(execution.cache_config_id,
                               do: Map.fetch!(cache_configs, execution.cache_config_id)
                             ),
                           retries:
                             if(execution.retry_limit == -1 || execution.retry_limit > 0,
                               do: %{
                                 limit:
                                   if(execution.retry_limit == -1,
                                     do: nil,
                                     else: execution.retry_limit
                                   ),
                                 backoff_min: execution.retry_backoff_min,
                                 backoff_max: execution.retry_backoff_max
                               }
                             ),
                           requires: requires,
                           # The spawning execution never takes a permit (no
                           # worker runs it); the gate moves to the spawned
                           # run's own initial step. Passed as the already-
                           # built key because the step stores the key, not
                           # the params it was derived from — the arguments
                           # are the same, so it's the same key either way.
                           concurrency_key: execution.concurrency_key,
                           concurrency_limit: execution.concurrency_limit,
                           group_key: execution.group_key,
                           group_limit: execution.group_limit
                         ) do
                      {:ok, _external_run_id, _external_step_id, spawned_execution_id, state} ->
                        {:ok, state} =
                          Lifecycle.process_result(
                            state,
                            execution.execution_id,
                            {:spawned, spawned_execution_id}
                          )

                        state
                    end

                  {state, assigned, unassigned, counts, gated}
                end
              end
            end
        end
      )

    state = Dependencies.update_concurrency_gates(state, gated_now)

    state =
      Enum.reduce(assigned, state, fn {execution, assigned_at}, state ->
        {root_module, root_target} =
          State.get_run_workflow(state, execution.run_external_id) ||
            raise "run_workflows missing entry for run #{execution.run_external_id}"

        Effects.emit(state, %ExecutionAssigned{
          execution:
            Ids.execution(
              execution.run_external_id,
              execution.step_number,
              execution.attempt
            ),
          run: execution.run_external_id,
          step: execution.step_number,
          attempt: execution.attempt,
          workspace: State.workspace_external_id(state, execution.workspace_id),
          module: execution.module,
          target: execution.target,
          type: execution.type,
          root_module: root_module,
          root_target: root_target,
          assigned_at: assigned_at
        })
      end)

    {state, next_launch_at} =
      if Enum.any?(unassigned) do
        # Track the most recent worker creation per pool, and which pools
        # already have a worker that isn't ready to accept work.  We skip
        # launching for pools that have a worker still pending activation
        # or that activated but hasn't declared what it can run (e.g.
        # due to a misconfigured command or working directory).
        {latest_pool_launch_at, pools_with_pending_worker} =
          state.workers
          |> Map.values()
          |> Enum.reduce({%{}, MapSet.new()}, fn worker, {latest, pending} ->
            latest =
              Map.update(latest, worker.pool_id, worker.created_at, &max(&1, worker.created_at))

            pending =
              with session_id when not is_nil(session_id) <- worker.session_id,
                   {:ok, session} <- Map.fetch(state.sessions, session_id),
                   false <- Fleet.session_ready?(session) do
                MapSet.put(pending, worker.pool_id)
              else
                _ -> pending
              end

            {latest, pending}
          end)

        unassigned
        |> Enum.group_by(& &1.workspace_id)
        |> Enum.reduce({state, nil}, fn {workspace_id, executions}, {state, next_launch_at} ->
          candidates =
            executions
            |> Enum.map(fn execution ->
              requires =
                Fleet.effective_requires(
                  tag_sets,
                  execution.run_requires_tag_set_id,
                  execution.requires_tag_set_id
                )

              Fleet.choose_pool(state, execution, requires)
            end)
            |> Enum.reject(&is_nil/1)
            |> Enum.uniq()
            |> Enum.reject(&MapSet.member?(pools_with_pending_worker, &1))
            |> Map.new(&{&1, pool_launch_due_at(state, &1, latest_pool_launch_at)})

          # A pool that wants a worker but can't have one yet is the only
          # reason an otherwise-quiet project still needs waking: nothing
          # else will happen until its throttle or backoff expires.
          next_launch_at =
            candidates
            |> Map.values()
            |> Enum.filter(&(&1 > now))
            |> Enum.min(fn -> nil end)
            |> earliest(next_launch_at)

          state =
            candidates
            |> Enum.filter(fn {_pool_id, due_at} -> due_at <= now end)
            |> Enum.map(&elem(&1, 0))
            |> Enum.reduce(state, fn pool_id, state ->
              case Workers.create_worker(state.db, pool_id) do
                {:ok, worker_id, worker_external_id, created_at} ->
                  {pool_name, pool} =
                    Enum.find(
                      Map.get(state.pools, workspace_id, %{}),
                      &(elem(&1, 1).id == pool_id)
                    )

                  # Create a session for the pool-launched worker
                  activation_timeout =
                    Map.get(pool, :activation_timeout, @default_activation_timeout_ms)

                  reconnection_timeout =
                    Map.get(pool, :reconnection_timeout, @default_reconnection_timeout_ms)

                  pool_accepts = Map.get(pool, :accepts, %{})

                  session_opts = [
                    provides: pool.provides,
                    accepts: pool_accepts,
                    activation_timeout: activation_timeout,
                    reconnection_timeout: reconnection_timeout
                  ]

                  {:ok, session_id, external_id, token, secret_hash, session_now} =
                    Sessions.create_session(state.db, workspace_id, worker_id, session_opts)

                  session = %{
                    external_id: external_id,
                    secret_hash: secret_hash,
                    connection: nil,
                    targets: %{},
                    queue: [],
                    starting: MapSet.new(),
                    executing: MapSet.new(),
                    concurrency: 0,
                    draining: false,
                    workspace_id: workspace_id,
                    provides: pool.provides,
                    accepts: pool_accepts,
                    worker_id: worker_id,
                    last_idle_at: session_now,
                    activated_at: nil,
                    declared_at: nil,
                    ready_deadline_at: nil,
                    activation_timeout: activation_timeout,
                    reconnection_timeout: reconnection_timeout,
                    total_executions: 0
                  }

                  state
                  |> put_in([Access.key(:sessions), session_id], session)
                  |> put_in([Access.key(:session_ids), external_id], session_id)
                  |> Fleet.schedule_session_expiry(session_id, activation_timeout)
                  |> Listeners.maybe_schedule_idle_shutdown()
                  |> put_in([Access.key(:workers), worker_id], %{
                    external_id: worker_external_id,
                    created_at: created_at,
                    pool_id: pool_id,
                    pool_name: pool_name,
                    workspace_id: workspace_id,
                    state: :active,
                    data: nil,
                    session_id: session_id,
                    stop_id: nil,
                    stop_retry_at: nil,
                    last_poll_at: nil,
                    polling: false,
                    poll_failures: 0,
                    first_poll_failure_at: nil
                  })
                  |> put_in([Access.key(:worker_external_ids), worker_external_id], worker_id)
                  |> Effects.emit(%WorkerCreated{
                    workspace: State.workspace_external_id(state, workspace_id),
                    pool: pool_name,
                    worker: worker_external_id,
                    created_at: created_at,
                    session: external_id
                  })
                  |> start_launch(
                    workspace_id,
                    pool_id,
                    pool_name,
                    pool,
                    worker_id,
                    worker_external_id,
                    token
                  )
              end
            end)

          {state, next_launch_at}
        end)
      else
        {state, nil}
      end

    next_execute_after =
      executions_future
      |> Enum.map(& &1.execute_after)
      |> Enum.min(fn -> nil end)

    state =
      state.workers
      |> Enum.filter(fn {_worker_id, worker} -> poll_due?(state, worker, now) end)
      |> Enum.reduce(state, fn {worker_id, worker}, state ->
        case worker_launcher(state, worker) do
          {:ok, launcher} ->
            state
            |> Fleet.call_launcher(launcher, :poll, [worker.data, launcher], fn state, result ->
              state = update_worker(state, worker_id, &%{&1 | polling: false})

              case result do
                {:ok, {:ok, true}} ->
                  clear_poll_failures(state, worker_id)

                {:ok, {:ok, false, error, logs}} ->
                  # The launcher knows the worker has gone, and this is the
                  # only place its exit code and log tail come from.
                  Fleet.deactivate_worker(state, worker_id, error, logs)

                {:ok, {:error, _reason}} ->
                  record_poll_failure(state, worker_id)

                :error ->
                  record_poll_failure(state, worker_id)
              end
            end)
            |> update_worker(worker_id, &%{&1 | last_poll_at: now, polling: true})

          # Nothing to ask the launcher with: a poll that couldn't be
          # answered, and tolerated the same way.
          {:error, _reason} ->
            state
            |> update_worker(worker_id, &%{&1 | last_poll_at: now})
            |> record_poll_failure(worker_id)
        end
      end)

    # A worker that connected but never said what it can run is broken
    # rather than idle - a bad command or working directory, typically -
    # and the idle timeout below deliberately doesn't apply to it, so this
    # is what bounds it. It counts against the pool for the same reason a
    # failed launch does: without that, the pool relaunches immediately
    # and repeats the whole thing.
    unready =
      Enum.filter(state.workers, fn {_worker_id, worker} ->
        case worker_session(state, worker) do
          {:ok, session} -> session.ready_deadline_at && now > session.ready_deadline_at
          :error -> false
        end
      end)

    state =
      Enum.reduce(unready, state, fn {worker_id, worker}, state ->
        state
        |> record_pool_launch_failure(worker.pool_id)
        |> Fleet.deactivate_worker(worker_id, "no_targets")
      end)

    # TODO: consider min/max pool size
    state =
      state.workers
      |> Enum.filter(fn {_worker_id, worker} ->
        # TODO: better way to check launched than checking existence of data?
        with true <- worker.state == :active && !is_nil(worker.data),
             {:ok, session} <- worker_session(state, worker),
             # Only a worker that has been in a position to take work can
             # be surplus to it. One that is still starting has an idle
             # time, but it means nothing.
             true <- Fleet.session_ready?(session) do
          Enum.empty?(session.starting) && Enum.empty?(session.executing) &&
            now - session.last_idle_at >= worker_idle_timeout_ms(state, worker)
        else
          _ -> false
        end
      end)
      |> Enum.reduce(state, fn {worker_id, worker}, state ->
        Fleet.update_worker_state(
          state,
          worker_id,
          :draining,
          worker.workspace_id,
          worker.pool_name
        )
      end)

    state =
      state.workers
      |> Enum.filter(fn {_worker_id, worker} -> stop_due?(state, worker, now) end)
      |> Enum.reduce(state, fn {worker_id, worker}, state ->
        {:ok, worker_stop_id, stopping_at} = Workers.create_worker_stop(state.db, worker_id)

        state =
          state
          |> update_worker(worker_id, &%{&1 | stop_id: worker_stop_id, stop_retry_at: nil})
          |> Effects.emit(%WorkerStopping{
            workspace: State.workspace_external_id(state, worker.workspace_id),
            pool: worker.pool_name,
            worker: worker.external_id,
            stopping_at: stopping_at
          })

        case worker_launcher(state, worker) do
          {:ok, launcher} ->
            Fleet.call_launcher(state, launcher, :stop, [worker.data, launcher], fn state,
                                                                                    result ->
              case result do
                {:ok, :ok} ->
                  {:ok, stopped_at} =
                    Workers.create_worker_stop_result(state.db, worker_stop_id, nil)

                  Effects.emit(state, %WorkerStopResult{
                    workspace: State.workspace_external_id(state, worker.workspace_id),
                    pool: worker.pool_name,
                    worker: worker.external_id,
                    stopped_at: stopped_at,
                    error: nil
                  })

                {:ok, {:error, reason}} ->
                  record_stop_failure(state, worker_id, worker, worker_stop_id, to_error(reason))

                :error ->
                  record_stop_failure(state, worker_id, worker, worker_stop_id, "stop_crashed")
              end
            end)

          {:error, reason} ->
            record_stop_failure(state, worker_id, worker, worker_stop_id, to_error(reason))
        end
      end)

    # While any worker exists there are deadlines to sweep for - polls,
    # idle drains, readiness, stop retries - so keep the periodic pass.
    # With no workers the only thing still on a clock is a pool waiting
    # out its throttle or backoff; with neither, nothing here will change
    # until something happens, and everything that happens asks for a
    # pass of its own.
    sweep_delay_ms =
      cond do
        map_size(state.workers) > 0 ->
          @sweep_interval_ms

        # Deactivating a worker above changed what the launch pass, which
        # has already run, would have decided - so run another, now that
        # its pool is no longer waiting on a worker that is never coming.
        unready != [] ->
          0

        true ->
          next_launch_at && max(0, next_launch_at - now)
      end

    delay_ms =
      [
        if(next_execute_after, do: trunc(next_execute_after) - System.os_time(:millisecond)),
        sweep_delay_ms
      ]
      |> Enum.reject(&is_nil/1)
      |> Enum.min(fn -> nil end)

    state =
      if delay_ms do
        if delay_ms > 0 do
          timer = Process.send_after(self(), :tick, delay_ms)
          Map.put(state, :tick_timer, timer)
        else
          send(self(), :tick)
          state
        end
      else
        state
      end

    state
  end

  # Applies `fun` to a worker, or does nothing if it has been deactivated
  # since - which a launcher callback landing late always might find.
  defp update_worker(state, worker_id, fun) do
    if Map.has_key?(state.workers, worker_id) do
      update_in(state, [Access.key(:workers), worker_id], fun)
    else
      state
    end
  end

  defp worker_session(state, worker) do
    if worker.session_id, do: Map.fetch(state.sessions, worker.session_id), else: :error
  end

  # When a pool may next launch: never sooner than the launch interval
  # after its last worker, and not until it has served out any backoff
  # from launches that failed.
  defp pool_launch_due_at(state, pool_id, latest_pool_launch_at) do
    throttled_until = Map.get(latest_pool_launch_at, pool_id, 0) + @pool_launch_interval_ms

    case Map.get(state.pool_failures, pool_id) do
      nil ->
        throttled_until

      %{failures: failures, last_attempt_at: last_attempt_at} ->
        max(throttled_until, last_attempt_at + pool_backoff_ms(failures))
    end
  end

  defp pool_backoff_ms(failures) do
    min(@pool_backoff_max_ms, @pool_backoff_base_ms * Integer.pow(2, min(failures - 1, 16)))
  end

  defp record_pool_launch_failure(state, pool_id) do
    update_in(
      state,
      [Access.key(:pool_failures), Access.key(pool_id, %{failures: 0, last_attempt_at: 0})],
      fn %{failures: failures} ->
        %{failures: failures + 1, last_attempt_at: System.os_time(:millisecond)}
      end
    )
  end

  # A pool can say how long its workers linger once idle, in seconds: a
  # worker that takes a while to start is worth keeping warm between
  # runs. One whose pool doesn't say gets the default.
  defp worker_idle_timeout_ms(state, worker) do
    case get_in(state.pools, [worker.workspace_id, worker.pool_name, :idle_timeout]) do
      seconds when is_integer(seconds) and seconds >= 0 -> seconds * 1000
      _ -> @default_worker_idle_timeout_ms
    end
  end

  # Starts a worker's launch, with the pool's secrets resolved for the
  # launcher to use. A secret that doesn't resolve fails the launch the
  # way the launcher failing would, so the worker records what went wrong.
  defp start_launch(
         state,
         workspace_id,
         pool_id,
         pool_name,
         pool,
         worker_id,
         worker_external_id,
         token
       ) do
    case resolve_launcher(state, workspace_id, pool.launcher) do
      {:ok, launcher} ->
        Fleet.call_launcher(
          state,
          launcher,
          :launch,
          [
            Fleet.build_launcher_env(state, workspace_id, token, launcher),
            Fleet.worker_args(pool),
            launcher,
            %{pool_name: pool_name}
          ],
          launch_callback(
            workspace_id,
            pool_id,
            pool_name,
            worker_id,
            worker_external_id,
            launcher
          )
        )

      {:error, error, detail} ->
        callback =
          launch_callback(workspace_id, pool_id, pool_name, worker_id, worker_external_id, nil)

        callback.(state, {:ok, {:error, error, detail}})
    end
  end

  defp launch_callback(workspace_id, pool_id, pool_name, worker_id, worker_external_id, launcher) do
    fn state, result ->
      # A launcher can say more than a code about why a
      # launch failed - an API's own message, typically -
      # and that goes where a log tail would.
      {data, error, detail} =
        case result do
          {:ok, {:ok, data}} -> {data, nil, nil}
          {:ok, {:error, error}} -> {nil, error, nil}
          {:ok, {:error, error, detail}} -> {nil, error, detail}
          :error -> {nil, "launch_crashed", nil}
        end

      {:ok, started_at} =
        Workers.create_worker_launch_result(state.db, worker_id, data, error)

      state =
        Effects.emit(state, %WorkerLaunchResult{
          workspace: State.workspace_external_id(state, workspace_id),
          pool: pool_name,
          worker: worker_external_id,
          started_at: started_at,
          error: error
        })

      cond do
        error ->
          # Deactivating the worker pops it from state, and
          # with it the only record that this pool was ever
          # tried - so count the failure first, or the pool
          # relaunches on the very next pass.
          state
          |> record_pool_launch_failure(pool_id)
          |> Fleet.deactivate_worker(worker_id, error, detail)

        Map.has_key?(state.workers, worker_id) ->
          put_in(
            state,
            [Access.key(:workers), worker_id, Access.key(:data)],
            data
          )

        true ->
          # The worker was deactivated while its launch was
          # in flight. Nothing will ever connect to what was
          # just started, and this result is the only thing
          # that knows how to reach it, so stop it here
          # rather than leaking it.
          Fleet.call_launcher(
            state,
            launcher,
            :stop,
            [data, launcher],
            fn state, _result -> state end
          )
      end
    end
  end

  # The launcher config with its secrets' values, for one call and then
  # forgotten. Failures are in the launcher's terms: a code for the
  # worker, and what was wrong for its logs.
  defp resolve_launcher(state, workspace_id, launcher) do
    workspace_name = state.workspaces[workspace_id].name

    case Coflux.Admin.Secrets.resolve_launcher(
           state.admin_db,
           state.project_id,
           workspace_name,
           launcher
         ) do
      {:ok, launcher} ->
        {:ok, launcher}

      {:error, {:secret_not_found, name}} ->
        {:error, "launch_secret_missing", "secret not found: #{name}"}

      {:error, {:secret_invalid, name}} ->
        {:error, "launch_secret_invalid", "secret can't be used: #{name}"}

      {:error, :no_secret} ->
        {:error, "launch_secret_missing", "secrets need COFLUX_SECRET to be configured"}
    end
  end

  # A worker's launcher config as its pool has it now, secrets resolved.
  defp worker_launcher(state, worker) do
    case Workspaces.get_launcher_for_pool(state.db, worker.pool_id) do
      {:ok, nil} ->
        {:error, :no_launcher}

      {:ok, launcher} ->
        Coflux.Admin.Secrets.resolve_launcher(
          state.admin_db,
          state.project_id,
          state.workspaces[worker.workspace_id].name,
          launcher
        )
    end
  end

  defp poll_due?(state, worker, now) do
    cond do
      is_nil(worker.data) ->
        false

      worker.polling ->
        false

      is_nil(worker.last_poll_at) ->
        true

      true ->
        connected =
          case worker_session(state, worker) do
            {:ok, session} -> !is_nil(session.connection)
            :error -> false
          end

        interval_ms =
          if connected,
            do: @connected_worker_poll_interval_ms,
            else: @disconnected_worker_poll_interval_ms

        now - worker.last_poll_at > interval_ms
    end
  end

  defp clear_poll_failures(state, worker_id) do
    update_worker(state, worker_id, &%{&1 | poll_failures: 0, first_poll_failure_at: nil})
  end

  # A launcher that couldn't answer says nothing about the worker, so this
  # only gives up once the failures have persisted for long enough to rule
  # out the launcher itself being briefly unavailable. Both bounds matter:
  # a disconnected worker is polled every few seconds, so a count alone
  # would expire inside an ordinary Docker daemon restart.
  defp record_poll_failure(state, worker_id) do
    case Map.fetch(state.workers, worker_id) do
      :error ->
        state

      {:ok, worker} ->
        now = System.os_time(:millisecond)
        failures = worker.poll_failures + 1
        first_failure_at = worker.first_poll_failure_at || now

        if failures >= @poll_failure_threshold and
             now - first_failure_at > @poll_failure_grace_ms do
          Fleet.deactivate_worker(state, worker_id, "poll_error")
        else
          update_worker(
            state,
            worker_id,
            &%{&1 | poll_failures: failures, first_poll_failure_at: first_failure_at}
          )
        end
    end
  end

  defp stop_due?(state, worker, now) do
    cond do
      # Nothing to ask the launcher about until the launch has landed.
      is_nil(worker.data) -> false
      # A stop is already in flight, or has already succeeded.
      worker.stop_id -> false
      # A previous stop failed; wait before asking again.
      worker.stop_retry_at && now < worker.stop_retry_at -> false
      # The session has gone, so there is nothing left to drain.
      is_nil(worker.session_id) -> true
      worker.state != :draining -> false
      true -> drained?(state, worker)
    end
  end

  defp drained?(state, worker) do
    case worker_session(state, worker) do
      {:ok, session} -> Enum.empty?(session.starting) && Enum.empty?(session.executing)
      :error -> true
    end
  end

  # A stop that failed is recorded as one: the container may well still be
  # running, and reporting it as stopped both misleads whoever is watching
  # and means nothing ever tries again. Clearing `stop_id` is what allows
  # the retry; `stop_retry_at` is what keeps it from being immediate.
  defp record_stop_failure(state, worker_id, worker, worker_stop_id, error) do
    {:ok, _} = Workers.create_worker_stop_result(state.db, worker_stop_id, error)

    state
    |> Effects.emit(%WorkerStopResult{
      workspace: State.workspace_external_id(state, worker.workspace_id),
      pool: worker.pool_name,
      worker: worker.external_id,
      stopped_at: nil,
      error: error
    })
    |> update_worker(
      worker_id,
      &%{&1 | stop_id: nil, stop_retry_at: System.os_time(:millisecond) + @stop_retry_interval_ms}
    )
  end

  defp earliest(nil, b), do: b
  defp earliest(a, nil), do: a
  defp earliest(a, b), do: min(a, b)

  defp to_error({:secret_not_found, name}), do: "secret_missing:#{name}"
  defp to_error({:secret_invalid, name}), do: "secret_invalid:#{name}"
  defp to_error(reason) when is_binary(reason), do: reason
  defp to_error(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp to_error(reason), do: inspect(reason)
end
