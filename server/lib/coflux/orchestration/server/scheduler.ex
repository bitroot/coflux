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

  # How long a worker sits idle before it is stopped.
  @worker_idle_timeout_ms 5_000

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

    state =
      if Enum.any?(unassigned) do
        # Track the most recent worker creation per pool, and which pools
        # already have a worker that isn't ready to accept work.  We skip
        # launching for pools that have a worker still pending activation
        # or that activated but hasn't registered any targets yet (e.g.
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
                   false <- session.activated_at != nil and Enum.any?(session.targets) do
                MapSet.put(pending, worker.pool_id)
              else
                _ -> pending
              end

            {latest, pending}
          end)

        unassigned
        |> Enum.group_by(& &1.workspace_id)
        |> Enum.reduce(state, fn {workspace_id, executions}, state ->
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
          |> Enum.filter(&(now - Map.get(latest_pool_launch_at, &1, 0) > 10_000))
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
                  activation_timeout: activation_timeout,
                  reconnection_timeout: reconnection_timeout,
                  total_executions: 0
                }

                state
                |> put_in([Access.key(:sessions), session_id], session)
                |> put_in([Access.key(:session_ids), external_id], session_id)
                |> Fleet.schedule_session_expiry(session_id, activation_timeout)
                |> Listeners.maybe_schedule_idle_shutdown()
                |> Fleet.call_launcher(
                  pool.launcher,
                  :launch,
                  [
                    Fleet.build_launcher_env(state, workspace_id, token, pool.launcher),
                    pool.modules,
                    pool.launcher,
                    %{pool_name: pool_name}
                  ],
                  fn state, result ->
                    {data, error} =
                      case result do
                        {:ok, {:ok, data}} -> {data, nil}
                        {:ok, {:error, error}} -> {nil, error}
                        :error -> {nil, "launch_crashed"}
                      end

                    {:ok, started_at} =
                      Workers.create_worker_launch_result(state.db, worker_id, data, error)

                    state =
                      state
                      |> put_in([Access.key(:workers), worker_id, Access.key(:data)], data)
                      |> Effects.emit(%WorkerLaunchResult{
                        workspace: State.workspace_external_id(state, workspace_id),
                        pool: pool_name,
                        worker: worker_external_id,
                        started_at: started_at,
                        error: error
                      })

                    state =
                      if error do
                        Fleet.deactivate_worker(state, worker_id, error)
                      else
                        state
                      end

                    state
                  end
                )
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
                  last_poll_at: nil
                })
                |> put_in([Access.key(:worker_external_ids), worker_external_id], worker_id)
                |> Effects.emit(%WorkerCreated{
                  workspace: State.workspace_external_id(state, workspace_id),
                  pool: pool_name,
                  worker: worker_external_id,
                  created_at: created_at,
                  session: external_id
                })
            end
          end)
        end)
      else
        state
      end

    next_execute_after =
      executions_future
      |> Enum.map(& &1.execute_after)
      |> Enum.min(fn -> nil end)

    state =
      state.workers
      |> Enum.filter(fn {_worker_id, worker} ->
        # TODO: don't poll if a poll is in progress?
        if worker.data do
          if is_nil(worker.last_poll_at) do
            true
          else
            connection =
              if worker.session_id && Map.has_key?(state.sessions, worker.session_id),
                do: state.sessions[worker.session_id].connection

            interval_ms =
              if connection,
                do: @connected_worker_poll_interval_ms,
                else: @disconnected_worker_poll_interval_ms

            now - worker.last_poll_at > interval_ms
          end
        else
          false
        end
      end)
      |> Enum.reduce(state, fn {worker_id, worker}, state ->
        {:ok, launcher} = Workspaces.get_launcher_for_pool(state.db, worker.pool_id)

        state
        |> Fleet.call_launcher(launcher, :poll, [worker.data], fn state, result ->
          case result do
            {:ok, {:ok, true}} ->
              state

            {:ok, {:ok, false, error, logs}} ->
              Fleet.deactivate_worker(state, worker_id, error, logs)

            {:ok, {:error, _reason}} ->
              Fleet.deactivate_worker(state, worker_id, "poll_error")

            :error ->
              # TODO: ?
              state
          end
        end)
        |> put_in([Access.key(:workers), worker_id, :last_poll_at], now)
      end)

    state =
      state.workers
      |> Enum.group_by(fn {_, worker} -> worker.pool_name end)
      |> Enum.flat_map(fn {_pool_name, workers} ->
        # TODO: consider min/max pool size
        Enum.filter(workers, fn {_worker_id, worker} ->
          # TODO: better way to check launched than checking existence of data?
          if worker.state == :active && worker.session_id && worker.data do
            session = Map.fetch!(state.sessions, worker.session_id)
            idle_time = now - session.last_idle_at

            if Enum.empty?(session.starting) && Enum.empty?(session.executing) &&
                 idle_time >= @worker_idle_timeout_ms do
              true
            end
          end
        end)
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
      |> Enum.filter(fn {_worker_id, worker} ->
        if worker.session_id do
          if worker.state == :draining && worker.data && !worker.stop_id do
            session = Map.fetch!(state.sessions, worker.session_id)
            Enum.empty?(session.starting) && Enum.empty?(session.executing)
          end
        else
          !is_nil(worker.data)
        end
      end)
      |> Enum.reduce(state, fn {worker_id, worker}, state ->
        {:ok, worker_stop_id, stopping_at} = Workers.create_worker_stop(state.db, worker_id)
        {:ok, launcher} = Workspaces.get_launcher_for_pool(state.db, worker.pool_id)

        state =
          state
          |> put_in([Access.key(:workers), worker_id, :stop_id], worker_stop_id)
          |> Effects.emit(%WorkerStopping{
            workspace: State.workspace_external_id(state, worker.workspace_id),
            pool: worker.pool_name,
            worker: worker.external_id,
            stopping_at: stopping_at
          })

        Fleet.call_launcher(state, launcher, :stop, [worker.data], fn state, result ->
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

            {:ok, {:error, _reason}} ->
              # Stop failed (e.g. connection refused) — treat as stopped
              {:ok, stopped_at} =
                Workers.create_worker_stop_result(state.db, worker_stop_id, nil)

              Effects.emit(state, %WorkerStopResult{
                workspace: State.workspace_external_id(state, worker.workspace_id),
                pool: worker.pool_name,
                worker: worker.external_id,
                stopped_at: stopped_at,
                error: nil
              })

            :error ->
              # TODO: get error details
              error = %{}

              {:ok, _} =
                Workers.create_worker_stop_result(state.db, worker_stop_id, error)

              state =
                Effects.emit(state, %WorkerStopResult{
                  workspace: State.workspace_external_id(state, worker.workspace_id),
                  pool: worker.pool_name,
                  worker: worker.external_id,
                  stopped_at: nil,
                  error: error
                })

              # TODO: unset 'stop_id' of worker in state? (so it can be retried? but somehow limit rate?)
              state
          end
        end)
      end)

    delay_ms =
      [
        if(next_execute_after, do: trunc(next_execute_after) - System.os_time(:millisecond)),
        if(state.workers, do: 5_000)
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
end
