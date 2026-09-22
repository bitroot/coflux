defmodule Coflux.Orchestration.Server.Fleet do
  @moduledoc """
  The workers available to run things, and how work is matched to them.

  A *session* is one worker's connection: what it declares it can run,
  how much of it at once, and what it is running now. A session outlives
  its connection - a worker that drops reconnects into the same session
  and picks up where it left off - which is why a session expires on a
  timer rather than on disconnect. Two timers apply: a worker that never
  connects expires on its activation timeout_ms, one that connected and went
  away on its reconnection timeout_ms. A third case is neither, and is
  handled by `Scheduler`: a worker that connects but never declares any
  targets is deactivated once its readiness deadline passes.

  A *pool* is a declaration that workers of some shape should exist, and
  the launcher is what makes them. Launching is asynchronous: the task is
  tracked, and its result is applied to the worker when it lands.

  Matching an execution to a session is the other half. A session may run
  it if it declares the target, has a free slot, isn't draining, and its
  tags satisfy what the execution requires - `provides` against the
  execution's `requires`, and the execution's tags against what the
  session `accepts`.
  """

  alias Coflux.Events.{SessionEnded, SessionUpdated, WorkerDeactivated, WorkerStateChanged}

  alias Coflux.Orchestration.{Sessions, Workers}

  alias Coflux.Orchestration.Server.{Effects, Lifecycle, Resolve, State, StreamDelivery}

  # A worker that never connects expires on the first; one that connected
  # and went away expires on the second.
  @default_activation_timeout_ms 600_000
  @default_reconnection_timeout_ms 30_000

  def resolve_worker_external_id(state, worker_external_id) do
    case Map.fetch(state.worker_external_ids, worker_external_id) do
      {:ok, worker_id} -> {:ok, worker_id}
      :error -> {:error, :no_worker}
    end
  end

  def lookup_worker(state, worker_id, expected_workspace_id) do
    if worker_id do
      case Map.fetch(state.workers, worker_id) do
        :error ->
          {:error, :no_worker}

        {:ok, worker} ->
          if worker.workspace_id != expected_workspace_id do
            {:error, :no_worker}
          else
            {:ok, worker}
          end
      end
    else
      {:ok, nil}
    end
  end

  def remove_session(state, session_id) do
    {:ok, _} = Sessions.expire_session(state.db, session_id)
    # Drop any stream subscriptions this session held — consumer has gone
    # away, so there's no one to push to. Do this before popping the
    # session from state.sessions since drop_session_subscriptions reads
    # the session's live execution set.
    state = StreamDelivery.drop_session_subscriptions(state, session_id)
    {session, state} = pop_in(state.sessions[session_id])
    state = Map.update!(state, :session_expiries, &Map.delete(&1, session_id))

    # starting/executing now contain external IDs - resolve to internal for process_result.
    # Session removal means no more notify_terminated for these executions, so we
    # write both results + completion here.
    state =
      session.executing
      |> MapSet.union(session.starting)
      |> Enum.reduce(state, fn ext_id, state ->
        execution_id = Map.fetch!(state.execution_ids, ext_id)
        {:ok, state} = Lifecycle.process_result(state, execution_id, :abandoned)
        Lifecycle.complete_execution(state, execution_id)
      end)
      |> Map.update!(:targets, fn all_targets ->
        Enum.reduce(
          session.targets,
          all_targets,
          fn {module_name, module_targets}, all_targets ->
            Enum.reduce(module_targets, all_targets, fn target_name, all_targets ->
              module = Map.fetch!(all_targets, module_name)
              target = Map.fetch!(module, target_name)
              target = Map.update!(target, :session_ids, &MapSet.delete(&1, session_id))

              if Enum.empty?(target.session_ids) do
                module = Map.delete(module, target_name)

                if Enum.empty?(module) do
                  Map.delete(all_targets, module_name)
                else
                  Map.put(all_targets, module_name, module)
                end
              else
                module = Map.put(module, target_name, target)
                Map.put(all_targets, module_name, module)
              end
            end)
          end
        )
      end)
      |> Map.update!(:session_ids, &Map.delete(&1, session.external_id))
      |> Map.update!(:waiting, fn waiting ->
        # waiting keys are tagged tuples ({:execution, _} | {:input, _});
        # each entry is a select waiter map keyed by from_ext_id (external).
        waiting
        |> Enum.map(fn {waiting_key, entries} ->
          {waiting_key,
           Enum.reject(entries, fn entry ->
             MapSet.member?(session.starting, entry.from_ext_id) ||
               MapSet.member?(session.executing, entry.from_ext_id)
           end)}
        end)
        |> Enum.reject(fn {_waiting_key, entries} -> entries == [] end)
        |> Map.new()
      end)
      |> Effects.emit(%SessionEnded{
        workspace: State.workspace_external_id(state, session.workspace_id),
        session: session.external_id
      })

    state =
      if session.worker_id do
        case Map.fetch(state.workers, session.worker_id) do
          {:ok, worker} ->
            if is_nil(worker.data) do
              # Worker never got a launch result — the launch was in-flight when
              # the server crashed. Deactivate it so it doesn't sit forever.
              deactivate_worker(state, session.worker_id, "launch_incomplete")
            else
              put_in(state, [Access.key(:workers), session.worker_id, :session_id], nil)
            end

          :error ->
            state
        end
      else
        state
      end

    state
  end

  def session_event(state, session) do
    data = build_session_data(state, session)

    %SessionUpdated{
      workspace: State.workspace_external_id(state, session.workspace_id),
      session: session.external_id,
      connected: data.connected,
      executing: data.executing,
      concurrency: data.concurrency,
      pool: data.pool_name,
      targets: data.targets,
      provides: data.provides,
      accepts: data.accepts,
      worker_state: data.worker_state,
      executions: data.executions
    }
  end

  def assign_targets(state, targets, session_id) do
    Enum.reduce(targets, state, fn {module, module_targets}, state ->
      Enum.reduce(module_targets, state, fn {type, target_names}, state ->
        Enum.reduce(target_names, state, fn target_name, state ->
          state
          |> update_in(
            [
              Access.key(:targets),
              Access.key(module, %{}),
              Access.key(target_name, %{type: nil, session_ids: MapSet.new()})
            ],
            fn target ->
              target
              |> Map.put(:type, type)
              |> Map.update!(:session_ids, &MapSet.put(&1, session_id))
            end
          )
          |> update_in(
            [Access.key(:sessions), session_id, :targets, Access.key(module, MapSet.new())],
            &MapSet.put(&1, target_name)
          )
        end)
      end)
    end)
  end

  @doc """
  Whether a session has ever been in a position to take work: it
  connected, and it said what it can run.

  Until both have happened the session has never been able to accept an
  execution, so the fact that it isn't running one says nothing about it
  being surplus - which is why the idle timeout_ms only applies from here.
  Declaring an *empty* set of targets still counts: the worker answered,
  it just has nothing to offer, and it should be allowed to drain like
  any other rather than pinning its pool open forever.
  """
  def session_ready?(session) do
    !is_nil(session.activated_at) && !is_nil(session.declared_at)
  end

  def session_at_capacity?(session) do
    if session.concurrency != 0 do
      load = MapSet.size(session.starting) + MapSet.size(session.executing)
      load >= session.concurrency
    else
      false
    end
  end

  def session_active?(session, state) do
    if session.worker_id do
      worker = Map.fetch!(state.workers, session.worker_id)
      worker.state == :active
    else
      true
    end
  end

  def session_pool_disabled?(session, state) do
    if session.worker_id do
      worker = Map.fetch!(state.workers, session.worker_id)
      workspace_pools = Map.get(state.pools, session.workspace_id, %{})
      pool = Map.get(workspace_pools, worker.pool_name)
      pool != nil && Map.get(pool, :state, :active) == :disabled
    else
      false
    end
  end

  def has_requirements?(provides, requires) do
    # TODO: case insensitive matching?
    Enum.all?(requires, fn {key, requires_values} ->
      (provides || %{})
      |> Map.get(key, [])
      |> Enum.any?(&(&1 in requires_values))
    end)
  end

  def merge_tag_sets(a, b) do
    Map.merge(a || %{}, b || %{}, fn _key, v1, v2 -> Enum.uniq(v1 ++ v2) end)
  end

  # Merge run-level requires with step-level requires (child overrides per key).
  def effective_requires(tag_sets, run_requires_tag_set_id, step_requires_tag_set_id) do
    run_requires =
      if run_requires_tag_set_id,
        do: Map.fetch!(tag_sets, run_requires_tag_set_id),
        else: %{}

    step_requires =
      if step_requires_tag_set_id,
        do: Map.fetch!(tag_sets, step_requires_tag_set_id),
        else: %{}

    run_requires
    |> Map.merge(step_requires)
    |> Map.reject(fn {_key, values} -> values == [] end)
  end

  def satisfies_accepts?(accepts, requires) do
    # Worker's accepts tags must all be present in the task's requires tags
    Enum.all?(accepts || %{}, fn {key, accepts_values} ->
      (requires || %{})
      |> Map.get(key, [])
      |> Enum.any?(&(&1 in accepts_values))
    end)
  end

  # Build the session data map sent to the Sessions topic.
  def build_session_data(state, session) do
    worker = session.worker_id && Map.get(state.workers, session.worker_id)

    # Build targets as %{module => [target_name]} (session.targets values are MapSets)
    targets =
      Map.new(session.targets, fn {module, target_names} ->
        {module, target_names |> MapSet.to_list() |> Enum.sort()}
      end)

    %{
      connected: !is_nil(session.connection),
      executing: session.starting |> MapSet.union(session.executing) |> Enum.count(),
      concurrency: session.concurrency,
      pool_name: if(worker, do: worker.pool_name),
      targets: targets,
      provides: session.provides,
      accepts: session.accepts,
      worker_state: if(worker, do: worker.state),
      executions: session.total_executions
    }
  end

  def choose_session(state, execution, requires) do
    target =
      state.targets
      |> Map.get(execution.module, %{})
      |> Map.get(execution.target)

    if target && target.type == execution.type do
      session_ids =
        Enum.filter(target.session_ids, fn session_id ->
          session = Map.fetch!(state.sessions, session_id)

          session.workspace_id == execution.workspace_id && session.connection &&
            !Map.get(session, :draining, false) &&
            !session_at_capacity?(session) &&
            session_active?(session, state) &&
            !session_pool_disabled?(session, state) &&
            has_requirements?(merge_tag_sets(session.provides, session.accepts), requires) &&
            satisfies_accepts?(session.accepts, requires)
        end)

      if Enum.any?(session_ids) do
        # TODO: prioritise (based on 'cost'?)
        Enum.random(session_ids)
      end
    end
  end

  def choose_pool(state, execution, requires) do
    pools =
      state.pools
      |> Map.get(execution.workspace_id, %{})
      |> Map.filter(fn {_, pool} ->
        Map.get(pool, :state, :active) != :disabled &&
          pool.launcher && pool_hosts_module?(pool.modules, execution.module) &&
          has_requirements?(merge_tag_sets(pool.provides, Map.get(pool, :accepts, %{})), requires) &&
          satisfies_accepts?(Map.get(pool, :accepts, %{}), requires)
      end)

    if Enum.any?(pools) do
      pools |> Map.values() |> Enum.map(& &1.id) |> Enum.random()
    end
  end

  # A pool's modules are what its workers are started with, so they mean
  # what they mean to discovery: a name covers that module and, if it's a
  # package, everything under it. No modules is no restriction.
  def pool_hosts_module?([], _module), do: true

  def pool_hosts_module?(modules, module) do
    Enum.any?(modules, fn name ->
      name == module || String.starts_with?(module, name <> ".")
    end)
  end

  # The arguments a launched worker is started with. A pool with no
  # modules hosts everything, and the worker has to be told so rather
  # than left to default: it would otherwise take `worker.modules` from
  # whatever coflux.toml its working directory holds, and host less than
  # the server routes to it.
  def worker_args(pool) do
    case pool.modules do
      [] -> ["--all-modules"]
      modules -> modules
    end
  end

  def process_launcher_result(state, task_ref, result) do
    callback = Map.fetch!(state.launcher_tasks, task_ref)

    state
    |> callback.(result)
    |> Map.update!(:launcher_tasks, &Map.delete(&1, task_ref))
  end

  def build_launcher_env(state, workspace_id, token, launcher) do
    coflux_host = launcher[:server_host] || Coflux.Config.server_host(state.project_id)

    base = %{
      "COFLUX_HOST" => coflux_host,
      "COFLUX_PROJECT" => state.project_id,
      "COFLUX_WORKSPACE" => state.workspaces[workspace_id].name,
      "COFLUX_SESSION" => token
    }

    base =
      case Map.get(launcher, :adapter) do
        nil -> base
        adapter -> Map.put(base, "COFLUX_WORKER_ADAPTER", Enum.join(adapter, ","))
      end

    base =
      case Map.get(launcher, :concurrency) do
        nil -> base
        concurrency -> Map.put(base, "COFLUX_WORKER_CONCURRENCY", Integer.to_string(concurrency))
      end

    base =
      case Map.get(launcher, :server_secure) do
        nil -> base
        true -> Map.put(base, "COFLUX_SECURE", "true")
        false -> Map.put(base, "COFLUX_SECURE", "false")
      end

    case Map.get(launcher, :env) do
      nil -> base
      env -> Map.merge(base, env)
    end
  end

  def call_launcher(state, launcher, fun, args, callback) do
    module =
      case launcher.type do
        :docker -> Coflux.DockerLauncher
        :process -> Coflux.ProcessLauncher
        :kubernetes -> Coflux.KubernetesLauncher
        :ecs -> Coflux.EcsLauncher
      end

    task = Task.Supervisor.async_nolink(Coflux.LauncherSupervisor, module, fun, args)

    put_in(state, [Access.key(:launcher_tasks), task.ref], callback)
  end

  def update_worker_state(
        state,
        worker_id,
        worker_state,
        workspace_id,
        pool_name,
        principal_id \\ nil
      ) do
    :ok = Workers.create_worker_state(state.db, worker_id, worker_state, principal_id)

    worker = state.workers[worker_id]

    state
    |> put_in(
      [Access.key(:workers), worker_id, :state],
      worker_state
    )
    |> Effects.emit(%WorkerStateChanged{
      workspace: State.workspace_external_id(state, workspace_id),
      pool: pool_name,
      worker: worker.external_id,
      state: worker_state
    })
  end

  @doc """
  Retires a worker: no more work, no more polling, and its session gone.

  Deactivation can be reached twice for the same worker - two launcher
  tasks landing on it, or a poll racing a stop - so a worker that has
  already gone is not an error, just nothing left to do.
  """
  def deactivate_worker(state, worker_id, error, logs \\ nil)

  def deactivate_worker(%{workers: workers} = state, worker_id, _error, _logs)
      when not is_map_key(workers, worker_id),
      do: state

  def deactivate_worker(state, worker_id, error, logs) do
    {:ok, deactivated_at} = Workers.create_worker_deactivation(state.db, worker_id, error, logs)

    {worker, state} = pop_in(state, [Access.key(:workers), worker_id])

    state = Map.update!(state, :worker_external_ids, &Map.delete(&1, worker.external_id))

    # Expire the worker's session so it can't reconnect to a deactivated worker.
    state =
      if worker.session_id && Map.has_key?(state.sessions, worker.session_id) do
        remove_session(state, worker.session_id)
      else
        state
      end

    Effects.emit(state, %WorkerDeactivated{
      workspace: State.workspace_external_id(state, worker.workspace_id),
      pool: worker.pool_name,
      worker: worker.external_id,
      deactivated_at: deactivated_at,
      error: error,
      logs: logs
    })
  end

  def schedule_session_expiry(state, session_id, timeout_ms) do
    expiry_at = System.os_time(:millisecond) + timeout_ms
    state = put_in(state.session_expiries[session_id], expiry_at)
    reschedule_expire_sessions_timer(state)
  end

  def cancel_session_expiry(state, session_id) do
    state = Map.update!(state, :session_expiries, &Map.delete(&1, session_id))
    reschedule_expire_sessions_timer(state)
  end

  def reschedule_expire_sessions_timer(state) do
    if state.expire_sessions_timer do
      Process.cancel_timer(state.expire_sessions_timer)
    end

    case state.session_expiries |> Map.values() |> Enum.min(fn -> nil end) do
      nil ->
        %{state | expire_sessions_timer: nil}

      next_expiry ->
        delay = max(0, next_expiry - System.os_time(:millisecond))
        timer = Process.send_after(self(), :expire_sessions, delay)
        %{state | expire_sessions_timer: timer}
    end
  end

  @doc """
  Restores the fleet from the database at boot: every active session with
  its tags and expiry, each one linked to its worker, and any worker that
  was launched but never got a session deactivated - nothing is going to
  connect to it.
  """
  def load(state) do
    # Load active sessions from DB
    {:ok, active_sessions} = Sessions.load_active_sessions(state.db)

    # Load total assignment counts per session for lifetime execution tracking
    {:ok, assignment_counts} = Sessions.get_assignment_counts(state.db)

    assignment_counts_by_session =
      Map.new(assignment_counts, fn {session_id, total} -> {session_id, total} end)

    state =
      Enum.reduce(
        active_sessions,
        state,
        fn {session_id, external_id, workspace_id, worker_id, provides_tag_set_id,
            accepts_tag_set_id, activation_timeout_ms, reconnection_timeout_ms, secret_hash,
            created_at, activated_at},
           state ->
          provides = Resolve.tag_set(state.db, provides_tag_set_id)
          accepts = Resolve.tag_set(state.db, accepts_tag_set_id)

          activation_timeout_ms = activation_timeout_ms || @default_activation_timeout_ms
          reconnection_timeout_ms = reconnection_timeout_ms || @default_reconnection_timeout_ms

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
            provides: provides,
            accepts: accepts,
            worker_id: worker_id,
            last_idle_at: activated_at || created_at,
            activated_at: activated_at,
            # Targets live only in memory, so a session that reconnects
            # after a restart has to declare them again - it is not ready
            # until it does, and the deadline for doing so is armed by
            # that reconnection rather than by the activation it did
            # before the restart.
            declared_at: nil,
            ready_deadline_at: nil,
            activation_timeout_ms: activation_timeout_ms,
            reconnection_timeout_ms: reconnection_timeout_ms,
            total_executions: Map.get(assignment_counts_by_session, session_id, 0)
          }

          state =
            state
            |> put_in([Access.key(:sessions), session_id], session)
            |> put_in([Access.key(:session_ids), external_id], session_id)

          # Schedule expiry - either activation (if never connected) or reconnection (if was connected)
          state =
            if activated_at do
              schedule_session_expiry(state, session_id, reconnection_timeout_ms)
            else
              schedule_session_expiry(state, session_id, activation_timeout_ms)
            end

          # Link session to worker if applicable
          if worker_id && Map.has_key?(state.workers, worker_id) do
            put_in(state, [Access.key(:workers), worker_id, Access.key(:session_id)], session_id)
          else
            state
          end
        end
      )

    # Deactivate orphaned workers that have no launch result (data: nil) and
    # no associated session. These were created but the server crashed before
    # a session could be created for them, so nothing will ever connect.
    # Workers that DO have a session are left alone — the session's activation
    # timeout will handle cleanup if the launched process never connects.
    state =
      state.workers
      |> Enum.filter(fn {_worker_id, worker} ->
        is_nil(worker.data) && is_nil(worker.session_id)
      end)
      |> Enum.reduce(state, fn {worker_id, _worker}, state ->
        deactivate_worker(state, worker_id, "server_restarted")
      end)

    state
  end
end
