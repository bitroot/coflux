defmodule Coflux.Orchestration.Server do
  use GenServer, restart: :transient
  require Logger

  alias Coflux.Store.{Bloom, Epochs, Index}
  alias Coflux.MapUtils

  alias Coflux.Orchestration.{
    Workspaces,
    Sessions,
    Runs,
    Results,
    Assets,
    Inputs,
    Values,
    CacheConfigs,
    TagSets,
    Workers,
    Manifests,
    Principals,
    Epoch
  }

  @default_activation_timeout_ms 600_000
  @default_reconnection_timeout_ms 30_000
  @target_runs_archive_depth 5
  @connected_worker_poll_interval_ms 30_000
  @disconnected_worker_poll_interval_ms 5_000
  @worker_idle_timeout_ms 5_000
  @rotation_check_interval_ms 60_000
  @rotation_size_threshold_bytes 100 * 1024 * 1024
  @idempotency_ttl_ms 24 * 60 * 60 * 1000
  @idle_timeout_ms 30_000

  defmodule State do
    defstruct project_id: nil,
              db: nil,
              epochs: nil,
              tick_timer: nil,
              expire_waiters_timer: nil,
              expire_sessions_timer: nil,
              idle_timer: nil,

              # id -> %{name, base_id, state}
              workspaces: %{},

              # workspace_id -> %{pool_name -> pool}
              pools: %{},

              # name -> id
              workspace_names: %{},

              # worker_id -> %{created_at, pool_id, pool_name, workspace_id, state, data, session_id, stop_id, last_poll_at}
              workers: %{},

              # ref -> {pid, session_id}
              connections: %{},

              # session_id -> %{external_id, connection, targets, queue, starting, executing, concurrency, workspace_id, provides, accepts, worker_id, last_idle_at, activated_at, activation_timeout, reconnection_timeout}
              sessions: %{},

              # external_id -> session_id
              session_ids: %{},

              # session_id -> expiry_timestamp_ms
              session_expiries: %{},

              # {module, target} -> %{type, session_ids}
              targets: %{},

              # external_id -> workspace_id
              workspace_external_ids: %{},

              # external_id -> worker_id
              worker_external_ids: %{},

              # execution_external_id -> execution_id (internal)
              execution_ids: %{},

              # ref -> topic
              listeners: %{},

              # topic -> %{ref -> pid}
              topics: %{},

              # topic -> [notification]
              notifications: %{},

              # execution_external_id -> [{from_execution_external_id, request_id, expire_at, suspend?}]
              waiting: %{},

              # task_ref -> callback
              launcher_tasks: %{},

              # Coflux.Store.Index struct for Bloom filter lookups
              epoch_index: nil,

              # ref of running background index build task
              index_task: nil,

              # [epoch_id] awaiting Bloom filter build (FIFO)
              index_queue: [],

              # run_external_id -> {root_module, root_target, MapSet of execution_ids}
              run_workflows: %{},

              # execution_id -> MapSet of execution_ids that this execution is waiting on
              pending_dependencies: %{},

              # execution_id -> MapSet of execution_ids that are waiting on this execution
              dependency_waiters: %{}
  end

  def start_link(opts) do
    {project_id, opts} = Keyword.pop!(opts, :project_id)
    GenServer.start_link(__MODULE__, project_id, opts)
  end

  def init(project_id) do
    index_path = ["projects", project_id, "orchestration", "index.json"]

    {:ok, epoch_index} =
      Index.load(index_path, ["runs", "cache_keys", "idempotency_keys", "input_external_ids"])

    unindexed_epoch_ids = Index.unindexed_epoch_ids(epoch_index)
    archived_epoch_ids = Index.all_epoch_ids(epoch_index)

    case Epochs.open(project_id, "orchestration",
           unindexed_epoch_ids: unindexed_epoch_ids,
           archived_epoch_ids: archived_epoch_ids
         ) do
      {:ok, epochs} ->
        db = Epochs.active_db(epochs)

        state = %State{
          project_id: project_id,
          db: db,
          epochs: epochs,
          epoch_index: epoch_index,
          index_queue: unindexed_epoch_ids
        }

        send(self(), :tick)

        {:ok, state, {:continue, :setup}}
    end
  end

  def handle_continue(:setup, state) do
    {:ok, workspaces} = Workspaces.get_all_workspaces(state.db)
    {:ok, workers} = Workers.get_active_workers(state.db)

    workspace_names =
      Map.new(workspaces, fn {workspace_id, workspace} ->
        {workspace.name, workspace_id}
      end)

    workspace_external_ids =
      Map.new(workspaces, fn {workspace_id, workspace} ->
        {workspace.external_id, workspace_id}
      end)

    workers =
      Enum.reduce(
        workers,
        %{},
        fn {worker_id, external_id, created_at, pool_id, pool_name, workspace_id, state, data},
           workers ->
          Map.put(workers, worker_id, %{
            external_id: external_id,
            created_at: created_at,
            pool_id: pool_id,
            pool_name: pool_name,
            workspace_id: workspace_id,
            state: state,
            data: data,
            session_id: nil,
            stop_id: nil,
            last_poll_at: nil
          })
        end
      )

    worker_external_ids =
      Map.new(workers, fn {worker_id, worker} ->
        {worker.external_id, worker_id}
      end)

    pools =
      workspaces
      |> Map.keys()
      |> Enum.reduce(%{}, fn workspace_id, pools ->
        {:ok, workspace_pools, _hash} = Workspaces.get_workspace_pools(state.db, workspace_id)
        Map.put(pools, workspace_id, workspace_pools)
      end)

    state =
      Map.merge(state, %{
        workspaces: workspaces,
        workspace_names: workspace_names,
        workspace_external_ids: workspace_external_ids,
        pools: pools,
        workers: workers,
        worker_external_ids: worker_external_ids
      })

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
            accepts_tag_set_id, activation_timeout, reconnection_timeout, secret_hash, created_at,
            activated_at},
           state ->
          provides =
            if provides_tag_set_id do
              case TagSets.get_tag_set(state.db, provides_tag_set_id) do
                {:ok, tag_set} -> tag_set
              end
            else
              %{}
            end

          accepts =
            if accepts_tag_set_id do
              case TagSets.get_tag_set(state.db, accepts_tag_set_id) do
                {:ok, tag_set} -> tag_set
              end
            else
              %{}
            end

          activation_timeout = activation_timeout || @default_activation_timeout_ms
          reconnection_timeout = reconnection_timeout || @default_reconnection_timeout_ms

          session = %{
            external_id: external_id,
            secret_hash: secret_hash,
            connection: nil,
            targets: %{},
            queue: [],
            starting: MapSet.new(),
            executing: MapSet.new(),
            concurrency: 0,
            workspace_id: workspace_id,
            provides: provides,
            accepts: accepts,
            worker_id: worker_id,
            last_idle_at: activated_at || created_at,
            activated_at: activated_at,
            activation_timeout: activation_timeout,
            reconnection_timeout: reconnection_timeout,
            total_executions: Map.get(assignment_counts_by_session, session_id, 0)
          }

          state =
            state
            |> put_in([Access.key(:sessions), session_id], session)
            |> put_in([Access.key(:session_ids), external_id], session_id)

          # Schedule expiry - either activation (if never connected) or reconnection (if was connected)
          state =
            if activated_at do
              schedule_session_expiry(state, session_id, reconnection_timeout)
            else
              schedule_session_expiry(state, session_id, activation_timeout)
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
        {execution_id, execution_external_id(run_ext_id, step_num, attempt)}
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
          # Session no longer active - abandon these executions
          Enum.reduce(execution_ids, state, fn execution_id, state ->
            {:ok, state} = process_result(state, execution_id, :abandoned)
            state
          end)
        end
      end)

    # Populate run_workflows lookup for all active runs
    {:ok, active_run_workflows} = Runs.get_active_run_workflows(state.db)

    state =
      Enum.reduce(active_run_workflows, state, fn {run_ext_id, module, target, _step_number,
                                                   _attempt, execution_id},
                                                  state ->
        track_run_execution(state, run_ext_id, execution_id, module, target)
      end)

    # Initialize pending dependency tracking for existing unassigned executions
    state = initialize_pending_dependencies(state)

    # Schedule periodic epoch rotation check
    Process.send_after(self(), :check_rotation, @rotation_check_interval_ms)

    # Kick off background Bloom filter builds for any unindexed epochs
    state = maybe_start_index_build(state)

    # Schedule idle shutdown if no sessions or listeners yet
    state = maybe_schedule_idle_shutdown(state)

    {:noreply, state}
  end

  def handle_call(:get_workspaces, _from, state) do
    workspaces =
      state.workspaces
      |> Enum.filter(fn {_, e} -> e.state != :archived end)
      |> Map.new(fn {workspace_id, workspace} ->
        base_external_id =
          if workspace.base_id do
            case Map.fetch(state.workspaces, workspace.base_id) do
              {:ok, base_ws} -> base_ws.external_id
              :error -> nil
            end
          end

        {workspace_id,
         %{
           name: workspace.name,
           base_id: workspace.base_id,
           base_external_id: base_external_id,
           external_id: workspace.external_id
         }}
      end)

    {:reply, {:ok, workspaces}, state}
  end

  # Principal management

  def handle_call({:ensure_principal, external_id}, _from, state) do
    {:ok, principal_id} = Principals.ensure_user(state.db, external_id)
    {:reply, {:ok, principal_id}, state}
  end

  # Token management

  def handle_call({:check_token, token_hash}, _from, state) do
    case Principals.check_token(state.db, token_hash) do
      {:ok, %{principal_id: principal_id, workspaces: workspaces}} ->
        {:reply, {:ok, %{workspaces: workspaces, principal_id: principal_id}}, state}

      {:error, :not_found} ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:create_token, name, principal_id, opts}, _from, state) do
    {:ok, result} = Principals.create_token(state.db, state.project_id, name, principal_id, opts)
    state = notify_listeners(state, :tokens, {:token, result.external_id, result})
    {:reply, {:ok, result}, state}
  end

  def handle_call(:list_tokens, _from, state) do
    {:ok, tokens} = Principals.list_tokens(state.db)
    {:reply, {:ok, tokens}, state}
  end

  def handle_call({:subscribe_tokens, pid}, _from, state) do
    {:ok, tokens} = Principals.list_tokens(state.db)
    {:ok, ref, state} = add_listener(state, :tokens, pid)
    {:reply, {:ok, tokens, ref}, state}
  end

  def handle_call({:revoke_token, token_id}, _from, state) do
    case Principals.revoke_token(state.db, token_id) do
      {:ok, external_id} ->
        state = notify_listeners(state, :tokens, {:token, external_id, nil})
        {:reply, {:ok, external_id}, state}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:get_token, external_id}, _from, state) do
    result = Principals.get_token_by_external_id(state.db, external_id)
    {:reply, result, state}
  end

  # Workspace management

  def handle_call({:create_workspace, name, base_external_id, access}, _from, state) do
    with :ok <- check_operator_access(access, name),
         {:ok, base_id} <- resolve_optional_workspace(state, base_external_id) do
      case Workspaces.create_workspace(state.db, name, base_id, access[:principal_id]) do
        {:ok, workspace_id, workspace} ->
          base_external_id =
            if workspace.base_id do
              case Map.fetch(state.workspaces, workspace.base_id) do
                {:ok, base} -> base.external_id
                :error -> nil
              end
            end

          state =
            state
            |> put_in([Access.key(:workspaces), workspace_id], workspace)
            |> put_in([Access.key(:workspace_names), workspace.name], workspace_id)
            |> put_in([Access.key(:workspace_external_ids), workspace.external_id], workspace_id)
            |> put_in([Access.key(:pools), workspace_id], %{})
            |> notify_listeners(
              :workspaces,
              {:workspace, workspace_id,
               %{
                 name: workspace.name,
                 base_external_id: base_external_id,
                 state: workspace.state,
                 external_id: workspace.external_id
               }}
            )
            |> flush_notifications()

          {:reply, {:ok, workspace_id, workspace.external_id}, state}

        {:error, error} ->
          {:reply, {:error, error}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:update_workspace, workspace_external_id, updates, access}, _from, state) do
    with {:ok, workspace_id} <- resolve_workspace_external_id(state, workspace_external_id),
         :ok <- check_operator_access(access, state.workspaces[workspace_id].name),
         :ok <- check_rename_allowed(access, updates[:name]) do
      # TODO: shut down/update pools
      case Workspaces.update_workspace(state.db, workspace_id, updates, access[:principal_id]) do
        {:ok, workspace} ->
          original_name = state.workspaces[workspace_id].name
          workspace = Map.put(workspace, :external_id, workspace_external_id)

          base_external_id =
            if workspace.base_id do
              case Map.fetch(state.workspaces, workspace.base_id) do
                {:ok, base} -> base.external_id
                :error -> nil
              end
            end

          state =
            state
            |> put_in([Access.key(:workspaces), workspace_id], workspace)
            |> Map.update!(:workspace_names, fn workspace_names ->
              workspace_names
              |> Map.delete(original_name)
              |> Map.put(workspace.name, workspace_id)
            end)
            |> notify_listeners(
              :workspaces,
              {:workspace, workspace_id,
               %{
                 name: workspace.name,
                 base_external_id: base_external_id,
                 state: workspace.state,
                 external_id: workspace.external_id
               }}
            )
            |> flush_notifications()

          send(self(), :tick)

          # TODO: return updated?
          {:reply, :ok, state}

        {:error, error} ->
          {:reply, {:error, error}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:pause_workspace, workspace_external_id, access}, _from, state) do
    case require_workspace(state, workspace_external_id, access) do
      {:ok, workspace_id, _} ->
        case Workspaces.pause_workspace(state.db, workspace_id, access[:principal_id]) do
          :ok ->
            state =
              state
              |> put_in([Access.key(:workspaces), workspace_id, Access.key(:state)], :paused)
              |> notify_listeners(:workspaces, {:state, workspace_external_id, :paused})
              |> flush_notifications()

            {:reply, :ok, state}
        end

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:resume_workspace, workspace_external_id, access}, _from, state) do
    case require_workspace(state, workspace_external_id, access) do
      {:ok, workspace_id, _} ->
        case Workspaces.resume_workspace(state.db, workspace_id, access[:principal_id]) do
          :ok ->
            state =
              state
              |> put_in([Access.key(:workspaces), workspace_id, Access.key(:state)], :active)
              |> notify_listeners(:workspaces, {:state, workspace_external_id, :active})
              |> flush_notifications()

            send(self(), :tick)

            {:reply, :ok, state}
        end

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:archive_workspace, workspace_external_id, access}, _from, state) do
    case require_workspace(state, workspace_external_id, access) do
      {:ok, workspace_id, _} ->
        case Workspaces.archive_workspace(state.db, workspace_id, access[:principal_id]) do
          :ok ->
            state =
              state.sessions
              |> Enum.filter(fn {_, s} -> s.workspace_id == workspace_id end)
              |> Enum.reduce(state, fn {session_id, session}, state ->
                state =
                  if session.connection do
                    {pid, ^session_id} = Map.fetch!(state.connections, session.connection)
                    send(pid, :stop)
                    Map.update!(state, :connections, &Map.delete(&1, session.connection))
                  else
                    state
                  end

                remove_session(state, session_id)
              end)

            state =
              case Runs.get_pending_executions_for_workspace(state.db, workspace_id) do
                {:ok, executions} ->
                  Enum.reduce(executions, state, fn {execution_id, _run_id, module}, state ->
                    case record_and_notify_result(
                           state,
                           execution_id,
                           :cancelled,
                           module
                         ) do
                      {:ok, state} -> state
                      {:error, :already_recorded} -> state
                    end
                  end)
              end

            state =
              state.workers
              |> Enum.reduce(state, fn {worker_id, worker}, state ->
                if worker.workspace_id == workspace_id && worker.state == :active do
                  update_worker_state(state, worker_id, :draining, workspace_id, worker.pool_name)
                else
                  state
                end
              end)
              |> put_in([Access.key(:workspaces), workspace_id, Access.key(:state)], :archived)
              |> notify_listeners(:workspaces, {:state, workspace_external_id, :archived})
              |> flush_notifications()
              |> maybe_schedule_idle_shutdown()

            {:reply, :ok, state}

          {:error, error} ->
            {:reply, {:error, error}, state}
        end

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:get_pools, workspace_external_id}, _from, state) do
    with {:ok, workspace_id, _} <- require_workspace(state, workspace_external_id) do
      case Workspaces.get_workspace_pools(state.db, workspace_id) do
        {:ok, pools, hash} ->
          {:reply, {:ok, pools, hash}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call(
        {:update_pools, workspace_external_id, desired_pools, expected_hash, access},
        _from,
        state
      ) do
    with {:ok, workspace_id, _} <-
           require_workspace(state, workspace_external_id, access) do
      case Workspaces.update_pools(
             state.db,
             workspace_id,
             desired_pools,
             expected_hash,
             access[:principal_id]
           ) do
        {:ok, changed_pool_names} ->
          # Drain workers only for pools that actually changed
          state =
            state.workers
            |> Enum.reduce(state, fn {worker_id, worker}, state ->
              if worker.state == :active &&
                   MapSet.member?(changed_pool_names, worker.pool_name) do
                update_worker_state(state, worker_id, :draining, workspace_id, worker.pool_name)
              else
                state
              end
            end)

          # Update in-memory pools (reload from DB to include :id and :state)
          ws_ext_id = workspace_external_id(state, workspace_id)

          {:ok, updated_pools, _hash} =
            Workspaces.get_workspace_pools(state.db, workspace_id)

          state =
            put_in(state, [Access.key(:pools), Access.key(workspace_id, %{})], updated_pools)

          # Send notifications only for changed pools
          state =
            Enum.reduce(changed_pool_names, state, fn name, state ->
              pool = Map.get(updated_pools, name)

              state
              |> notify_listeners({:pool, ws_ext_id, name}, {:updated, pool})
              |> notify_listeners({:pools, ws_ext_id}, {:pool, name, pool})
            end)

          state = flush_notifications(state)

          {:reply, :ok, state}

        {:error, :conflict} ->
          {:reply, {:error, :conflict}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call(
        {:create_pool, workspace_external_id, pool_name, pool, access},
        _from,
        state
      ) do
    with {:ok, workspace_id, _} <-
           require_workspace(state, workspace_external_id, access) do
      case Workspaces.create_pool(
             state.db,
             workspace_id,
             pool_name,
             pool,
             access[:principal_id]
           ) do
        {:ok, _pool_id} ->
          {:ok, updated_pools, _hash} =
            Workspaces.get_workspace_pools(state.db, workspace_id)

          ws_ext_id = workspace_external_id(state, workspace_id)
          pool = Map.get(updated_pools, pool_name)

          state =
            state
            |> put_in([Access.key(:pools), Access.key(workspace_id, %{})], updated_pools)
            |> notify_listeners({:pool, ws_ext_id, pool_name}, {:updated, pool})
            |> notify_listeners({:pools, ws_ext_id}, {:pool, pool_name, pool})
            |> flush_notifications()

          {:reply, :ok, state}

        {:error, :already_exists} ->
          {:reply, {:error, :already_exists}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call(
        {:update_pool, workspace_external_id, pool_name, pool_patch, access},
        _from,
        state
      ) do
    with {:ok, workspace_id, _} <-
           require_workspace(state, workspace_external_id, access) do
      case Workspaces.update_pool(
             state.db,
             workspace_id,
             pool_name,
             pool_patch,
             access[:principal_id]
           ) do
        {:ok, _pool_id} ->
          state =
            state.workers
            |> Enum.reduce(state, fn {worker_id, worker}, state ->
              if worker.state == :active && worker.pool_name == pool_name do
                update_worker_state(state, worker_id, :draining, workspace_id, pool_name)
              else
                state
              end
            end)

          # Reload pools from DB to get the merged result with :id and :state
          {:ok, updated_pools, _hash} =
            Workspaces.get_workspace_pools(state.db, workspace_id)

          ws_ext_id = workspace_external_id(state, workspace_id)
          pool = Map.get(updated_pools, pool_name)

          state =
            state
            |> put_in([Access.key(:pools), Access.key(workspace_id, %{})], updated_pools)
            |> notify_listeners({:pool, ws_ext_id, pool_name}, {:updated, pool})
            |> notify_listeners({:pools, ws_ext_id}, {:pool, pool_name, pool})
            |> flush_notifications()

          {:reply, :ok, state}

        {:error, :not_found} ->
          {:reply, {:error, :not_found}, state}

        {:error, :type_change} ->
          {:reply, {:error, :type_change}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:disable_pool, workspace_external_id, pool_name, access}, _from, state) do
    with {:ok, workspace_id, _} <-
           require_workspace(state, workspace_external_id, access) do
      :ok = Workspaces.disable_pool(state.db, workspace_id, pool_name, access[:principal_id])

      state =
        state
        |> put_in(
          [Access.key(:pools), Access.key(workspace_id, %{}), Access.key(pool_name, %{}), :state],
          :disabled
        )
        |> notify_listeners(
          {:pool, workspace_external_id, pool_name},
          {:state, :disabled}
        )
        |> notify_listeners(
          {:pools, workspace_external_id},
          {:pool_state, pool_name, :disabled}
        )
        |> flush_notifications()

      send(self(), :tick)

      {:reply, :ok, state}
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:enable_pool, workspace_external_id, pool_name, access}, _from, state) do
    with {:ok, workspace_id, _} <-
           require_workspace(state, workspace_external_id, access) do
      :ok = Workspaces.enable_pool(state.db, workspace_id, pool_name, access[:principal_id])

      state =
        state
        |> put_in(
          [Access.key(:pools), Access.key(workspace_id, %{}), Access.key(pool_name, %{}), :state],
          :active
        )
        |> notify_listeners(
          {:pool, workspace_external_id, pool_name},
          {:state, :active}
        )
        |> notify_listeners(
          {:pools, workspace_external_id},
          {:pool_state, pool_name, :active}
        )
        |> flush_notifications()

      send(self(), :tick)

      {:reply, :ok, state}
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:stop_worker, workspace_external_id, worker_external_id, access}, _from, state) do
    with {:ok, workspace_id, _} <-
           require_workspace(state, workspace_external_id, access),
         {:ok, worker_id} <- resolve_worker_external_id(state, worker_external_id),
         {:ok, worker} <- lookup_worker(state, worker_id, workspace_id) do
      state =
        state
        |> update_worker_state(
          worker_id,
          :draining,
          workspace_id,
          worker.pool_name,
          access[:principal_id]
        )
        |> flush_notifications()

      send(self(), :tick)

      {:reply, :ok, state}
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call(
        {:resume_worker, workspace_external_id, worker_external_id, access},
        _from,
        state
      ) do
    with {:ok, workspace_id, _} <-
           require_workspace(state, workspace_external_id, access),
         {:ok, worker_id} <- resolve_worker_external_id(state, worker_external_id),
         {:ok, worker} <- lookup_worker(state, worker_id, workspace_id) do
      state =
        state
        |> update_worker_state(
          worker_id,
          :active,
          workspace_id,
          worker.pool_name,
          access[:principal_id]
        )
        |> flush_notifications()

      send(self(), :tick)

      {:reply, :ok, state}
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call(
        {:register_manifests, workspace_external_id, manifests, access},
        _from,
        state
      ) do
    case require_workspace(state, workspace_external_id, access) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, workspace_id, _} ->
        case Manifests.register_manifests(
               state.db,
               workspace_id,
               manifests,
               access[:principal_id]
             ) do
          :ok ->
            ws_ext_id = workspace_external_id(state, workspace_id)

            state =
              manifests
              |> Enum.reduce(state, fn {module, workflows}, state ->
                Enum.reduce(workflows, state, fn {target_name, target}, state ->
                  notify_listeners(
                    state,
                    {:workflow, module, target_name, ws_ext_id},
                    {:target, target}
                  )
                end)
              end)
              |> notify_listeners(
                {:modules, ws_ext_id},
                {:manifests, manifests}
              )
              |> notify_listeners(
                {:manifests, ws_ext_id},
                {:manifests, manifests}
              )
              |> notify_listeners(
                {:targets, ws_ext_id},
                {:manifests,
                 Map.new(manifests, fn {module_name, workflows} ->
                   {module_name, MapSet.new(Map.keys(workflows))}
                 end)}
              )
              |> flush_notifications()

            {:reply, :ok, state}
        end
    end
  end

  def handle_call({:archive_module, workspace_external_id, module_name, access}, _from, state) do
    case require_workspace(state, workspace_external_id, access) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, workspace_id, _} ->
        case Manifests.archive_module(state.db, workspace_id, module_name, access[:principal_id]) do
          :ok ->
            ws_ext_id = workspace_external_id(state, workspace_id)

            state =
              state
              |> notify_listeners(
                {:modules, ws_ext_id},
                {:manifest, module_name, nil}
              )
              |> notify_listeners(
                {:manifests, ws_ext_id},
                {:manifest, module_name, nil}
              )
              |> flush_notifications()

            {:reply, :ok, state}
        end
    end
  end

  def handle_call({:get_manifests, workspace_external_id}, _from, state) do
    case require_workspace(state, workspace_external_id) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, workspace_id, _} ->
        {:ok, manifests} = Manifests.get_latest_manifests(state.db, workspace_id)
        {:reply, {:ok, manifests}, state}
    end
  end

  def handle_call({:subscribe_manifests, workspace_external_id, pid}, _from, state) do
    case require_workspace(state, workspace_external_id) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, workspace_id, _} ->
        {:ok, manifests} = Manifests.get_latest_manifests(state.db, workspace_id)
        {:ok, ref, state} = add_listener(state, {:manifests, workspace_external_id}, pid)
        {:reply, {:ok, manifests, ref}, state}
    end
  end

  def handle_call({:get_workflow, workspace_external_id, module, target_name}, _from, state) do
    with {:ok, workspace_id, _} <- require_workspace(state, workspace_external_id),
         {:ok, workflow} <-
           Manifests.get_latest_workflow(state.db, workspace_id, module, target_name) do
      {:reply, {:ok, workflow}, state}
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:create_session, workspace_external_id, access, opts}, _from, state) do
    provides = Keyword.get(opts, :provides, %{})
    accepts = Keyword.get(opts, :accepts, %{})
    activation_timeout = Keyword.get(opts, :activation_timeout, @default_activation_timeout_ms)

    reconnection_timeout =
      Keyword.get(opts, :reconnection_timeout, @default_reconnection_timeout_ms)

    with {:ok, workspace_id, _} <-
           require_workspace(state, workspace_external_id, access) do
      db_opts = [
        provides: provides,
        accepts: accepts,
        activation_timeout: activation_timeout,
        reconnection_timeout: reconnection_timeout,
        created_by: access[:principal_id]
      ]

      case Sessions.create_session(state.db, workspace_id, nil, db_opts) do
        {:ok, session_id, external_session_id, token, secret_hash, now} ->
          session = %{
            external_id: external_session_id,
            secret_hash: secret_hash,
            connection: nil,
            targets: %{},
            queue: [],
            starting: MapSet.new(),
            executing: MapSet.new(),
            concurrency: 0,
            workspace_id: workspace_id,
            provides: provides,
            accepts: accepts,
            worker_id: nil,
            last_idle_at: now,
            activated_at: nil,
            activation_timeout: activation_timeout,
            reconnection_timeout: reconnection_timeout,
            total_executions: 0
          }

          state =
            state
            |> put_in([Access.key(:sessions), session_id], session)
            |> put_in([Access.key(:session_ids), external_session_id], session_id)
            |> schedule_session_expiry(session_id, activation_timeout)
            |> maybe_schedule_idle_shutdown()

          {:reply, {:ok, token}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:verify_session, token}, _from, state) do
    with {:ok, external_id, secret} <- Sessions.parse_token(token),
         {:ok, session_id} <- Map.fetch(state.session_ids, external_id),
         session = Map.fetch!(state.sessions, session_id),
         :ok <- verify_session_secret(secret, session.secret_hash) do
      workspace = Map.fetch!(state.workspaces, session.workspace_id)
      {:reply, {:ok, %{type: :session, workspaces: [workspace.name], principal_id: nil}}, state}
    else
      _ -> {:reply, {:error, :session_invalid}, state}
    end
  end

  def handle_call({:resume_session, token, workspace_external_id, pid}, _from, state) do
    with {:ok, external_id, secret} <- Sessions.parse_token(token),
         {:ok, session_id} <- Map.fetch(state.session_ids, external_id),
         session = Map.fetch!(state.sessions, session_id),
         :ok <- verify_session_secret(secret, session.secret_hash),
         {:ok, workspace_id, _} <- require_workspace(state, workspace_external_id),
         :ok <- require_workspace_match(session.workspace_id, workspace_id) do
      activated_at =
        if is_nil(session.activated_at) do
          {:ok, now} = Sessions.activate_session(state.db, session_id)
          now
        else
          session.activated_at
        end

      # Cancel any pending expiry (activation or reconnection)
      state = cancel_session_expiry(state, session_id)

      state =
        if session.connection do
          {{pid, ^session_id}, state} = pop_in(state.connections[session.connection])
          # TODO: better reason?
          Process.exit(pid, :kill)
          state
        else
          state
        end

      ref = Process.monitor(pid)

      state.sessions[session_id].queue
      |> Enum.reverse()
      |> Enum.each(&send(pid, &1))

      state =
        state
        |> put_in([Access.key(:connections), ref], {pid, session_id})
        |> update_in(
          [Access.key(:sessions), session_id],
          &Map.merge(&1, %{connection: ref, queue: [], activated_at: activated_at})
        )

      state =
        notify_listeners(
          state,
          {:sessions, workspace_external_id(state, session.workspace_id)},
          {:session, session.external_id, build_session_data(state, session)}
        )

      state =
        case session.worker_id && Map.fetch(state.workers, session.worker_id) do
          {:ok, _worker} ->
            put_in(
              state,
              [Access.key(:workers), session.worker_id, Access.key(:session_id)],
              session_id
            )

          _ ->
            state
        end

      # starting/executing already contain external IDs
      external_execution_ids =
        session.executing
        |> MapSet.union(session.starting)
        |> MapSet.to_list()

      send(self(), :tick)

      state = flush_notifications(state)
      {:reply, {:ok, external_id, external_execution_ids}, state}
    else
      :error ->
        {:reply, {:error, :session_invalid}, state}

      {:error, :session_invalid} ->
        {:reply, {:error, :session_invalid}, state}

      {:error, :workspace_invalid} ->
        {:reply, {:error, :workspace_mismatch}, state}

      {:error, :workspace_mismatch} ->
        {:reply, {:error, :workspace_mismatch}, state}
    end
  end

  def handle_call({:declare_targets, external_id, targets, concurrency}, _from, state) do
    session_id = Map.fetch!(state.session_ids, external_id)

    now = System.os_time(:millisecond)

    state =
      state
      |> assign_targets(targets, session_id)
      |> put_in([Access.key(:sessions), session_id, :concurrency], concurrency)
      |> put_in([Access.key(:sessions), session_id, :last_idle_at], now)

    session = Map.fetch!(state.sessions, session_id)

    state =
      state
      |> notify_listeners(
        {:sessions, workspace_external_id(state, session.workspace_id)},
        {:session, session.external_id, build_session_data(state, session)}
      )
      |> flush_notifications()

    send(self(), :tick)

    {:reply, :ok, state}
  end

  def handle_call({:start_run, module, target_name, type, arguments, access, opts}, _from, state) do
    workspace_external_id = Keyword.get(opts, :workspace)

    case require_workspace(state, workspace_external_id, access) do
      {:ok, workspace_id, _} ->
        client_key = Keyword.get(opts, :idempotency_key)
        ws_ext_id = workspace_external_id(state, workspace_id)

        case maybe_find_idempotent_run(state, client_key, ws_ext_id) do
          {:hit, ext_run_id, step_number, attempt} ->
            execution_external_id = execution_external_id(ext_run_id, step_number, attempt)
            {:reply, {:ok, ext_run_id, step_number, execution_external_id}, state}

          :miss ->
            opts =
              if client_key do
                hashed = Runs.build_idempotency_key(ws_ext_id, client_key)
                Keyword.put(opts, :idempotency_key, hashed)
              else
                opts
              end

            {:ok, external_run_id, step_number, _execution_id, state} =
              schedule_run(
                state,
                module,
                target_name,
                type,
                arguments,
                workspace_id,
                Keyword.put(opts, :created_by, access[:principal_id])
              )

            execution_external_id = execution_external_id(external_run_id, step_number, 1)

            send(self(), :tick)
            state = flush_notifications(state)

            {:reply, {:ok, external_run_id, step_number, execution_external_id}, state}
        end

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call(
        {:schedule_step, parent_external_id, module, target_name, type, arguments, opts},
        _from,
        state
      ) do
    parent_id = Map.fetch!(state.execution_ids, parent_external_id)
    {:ok, parent_run_id} = Runs.get_execution_run_id(state.db, parent_id)
    {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, parent_id)
    {:ok, run} = Runs.get_run_by_id(state.db, parent_run_id)

    cache_workspace_ids = get_cache_workspace_ids(state, workspace_id)
    arguments = Enum.map(arguments, &normalize_value(&1))

    # Inherit run-level memo if the step doesn't specify its own
    opts =
      if is_nil(Keyword.get(opts, :memo)) and run.memo do
        Keyword.put(opts, :memo, true)
      else
        opts
      end

    case Runs.schedule_step(
           state.db,
           run.id,
           parent_id,
           module,
           target_name,
           type,
           arguments,
           workspace_id,
           cache_workspace_ids,
           opts
         ) do
      {:ok,
       %{
         step_id: step_id,
         step_number: step_number,
         execution_id: execution_id,
         attempt: attempt,
         created_at: created_at,
         cache_key: cache_key,
         memo_key: memo_key,
         memo_hit: memo_hit,
         child_added: child_added
       }} ->
        # Compute and register pending dependencies for non-memoised executions
        {state, pending_dependencies} =
          if step_id && !memo_hit do
            wait_for = Keyword.get(opts, :wait_for) || []

            pending_dependencies =
              compute_pending_dependencies(state.db, execution_id, wait_for, step_id)

            state =
              register_pending_dependencies(state, execution_id, pending_dependencies)

            {state, pending_dependencies}
          else
            {state, MapSet.new()}
          end

        group_id = Keyword.get(opts, :group_id)
        cache = Keyword.get(opts, :cache)
        retries = Keyword.get(opts, :retries)
        timeout = Keyword.get(opts, :timeout, 0)
        delay = Keyword.get(opts, :delay, 0)
        execute_after = if delay > 0, do: created_at + delay
        step_requires = Keyword.get(opts, :requires) || %{}

        run_requires =
          if run.requires_tag_set_id do
            case TagSets.get_tag_set(state.db, run.requires_tag_set_id) do
              {:ok, tag_set} -> tag_set
            end
          else
            %{}
          end

        requires =
          run_requires
          |> Map.merge(step_requires)
          |> Map.reject(fn {_key, values} -> values == [] end)

        execution_external_id = execution_external_id(run.external_id, step_number, attempt)

        parent_execution_external_id =
          if parent_id do
            {:ok, {r, s, a}} = Runs.get_execution_key(state.db, parent_id)
            execution_external_id(r, s, a)
          end

        ws_ext_id = workspace_external_id(state, workspace_id)

        state =
          if !memo_hit do
            arguments = Enum.map(arguments, &build_value(&1, state.db))

            recurrent = Keyword.get(opts, :recurrent, false)

            state
            |> notify_listeners(
              {:run, run.external_id},
              {:step, step_number,
               %{
                 module: module,
                 target: target_name,
                 type: type,
                 parent_id: parent_execution_external_id,
                 cache_config: cache,
                 cache_key: cache_key,
                 memo_key: memo_key,
                 retries: retries,
                 recurrent: recurrent,
                 timeout: timeout,
                 created_at: created_at,
                 arguments: arguments,
                 requires: step_requires
               }, ws_ext_id}
            )
            |> notify_listeners(
              {:run, run.external_id},
              {:execution, step_number, attempt, execution_external_id, ws_ext_id, created_at,
               execute_after, %{}, nil}
            )
          else
            state
          end

        state =
          if child_added do
            notify_listeners(
              state,
              {:run, run.external_id},
              {:child, parent_execution_external_id, {step_number, attempt, group_id}}
            )
          else
            state
          end

        state =
          if !memo_hit do
            execute_at = execute_after || created_at

            {root_module, root_target} =
              get_run_workflow(state, run.external_id) ||
                raise "run_workflows missing entry for run #{run.external_id}"

            state =
              state
              |> track_run_execution(run.external_id, execution_id, root_module, root_target)
              |> notify_listeners(
                {:modules, ws_ext_id},
                {:scheduled, {root_module, root_target}, run.external_id, execution_external_id,
                 execute_at}
              )
              |> notify_listeners(
                {:queue, ws_ext_id},
                {:scheduled, execution_external_id, module, target_name, run.external_id,
                 step_number, attempt, execute_after, created_at,
                 pending_dependency_external_ids(state.db, pending_dependencies), requires}
              )

            send(self(), :tick)

            state
          else
            state
          end

        state =
          state
          |> notify_listeners(
            {:targets, ws_ext_id},
            {:step, module, target_name, type, run.external_id, step_number, attempt}
          )
          |> flush_notifications()

        # Return extended metadata for log references
        execution_metadata = %{
          run_id: run.external_id,
          step_id: "#{run.external_id}:#{step_number}",
          step_number: step_number,
          attempt: attempt,
          module: module,
          target: target_name
        }

        {:reply, {:ok, run.external_id, step_number, execution_external_id, execution_metadata},
         state}
    end
  end

  def handle_call({:register_group, parent_external_id, group_id, name}, _from, state) do
    parent_id = Map.fetch!(state.execution_ids, parent_external_id)
    {:ok, {run_external_id}} = Runs.get_external_run_id_for_execution(state.db, parent_id)

    case Runs.create_group(state.db, parent_id, group_id, name) do
      :ok ->
        state =
          state
          |> notify_listeners(
            {:run, run_external_id},
            {:group, parent_external_id, group_id, name}
          )
          |> flush_notifications()

        {:reply, :ok, state}
    end
  end

  def handle_call({:rerun_step, step_id, workspace_external_id, access}, _from, state) do
    with {:ok, run_external_id, step_number} <- parse_step_id(step_id),
         {:ok, workspace_id, _} <-
           require_workspace(state, workspace_external_id, access),
         {:ok, run} when not is_nil(run) <-
           ensure_run_in_active_epoch(state, run_external_id),
         {:ok, step} when not is_nil(step) <-
           Runs.get_step_by_number(state.db, run.id, step_number) do
      base_execution_id =
        if step.parent_id do
          step.parent_id
        else
          case Runs.get_first_step_execution_id(state.db, step.id) do
            {:ok, execution_id} -> execution_id
          end
        end

      {:ok, base_workspace_id} =
        Runs.get_workspace_id_for_execution(state.db, base_execution_id)

      if base_workspace_id == workspace_id ||
           is_workspace_ancestor?(state, base_workspace_id, workspace_id) do
        state = cancel_active_step_executions(state, step.id, workspace_id)

        {:ok, _execution_id, attempt, state} =
          rerun_step(state, step, workspace_id, created_by: access[:principal_id])

        execution_external_id = execution_external_id(run_external_id, step_number, attempt)

        state = flush_notifications(state)
        {:reply, {:ok, execution_external_id, attempt}, state}
      else
        {:reply, {:error, :workspace_invalid}, state}
      end
    else
      {:error, :invalid} -> {:reply, {:error, :invalid}, state}
      {:error, :forbidden} -> {:reply, {:error, :forbidden}, state}
      {:error, :workspace_invalid} -> {:reply, {:error, :workspace_invalid}, state}
      {:ok, nil} -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(
        {:cancel_execution, workspace_external_id, execution_external_id, access},
        _from,
        state
      ) do
    with {:ok, workspace_id, _} <-
           require_workspace(state, workspace_external_id, access),
         {:ok, run_ext_id, step_number, attempt} <-
           parse_execution_external_id(execution_external_id),
         {:ok, {execution_id}} <-
           Runs.get_execution_id(state.db, run_ext_id, step_number, attempt) do
      active_id = resolve_active_execution(state.db, execution_id)

      state = do_cancel_execution(state, active_id, workspace_id)
      state = flush_notifications(state)
      {:reply, :ok, state}
    else
      {:error, :invalid_format} ->
        {:reply, {:error, :not_found}, state}

      {:ok, nil} ->
        {:reply, {:error, :not_found}, state}

      {:error, :workspace_invalid} ->
        {:reply, {:error, :not_found}, state}

      {:error, :forbidden} ->
        {:reply, {:error, :forbidden}, state}

      {:error, :not_found} ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:execution_started, _external_execution_id, _metadata}, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call({:define_metric, external_execution_id, key, definition}, _from, state) do
    state =
      case Map.fetch(state.execution_ids, external_execution_id) do
        {:ok, execution_id} ->
          {:ok, _} = Runs.define_metric(state.db, execution_id, key, definition)

          # Notify the run topic
          case Runs.get_execution_run_id(state.db, execution_id) do
            {:ok, run_id} ->
              case Runs.get_run_by_id(state.db, run_id) do
                {:ok, run} ->
                  notify_listeners(
                    state,
                    {:run, run.external_id},
                    {:metric_defined, external_execution_id, key, definition}
                  )

                _ ->
                  state
              end

            _ ->
              state
          end

        :error ->
          state
      end

    {:reply, :ok, state}
  end

  def handle_call({:record_heartbeats, executions, external_session_id}, _from, state) do
    # TODO: handle execution statuses?
    case Map.fetch(state.session_ids, external_session_id) do
      {:ok, session_id} ->
        session = Map.fetch!(state.sessions, session_id)

        # executions is a map of external_id -> status
        # starting/executing now hold external IDs
        reported_ext_ids = executions |> Map.keys() |> MapSet.new()

        # Move from starting to executing: executions that were starting and are now reported
        state =
          session.starting
          |> MapSet.intersection(reported_ext_ids)
          |> Enum.reduce(state, fn ext_id, state ->
            update_in(state.sessions[session_id].starting, &MapSet.delete(&1, ext_id))
          end)

        # Abandon executions that were executing but are no longer reported
        state =
          session.executing
          |> MapSet.difference(reported_ext_ids)
          |> Enum.reduce(state, fn ext_id, state ->
            execution_id = Map.fetch!(state.execution_ids, ext_id)

            case Results.has_result?(state.db, execution_id) do
              {:ok, false} ->
                {:ok, state} = process_result(state, execution_id, :abandoned)
                state

              {:ok, true} ->
                state
            end
          end)

        # Check for executions reported but not in starting/executing - abort if already have result
        state =
          reported_ext_ids
          |> MapSet.difference(session.starting)
          |> MapSet.difference(session.executing)
          |> Enum.reduce(state, fn ext_id, state ->
            case Map.fetch(state.execution_ids, ext_id) do
              {:ok, execution_id} ->
                case Results.has_result?(state.db, execution_id) do
                  {:ok, false} ->
                    state

                  {:ok, true} ->
                    send_session(state, session_id, {:abort, ext_id})
                end

              :error ->
                state
            end
          end)

        # Record heartbeats using internal IDs for DB
        resolved_executions =
          executions
          |> Enum.flat_map(fn {ext_id, v} ->
            case Map.fetch(state.execution_ids, ext_id) do
              {:ok, int_id} -> [{int_id, v}]
              :error -> []
            end
          end)
          |> Map.new()

        state =
          case Runs.record_hearbeats(state.db, resolved_executions) do
            {:ok, _created_at} ->
              put_in(state.sessions[session_id].executing, reported_ext_ids)
          end

        session = Map.fetch!(state.sessions, session_id)

        state =
          state
          |> notify_listeners(
            {:sessions, workspace_external_id(state, session.workspace_id)},
            {:executing, session.external_id,
             session.starting |> MapSet.union(session.executing) |> Enum.count()}
          )
          |> flush_notifications()

        {:reply, :ok, state}

      :error ->
        {:reply, {:error, :session_invalid}, state}
    end
  end

  def handle_call({:notify_terminated, external_execution_ids}, _from, state) do
    now = System.os_time(:millisecond)

    state =
      external_execution_ids
      |> Enum.reduce(state, fn ext_id, state ->
        # If execution has no result recorded, mark it as abandoned
        state =
          case Map.fetch(state.execution_ids, ext_id) do
            {:ok, execution_id} ->
              case Results.has_result?(state.db, execution_id) do
                {:ok, false} ->
                  {:ok, state} = process_result(state, execution_id, :abandoned)
                  state

                {:ok, true} ->
                  state
              end

            :error ->
              state
          end

        # Remove from execution_ids cache
        state = Map.update!(state, :execution_ids, &Map.delete(&1, ext_id))

        # Remove from session's starting/executing (using external IDs directly)
        case find_session_for_execution(state, ext_id) do
          {:ok, session_id} ->
            state =
              update_in(state.sessions[session_id], fn session ->
                starting = MapSet.delete(session.starting, ext_id)
                executing = MapSet.delete(session.executing, ext_id)

                last_idle_at =
                  if Enum.empty?(starting) && Enum.empty?(executing),
                    do: now,
                    else: session.last_idle_at

                Map.merge(session, %{
                  starting: starting,
                  executing: executing,
                  last_idle_at: last_idle_at
                })
              end)

            session = Map.fetch!(state.sessions, session_id)
            executing = session.starting |> MapSet.union(session.executing) |> Enum.count()

            notify_listeners(
              state,
              {:sessions, workspace_external_id(state, session.workspace_id)},
              {:executing, session.external_id, executing}
            )

          :error ->
            state
        end
      end)
      |> flush_notifications()

    send(self(), :tick)

    {:reply, :ok, state}
  end

  def handle_call({:record_result, execution_external_id, result}, _from, state) do
    case Map.fetch(state.execution_ids, execution_external_id) do
      {:ok, execution_id} ->
        case process_result(state, execution_id, result) do
          {:ok, state} ->
            state = flush_notifications(state)
            {:reply, :ok, state}
        end

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(
        {:get_result, execution_external_id, from_execution_external_id, timeout_ms, suspend,
         request_id},
        _from,
        state
      ) do
    # Resolve external execution ID to internal
    execution_id =
      case resolve_internal_execution_id(state, execution_external_id) do
        {:ok, id} -> {:ok, id}
        {:error, :not_found} -> :error
      end

    case execution_id do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, execution_id} ->
        # Resolve from_execution_id from external to internal only for DB calls
        from_execution_id =
          if from_execution_external_id do
            case resolve_internal_execution_id(state, from_execution_external_id) do
              {:ok, id} -> id
              {:error, :not_found} -> nil
            end
          end

        # TODO: check execution_id exists? (call resolve_result first?)
        # TODO: require from_execution_id to be set if timeout_ms is specified?

        state =
          if from_execution_id do
            {:ok, dep_ref_id} = Runs.create_execution_ref_for(state.db, execution_id)
            {:ok, id} = Runs.record_result_dependency(state.db, from_execution_id, dep_ref_id)

            if id do
              {:ok, {run_external_id}} =
                Runs.get_external_run_id_for_execution(state.db, from_execution_id)

              # TODO: only resolve if there are listeners to notify
              {dep_ext_id, _module, _target} =
                dependency =
                resolve_execution_ref(state.db, dep_ref_id)

              notify_listeners(
                state,
                {:run, run_external_id},
                {:result_dependency, from_execution_external_id, dep_ext_id, dependency}
              )
            else
              state
            end
          else
            state
          end

        {result, state} =
          case resolve_result(state.db, execution_id) do
            {:pending, pending_execution_id} ->
              cond do
                timeout_ms == 0 && suspend ->
                  # Immediate suspend
                  {:ok, state} =
                    process_result(
                      state,
                      from_execution_id,
                      {:suspended, nil, [{:execution, pending_execution_id}]}
                    )

                  {{:ok, :suspended}, state}

                timeout_ms == 0 ->
                  # Immediate poll: no result available
                  {{:ok, nil}, state}

                true ->
                  # Wait for result (with or without timeout)
                  now = System.monotonic_time(:millisecond)
                  expire_at = if timeout_ms, do: now + timeout_ms

                  # Use the external ID of the *resolved* pending execution as the waiting key.
                  # This is critical for spawned chains: resolve_result follows
                  # initial→spawned and returns the spawned execution's ID, which is
                  # the one that will eventually complete and trigger notify_waiting.
                  pending_ext_id =
                    case Runs.get_execution_key(state.db, pending_execution_id) do
                      {:ok, {r, s, a}} -> execution_external_id(r, s, a)
                    end

                  state =
                    update_in(
                      state,
                      [Access.key(:waiting), Access.key(pending_ext_id, [])],
                      &[{from_execution_external_id, request_id, expire_at, suspend} | &1]
                    )

                  state =
                    if timeout_ms do
                      reschedule_expire_waiters(state)
                    else
                      state
                    end

                  {:wait, state}
              end

            {:ok, result} ->
              # Only enrich value results with resolved references (asset metadata, execution metadata)
              # Other result types (error, abandoned, etc.) don't need enrichment for the client
              result =
                case result do
                  {:value, value} -> {:value, build_value(value, state.db)}
                  other -> other
                end

              {{:ok, result}, state}
          end

        state = flush_notifications(state)

        {:reply, result, state}
    end
  end

  def handle_call({:put_asset, execution_external_id, name, entries}, _from, state) do
    execution_id = Map.fetch!(state.execution_ids, execution_external_id)
    {:ok, {run_external_id}} = Runs.get_external_run_id_for_execution(state.db, execution_id)

    {:ok, asset_id, external_id, asset_name, total_count, total_size, entry} =
      Assets.get_or_create_asset(state.db, name, entries)

    :ok = Results.put_execution_asset(state.db, execution_id, asset_id)

    state =
      state
      |> notify_listeners(
        {:run, run_external_id},
        {:asset, execution_external_id, external_id, {asset_name, total_count, total_size, entry}}
      )
      |> flush_notifications()

    asset_metadata = %{
      name: asset_name,
      total_count: total_count,
      total_size: total_size
    }

    {:reply, {:ok, external_id, asset_metadata}, state}
  end

  def handle_call({:get_asset, asset_external_id, from_execution_external_id}, _from, state) do
    case Assets.get_asset_by_external_id(state.db, asset_external_id) do
      {:ok, asset_id, name, entries} ->
        state =
          if from_execution_external_id do
            from_execution_id = Map.fetch!(state.execution_ids, from_execution_external_id)
            {:ok, _} = Runs.record_asset_dependency(state.db, from_execution_id, asset_id)

            {:ok, {run_external_id}} =
              Runs.get_external_run_id_for_execution(state.db, from_execution_id)

            {^asset_external_id, asset_name, total_count, total_size, entry} =
              resolve_asset(state.db, asset_id)

            notify_listeners(
              state,
              {:run, run_external_id},
              {:asset_dependency, from_execution_external_id, asset_external_id,
               {asset_name, total_count, total_size, entry}}
            )
          else
            state
          end

        state = flush_notifications(state)
        {:reply, {:ok, name, entries}, state}

      {:error, :not_found} ->
        {:reply, {:error, :not_found}, state}
    end
  end

  # --- Input management ---

  def handle_call(
        {:submit_input, execution_external_id, template, placeholders, schema_json, key, title,
         actions, initial},
        _from,
        state
      ) do
    execution_id = Map.fetch!(state.execution_ids, execution_external_id)
    {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)
    {:ok, run_id} = Runs.get_run_id_for_execution(state.db, execution_id)

    now = System.system_time(:millisecond)

    # Validate and get-or-create schema
    schema_id =
      if schema_json do
        case Coflux.JsonSchema.validate_schema(schema_json) do
          :ok ->
            {:ok, id} = Inputs.get_or_create_schema(state.db, schema_json)
            id

          {:error, reason} ->
            throw({:reply, {:error, {:invalid_schema, reason}}, state})
        end
      end

    # Validate initial value
    if initial do
      if schema_json do
        initial_value = Jason.decode!(initial)

        case Coflux.JsonSchema.validate_partial(initial_value, schema_json) do
          :ok -> :ok
          {:error, reason} -> throw({:reply, {:error, {:invalid_initial, reason}}, state})
        end
      else
        throw({:reply, {:error, {:invalid_initial, "initial value requires a schema"}}, state})
      end
    end

    # Get-or-create placeholder values
    placeholder_value_ids =
      Enum.map(placeholders, fn {placeholder, value} ->
        {:ok, value_id} = Values.get_or_create_value(state.db, normalize_value(value))
        {placeholder, value_id}
      end)

    # Get-or-create prompt
    {:ok, prompt_id} = Inputs.get_or_create_prompt(state.db, template, placeholder_value_ids)

    # Check for existing input by key (run-scoped, workspace-ancestry-aware)
    {_input_id, input_external_id, created} =
      if key do
        workspace_ids = get_cache_workspace_ids(state, workspace_id)

        case Inputs.find_input_by_key(state.db, run_id, workspace_ids, key) do
          {:ok,
           {existing_id, existing_ext_id, existing_prompt_id, existing_schema_id, _existing_title,
            _existing_actions}} ->
            if existing_prompt_id == prompt_id && existing_schema_id == schema_id do
              {existing_id, existing_ext_id, false}
            else
              throw({:reply, {:error, :input_mismatch}, state})
            end

          {:ok, nil} ->
            {:ok, id, ext_id} =
              Inputs.create_input(
                state.db,
                execution_id,
                workspace_id,
                prompt_id,
                schema_id,
                key,
                title,
                actions,
                initial,
                now
              )

            {id, ext_id, true}
        end
      else
        {:ok, id, ext_id} =
          Inputs.create_input(
            state.db,
            execution_id,
            workspace_id,
            prompt_id,
            schema_id,
            nil,
            title,
            actions,
            initial,
            now
          )

        {id, ext_id, true}
      end

    state =
      if created do
        {:ok, {run_external_id}} =
          Runs.get_external_run_id_for_execution(state.db, execution_id)

        notify_listeners(
          state,
          {:run, run_external_id},
          {:input_submitted, execution_external_id, input_external_id, title}
        )
      else
        state
      end

    state = flush_notifications(state)
    {:reply, {:ok, input_external_id}, state}
  catch
    {:reply, error, state} ->
      {:reply, error, state}
  end

  def handle_call(
        {:resolve_input, input_external_id, from_execution_external_id, timeout_ms, suspend,
         request_id},
        _from,
        state
      ) do
    case find_input_in_active_or_archives(state, input_external_id) do
      {:ok, nil} ->
        {:reply, {:error, :not_found}, state}

      {:ok,
       {input_id, _execution_id, workspace_id, _key, _prompt_id, _schema_id, title, _actions,
        _initial, created_at}} ->
        from_execution_id =
          case resolve_internal_execution_id(state, from_execution_external_id) do
            {:ok, id} -> id
            {:error, :not_found} -> nil
          end

        now = System.system_time(:millisecond)

        # Record dependency and notify
        state =
          if from_execution_id do
            {:ok, is_new} =
              Inputs.record_input_dependency(state.db, from_execution_id, input_id, now)

            if is_new do
              {:ok, {run_external_id}} =
                Runs.get_external_run_id_for_execution(state.db, from_execution_id)

              response_type =
                case Inputs.get_input_response(state.db, input_id) do
                  {:ok, nil} -> nil
                  {:ok, {:value, _, _, _}} -> :value
                  {:ok, {:dismissed, _, _}} -> :dismissed
                end

              ws_ext_id = workspace_external_id(state, workspace_id)
              {:ok, {_, s_num, att}} = Runs.get_execution_key(state.db, from_execution_id)
              dep_execution_ext_id = "#{run_external_id}:#{s_num}:#{att}"

              state
              |> notify_listeners(
                {:run, run_external_id},
                {:input_dependency, from_execution_external_id, input_external_id, title,
                 response_type}
              )
              |> notify_listeners(
                {:inputs, ws_ext_id},
                {:input_dependency_active, input_external_id, dep_execution_ext_id,
                 run_external_id, created_at, title}
              )
              |> notify_listeners(
                {:input, input_external_id},
                {:active, true}
              )
            else
              state
            end
          else
            state
          end

        # Check for existing response
        case Inputs.get_input_response(state.db, input_id) do
          {:ok, nil} ->
            # No response yet — wait or suspend
            cond do
              timeout_ms == 0 && suspend ->
                {:ok, state} =
                  process_result(
                    state,
                    from_execution_id,
                    {:suspended, nil, [{:input, input_id}]}
                  )

                state = flush_notifications(state)
                {:reply, {:ok, :suspended}, state}

              timeout_ms == 0 ->
                state = flush_notifications(state)
                {:reply, {:ok, nil}, state}

              true ->
                monotonic_now = System.monotonic_time(:millisecond)
                expire_at = if timeout_ms, do: monotonic_now + timeout_ms

                waiting_key = {:input, input_external_id}

                state =
                  update_in(
                    state,
                    [Access.key(:waiting), Access.key(waiting_key, [])],
                    &[{from_execution_external_id, request_id, expire_at, suspend} | &1]
                  )

                state =
                  if timeout_ms do
                    reschedule_expire_waiters(state)
                  else
                    state
                  end

                state = flush_notifications(state)
                {:reply, :wait, state}
            end

          {:ok, {:value, value, _created_at, _created_by}} ->
            state = flush_notifications(state)
            {:reply, {:ok, {:value, value}}, state}

          {:ok, {:dismissed, _created_at, _created_by}} ->
            state = flush_notifications(state)
            {:reply, {:ok, :dismissed}, state}
        end
    end
  end

  def handle_call({:respond_input, input_external_id, value, access}, _from, state) do
    case find_input_in_active_or_archives(state, input_external_id) do
      {:ok, nil} ->
        {:reply, {:error, :not_found}, state}

      {:ok,
       {input_id, execution_id, workspace_id, _key, _prompt_id, schema_id, _title, _actions,
        _initial, _created_at}} ->
        # Validate response against schema if one exists
        if schema_id do
          {:ok, schema_json} = Inputs.get_input_schema(state.db, schema_id)

          case Coflux.JsonSchema.validate_value(value, schema_json) do
            :ok -> :ok
            {:error, reason} -> throw({:reply, {:error, {:validation_failed, reason}}, state})
          end
        end

        now = System.system_time(:millisecond)

        created_by =
          case access do
            %{principal_id: id} -> id
            _ -> nil
          end

        value_json = Jason.encode!(value)

        case Inputs.record_input_response(
               state.db,
               input_id,
               Inputs.type_value(),
               value_json,
               now,
               created_by
             ) do
          {:ok, true} ->
            # Notify waiters
            waiting_key = {:input, input_external_id}
            {input_waiting, waiting} = Map.pop(state.waiting, waiting_key, [])
            state = Map.put(state, :waiting, waiting)

            state =
              Enum.reduce(input_waiting, state, fn {from_ext_id, request_id, _, _}, state ->
                case find_session_for_execution(state, from_ext_id) do
                  {:ok, session_id} ->
                    send_session(state, session_id, {:response, request_id, {:value, value}})

                  :error ->
                    state
                end
              end)

            # Resolve input dependency for any suspended executions waiting on this input
            state = update_dependencies_on_input(state, input_id)

            # Notify run topic
            {:ok, {run_external_id}} =
              Runs.get_external_run_id_for_execution(state.db, execution_id)

            ws_ext_id = workspace_external_id(state, workspace_id)

            response = build_input_response(state.db, input_id)

            state =
              state
              |> notify_listeners(
                {:run, run_external_id},
                {:input_response, input_external_id, :value}
              )
              |> notify_dependent_runs(input_id, input_external_id, :value, run_external_id)
              |> notify_listeners(
                {:inputs, ws_ext_id},
                {:input_responded, input_external_id, now}
              )
              |> notify_listeners(
                {:input, input_external_id},
                {:response, response}
              )
              |> flush_notifications()

            {:reply, :ok, state}

          {:error, :already_responded} ->
            {:reply, {:error, :already_responded}, state}
        end
    end
  catch
    {:reply, error, state} ->
      {:reply, error, state}
  end

  def handle_call({:get_input, input_external_id}, _from, state) do
    case find_input_in_active_or_archives(state, input_external_id) do
      {:ok, nil} ->
        {:reply, {:error, :not_found}, state}

      {:ok,
       {input_id, _execution_id, _workspace_id, key, prompt_id, schema_id, title, actions,
        initial, created_at}} ->
        {:ok, template, placeholder_values} = Inputs.get_input_prompt(state.db, prompt_id)

        schema =
          if schema_id do
            {:ok, s} = Inputs.get_input_schema(state.db, schema_id)
            s
          end

        response = build_input_response(state.db, input_id)

        parsed_actions =
          if actions do
            Jason.decode!(actions)
          end

        resolved_placeholders =
          Map.new(placeholder_values, fn {k, v} -> {k, build_value(v, state.db)} end)

        {:reply,
         {:ok,
          %{
            key: key,
            template: template,
            placeholders: resolved_placeholders,
            schema: schema,
            initial: initial,
            title: title,
            actions: parsed_actions,
            created_at: created_at,
            response: response
          }}, state}
    end
  end

  def handle_call({:subscribe_input, input_external_id, pid}, _from, state) do
    case find_input_in_active_or_archives(state, input_external_id) do
      {:ok, nil} ->
        {:reply, {:error, :not_found}, state}

      {:ok,
       {input_id, _execution_id, _workspace_id, key, prompt_id, schema_id, title, actions,
        initial, created_at}} ->
        {:ok, ref, state} = add_listener(state, {:input, input_external_id}, pid)

        {:ok, template, placeholder_values} = Inputs.get_input_prompt(state.db, prompt_id)

        schema =
          if schema_id do
            {:ok, s} = Inputs.get_input_schema(state.db, schema_id)
            s
          end

        response = build_input_response(state.db, input_id)

        parsed_actions =
          if actions do
            Jason.decode!(actions)
          end

        resolved_placeholders =
          Map.new(placeholder_values, fn {k, v} -> {k, build_value(v, state.db)} end)

        active = Inputs.has_active_dependency?(state.db, input_id)

        {:reply,
         {:ok,
          %{
            key: key,
            template: template,
            placeholders: resolved_placeholders,
            schema: schema,
            initial: initial,
            title: title,
            actions: parsed_actions,
            created_at: created_at,
            response: response,
            active: active
          }, ref}, state}
    end
  end

  def handle_call({:subscribe_inputs, workspace_external_id, pid}, _from, state) do
    case resolve_workspace_external_id(state, workspace_external_id) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, workspace_id} ->
        {:ok, ref, state} = add_listener(state, {:inputs, workspace_external_id}, pid)

        {:ok, rows} = Inputs.get_inputs_for_workspace(state.db, workspace_id)

        inputs =
          Map.new(rows, fn {_id, external_id, _execution_id, _workspace_id, _key, _prompt_id,
                            _schema_id, created_at, title, _has_response, _step_id, _run_id,
                            run_external_id, step_number, attempt, _module, _target} ->
            execution_id = "#{run_external_id}:#{step_number}:#{attempt}"

            {external_id,
             %{
               executionId: execution_id,
               runId: run_external_id,
               createdAt: created_at,
               title: title
             }}
          end)

        {:reply, {:ok, inputs, ref}, state}
    end
  end

  def handle_call({:dismiss_input, input_external_id, access}, _from, state) do
    case find_input_in_active_or_archives(state, input_external_id) do
      {:ok, nil} ->
        {:reply, {:error, :not_found}, state}

      {:ok,
       {input_id, execution_id, workspace_id, _key, _prompt_id, _schema_id, _title, _actions,
        _initial, _created_at}} ->
        now = System.system_time(:millisecond)

        created_by =
          case access do
            %{principal_id: id} -> id
            _ -> nil
          end

        case Inputs.record_input_response(
               state.db,
               input_id,
               Inputs.type_dismissed(),
               nil,
               now,
               created_by
             ) do
          {:ok, true} ->
            # Cancel waiters
            waiting_key = {:input, input_external_id}
            {input_waiting, waiting} = Map.pop(state.waiting, waiting_key, [])
            state = Map.put(state, :waiting, waiting)

            state =
              Enum.reduce(input_waiting, state, fn {from_ext_id, request_id, _, _}, state ->
                case find_session_for_execution(state, from_ext_id) do
                  {:ok, session_id} ->
                    send_session(state, session_id, {:response, request_id, :dismissed})

                  :error ->
                    state
                end
              end)

            # Resolve input dependency for any suspended executions waiting on this input
            state = update_dependencies_on_input(state, input_id)

            # Notify topics
            {:ok, {run_external_id}} =
              Runs.get_external_run_id_for_execution(state.db, execution_id)

            ws_ext_id = workspace_external_id(state, workspace_id)

            response = build_input_response(state.db, input_id)

            state =
              state
              |> notify_listeners(
                {:run, run_external_id},
                {:input_response, input_external_id, :dismissed}
              )
              |> notify_dependent_runs(input_id, input_external_id, :dismissed, run_external_id)
              |> notify_listeners(
                {:inputs, ws_ext_id},
                {:input_responded, input_external_id, now}
              )
              |> notify_listeners(
                {:input, input_external_id},
                {:response, response}
              )
              |> flush_notifications()

            {:reply, :ok, state}

          {:error, :already_responded} ->
            {:reply, {:error, :already_responded}, state}
        end
    end
  end

  def handle_call({:subscribe_workspaces, pid}, _from, state) do
    {:ok, ref, state} = add_listener(state, :workspaces, pid)

    workspaces =
      Map.new(state.workspaces, fn {workspace_id, workspace} ->
        base_external_id =
          if workspace.base_id do
            case Map.fetch(state.workspaces, workspace.base_id) do
              {:ok, base} -> base.external_id
              :error -> nil
            end
          end

        {workspace_id,
         %{
           name: workspace.name,
           external_id: workspace.external_id,
           base_id: workspace.base_id,
           base_external_id: base_external_id,
           state: workspace.state
         }}
      end)

    {:reply, {:ok, workspaces, ref}, state}
  end

  def handle_call({:subscribe_modules, workspace_external_id, pid}, _from, state) do
    case resolve_workspace_external_id(state, workspace_external_id) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, workspace_id} ->
        {:ok, manifests} = Manifests.get_latest_manifests(state.db, workspace_id)

        # Get all active executions (both assigned and unassigned) with their root workflow
        {:ok, active_executions} = Runs.get_active_run_workflows(state.db, workspace_id)

        # Build active_runs: {root_module, root_target} -> %{run_ext_id -> MapSet of execution_ext_ids}
        active_runs =
          Enum.reduce(active_executions, %{}, fn {run_ext_id, root_module, root_target,
                                                  step_number, attempt, _execution_id},
                                                 active_runs ->
            ext_id = execution_external_id(run_ext_id, step_number, attempt)
            key = {root_module, root_target}

            active_runs
            |> Map.put_new(key, %{})
            |> Map.update!(key, fn runs ->
              Map.update(runs, run_ext_id, MapSet.new([ext_id]), &MapSet.put(&1, ext_id))
            end)
          end)

        {:ok, ref, state} =
          add_listener(state, {:modules, workspace_external_id}, pid)

        {:reply, {:ok, manifests, active_runs, ref}, state}
    end
  end

  def handle_call({:subscribe_queue, workspace_external_id, pid}, _from, state) do
    case resolve_workspace_external_id(state, workspace_external_id) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, workspace_id} ->
        {:ok, executions} = Runs.get_queue_executions(state.db, workspace_id)

        # Resolve tag sets, de-duplicating by ID
        tag_sets =
          executions
          |> Enum.flat_map(fn row -> [elem(row, 8), elem(row, 9)] end)
          |> Enum.reject(&is_nil/1)
          |> Enum.uniq()
          |> Map.new(fn tag_set_id ->
            {:ok, tag_set} = TagSets.get_tag_set(state.db, tag_set_id)
            {tag_set_id, tag_set}
          end)

        # Build a map of execution_external_id -> [dependency_external_id]
        # from the in-memory pending_dependencies (which uses internal IDs)
        dependencies =
          build_queue_dependencies(state, workspace_id)

        {:ok, ref, state} = add_listener(state, {:queue, workspace_external_id}, pid)
        {:reply, {:ok, executions, tag_sets, dependencies, ref}, state}
    end
  end

  def handle_call({:subscribe_pools, workspace_external_id, pid}, _from, state) do
    case resolve_workspace_external_id(state, workspace_external_id) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, workspace_id} ->
        # TODO: include non-active pools that contain active workers
        pools = Map.get(state.pools, workspace_id, %{})
        {:ok, ref, state} = add_listener(state, {:pools, workspace_external_id}, pid)
        {:reply, {:ok, pools, ref}, state}
    end
  end

  def handle_call({:subscribe_pool, workspace_external_id, pool_name, pid}, _from, state) do
    case resolve_workspace_external_id(state, workspace_external_id) do
      {:ok, workspace_id} ->
        pool = state.pools |> Map.get(workspace_id, %{}) |> Map.get(pool_name)
        {:ok, pool_workers} = Workers.get_pool_workers(state.db, pool_name)

        if is_nil(pool) and pool_workers == [] do
          {:reply, {:error, :not_found}, state}
        else
          # TODO: include 'active' workers that aren't in this (potentially limited) list

          workers =
            Map.new(
              pool_workers,
              fn {worker_id, worker_external_id, starting_at, started_at, start_error,
                  stopping_at, stopped_at, stop_error, deactivated_at, error, logs,
                  total_executions} ->
                worker = Map.get(state.workers, worker_id)

                session_external_id =
                  if worker && worker.session_id do
                    case Map.fetch(state.sessions, worker.session_id) do
                      {:ok, session} -> session.external_id
                      :error -> nil
                    end
                  end

                # TODO: include pool_id?
                {worker_external_id,
                 %{
                   starting_at: starting_at,
                   started_at: started_at,
                   start_error: start_error,
                   stopping_at: stopping_at,
                   stopped_at: stopped_at,
                   stop_error: stop_error,
                   deactivated_at: deactivated_at,
                   error: error,
                   logs: logs,
                   state: if(worker, do: worker.state),
                   session_external_id: session_external_id,
                   total_executions: total_executions
                 }}
              end
            )

          {:ok, ref, state} = add_listener(state, {:pool, workspace_external_id, pool_name}, pid)
          {:reply, {:ok, pool, workers, ref}, state}
        end

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:subscribe_sessions, workspace_external_id, pid}, _from, state) do
    case resolve_workspace_external_id(state, workspace_external_id) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, workspace_id} ->
        sessions =
          state.sessions
          |> Enum.filter(fn {_, session} ->
            session.workspace_id == workspace_id
          end)
          |> Map.new(fn {_session_id, session} ->
            {session.external_id, build_session_data(state, session)}
          end)

        {:ok, ref, state} = add_listener(state, {:sessions, workspace_external_id}, pid)
        {:reply, {:ok, sessions, ref}, state}
    end
  end

  def handle_call(
        {:subscribe_workflow, module, target_name, workspace_external_id, max_runs, pid},
        _from,
        state
      ) do
    with {:ok, workspace_id} <- resolve_workspace_external_id(state, workspace_external_id),
         {:ok, workflow} <-
           Manifests.get_latest_workflow(state.db, workspace_id, module, target_name),
         {:ok, instruction} <-
           if(workflow && workflow.instruction_id,
             do: Manifests.get_instruction(state.db, workflow.instruction_id),
             else: {:ok, nil}
           ) do
      runs =
        get_target_runs_across_epochs(
          state,
          module,
          target_name,
          :workflow,
          workspace_id,
          max_runs
        )

      if is_nil(workflow) and runs == [] do
        {:reply, {:error, :not_found}, state}
      else
        {:ok, ref, state} =
          add_listener(state, {:workflow, module, target_name, workspace_external_id}, pid)

        {:reply, {:ok, workflow, instruction, runs, ref}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:subscribe_run, external_run_id, pid}, _from, state) do
    case find_and_build_run_data(state, external_run_id) do
      {:ok, run, parent, steps} ->
        {:ok, ref, state} = add_listener(state, {:run, run.external_id}, pid)
        {:reply, {:ok, run, parent, steps, ref}, state}

      :not_found ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:subscribe_targets, workspace_external_id, pid}, _from, state) do
    case resolve_workspace_external_id(state, workspace_external_id) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, workspace_id} ->
        # TODO: indicate which are archived
        {:ok, workflows} = Manifests.get_all_workflows_for_workspace(state.db, workspace_id)

        {:ok, steps} = Runs.get_steps_for_workspace(state.db, workspace_id)

        result =
          Enum.reduce(workflows, %{}, fn {module_name, target_names}, result ->
            Enum.reduce(target_names, result, fn target_name, result ->
              put_in(
                result,
                [Access.key(module_name, %{}), target_name],
                {:workflow, nil}
              )
            end)
          end)

        result =
          Enum.reduce(
            steps,
            result,
            fn {module_name, target_name, target_type, run_external_id, step_number, attempt},
               result ->
              put_in(
                result,
                [Access.key(module_name, %{}), target_name],
                {target_type, {run_external_id, step_number, attempt}}
              )
            end
          )

        {:ok, ref, state} = add_listener(state, {:targets, workspace_external_id}, pid)
        {:reply, {:ok, result, ref}, state}
    end
  end

  def handle_call(:rotate_epoch, _from, state) do
    state = do_rotate_epoch(state)
    {:reply, :ok, state}
  end

  def handle_cast({:unsubscribe, ref}, state) do
    Process.demonitor(ref, [:flush])

    state =
      state
      |> remove_listener(ref)
      |> maybe_schedule_idle_shutdown()

    {:noreply, state}
  end

  def handle_info(:expire_sessions, state) do
    now = System.os_time(:millisecond)

    {expired, remaining} =
      state.session_expiries
      |> Enum.split_with(fn {_, expiry_at} -> expiry_at <= now end)

    state = %{state | session_expiries: Map.new(remaining), expire_sessions_timer: nil}

    state =
      Enum.reduce(expired, state, fn {session_id, _}, state ->
        # Only remove if still disconnected (connected sessions shouldn't be in expiries)
        case Map.fetch(state.sessions, session_id) do
          {:ok, session} when is_nil(session.connection) ->
            remove_session(state, session_id)

          _ ->
            state
        end
      end)

    state =
      state
      |> reschedule_expire_sessions_timer()
      |> flush_notifications()
      |> maybe_schedule_idle_shutdown()

    {:noreply, state}
  end

  def handle_info({:idle_shutdown, ref}, state) do
    case state.idle_timer do
      {_timer, ^ref} ->
        if Enum.empty?(state.sessions) and Enum.empty?(state.listeners) do
          Logger.info("shutting down idle orchestration server for project #{state.project_id}")
          {:stop, :normal, state}
        else
          {:noreply, %{state | idle_timer: nil}}
        end

      _ ->
        # Stale timer message, ignore
        {:noreply, state}
    end
  end

  def handle_info(:check_rotation, state) do
    db_size = Epochs.active_db_size(state.epochs)

    state =
      if db_size >= @rotation_size_threshold_bytes do
        do_rotate_epoch(state)
      else
        state
      end

    Process.send_after(self(), :check_rotation, @rotation_check_interval_ms)
    {:noreply, state}
  end

  def handle_info(:tick, state) do
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
      split_executions(executions, now)

    state =
      executions_defer
      |> Enum.reverse()
      |> Enum.reduce(state, fn {execution_id, defer_id, _run_id, module}, state ->
        case record_and_notify_result(
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
      |> Enum.reduce(%{}, fn tag_set_id, tag_sets ->
        case TagSets.get_tag_set(state.db, tag_set_id) do
          {:ok, tag_set} -> Map.put(tag_sets, tag_set_id, tag_set)
        end
      end)

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

    {state, assigned, unassigned} =
      Enum.reduce(
        executions_due,
        {state, [], []},
        fn
          execution, {state, assigned, unassigned} ->
            # TODO: support caching for other attempts?
            cached_result =
              if execution.attempt == 1 && execution.cache_config_id do
                cache_workspace_ids = get_cache_workspace_ids(state, execution.workspace_id)
                cache = Map.fetch!(cache_configs, execution.cache_config_id)
                recorded_after = if cache.max_age, do: now - cache.max_age, else: 0

                find_cached_execution_across_epochs(
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

              {:ok, state} =
                process_result(state, execution.execution_id, result)

              {state, assigned, unassigned}
            else
              # Skip executions whose dependencies haven't resolved yet
              has_pending = Map.has_key?(state.pending_dependencies, execution.execution_id)

              if has_pending do
                {state, assigned, unassigned}
              else
                requires =
                  effective_requires(
                    tag_sets,
                    execution.run_requires_tag_set_id,
                    execution.requires_tag_set_id
                  )

                if execution.type == :task || !execution.parent_id do
                  case choose_session(state, execution, requires) do
                    nil ->
                      {state, assigned, [execution | unassigned]}

                    session_id ->
                      {:ok, assigned_at} =
                        Runs.assign_execution(state.db, execution.execution_id, session_id)

                      {:ok, arguments} = Runs.get_step_arguments(state.db, execution.step_id)

                      # Enrich arguments with resolved references (asset/execution metadata)
                      enriched_arguments = Enum.map(arguments, &build_value(&1, state.db))

                      workspace_external_id = state.workspaces[execution.workspace_id].external_id

                      execution_external_id =
                        execution_external_id(
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
                        |> send_session(
                          session_id,
                          {:execute, execution_external_id, execution.module, execution.target,
                           enriched_arguments, execution.run_external_id, workspace_external_id,
                           execution.timeout}
                        )

                      # Notify sessions topic of updated total
                      session = Map.fetch!(state.sessions, session_id)

                      state =
                        notify_listeners(
                          state,
                          {:sessions, workspace_external_id},
                          {:executions, session.external_id, session.total_executions}
                        )

                      state =
                        if session.worker_id do
                          worker = Map.get(state.workers, session.worker_id)

                          if worker do
                            notify_listeners(
                              state,
                              {:pool, workspace_external_id, worker.pool_name},
                              {:worker_executions, worker.external_id, session.total_executions}
                            )
                          else
                            state
                          end
                        else
                          state
                        end

                      {state, [{execution, assigned_at} | assigned], unassigned}
                  end
                else
                  {:ok, arguments} = Runs.get_step_arguments(state.db, execution.step_id)

                  state =
                    case schedule_run(
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
                           requires: requires
                         ) do
                      {:ok, _external_run_id, _external_step_id, spawned_execution_id, state} ->
                        {:ok, state} =
                          process_result(
                            state,
                            execution.execution_id,
                            {:spawned, spawned_execution_id}
                          )

                        state
                    end

                  {state, assigned, unassigned}
                end
              end
            end
        end
      )

    state =
      assigned
      |> Enum.group_by(fn {execution, _assigned_at} -> execution.run_external_id end)
      |> Enum.reduce(state, fn {run_external_id, executions}, state ->
        assigned_map =
          Map.new(executions, fn {execution, assigned_at} ->
            ext_id =
              execution_external_id(
                execution.run_external_id,
                execution.step_number,
                execution.attempt
              )

            {ext_id, assigned_at}
          end)

        notify_listeners(state, {:run, run_external_id}, {:assigned, assigned_map})
      end)

    # Group by workspace, then by root workflow (for :modules topic notifications)
    assigned_by_workflow =
      assigned
      |> Enum.group_by(fn {execution, _} -> execution.workspace_id end)
      |> Map.new(fn {workspace_id, executions} ->
        {workspace_id,
         executions
         |> Enum.group_by(
           fn {execution, _} ->
             get_run_workflow(state, execution.run_external_id) ||
               raise "run_workflows missing entry for run #{execution.run_external_id}"
           end,
           fn {execution, _} ->
             {execution.run_external_id,
              execution_external_id(
                execution.run_external_id,
                execution.step_number,
                execution.attempt
              )}
           end
         )}
      end)

    state =
      Enum.reduce(assigned_by_workflow, state, fn {workspace_id, workflow_executions}, state ->
        ws_ext_id = workspace_external_id(state, workspace_id)

        all_ext_ids =
          workflow_executions
          |> Map.values()
          |> List.flatten()
          |> Enum.map(&elem(&1, 1))
          |> MapSet.new()

        queue_executions =
          Enum.reduce(assigned, %{}, fn {execution, assigned_at}, acc ->
            ext_id =
              execution_external_id(
                execution.run_external_id,
                execution.step_number,
                execution.attempt
              )

            if MapSet.member?(all_ext_ids, ext_id) do
              Map.put(acc, ext_id, assigned_at)
            else
              acc
            end
          end)

        state
        |> notify_listeners(
          {:modules, ws_ext_id},
          {:assigned, workflow_executions}
        )
        |> notify_listeners(
          {:queue, ws_ext_id},
          {:assigned, queue_executions}
        )
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
              effective_requires(
                tag_sets,
                execution.run_requires_tag_set_id,
                execution.requires_tag_set_id
              )

            choose_pool(state, execution, requires)
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
                |> schedule_session_expiry(session_id, activation_timeout)
                |> maybe_schedule_idle_shutdown()
                |> call_launcher(
                  pool.launcher,
                  :launch,
                  [
                    build_launcher_env(state, workspace_id, token, pool.launcher),
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
                      |> notify_listeners(
                        {:pool, workspace_external_id(state, workspace_id), pool_name},
                        {:launch_result, worker_external_id, started_at, error}
                      )

                    state =
                      if error do
                        deactivate_worker(state, worker_id, error)
                      else
                        state
                      end

                    flush_notifications(state)
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
                |> notify_listeners(
                  {:pool, workspace_external_id(state, workspace_id), pool_name},
                  {:worker, worker_id, worker_external_id, created_at, external_id}
                )
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
        |> call_launcher(launcher, :poll, [worker.data], fn state, result ->
          case result do
            {:ok, {:ok, true}} ->
              state

            {:ok, {:ok, false, error, logs}} ->
              deactivate_worker(state, worker_id, error, logs)

            {:ok, {:error, _reason}} ->
              deactivate_worker(state, worker_id, "poll_error")

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
        update_worker_state(state, worker_id, :draining, worker.workspace_id, worker.pool_name)
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
          |> notify_listeners(
            {:pool, workspace_external_id(state, worker.workspace_id), worker.pool_name},
            {:worker_stopping, worker.external_id, stopping_at}
          )

        call_launcher(state, launcher, :stop, [worker.data], fn state, result ->
          case result do
            {:ok, :ok} ->
              {:ok, stopped_at} =
                Workers.create_worker_stop_result(state.db, worker_stop_id, nil)

              state
              |> notify_listeners(
                {:pool, workspace_external_id(state, worker.workspace_id), worker.pool_name},
                {:worker_stop_result, worker.external_id, stopped_at, nil}
              )

            {:ok, {:error, _reason}} ->
              # Stop failed (e.g. connection refused) — treat as stopped
              {:ok, stopped_at} =
                Workers.create_worker_stop_result(state.db, worker_stop_id, nil)

              state
              |> notify_listeners(
                {:pool, workspace_external_id(state, worker.workspace_id), worker.pool_name},
                {:worker_stop_result, worker.external_id, stopped_at, nil}
              )

            :error ->
              # TODO: get error details
              error = %{}

              {:ok, _} =
                Workers.create_worker_stop_result(state.db, worker_stop_id, error)

              state =
                notify_listeners(
                  state,
                  {:pool, workspace_external_id(state, worker.workspace_id), worker.pool_name},
                  {:worker_stop_result, worker.external_id, nil, error}
                )

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

    state = flush_notifications(state)

    {:noreply, state}
  end

  def handle_info(:expire_waiters, state) do
    now = System.monotonic_time(:millisecond)

    # waiting map now uses external IDs
    # Collect expired entries, grouped by from_ext_id with their suspend flag
    {to_suspend, to_notify_not_ready} =
      Enum.reduce(
        state.waiting,
        {%{}, []},
        fn {execution_ext_id, execution_waiting}, {to_suspend, to_notify} ->
          Enum.reduce(
            execution_waiting,
            {to_suspend, to_notify},
            fn {from_ext_id, request_id, expire_at, suspend_flag}, {to_suspend, to_notify} ->
              if expire_at && expire_at <= now do
                if suspend_flag do
                  {Map.update(
                     to_suspend,
                     from_ext_id,
                     [execution_ext_id],
                     &[execution_ext_id | &1]
                   ), to_notify}
                else
                  {to_suspend, [{from_ext_id, request_id} | to_notify]}
                end
              else
                {to_suspend, to_notify}
              end
            end
          )
        end
      )

    # Remove only non-suspend expired entries from waiting map.
    # Suspend entries are left for cleanup_execution (inside abort_execution)
    # to find and respond to.
    state =
      update_in(state, [Access.key(:waiting)], fn waiting ->
        waiting
        |> Enum.map(fn {ext_id, entries} ->
          remaining =
            Enum.reject(entries, fn {_, _, expire_at, suspend_flag} ->
              expire_at && expire_at <= now && !suspend_flag
            end)

          {ext_id, remaining}
        end)
        |> Enum.reject(fn {_, entries} -> entries == [] end)
        |> Map.new()
      end)

    # Handle poll (non-suspend) timeouts: send nil result (no result available)
    state =
      Enum.reduce(
        to_notify_not_ready,
        state,
        fn {from_ext_id, request_id}, state ->
          case find_session_for_execution(state, from_ext_id) do
            {:ok, session_id} ->
              send_session(state, session_id, {:result, request_id, nil})

            :error ->
              state
          end
        end
      )

    # Handle suspend timeouts: record suspension and abort
    # (abort_execution → cleanup_execution sends the :suspended response
    # for any pending requests found in the waiting map)
    state =
      Enum.reduce(
        to_suspend,
        state,
        fn {from_ext_id, dependency_ext_ids}, state ->
          from_execution_id = Map.fetch!(state.execution_ids, from_ext_id)

          dependency_keys =
            Enum.flat_map(dependency_ext_ids, fn
              {:input, input_ext_id} ->
                case Inputs.get_input_id(state.db, input_ext_id) do
                  {:ok, id} -> [{:input, id}]
                  {:error, :not_found} -> []
                end

              dep_ext_id ->
                case resolve_internal_execution_id(state, dep_ext_id) do
                  {:ok, id} -> [{:execution, id}]
                  {:error, :not_found} -> []
                end
            end)

          {:ok, state} =
            process_result(state, from_execution_id, {:suspended, nil, dependency_keys})

          state
        end
      )

    state =
      state
      |> reschedule_expire_waiters()
      |> flush_notifications()

    {:noreply, state}
  end

  def handle_info(
        {task_ref, {run_bloom, cache_bloom, idempotency_bloom, input_ext_id_bloom}},
        state
      )
      when task_ref == state.index_task do
    Process.demonitor(task_ref, [:flush])
    [epoch_id | rest] = state.index_queue

    epoch_index =
      Index.update_filters(state.epoch_index, epoch_id, %{
        "runs" => run_bloom,
        "cache_keys" => cache_bloom,
        "idempotency_keys" => idempotency_bloom,
        "input_external_ids" => input_ext_id_bloom
      })

    :ok = Index.save(epoch_index)
    epochs = Epochs.promote_to_indexed(state.epochs, epoch_id)

    state =
      %{state | epoch_index: epoch_index, epochs: epochs, index_task: nil, index_queue: rest}

    {:noreply, maybe_start_index_build(state)}
  end

  def handle_info({task_ref, result}, state) when is_map_key(state.launcher_tasks, task_ref) do
    state = process_launcher_result(state, task_ref, {:ok, result})
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    cond do
      Map.has_key?(state.connections, ref) ->
        {{^pid, session_id}, state} = pop_in(state.connections[ref])

        state =
          case Map.fetch(state.sessions, session_id) do
            {:ok, session} ->
              # TODO: (re-)schedule timer when receiving heartbeats?
              state =
                state
                |> update_in(
                  [Access.key(:sessions), session_id],
                  &Map.put(&1, :connection, nil)
                )
                |> schedule_session_expiry(session_id, session.reconnection_timeout)
                |> notify_listeners(
                  {:sessions, workspace_external_id(state, session.workspace_id)},
                  {:connected, session.external_id, false}
                )

              state

            :error ->
              state
          end

        state = flush_notifications(state)

        {:noreply, state}

      Map.has_key?(state.listeners, ref) ->
        state =
          state
          |> remove_listener(ref)
          |> maybe_schedule_idle_shutdown()

        {:noreply, state}

      Map.has_key?(state.launcher_tasks, ref) ->
        state = process_launcher_result(state, ref, :error)
        {:noreply, state}

      ref == state.index_task ->
        # Drop failed epoch from queue; it remains unindexed and will be
        # retried on next server startup (via Index.unindexed_epoch_ids)
        [failed | rest] = state.index_queue
        Logger.warning("index build failed for epoch #{failed} in project #{state.project_id}")
        state = %{state | index_task: nil, index_queue: rest}
        state = maybe_start_index_build(state)
        {:noreply, state}

      true ->
        {:noreply, state}
    end
  end

  def terminate(_reason, state) do
    if state.idle_timer do
      {timer, _ref} = state.idle_timer
      Process.cancel_timer(timer)
    end

    if state.epochs do
      Epochs.close(state.epochs)
    end
  end

  # Private helper functions

  # Follow the spawned result chain to find the currently-active execution.
  # When execution A spawns B (which spawns C, etc.), we need to find the
  # execution that doesn't yet have a result.
  defp resolve_active_execution(db, execution_id) do
    case Results.get_result(db, execution_id) do
      {:ok, {{:spawned, successor_id}, _created_at, _created_by}} ->
        resolve_active_execution(db, successor_id)

      _ ->
        execution_id
    end
  end

  # Cancel a single execution: record :cancelled, abort if assigned, cancel descendants.
  defp do_cancel_execution(state, execution_id, workspace_id) do
    state =
      case record_and_notify_result(state, execution_id, :cancelled, nil) do
        {:ok, state} -> state
        {:error, :already_recorded} -> state
      end

    state =
      case Runs.get_execution_key(state.db, execution_id) do
        {:ok, {r, s, a}} ->
          abort_execution(state, execution_external_id(r, s, a))

        {:error, :not_found} ->
          state
      end

    cancel_descendants(state, execution_id, workspace_id)
  end

  # Cancel all active (unresolved) executions for a step in a workspace.
  defp cancel_active_step_executions(state, step_id, workspace_id) do
    {:ok, active_execution_ids} =
      Runs.get_active_execution_ids_for_step(state.db, step_id, workspace_id)

    Enum.reduce(active_execution_ids, state, fn exec_id, state ->
      do_cancel_execution(state, exec_id, workspace_id)
    end)
  end

  # Cancel all descendant executions of a given execution (excludes the
  # execution itself). Follows both step parent-child links and spawned
  # result chains via the recursive CTE in get_execution_descendants.
  defp cancel_descendants(state, execution_id, workspace_id) do
    {:ok, executions} = Runs.get_execution_descendants(state.db, execution_id)

    executions
    |> Enum.filter(fn {exec_id, _module, _assigned_at, _completed_at, exec_workspace_id} ->
      exec_id != execution_id && exec_workspace_id == workspace_id
    end)
    |> Enum.reduce(state, fn {exec_id, _module, _assigned_at, completed_at, _}, state ->
      if !completed_at do
        do_cancel_execution(state, exec_id, workspace_id)
      else
        state
      end
    end)
  end

  defp require_workspace(state, workspace_external_id, access \\ nil) do
    case Map.fetch(state.workspace_external_ids, workspace_external_id) do
      {:ok, workspace_id} ->
        workspace = Map.fetch!(state.workspaces, workspace_id)

        cond do
          workspace.state == :archived ->
            {:error, :workspace_invalid}

          access != nil and not operator?(access[:workspaces], workspace.name) ->
            {:error, :forbidden}

          true ->
            {:ok, workspace_id, workspace}
        end

      :error ->
        {:error, :workspace_invalid}
    end
  end

  defp resolve_workspace_external_id(state, workspace_external_id) do
    case Map.fetch(state.workspace_external_ids, workspace_external_id) do
      {:ok, workspace_id} -> {:ok, workspace_id}
      :error -> {:error, :workspace_invalid}
    end
  end

  defp resolve_optional_workspace(_state, nil), do: {:ok, nil}

  defp resolve_optional_workspace(state, external_id) do
    case Map.fetch(state.workspace_external_ids, external_id) do
      {:ok, workspace_id} -> {:ok, workspace_id}
      :error -> {:error, %{base_id: "invalid"}}
    end
  end

  defp operator?(:all, _workspace), do: true

  defp operator?(patterns, workspace) do
    Enum.any?(patterns, &workspace_matches?(workspace, &1))
  end

  defp workspace_matches?(_workspace, "*"), do: true
  defp workspace_matches?(workspace, workspace), do: true

  defp workspace_matches?(workspace, pattern) do
    if String.ends_with?(pattern, "/*") do
      String.starts_with?(workspace, String.slice(pattern, 0..-2//1))
    else
      false
    end
  end

  defp check_operator_access(nil, _name), do: :ok

  defp check_operator_access(access, name) do
    if operator?(access[:workspaces], name), do: :ok, else: {:error, :name_restricted}
  end

  defp check_rename_allowed(_access, nil), do: :ok
  defp check_rename_allowed(nil, _name), do: :ok

  defp check_rename_allowed(access, new_name) do
    if operator?(access[:workspaces], new_name), do: :ok, else: {:error, :name_restricted}
  end

  defp verify_session_secret(secret, secret_hash) do
    if Sessions.verify_secret(secret, secret_hash), do: :ok, else: {:error, :session_invalid}
  end

  defp require_workspace_match(workspace_id, expected_workspace_id) do
    if workspace_id == expected_workspace_id, do: :ok, else: {:error, :workspace_mismatch}
  end

  defp is_workspace_ancestor?(state, maybe_ancestor_id, workspace_id) do
    # TODO: avoid cycle?
    workspace = Map.fetch!(state.workspaces, workspace_id)

    cond do
      !workspace.base_id ->
        false

      workspace.base_id == maybe_ancestor_id ->
        true

      true ->
        is_workspace_ancestor?(state, maybe_ancestor_id, workspace.base_id)
    end
  end

  defp get_cache_workspace_ids(state, workspace_id, ids \\ []) do
    workspace = Map.fetch!(state.workspaces, workspace_id)

    if workspace.base_id do
      get_cache_workspace_ids(state, workspace.base_id, [workspace_id | ids])
    else
      [workspace_id | ids]
    end
  end

  defp resolve_worker_external_id(state, worker_external_id) do
    case Map.fetch(state.worker_external_ids, worker_external_id) do
      {:ok, worker_id} -> {:ok, worker_id}
      :error -> {:error, :no_worker}
    end
  end

  defp lookup_worker(state, worker_id, expected_workspace_id) do
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

  defp remove_session(state, session_id) do
    {:ok, _} = Sessions.expire_session(state.db, session_id)
    {session, state} = pop_in(state.sessions[session_id])
    state = Map.update!(state, :session_expiries, &Map.delete(&1, session_id))

    # starting/executing now contain external IDs - resolve to internal for process_result
    state =
      session.executing
      |> MapSet.union(session.starting)
      |> Enum.reduce(state, fn ext_id, state ->
        execution_id = Map.fetch!(state.execution_ids, ext_id)
        {:ok, state} = process_result(state, execution_id, :abandoned)
        state
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
        # waiting keys and from_execution_ids are now external IDs
        waiting
        |> Enum.map(fn {execution_ext_id, execution_waiting} ->
          {execution_ext_id,
           Enum.reject(execution_waiting, fn {from_ext_id, _, _, _} ->
             MapSet.member?(session.starting, from_ext_id) ||
               MapSet.member?(session.executing, from_ext_id)
           end)}
        end)
        |> Enum.reject(fn {_execution_ext_id, execution_waiting} ->
          Enum.empty?(execution_waiting)
        end)
        |> Map.new()
      end)
      |> notify_listeners(
        {:sessions, workspace_external_id(state, session.workspace_id)},
        {:session, session.external_id, nil}
      )

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

  defp split_executions(executions, now) do
    {executions_due, executions_future, executions_defer, _} =
      executions
      |> Enum.reverse()
      |> Enum.reduce(
        {[], [], [], %{}},
        fn execution, {due, future, defer, defer_keys} ->
          defer_key =
            execution.defer_key &&
              {execution.module, execution.target, execution.workspace_id, execution.defer_key}

          defer_id = defer_key && Map.get(defer_keys, defer_key)

          if defer_id do
            {due, future,
             [{execution.execution_id, defer_id, execution.run_id, execution.module} | defer],
             defer_keys}
          else
            defer_keys =
              if defer_key do
                Map.put(defer_keys, defer_key, execution.execution_id)
              else
                defer_keys
              end

            if is_nil(execution.execute_after) || execution.execute_after <= now do
              {[execution | due], future, defer, defer_keys}
            else
              {due, [execution | future], defer, defer_keys}
            end
          end
        end
      )

    {executions_due, executions_future, executions_defer}
  end

  defp schedule_run(state, module, target_name, type, arguments, workspace_id, opts) do
    cache_workspace_ids = get_cache_workspace_ids(state, workspace_id)
    created_by = Keyword.get(opts, :created_by)

    case Runs.schedule_run(
           state.db,
           module,
           target_name,
           type,
           arguments,
           workspace_id,
           cache_workspace_ids,
           Keyword.put(opts, :created_by, created_by)
         ) do
      {:ok,
       %{
         step_id: step_id,
         external_run_id: external_run_id,
         step_number: step_number,
         execution_id: execution_id,
         attempt: attempt,
         created_at: created_at
       }} ->
        delay = Keyword.get(opts, :delay, 0)
        execute_after = if delay > 0, do: created_at + delay
        execute_at = execute_after || created_at

        principal =
          case Principals.get_principal(state.db, created_by) do
            {:ok, {type, external_id}} -> %{type: type, external_id: external_id}
            {:ok, nil} -> nil
          end

        execution_external_id =
          execution_external_id(external_run_id, step_number, attempt)

        ws_ext_id = workspace_external_id(state, workspace_id)

        # Compute and register pending dependencies
        wait_for = Keyword.get(opts, :wait_for) || []
        requires = Keyword.get(opts, :requires) || %{}

        {state, pending_dependencies} =
          if step_id do
            pending_dependencies =
              compute_pending_dependencies(state.db, execution_id, wait_for, step_id)

            state =
              register_pending_dependencies(state, execution_id, pending_dependencies)

            {state, pending_dependencies}
          else
            {state, MapSet.new()}
          end

        state =
          state
          |> put_in([Access.key(:execution_ids), execution_external_id], execution_id)
          |> track_run_execution(external_run_id, execution_id, module, target_name)
          |> notify_listeners(
            {:workflow, module, target_name, ws_ext_id},
            {:run, external_run_id, created_at, principal}
          )
          |> notify_listeners(
            {:modules, ws_ext_id},
            {:scheduled, {module, target_name}, external_run_id, execution_external_id,
             execute_at}
          )
          |> notify_listeners(
            {:queue, ws_ext_id},
            {:scheduled, execution_external_id, module, target_name, external_run_id, step_number,
             attempt, execute_after, created_at,
             pending_dependency_external_ids(state.db, pending_dependencies), requires}
          )
          |> notify_listeners(
            {:targets, ws_ext_id},
            {:step, module, target_name, type, external_run_id, step_number, attempt}
          )

        {:ok, external_run_id, step_number, execution_id, state}
    end
  end

  defp rerun_step(state, step, workspace_id, opts) do
    execute_after = Keyword.get(opts, :execute_after, nil)
    dependency_keys = Keyword.get(opts, :dependency_keys, [])
    created_by = Keyword.get(opts, :created_by)

    # Separate execution and input dependencies
    {exec_deps, input_deps} =
      Enum.reduce(dependency_keys, {[], []}, fn
        {:execution, id}, {execs, inputs} -> {[id | execs], inputs}
        {:input, id}, {execs, inputs} -> {execs, [id | inputs]}
      end)

    # Convert internal dependency execution IDs to execution_ref IDs
    dependency_ref_ids =
      Enum.map(exec_deps, fn dep_id ->
        {:ok, ref_id} = Runs.create_execution_ref_for(state.db, dep_id)
        ref_id
      end)

    # TODO: only get run if needed for notify?
    {:ok, run} = Runs.get_run_by_id(state.db, step.run_id)

    case Runs.rerun_step(
           state.db,
           step.id,
           workspace_id,
           execute_after,
           dependency_ref_ids,
           input_deps,
           created_by
         ) do
      {:ok, execution_id, attempt, created_at} ->
        {run_module, run_target} =
          case get_run_workflow(state, run.external_id) do
            {_, _} = workflow ->
              workflow

            nil ->
              {:ok, workflow} = Runs.get_run_target(state.db, run.id)
              workflow
          end

        state = track_run_execution(state, run.external_id, execution_id, run_module, run_target)

        execute_at = execute_after || created_at

        principal =
          case Principals.get_principal(state.db, created_by) do
            {:ok, {type, external_id}} -> %{type: type, external_id: external_id}
            {:ok, nil} -> nil
          end

        dependencies =
          Map.new(dependency_ref_ids, fn ref_id ->
            {ext_id, _module, _target} = execution = resolve_execution_ref(state.db, ref_id)
            {ext_id, execution}
          end)

        # Compute and register pending dependencies
        pending_dependencies =
          compute_pending_dependencies(state.db, execution_id, step.wait_for || [], step.id)

        state =
          register_pending_dependencies(state, execution_id, pending_dependencies)

        step_requires =
          if step.requires_tag_set_id do
            {:ok, tag_set} = TagSets.get_tag_set(state.db, step.requires_tag_set_id)
            tag_set
          else
            %{}
          end

        run_requires =
          if run.requires_tag_set_id do
            {:ok, tag_set} = TagSets.get_tag_set(state.db, run.requires_tag_set_id)
            tag_set
          else
            %{}
          end

        requires =
          run_requires
          |> Map.merge(step_requires)
          |> Map.reject(fn {_key, values} -> values == [] end)

        execution_external_id =
          execution_external_id(run.external_id, step.number, attempt)

        ws_ext_id = workspace_external_id(state, workspace_id)

        state =
          state
          |> put_in([Access.key(:execution_ids), execution_external_id], execution_id)
          |> notify_listeners(
            {:run, run.external_id},
            {:execution, step.number, attempt, execution_external_id, ws_ext_id, created_at,
             execute_after, dependencies, principal}
          )
          |> notify_listeners(
            {:modules, ws_ext_id},
            {:scheduled, {run_module, run_target}, run.external_id, execution_external_id,
             execute_at}
          )
          |> notify_listeners(
            {:queue, ws_ext_id},
            {:scheduled, execution_external_id, step.module, step.target, run.external_id,
             step.number, attempt, execute_after, created_at,
             pending_dependency_external_ids(state.db, pending_dependencies), requires}
          )
          |> notify_listeners(
            {:targets, ws_ext_id},
            {:step, step.module, step.target, step.type, run.external_id, step.number, attempt}
          )

        # Notify run topic about input dependencies for this execution
        state =
          Enum.reduce(input_deps, state, fn input_id, state ->
            case Inputs.get_input_by_id(state.db, input_id) do
              {:ok,
               {input_ext_id, _key, _prompt_id, _schema_id, input_title, _actions, _created_at}} ->
                response_type =
                  case Inputs.get_input_response(state.db, input_id) do
                    {:ok, nil} -> nil
                    {:ok, {:value, _, _, _}} -> :value
                    {:ok, {:dismissed, _, _}} -> :dismissed
                  end

                notify_listeners(
                  state,
                  {:run, run.external_id},
                  {:input_dependency, execution_external_id, input_ext_id, input_title,
                   response_type}
                )

              _ ->
                state
            end
          end)

        principal =
          case run.created_by do
            %{type: type, external_id: external_id} -> %{type: type, external_id: external_id}
            nil -> nil
          end

        state =
          case step.type do
            :workflow ->
              notify_listeners(
                state,
                {:workflow, run_module, run_target, ws_ext_id},
                {:run, run.external_id, run.created_at, principal}
              )

            _other ->
              state
          end

        send(self(), :tick)

        {:ok, execution_id, attempt, state}
    end
  end

  defp result_retryable?(result) do
    case result do
      {:error, _, _, _, false} -> false
      {:error, _, _, _, _} -> true
      :abandoned -> true
      :timeout -> true
      _ -> false
    end
  end

  defp workspace_external_id(state, workspace_id) do
    state.workspaces[workspace_id].external_id
  end

  defp decode_input_response_type(nil), do: nil
  defp decode_input_response_type(1), do: :value
  defp decode_input_response_type(2), do: :dismissed

  defp build_input_response(db, input_id) do
    case Inputs.get_input_response(db, input_id) do
      {:ok, nil} ->
        nil

      {:ok, {:value, value, created_at, created_by_id}} ->
        principal =
          case Principals.get_principal(db, created_by_id) do
            {:ok, {type, external_id}} -> %{type: type, external_id: external_id}
            {:ok, nil} -> nil
          end

        %{type: :value, value: value, created_at: created_at, created_by: principal}

      {:ok, {:dismissed, created_at, created_by_id}} ->
        principal =
          case Principals.get_principal(db, created_by_id) do
            {:ok, {type, external_id}} -> %{type: type, external_id: external_id}
            {:ok, nil} -> nil
          end

        %{type: :dismissed, value: nil, created_at: created_at, created_by: principal}
    end
  end

  defp do_rotate_epoch(state) do
    epoch_id = Epochs.next_epoch_id(state.epochs)

    # Write placeholder entry to index first (null value)
    epoch_index = Index.add_epoch(state.epoch_index, epoch_id, System.os_time(:millisecond))
    :ok = Index.save(epoch_index)

    # Now rotate
    {:ok, new_epochs, old_db} = Epochs.rotate(state.epochs, epoch_id)
    new_db = Epochs.active_db(new_epochs)

    id_mappings = Epoch.copy_config(old_db, new_db)

    state
    |> Map.put(:epochs, new_epochs)
    |> Map.put(:db, new_db)
    |> Map.put(:epoch_index, epoch_index)
    |> Map.update!(:index_queue, &(&1 ++ [epoch_id]))
    |> remap_config_ids(id_mappings)
    |> copy_in_flight_runs()
    |> Map.put(:pending_dependencies, %{})
    |> Map.put(:dependency_waiters, %{})
    |> initialize_pending_dependencies()
    |> maybe_start_index_build()
  end

  defp copy_in_flight_runs(state) do
    # Collect all external execution IDs that sessions are currently tracking
    in_flight_ext_ids =
      state.sessions
      |> Enum.flat_map(fn {_sid, session} ->
        MapSet.to_list(session.executing) ++ MapSet.to_list(session.starting)
      end)

    # Extract unique run external IDs from the execution references
    run_ext_ids =
      in_flight_ext_ids
      |> Enum.map(fn ext_id ->
        {:ok, run_ext_id, _step, _attempt} = parse_execution_external_id(ext_id)
        run_ext_id
      end)
      |> Enum.uniq()

    # Copy each run from archives into the active epoch
    Enum.each(run_ext_ids, fn run_ext_id ->
      {:ok, _remap} = find_and_copy_run_from_archives(state, run_ext_id)
    end)

    # Repopulate execution_ids cache from the new active DB
    execution_ids =
      Map.new(in_flight_ext_ids, fn ext_id ->
        {:ok, run_ext_id, step_num, attempt} = parse_execution_external_id(ext_id)
        {:ok, {id}} = Runs.get_execution_id(state.db, run_ext_id, step_num, attempt)
        {ext_id, id}
      end)

    %{state | execution_ids: execution_ids}
  end

  defp maybe_start_index_build(%{index_task: nil, index_queue: [epoch_id | _]} = state) do
    path = Epochs.archive_path(state.epochs, epoch_id)

    task =
      Task.Supervisor.async_nolink(Coflux.LauncherSupervisor, fn ->
        {:ok, db} = Exqlite.Sqlite3.open(path)

        try do
          build_blooms_for_epoch(db)
        after
          Exqlite.Sqlite3.close(db)
        end
      end)

    %{state | index_task: task.ref}
  end

  defp maybe_start_index_build(state), do: state

  defp build_blooms_for_epoch(db) do
    {:ok, run_ids} = Runs.get_all_run_external_ids(db)
    runs = Bloom.new(max(100, length(run_ids)))
    runs = Enum.reduce(run_ids, runs, &Bloom.add(&2, &1))

    {:ok, cache_keys_list} = Runs.get_all_cache_keys(db)
    cache_keys = Bloom.new(max(100, length(cache_keys_list)))
    cache_keys = Enum.reduce(cache_keys_list, cache_keys, &Bloom.add(&2, &1))

    {:ok, idemp_keys_list} = Runs.get_all_idempotency_keys(db)
    idempotency_keys = Bloom.new(max(100, length(idemp_keys_list)))
    idempotency_keys = Enum.reduce(idemp_keys_list, idempotency_keys, &Bloom.add(&2, &1))

    {:ok, input_ext_ids} = Inputs.get_all_input_external_ids(db)
    input_external_ids = Bloom.new(max(100, length(input_ext_ids)))
    input_external_ids = Enum.reduce(input_ext_ids, input_external_ids, &Bloom.add(&2, &1))

    {runs, cache_keys, idempotency_keys, input_external_ids}
  end

  defp remap_config_ids(state, %{} = mappings) do
    ws_map = Map.get(mappings, :workspace_ids, %{})
    session_map = Map.get(mappings, :session_ids, %{})
    worker_map = Map.get(mappings, :worker_ids, %{})
    pool_map = Map.get(mappings, :pool_ids, %{})

    # Remap workspaces: rekey map, update base_id values
    workspaces =
      Map.new(state.workspaces, fn {old_id, workspace} ->
        new_id = Map.fetch!(ws_map, old_id)

        new_base_id =
          if workspace.base_id,
            do: Map.fetch!(ws_map, workspace.base_id),
            else: nil

        {new_id, %{workspace | base_id: new_base_id}}
      end)

    # Remap workspace_names: values are workspace IDs
    workspace_names =
      Map.new(state.workspace_names, fn {name, old_id} ->
        {name, Map.fetch!(ws_map, old_id)}
      end)

    # Remap workspace_external_ids: values are workspace IDs
    workspace_external_ids =
      Map.new(state.workspace_external_ids, fn {ext_id, old_id} ->
        {ext_id, Map.fetch!(ws_map, old_id)}
      end)

    # Remap pools: rekey by new workspace_id, pool values contain pool_definition_id etc.
    pools =
      Map.new(state.pools, fn {old_ws_id, ws_pools} ->
        new_ws_id = Map.fetch!(ws_map, old_ws_id)

        new_ws_pools =
          Map.new(ws_pools, fn {pool_name, pool} ->
            new_pool = %{pool | id: Map.fetch!(pool_map, pool.id)}
            {pool_name, new_pool}
          end)

        {new_ws_id, new_ws_pools}
      end)

    # Remap workers: rekey map, update pool_id, workspace_id, session_id
    workers =
      Map.new(state.workers, fn {old_id, worker} ->
        new_id = Map.fetch!(worker_map, old_id)

        new_worker = %{
          worker
          | pool_id: Map.fetch!(pool_map, worker.pool_id),
            workspace_id: Map.fetch!(ws_map, worker.workspace_id),
            session_id:
              if(worker.session_id,
                do: Map.fetch!(session_map, worker.session_id),
                else: nil
              )
        }

        {new_id, new_worker}
      end)

    # Remap worker_external_ids: values are worker IDs
    worker_external_ids =
      Map.new(state.worker_external_ids, fn {ext_id, old_id} ->
        {ext_id, Map.fetch!(worker_map, old_id)}
      end)

    # Remap sessions: rekey map, update workspace_id, worker_id
    sessions =
      Map.new(state.sessions, fn {old_id, session} ->
        new_id = Map.fetch!(session_map, old_id)

        new_session = %{
          session
          | workspace_id: Map.fetch!(ws_map, session.workspace_id),
            worker_id:
              if(session.worker_id,
                do: Map.fetch!(worker_map, session.worker_id),
                else: nil
              )
        }

        {new_id, new_session}
      end)

    # Remap session_ids: values are session IDs
    session_ids =
      Map.new(state.session_ids, fn {ext_id, old_id} ->
        {ext_id, Map.fetch!(session_map, old_id)}
      end)

    # Remap session_expiries: rekey by new session_id
    session_expiries =
      Map.new(state.session_expiries, fn {old_id, expiry} ->
        {Map.fetch!(session_map, old_id), expiry}
      end)

    # Remap connections: values contain session_id
    connections =
      Map.new(state.connections, fn {ref, {pid, old_session_id}} ->
        {ref, {pid, Map.fetch!(session_map, old_session_id)}}
      end)

    # Remap targets: session_ids in MapSets
    targets =
      Map.new(state.targets, fn {module, module_targets} ->
        new_module_targets =
          Map.new(module_targets, fn {target_name, target} ->
            new_session_ids =
              MapSet.new(target.session_ids, fn old_sid ->
                Map.fetch!(session_map, old_sid)
              end)

            {target_name, %{target | session_ids: new_session_ids}}
          end)

        {module, new_module_targets}
      end)

    %{
      state
      | workspaces: workspaces,
        workspace_names: workspace_names,
        workspace_external_ids: workspace_external_ids,
        pools: pools,
        workers: workers,
        worker_external_ids: worker_external_ids,
        sessions: sessions,
        session_ids: session_ids,
        session_expiries: session_expiries,
        connections: connections,
        targets: targets
    }
  end

  defp resolve_internal_execution_id(state, external_id) do
    case Map.fetch(state.execution_ids, external_id) do
      {:ok, id} ->
        {:ok, id}

      :error ->
        with {:ok, run_ext_id, step_num, attempt} <- parse_execution_external_id(external_id) do
          # First check active epoch
          case Runs.get_execution_id(state.db, run_ext_id, step_num, attempt) do
            {:ok, {id}} when not is_nil(id) ->
              {:ok, id}

            _ ->
              # Not in active epoch - search archived epochs and copy forward
              case find_and_copy_run_from_archives(state, run_ext_id) do
                {:ok, _remap} ->
                  # Run was copied to active epoch, now resolve again
                  case Runs.get_execution_id(state.db, run_ext_id, step_num, attempt) do
                    {:ok, {id}} when not is_nil(id) -> {:ok, id}
                    _ -> {:error, :not_found}
                  end

                :not_found ->
                  {:error, :not_found}
              end
          end
        else
          _ -> {:error, :not_found}
        end
    end
  end

  defp get_target_runs_across_epochs(state, module, target_name, type, workspace_id, limit) do
    {:ok, runs} =
      Runs.get_target_runs(state.db, module, target_name, type, workspace_id, limit)

    if length(runs) < limit do
      workspace_external_id = workspace_external_id(state, workspace_id)

      unindexed_map =
        state.epochs
        |> Epochs.unindexed_dbs()
        |> Map.new()

      indexed_epoch_ids =
        Index.indexed_epoch_ids(state.epoch_index, "runs")

      # Merge unindexed + indexed epoch IDs, sort newest first, take depth limit
      all_epoch_ids =
        Map.keys(unindexed_map) ++ indexed_epoch_ids

      archives =
        all_epoch_ids
        |> Enum.sort(:desc)
        |> Enum.take(@target_runs_archive_depth)
        |> Enum.map(fn epoch_id ->
          case Map.fetch(unindexed_map, epoch_id) do
            {:ok, db} -> {:open, db}
            :error -> {:closed, epoch_id}
          end
        end)

      seen = MapSet.new(runs, &elem(&1, 0))

      archives
      |> Enum.reduce_while({runs, seen}, fn archive, {acc, seen} ->
        remaining = limit - length(acc)

        archive_runs =
          query_archive_target_runs(
            state,
            archive,
            module,
            target_name,
            type,
            workspace_external_id,
            remaining
          )

        new_runs = Enum.reject(archive_runs, &MapSet.member?(seen, elem(&1, 0)))
        new_seen = MapSet.union(seen, MapSet.new(new_runs, &elem(&1, 0)))
        combined = acc ++ new_runs

        if length(combined) >= limit do
          {:halt, {Enum.take(combined, limit), new_seen}}
        else
          {:cont, {combined, new_seen}}
        end
      end)
      |> elem(0)
    else
      runs
    end
  end

  defp query_archive_target_runs(
         _state,
         {:open, db},
         module,
         target_name,
         type,
         workspace_external_id,
         limit
       ) do
    do_query_archive_target_runs(db, module, target_name, type, workspace_external_id, limit)
  end

  defp query_archive_target_runs(
         state,
         {:closed, epoch_id},
         module,
         target_name,
         type,
         workspace_external_id,
         limit
       ) do
    path = Epochs.archive_path(state.epochs, epoch_id)

    case Exqlite.Sqlite3.open(path) do
      {:ok, db} ->
        try do
          do_query_archive_target_runs(
            db,
            module,
            target_name,
            type,
            workspace_external_id,
            limit
          )
        after
          Exqlite.Sqlite3.close(db)
        end

      {:error, _} ->
        []
    end
  end

  defp do_query_archive_target_runs(db, module, target_name, type, workspace_external_id, limit) do
    case Workspaces.get_workspace_id(db, workspace_external_id) do
      {:ok, workspace_id} when not is_nil(workspace_id) ->
        {:ok, runs} =
          Runs.get_target_runs(db, module, target_name, type, workspace_id, limit)

        runs

      {:ok, nil} ->
        []
    end
  end

  defp find_and_build_run_data(state, external_run_id) do
    case Runs.get_run_by_external_id(state.db, external_run_id) do
      {:ok, run} when not is_nil(run) ->
        build_run_data(state.db, run)

      {:ok, nil} ->
        query_fn = fn archive_db ->
          case Runs.get_run_by_external_id(archive_db, external_run_id) do
            {:ok, run} when not is_nil(run) ->
              {:found, build_run_data(archive_db, run)}

            {:ok, nil} ->
              :not_found
          end
        end

        bloom_fn = fn epoch_index ->
          Index.find_epochs(epoch_index, "runs", external_run_id)
        end

        case search_archived_epochs(state, query_fn, bloom_fn) do
          {:found, result} -> result
          :not_found -> :not_found
        end
    end
  end

  defp build_run_data(db, run) do
    parent =
      if run.parent_ref_id do
        resolve_execution_ref(db, run.parent_ref_id)
      end

    run_requires =
      if run.requires_tag_set_id do
        case TagSets.get_tag_set(db, run.requires_tag_set_id) do
          {:ok, tag_set} -> tag_set
        end
      else
        %{}
      end

    run = Map.put(run, :requires, run_requires)

    {:ok, steps} = Runs.get_run_steps(db, run.id)
    {:ok, run_executions} = Runs.get_run_executions(db, run.id)
    {:ok, run_dependencies} = Runs.get_run_dependencies(db, run.id)
    {:ok, run_children} = Runs.get_run_children(db, run.id)
    {:ok, groups} = Runs.get_groups_for_run(db, run.id)
    {:ok, run_metric_defs} = Runs.get_run_metric_definitions(db, run.id)
    {:ok, run_input_deps} = Inputs.get_input_dependencies_for_run(db, run.id)
    {:ok, run_submitted_inputs} = Inputs.get_submitted_inputs_for_run(db, run.id)
    {:ok, run_asset_deps} = Runs.get_asset_dependencies_for_run(db, run.id)

    submitted_inputs_by_execution =
      run_submitted_inputs
      |> Enum.group_by(
        fn {execution_id, _ext_id, _title, _response_type} -> execution_id end,
        fn {_execution_id, ext_id, title, response_type} ->
          {ext_id,
           %{
             title: title,
             status: if(response_type, do: decode_input_response_type(response_type))
           }}
        end
      )
      |> Map.new(fn {execution_id, inputs} -> {execution_id, Map.new(inputs)} end)

    input_deps_by_execution =
      run_input_deps
      |> Enum.group_by(
        fn {execution_id, _ext_id, _key, _prompt_id, _title, _created_at, _response_type,
            _response_value, _responded_at, _created_by} ->
          execution_id
        end,
        fn {_execution_id, ext_id, _key, _prompt_id, title, _created_at, response_type,
            _response_value, _responded_at, _response_created_by} ->
          {ext_id,
           {:input, title, if(response_type, do: decode_input_response_type(response_type))}}
        end
      )
      |> Map.new(fn {execution_id, deps} -> {execution_id, Map.new(deps)} end)

    asset_deps_by_execution =
      run_asset_deps
      |> Enum.group_by(
        fn {execution_id, _asset_id} -> execution_id end,
        fn {_execution_id, asset_id} ->
          {external_id, name, total_count, total_size, entry} = resolve_asset(db, asset_id)
          {external_id, {:asset, {name, total_count, total_size, entry}}}
        end
      )
      |> Map.new(fn {execution_id, deps} -> {execution_id, Map.new(deps)} end)

    metric_definitions_by_execution =
      Enum.group_by(
        run_metric_defs,
        fn {execution_id, _, _, _, _, _, _, _, _, _, _} -> execution_id end,
        fn {_, key, group, group_units, group_lower, group_upper, scale, units, progress, lower,
            upper} ->
          {key,
           %{
             group: group,
             group_units: group_units,
             group_lower: group_lower,
             group_upper: group_upper,
             scale: scale,
             units: units,
             progress: progress == 1,
             lower: lower,
             upper: upper
           }}
        end
      )
      |> Map.new(fn {execution_id, defs} -> {execution_id, Map.new(defs)} end)

    cache_configs =
      steps
      |> Enum.map(& &1.cache_config_id)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()
      |> Enum.reduce(%{}, fn cache_config_id, cache_configs ->
        case CacheConfigs.get_cache_config(db, cache_config_id) do
          {:ok, cache_config} -> Map.put(cache_configs, cache_config_id, cache_config)
        end
      end)

    results =
      run_executions
      |> Enum.map(&elem(&1, 0))
      |> Enum.reduce(%{}, fn execution_id, results ->
        {result, completed_at, result_created_by} =
          case Results.get_result(db, execution_id) do
            {:ok, {result, completed_at, created_by}} ->
              result = build_result(result, db)
              {result, completed_at, created_by}

            {:ok, nil} ->
              {nil, nil, nil}
          end

        Map.put(results, execution_id, {result, completed_at, result_created_by})
      end)

    steps =
      Map.new(steps, fn step ->
        {:ok, arguments} = Runs.get_step_arguments(db, step.id)
        arguments = Enum.map(arguments, &build_value(&1, db))

        requires =
          if step.requires_tag_set_id do
            case TagSets.get_tag_set(db, step.requires_tag_set_id) do
              {:ok, requires} -> requires
            end
          else
            %{}
          end

        parent_execution_external_id =
          if step.parent_id do
            {:ok, {r, s, a}} = Runs.get_execution_key(db, step.parent_id)
            execution_external_id(r, s, a)
          end

        {step.number,
         %{
           module: step.module,
           target: step.target,
           type: step.type,
           parent_id: parent_execution_external_id,
           cache_config:
             if(step.cache_config_id, do: Map.fetch!(cache_configs, step.cache_config_id)),
           cache_key: step.cache_key,
           memo_key: step.memo_key,
           retry_limit: step.retry_limit,
           retry_backoff_min: step.retry_backoff_min,
           retry_backoff_max: step.retry_backoff_max,
           recurrent: step.recurrent,
           timeout: step.timeout,
           created_at: step.created_at,
           arguments: arguments,
           requires: requires,
           executions:
             run_executions
             |> Enum.filter(&(elem(&1, 1) == step.id))
             |> Map.new(fn {execution_id, _step_id, attempt, workspace_id, execute_after,
                            created_at, assigned_at, created_by_user_ext_id,
                            created_by_token_ext_id} ->
               execution_created_by =
                 case {created_by_user_ext_id, created_by_token_ext_id} do
                   {nil, nil} -> nil
                   {user_ext_id, nil} -> %{type: "user", external_id: user_ext_id}
                   {nil, token_ext_id} -> %{type: "token", external_id: token_ext_id}
                 end

               {:ok, {r, s, a}} = Runs.get_execution_key(db, execution_id)
               exec_external_id = execution_external_id(r, s, a)

               {:ok, workspace_external_id} =
                 Workspaces.get_workspace_external_id(db, workspace_id)

               {result, completed_at, result_created_by} = Map.fetch!(results, execution_id)

               execution_groups =
                 groups
                 |> Enum.filter(fn {e_id, _, _} -> e_id == execution_id end)
                 |> Map.new(fn {_, group_id, name} -> {group_id, name} end)

               # TODO: load assets in one query
               {:ok, asset_ids} = Results.get_assets_for_execution(db, execution_id)

               assets =
                 asset_ids
                 |> Enum.map(&resolve_asset(db, &1))
                 |> Map.new(fn {external_id, name, total_count, total_size, entry} ->
                   {external_id, {name, total_count, total_size, entry}}
                 end)

               # TODO: batch? get `get_dependencies` to resolve?
               result_deps =
                 run_dependencies
                 |> Map.get(execution_id, [])
                 |> Map.new(fn dependency_ref_id ->
                   {ext_id, _module, _target} =
                     execution =
                     resolve_execution_ref(db, dependency_ref_id)

                   {ext_id, {:result, execution}}
                 end)

               dependencies =
                 Map.merge(
                   result_deps,
                   Map.merge(
                     Map.get(input_deps_by_execution, execution_id, %{}),
                     Map.get(asset_deps_by_execution, execution_id, %{})
                   )
                 )

               {attempt,
                %{
                  execution_id: exec_external_id,
                  workspace_id: workspace_external_id,
                  created_at: created_at,
                  created_by: execution_created_by,
                  execute_after: execute_after,
                  assigned_at: assigned_at,
                  completed_at: completed_at,
                  groups: execution_groups,
                  assets: assets,
                  dependencies: dependencies,
                  inputs: Map.get(submitted_inputs_by_execution, execution_id, %{}),
                  result: result,
                  result_created_by: result_created_by,
                  children: Map.get(run_children, execution_id, []),
                  metric_definitions: Map.get(metric_definitions_by_execution, execution_id, %{})
                }}
             end)
         }}
      end)

    {:ok, run, parent, steps}
  end

  defp ensure_run_in_active_epoch(state, run_external_id) do
    case Runs.get_run_by_external_id(state.db, run_external_id) do
      {:ok, run} when not is_nil(run) ->
        {:ok, run}

      {:ok, nil} ->
        case find_and_copy_run_from_archives(state, run_external_id) do
          {:ok, _remap} ->
            Runs.get_run_by_external_id(state.db, run_external_id)

          :not_found ->
            {:ok, nil}
        end
    end
  end

  defp maybe_find_idempotent_run(_state, nil, _ws_ext_id), do: :miss

  defp maybe_find_idempotent_run(state, client_key, ws_ext_id) do
    hashed_key = Runs.build_idempotency_key(ws_ext_id, client_key)
    created_after = System.os_time(:millisecond) - @idempotency_ttl_ms

    # Tier 0: Check active epoch
    case Runs.find_run_by_idempotency_key(state.db, hashed_key, created_after) do
      {:ok, {ext_run_id, step_number, attempt}} ->
        {:hit, ext_run_id, step_number, attempt}

      {:ok, nil} ->
        # Search archived epochs
        query_fn = fn archive_db ->
          case Runs.find_run_by_idempotency_key(archive_db, hashed_key, created_after) do
            {:ok, {ext_run_id, step_number, attempt}} ->
              {:found, {:hit, ext_run_id, step_number, attempt}}

            {:ok, nil} ->
              :not_found
          end
        end

        bloom_fn = fn epoch_index ->
          Index.find_epochs(epoch_index, "idempotency_keys", hashed_key, created_after)
        end

        case search_archived_epochs(state, query_fn, bloom_fn) do
          {:found, result} -> result
          :not_found -> :miss
        end
    end
  end

  defp find_input_in_active_or_archives(state, input_external_id) do
    case Inputs.get_input_by_external_id(state.db, input_external_id) do
      {:ok, nil} ->
        # Search archived epochs and copy forward if found
        query_fn = fn archive_db ->
          case Inputs.get_input_by_external_id(archive_db, input_external_id) do
            {:ok, nil} ->
              :not_found

            {:ok, {input_id, _, _, _, _, _, _, _, _, _}} ->
              # Found in archive — copy it to active epoch
              Epoch.ensure_input(archive_db, state.db, input_id)

              # Re-read from the active DB (now copied)
              case Inputs.get_input_by_external_id(state.db, input_external_id) do
                {:ok, nil} -> :not_found
                {:ok, result} -> {:found, result}
              end
          end
        end

        bloom_fn = fn epoch_index ->
          Index.find_epochs(epoch_index, "input_external_ids", input_external_id)
        end

        case search_archived_epochs(state, query_fn, bloom_fn) do
          {:found, result} -> {:ok, result}
          :not_found -> {:ok, nil}
        end

      result ->
        result
    end
  end

  defp find_and_copy_run_from_archives(state, run_external_id) do
    query_fn = fn archive_db ->
      if Runs.run_exists?(archive_db, run_external_id) do
        {:ok, remap} = Epoch.copy_run(archive_db, state.db, run_external_id)
        {:found, {:ok, remap}}
      else
        :not_found
      end
    end

    bloom_fn = fn epoch_index ->
      Index.find_epochs(epoch_index, "runs", run_external_id)
    end

    case search_archived_epochs(state, query_fn, bloom_fn) do
      {:found, result} -> result
      :not_found -> :not_found
    end
  end

  defp find_cached_execution_across_epochs(
         state,
         cache_workspace_ids,
         step_id,
         cache_key,
         recorded_after
       ) do
    # Tier 0: Check active epoch
    case Runs.find_cached_execution(
           state.db,
           cache_workspace_ids,
           step_id,
           cache_key,
           recorded_after
         ) do
      {:ok, cached_execution_id} when not is_nil(cached_execution_id) ->
        {:in_epoch, cached_execution_id}

      {:ok, nil} ->
        workspace_external_ids =
          Enum.map(cache_workspace_ids, &workspace_external_id(state, &1))

        query_fn = fn archive_db ->
          archive_workspace_ids =
            Enum.flat_map(workspace_external_ids, fn ext_id ->
              case Workspaces.get_workspace_id(archive_db, ext_id) do
                {:ok, id} when not is_nil(id) -> [id]
                {:ok, nil} -> []
              end
            end)

          if archive_workspace_ids == [] do
            :not_found
          else
            case Runs.find_cached_execution(
                   archive_db,
                   archive_workspace_ids,
                   nil,
                   cache_key,
                   recorded_after
                 ) do
              {:ok, archive_exec_id} when not is_nil(archive_exec_id) ->
                # Instead of copying the entire run, resolve the result value
                # from the archive and create an execution_ref
                case resolve_result(archive_db, archive_exec_id) do
                  {:ok, {:value, value}} ->
                    # Create execution ref for the cached execution itself
                    {:ok, {run_ext, step_num, attempt, module, target}} =
                      Runs.get_run_by_execution(archive_db, archive_exec_id)

                    {:ok, ref_id} =
                      Runs.get_or_create_execution_ref(
                        state.db,
                        run_ext,
                        step_num,
                        attempt,
                        module,
                        target
                      )

                    {:found, {:resolved, ref_id, value}}

                  {:ok, _other} ->
                    :not_found

                  {:pending, _} ->
                    :not_found
                end

              {:ok, nil} ->
                :not_found
            end
          end
        end

        bloom_fn = fn epoch_index ->
          if is_binary(cache_key) do
            Index.find_epochs(epoch_index, "cache_keys", cache_key)
          else
            []
          end
        end

        case search_archived_epochs(state, query_fn, bloom_fn) do
          {:found, result} -> result
          :not_found -> nil
        end
    end
  end

  # Searches archived epochs across both tiers (unindexed, then indexed via Bloom).
  # `query_fn` receives an archive DB handle and returns `{:found, result}` or `:not_found`.
  # `bloom_fn` receives the epoch index and returns candidate epoch IDs.
  defp search_archived_epochs(state, query_fn, bloom_fn) do
    # Tier 1: Check unindexed DBs (always open, newest first)
    unindexed = Epochs.unindexed_dbs(state.epochs)

    result =
      Enum.reduce_while(unindexed, :not_found, fn {_epoch_id, archive_db}, :not_found ->
        case query_fn.(archive_db) do
          {:found, _} = found -> {:halt, found}
          :not_found -> {:cont, :not_found}
        end
      end)

    case result do
      {:found, _} ->
        result

      :not_found ->
        # Tier 2: Consult Bloom index for indexed epochs, open/query/close on demand
        unindexed_ids = MapSet.new(unindexed, fn {id, _db} -> id end)

        candidate_epoch_ids =
          bloom_fn.(state.epoch_index)
          |> Enum.reject(&MapSet.member?(unindexed_ids, &1))

        Enum.reduce_while(candidate_epoch_ids, :not_found, fn epoch_id, :not_found ->
          path = Epochs.archive_path(state.epochs, epoch_id)

          case Exqlite.Sqlite3.open(path) do
            {:ok, archive_db} ->
              try do
                case query_fn.(archive_db) do
                  {:found, _} = found -> {:halt, found}
                  :not_found -> {:cont, :not_found}
                end
              after
                Exqlite.Sqlite3.close(archive_db)
              end

            {:error, _} ->
              {:cont, :not_found}
          end
        end)
    end
  end

  defp execution_external_id(run_external_id, step_number, attempt) do
    "#{run_external_id}:#{step_number}:#{attempt}"
  end

  defp track_run_execution(state, run_ext_id, execution_id, root_module, root_target) do
    Map.update!(state, :run_workflows, fn rw ->
      Map.update(rw, run_ext_id, {root_module, root_target, MapSet.new([execution_id])}, fn {m, t,
                                                                                             ids} ->
        {m, t, MapSet.put(ids, execution_id)}
      end)
    end)
  end

  defp untrack_run_execution(state, run_ext_id, execution_id) do
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

  defp get_run_workflow(state, run_ext_id) do
    case Map.fetch(state.run_workflows, run_ext_id) do
      {:ok, {m, t, _}} -> {m, t}
      :error -> nil
    end
  end

  defp parse_execution_external_id(external_id) do
    case String.split(external_id, ":") do
      [run_external_id, step_number_s, attempt_s] ->
        with {step_number, ""} <- Integer.parse(step_number_s),
             {attempt, ""} <- Integer.parse(attempt_s) do
          {:ok, run_external_id, step_number, attempt}
        else
          _ -> {:error, :invalid_format}
        end

      _ ->
        {:error, :invalid_format}
    end
  end

  defp parse_step_id(step_id) do
    case String.split(step_id, ":", parts: 2) do
      [run_external_id, step_number_s] ->
        case Integer.parse(step_number_s) do
          {step_number, ""} -> {:ok, run_external_id, step_number}
          _ -> {:error, :invalid}
        end

      _ ->
        {:error, :invalid}
    end
  end

  defp resolve_execution(db, execution_id) do
    {:ok, {external_run_id, step_number, step_attempt, module, target}} =
      Runs.get_run_by_execution(db, execution_id)

    {execution_external_id(external_run_id, step_number, step_attempt), module, target}
  end

  defp resolve_execution_ref(db, ref_id) do
    {:ok, {run_ext, step_num, attempt, module, target}} = Runs.get_execution_ref(db, ref_id)
    {execution_external_id(run_ext, step_num, attempt), module, target}
  end

  defp resolve_asset(db, asset_id) do
    case Assets.get_asset_summary(db, asset_id) do
      {:ok, external_id, name, total_count, total_size, entry} ->
        {external_id, name, total_count, total_size, entry}
    end
  end

  defp resolve_references(db, references) do
    Enum.map(references, fn
      {:fragment, format, blob_key, size, metadata} ->
        {:fragment, format, blob_key, size, metadata}

      {:execution, run_ext, step_num, attempt} ->
        ext_id = execution_external_id(run_ext, step_num, attempt)

        {module, target} =
          case Runs.get_module_target(db, run_ext, step_num, attempt) do
            {:ok, {m, t}} -> {m, t}
            {:ok, nil} -> {nil, nil}
          end

        {:execution, ext_id, {module, target}}

      {:asset, external_id} ->
        {:ok, asset_id} = Assets.get_asset_id(db, external_id)
        {^external_id, name, total_count, total_size, entry} = resolve_asset(db, asset_id)
        {:asset, external_id, {name, total_count, total_size, entry}}

      {:input, external_id} ->
        {:input, external_id}
    end)
  end

  defp normalize_references(references) do
    Enum.map(references, fn
      {:execution, execution_external_id} ->
        {:ok, run_ext, step_num, attempt} =
          parse_execution_external_id(execution_external_id)

        {:execution, run_ext, step_num, attempt}

      {:input, external_id} ->
        {:input, external_id}

      ref ->
        ref
    end)
  end

  defp normalize_value({:raw, data, refs}),
    do: {:raw, data, normalize_references(refs)}

  defp normalize_value({:blob, key, size, refs}),
    do: {:blob, key, size, normalize_references(refs)}

  defp build_value(value, db) do
    case value do
      {:raw, data, references} ->
        {:raw, data, resolve_references(db, references)}

      {:blob, key, size, references} ->
        {:blob, key, size, resolve_references(db, references)}
    end
  end

  defp is_result_final?(result) do
    case result do
      {:error, _, _, _, retry_id, _retryable} -> is_nil(retry_id)
      {:error, _, _, _, retry_id} -> is_nil(retry_id)
      {:value, _} -> true
      {:abandoned, retry_id} -> is_nil(retry_id)
      :cancelled -> true
      {:timeout, retry_id} -> is_nil(retry_id)
      {:suspended, _} -> false
      {:recurred, _} -> false
      {:deferred, _} -> false
      {:cached, _} -> false
      {:spawned, _} -> false
      # Resolved ref forms are final (value is already resolved)
      {:deferred, _, _} -> true
      {:cached, _, _} -> true
      {:spawned, _, _} -> true
    end
  end

  defp build_result(result, db) do
    case result do
      {:error, type, message, frames, retry_id, retryable} ->
        retry = if retry_id, do: resolve_execution(db, retry_id)
        {:error, type, message, frames, retry, retryable}

      {:error, type, message, frames, retry_id} ->
        retry = if retry_id, do: resolve_execution(db, retry_id)
        {:error, type, message, frames, retry}

      {:value, value} ->
        {:value, build_value(value, db)}

      {:abandoned, retry_id} ->
        retry = if retry_id, do: resolve_execution(db, retry_id)
        {:abandoned, retry}

      :cancelled ->
        :cancelled

      {:timeout, retry_id} ->
        retry = if retry_id, do: resolve_execution(db, retry_id)
        {:timeout, retry}

      {:suspended, successor_id} ->
        successor = if successor_id, do: resolve_execution(db, successor_id)
        {:suspended, successor}

      {:recurred, successor_id} ->
        successor = if successor_id, do: resolve_execution(db, successor_id)
        {:recurred, successor}

      # In-flight successor (successor_id is an internal execution ID)
      {type, execution_id}
      when type in [:deferred, :cached, :spawned] and is_integer(execution_id) ->
        execution_result =
          case resolve_result(db, execution_id) do
            {:ok, execution_result} -> execution_result
            {:pending, _execution_id} -> nil
          end

        {type, resolve_execution(db, execution_id), build_result(execution_result, db)}

      # Resolved ref form (ref_id + value)
      {type, ref_id, value} when type in [:deferred, :cached, :spawned] ->
        execution_metadata = resolve_execution_ref(db, ref_id)
        resolved_value = build_value(value, db)
        {type, execution_metadata, {:value, resolved_value}}

      nil ->
        nil
    end
  end

  defp record_and_notify_result(state, execution_id, result, _module, created_by \\ nil) do
    {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)
    {:ok, successors} = Runs.get_result_successors(state.db, execution_id)
    {:ok, {r, s, a}} = Runs.get_execution_key(state.db, execution_id)
    execution_external_id = execution_external_id(r, s, a)

    result =
      case result do
        {:value, value} ->
          {:value, normalize_value(value)}

        other ->
          other
      end

    case Results.record_result(state.db, execution_id, result, created_by) do
      {:ok, created_at} ->
        state =
          state
          |> notify_waiting(execution_id)
          |> update_dependencies_on_result(execution_id)
          |> unregister_pending_dependencies(execution_id)

        final = is_result_final?(result)
        result = build_result(result, state.db)

        principal =
          case Principals.get_principal(state.db, created_by) do
            {:ok, {type, external_id}} -> %{type: type, external_id: external_id}
            {:ok, nil} -> nil
          end

        ws_ext_id = workspace_external_id(state, workspace_id)

        # get_result_successors now returns {run_external_id, successor_id}
        state =
          successors
          |> Enum.reduce(state, fn {run_external_id, successor_id}, state ->
            cond do
              successor_id == execution_id ->
                notify_listeners(
                  state,
                  {:run, run_external_id},
                  {:result, execution_external_id, result, created_at, principal}
                )

              final ->
                {:ok, {r, s, a}} = Runs.get_execution_key(state.db, successor_id)
                successor_external_id = execution_external_id(r, s, a)

                notify_listeners(
                  state,
                  {:run, run_external_id},
                  # TODO: better name?
                  {:result_result, successor_external_id, result, created_at, principal}
                )

              true ->
                state
            end
          end)
          |> then(fn state ->
            case untrack_run_execution(state, r, execution_id) do
              {{root_module, root_target}, state} ->
                notify_listeners(
                  state,
                  {:modules, ws_ext_id},
                  {:completed, {root_module, root_target}, r, execution_external_id}
                )

              {nil, state} ->
                state
            end
          end)
          |> notify_listeners(
            {:queue, ws_ext_id},
            {:completed, execution_external_id}
          )

        # Check if any input dependencies became inactive
        state =
          case Inputs.get_input_dependencies_for_execution(state.db, execution_id) do
            {:ok, deps} ->
              Enum.reduce(deps, state, fn {input_id}, state ->
                if Inputs.has_active_dependency?(state.db, input_id) do
                  state
                else
                  {:ok, input_ext_id} = Inputs.get_input_external_id(state.db, input_id)

                  state
                  |> notify_listeners(
                    {:inputs, ws_ext_id},
                    {:input_dependency_inactive, input_ext_id}
                  )
                  |> notify_listeners(
                    {:input, input_ext_id},
                    {:active, false}
                  )
                end
              end)

            _ ->
              state
          end

        # TODO: only if there's an execution waiting for this result?
        send(self(), :tick)

        {:ok, state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp process_result(state, execution_id, result, created_by \\ nil) do
    case Results.has_result?(state.db, execution_id) do
      {:ok, true} ->
        {:ok, state}

      {:ok, false} ->
        {:ok, step} = Runs.get_step_for_execution(state.db, execution_id)
        {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)

        execution_ext_id =
          case Runs.get_execution_key(state.db, execution_id) do
            {:ok, {r, s, a}} -> execution_external_id(r, s, a)
            {:error, :not_found} -> nil
          end

        {retry_id, recurred?, state} =
          cond do
            match?({:suspended, _, _}, result) ->
              {:suspended, execute_after, dependency_keys} = result

              # TODO: limit the number of times a step can suspend? (or rate?)

              {:ok, retry_id, _, state} =
                rerun_step(state, step, workspace_id,
                  execute_after: execute_after,
                  dependency_keys: dependency_keys
                )

              state =
                if execution_ext_id do
                  abort_execution(state, execution_ext_id)
                else
                  state
                end

              {retry_id, false, state}

            result_retryable?(result) && step.retry_limit == -1 ->
              # Unlimited retries - random delay between min and max
              delay_ms =
                step.retry_backoff_min +
                  :rand.uniform() * (step.retry_backoff_max - step.retry_backoff_min)

              execute_after = System.os_time(:millisecond) + delay_ms

              {:ok, retry_id, _, state} =
                rerun_step(state, step, workspace_id, execute_after: execute_after)

              {retry_id, false, state}

            result_retryable?(result) && step.retry_limit > 0 ->
              # Limited retries - check consecutive failures
              {:ok, result_types} =
                Runs.get_step_result_types(state.db, step.id, step.retry_limit + 1)

              consecutive_failures =
                result_types
                |> Enum.take_while(&(&1 in [0, 2, 8]))
                |> Enum.count()

              if consecutive_failures < step.retry_limit do
                # TODO: add jitter (within min/max delay)
                delay_ms =
                  step.retry_backoff_min +
                    consecutive_failures / max(step.retry_limit - 1, 1) *
                      (step.retry_backoff_max - step.retry_backoff_min)

                execute_after = System.os_time(:millisecond) + delay_ms

                {:ok, retry_id, _, state} =
                  rerun_step(state, step, workspace_id, execute_after: execute_after)

                {retry_id, false, state}
              else
                {nil, false, state}
              end

            step.recurrent == 1 and match?({:value, {:raw, nil, []}}, result) ->
              # Null return from recurrent step: schedule next iteration via :recurred
              execute_after =
                if step.delay > 0 do
                  System.os_time(:millisecond) + step.delay
                end

              {:ok, retry_id, _, state} =
                rerun_step(state, step, workspace_id, execute_after: execute_after)

              {retry_id, true, state}

            step.recurrent == 1 and match?({:value, _}, result) ->
              # Non-null return from recurrent step: stop recurrence
              {nil, false, state}

            true ->
              {nil, false, state}
          end

        result =
          case result do
            {:error, type, message, frames, retryable} ->
              {:error, type, message, frames, retry_id, retryable}

            :abandoned ->
              {:abandoned, retry_id}

            :timeout ->
              {:timeout, retry_id}

            {:suspended, _, _} ->
              {:suspended, retry_id}

            {:value, _} when recurred? ->
              {:recurred, retry_id}

            other ->
              other
          end

        state =
          case record_and_notify_result(
                 state,
                 execution_id,
                 result,
                 step.module,
                 created_by
               ) do
            {:ok, state} -> state
            {:error, :already_recorded} -> state
          end

        # Cancel descendant executions for timeouts and cancellations
        state =
          if match?({:timeout, _}, result) or result == :cancelled do
            cancel_descendants(state, execution_id, workspace_id)
          else
            state
          end

        {:ok, state}
    end
  end

  defp resolve_result(db, execution_id) do
    # TODO: check execution exists?
    case Results.get_result(db, execution_id) do
      {:ok, nil} ->
        {:pending, execution_id}

      {:ok, {result, _created_at, _created_by}} ->
        case result do
          {:error, _, _, _, execution_id, _retryable} when not is_nil(execution_id) ->
            resolve_result(db, execution_id)

          {:abandoned, execution_id} when not is_nil(execution_id) ->
            resolve_result(db, execution_id)

          {:timeout, execution_id} when not is_nil(execution_id) ->
            resolve_result(db, execution_id)

          # In-flight successor (follow chain)
          {:deferred, execution_id} when is_integer(execution_id) ->
            resolve_result(db, execution_id)

          {:cached, execution_id} when is_integer(execution_id) ->
            resolve_result(db, execution_id)

          {:suspended, execution_id} ->
            resolve_result(db, execution_id)

          {:recurred, execution_id} ->
            resolve_result(db, execution_id)

          {:spawned, execution_id} when is_integer(execution_id) ->
            resolve_result(db, execution_id)

          # Resolved ref forms — value_id is already loaded, return directly
          {:deferred, _ref_id, value} ->
            {:ok, {:value, value}}

          {:cached, _ref_id, value} ->
            {:ok, {:value, value}}

          {:spawned, _ref_id, value} ->
            {:ok, {:value, value}}

          other ->
            {:ok, other}
        end
    end
  end

  defp assign_targets(state, targets, session_id) do
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

  defp add_listener(state, topic, pid) do
    ref = Process.monitor(pid)

    state =
      state
      |> put_in([Access.key(:listeners), ref], topic)
      |> put_in([Access.key(:topics), Access.key(topic, %{}), ref], pid)
      |> maybe_schedule_idle_shutdown()

    {:ok, ref, state}
  end

  defp remove_listener(state, ref) do
    case Map.fetch(state.listeners, ref) do
      {:ok, topic} ->
        state
        |> Map.update!(:listeners, &Map.delete(&1, ref))
        |> Map.update!(:topics, fn topics ->
          MapUtils.delete_in(topics, [topic, ref])
        end)
    end
  end

  defp notify_dependent_runs(state, input_id, input_external_id, response_type, owner_run_ext_id) do
    {:ok, rows} = Inputs.get_dependent_run_external_ids(state.db, input_id)

    Enum.reduce(rows, state, fn {run_ext_id}, state ->
      if run_ext_id == owner_run_ext_id do
        # Already notified via the direct notification above
        state
      else
        notify_listeners(
          state,
          {:run, run_ext_id},
          {:input_response, input_external_id, response_type}
        )
      end
    end)
  end

  defp notify_listeners(state, topic, payload) do
    if Map.has_key?(state.topics, topic) do
      update_in(state.notifications[topic], &[payload | &1 || []])
    else
      state
    end
  end

  defp flush_notifications(state) do
    Enum.each(state.notifications, fn {topic, notifications} ->
      notifications = Enum.reverse(notifications)

      state.topics
      |> Map.get(topic, %{})
      |> Enum.each(fn {ref, pid} ->
        send(pid, {:topic, ref, notifications})
      end)
    end)

    Map.put(state, :notifications, %{})
  end

  defp send_session(state, session_id, message) do
    session = state.sessions[session_id]

    if session.connection do
      {pid, ^session_id} = state.connections[session.connection]
      send(pid, message)
      state
    else
      update_in(state.sessions[session_id].queue, &[message | &1])
    end
  end

  defp session_at_capacity?(session) do
    if session.concurrency != 0 do
      load = MapSet.size(session.starting) + MapSet.size(session.executing)
      load >= session.concurrency
    else
      false
    end
  end

  defp session_active?(session, state) do
    if session.worker_id do
      worker = Map.fetch!(state.workers, session.worker_id)
      worker.state == :active
    else
      true
    end
  end

  defp session_pool_disabled?(session, state) do
    if session.worker_id do
      worker = Map.fetch!(state.workers, session.worker_id)
      workspace_pools = Map.get(state.pools, session.workspace_id, %{})
      pool = Map.get(workspace_pools, worker.pool_name)
      pool != nil && Map.get(pool, :state, :active) == :disabled
    else
      false
    end
  end

  defp has_requirements?(provides, requires) do
    # TODO: case insensitive matching?
    Enum.all?(requires, fn {key, requires_values} ->
      (provides || %{})
      |> Map.get(key, [])
      |> Enum.any?(&(&1 in requires_values))
    end)
  end

  defp merge_tag_sets(a, b) do
    Map.merge(a || %{}, b || %{}, fn _key, v1, v2 -> Enum.uniq(v1 ++ v2) end)
  end

  # Merge run-level requires with step-level requires (child overrides per key).
  defp effective_requires(tag_sets, run_requires_tag_set_id, step_requires_tag_set_id) do
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

  defp satisfies_accepts?(accepts, requires) do
    # Worker's accepts tags must all be present in the task's requires tags
    Enum.all?(accepts || %{}, fn {key, accepts_values} ->
      (requires || %{})
      |> Map.get(key, [])
      |> Enum.any?(&(&1 in accepts_values))
    end)
  end

  # Build the session data map sent to the Sessions topic.
  defp build_session_data(state, session) do
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

  # Convert a set of tagged pending dependency keys to a list of external ID strings.
  defp pending_dependency_external_ids(db, pending_dependency_ids) do
    pending_dependency_ids
    |> Enum.map(fn
      {:execution, dependency_id} ->
        case Runs.get_execution_key(db, dependency_id) do
          {:ok, {r, s, a}} -> execution_external_id(r, s, a)
          {:error, _} -> nil
        end

      {:input, _input_id} ->
        # Input dependencies are not shown in the queue
        nil
    end)
    |> Enum.reject(&is_nil/1)
  end

  # Send a queue notification with the current pending dependencies for an execution.
  # Converts tagged dependency keys to external IDs.
  defp notify_queue_dependencies(state, execution_id, pending_dependency_ids) do
    case Runs.get_execution_key(state.db, execution_id) do
      {:ok, {r, s, a}} ->
        execution_ext_id = execution_external_id(r, s, a)

        dependency_ext_ids =
          pending_dependency_external_ids(state.db, pending_dependency_ids)

        {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)
        ws_ext_id = workspace_external_id(state, workspace_id)

        notify_listeners(
          state,
          {:queue, ws_ext_id},
          {:dependencies, execution_ext_id, dependency_ext_ids}
        )

      {:error, _} ->
        state
    end
  end

  # Build a map of execution_external_id -> [dependency_external_id] for all
  # pending executions in the given workspace, for the Queue topic snapshot.
  defp build_queue_dependencies(state, workspace_id) do
    state.pending_dependencies
    |> Enum.reduce(%{}, fn {execution_id, dependency_ids}, acc ->
      case Runs.get_workspace_id_for_execution(state.db, execution_id) do
        {:ok, ^workspace_id} ->
          case Runs.get_execution_key(state.db, execution_id) do
            {:ok, {r, s, a}} ->
              ext_id = execution_external_id(r, s, a)

              dependency_ext_ids =
                pending_dependency_external_ids(state.db, dependency_ids)

              Map.put(acc, ext_id, dependency_ext_ids)

            {:error, _} ->
              acc
          end

        _ ->
          acc
      end
    end)
  end

  # Initialize pending_dependencies and dependency_waiters for all existing unassigned executions.
  defp initialize_pending_dependencies(state) do
    {:ok, executions} = Runs.get_unassigned_executions(state.db)

    Enum.reduce(executions, state, fn execution, state ->
      pending_dependencies =
        compute_pending_dependencies(
          state.db,
          execution.execution_id,
          execution.wait_for,
          execution.step_id
        )

      register_pending_dependencies(state, execution.execution_id, pending_dependencies)
    end)
  end

  # Compute the set of execution IDs that the given execution is waiting on.
  # This covers both argument references (when wait_for is set) and result_dependencies.
  defp compute_pending_dependencies(db, execution_id, wait_for, step_id) do
    # Collect pending execution IDs from argument references
    argument_dependencies =
      if wait_for && wait_for != [] do
        {:ok, arguments} = Runs.get_step_arguments(db, step_id)

        wait_for
        |> Enum.flat_map(fn index ->
          case Enum.at(arguments, index) do
            {:raw, _, references} -> references
            {:blob, _, _, references} -> references
            nil -> []
          end
        end)
        |> collect_pending_execution_ids(db, MapSet.new())
      else
        MapSet.new()
      end

    # Collect pending execution IDs from result_dependencies
    result_dependencies =
      case Runs.get_result_dependencies(db, execution_id) do
        {:ok, dependencies} ->
          Enum.reduce(dependencies, MapSet.new(), fn {dependency_ref_id}, acc ->
            {:ok, {run_ext, step_num, attempt, _, _}} =
              Runs.get_execution_ref(db, dependency_ref_id)

            case Runs.get_execution_id(db, run_ext, step_num, attempt) do
              {:ok, {dependency_execution_id}} when not is_nil(dependency_execution_id) ->
                case resolve_result(db, dependency_execution_id) do
                  {:ok, _} -> acc
                  {:pending, pending_id} -> MapSet.put(acc, {:execution, pending_id})
                end

              _ ->
                acc
            end
          end)
      end

    # Collect pending input dependencies
    input_dependencies =
      case Runs.get_input_dependencies(db, execution_id) do
        {:ok, deps} ->
          Enum.reduce(deps, MapSet.new(), fn {input_id}, acc ->
            if Inputs.is_input_responded?(db, input_id) do
              acc
            else
              MapSet.put(acc, {:input, input_id})
            end
          end)
      end

    argument_dependencies
    |> MapSet.union(result_dependencies)
    |> MapSet.union(input_dependencies)
  end

  # Walk references and collect tagged dependency keys that are still pending.
  defp collect_pending_execution_ids(references, db, seen) do
    Enum.reduce(references, MapSet.new(), fn
      {:execution, run_ext, step_num, attempt}, acc ->
        case Runs.get_execution_id(db, run_ext, step_num, attempt) do
          {:ok, {execution_id}} when not is_nil(execution_id) ->
            if MapSet.member?(seen, execution_id) do
              acc
            else
              case resolve_result(db, execution_id) do
                {:ok, {:value, value}} ->
                  inner_refs =
                    case value do
                      {:raw, _, refs} -> refs
                      {:blob, _, _, refs} -> refs
                      _ -> []
                    end

                  inner_pending =
                    collect_pending_execution_ids(
                      inner_refs,
                      db,
                      MapSet.put(seen, execution_id)
                    )

                  MapSet.union(acc, inner_pending)

                {:ok, _} ->
                  acc

                {:pending, pending_id} ->
                  MapSet.put(acc, {:execution, pending_id})
              end
            end

          _ ->
            acc
        end

      {:fragment, _format, _blob_key, _size, _metadata}, acc ->
        acc

      {:asset, _external_id}, acc ->
        acc
    end)
  end

  # Register an execution's pending dependencies in state.
  # Only adds entries if there are actual pending dependencies.
  defp register_pending_dependencies(state, execution_id, dependencies) do
    if MapSet.size(dependencies) == 0 do
      state
    else
      state =
        put_in(state, [Access.key(:pending_dependencies), execution_id], dependencies)

      Enum.reduce(dependencies, state, fn dependency_id, state ->
        update_in(
          state,
          [Access.key(:dependency_waiters), Access.key(dependency_id, MapSet.new())],
          &MapSet.put(&1, execution_id)
        )
      end)
    end
  end

  # Remove an execution from the dependency tracking (when assigned or completed).
  defp unregister_pending_dependencies(state, execution_id) do
    case Map.fetch(state.pending_dependencies, execution_id) do
      {:ok, dependencies} ->
        state =
          Enum.reduce(dependencies, state, fn dependency_id, state ->
            state =
              update_in(
                state,
                [Access.key(:dependency_waiters), Access.key(dependency_id, MapSet.new())],
                &MapSet.delete(&1, execution_id)
              )

            # Clean up empty waiter entries
            if MapSet.size(state.dependency_waiters[dependency_id] || MapSet.new()) == 0 do
              update_in(
                state,
                [Access.key(:dependency_waiters)],
                &Map.delete(&1, dependency_id)
              )
            else
              state
            end
          end)

        update_in(state, [Access.key(:pending_dependencies)], &Map.delete(&1, execution_id))

      :error ->
        state
    end
  end

  # Called when a result is recorded for an execution. Updates dependency_waiters
  # and pending_dependencies for any executions that were waiting on this one.
  # Handles two cases:
  # 1. Result redirects (spawned, deferred, etc.) — follows the chain to find
  #    the new pending execution.
  # 2. Result is a value containing inner execution references (wait_for
  #    semantics) — extracts any still-pending references from the value.
  defp update_dependencies_on_result(state, execution_id) do
    dependency_key = {:execution, execution_id}

    case Map.fetch(state.dependency_waiters, dependency_key) do
      {:ok, waiters} ->
        state =
          update_in(
            state,
            [Access.key(:dependency_waiters)],
            &Map.delete(&1, dependency_key)
          )

        # Determine new pending dependencies that replace this resolved one.
        # This handles both redirect chains and inner value references.
        new_pending =
          case resolve_result(state.db, execution_id) do
            {:pending, new_id} when new_id != execution_id ->
              MapSet.new([{:execution, new_id}])

            {:ok, {:value, value}} ->
              # The result is a value — check for inner execution references
              # that are still pending (needed for wait_for semantics).
              inner_references =
                case value do
                  {:raw, _, references} -> references
                  {:blob, _, _, references} -> references
                  _ -> []
                end

              collect_pending_execution_ids(
                inner_references,
                state.db,
                MapSet.new([execution_id])
              )

            _ ->
              MapSet.new()
          end

        Enum.reduce(waiters, state, fn waiter_id, state ->
          case Map.fetch(state.pending_dependencies, waiter_id) do
            {:ok, current} ->
              updated =
                current
                |> MapSet.delete(dependency_key)
                |> MapSet.union(new_pending)

              # Register waiter with new dependency_waiters entries
              state
              |> then(fn state ->
                Enum.reduce(new_pending, state, fn new_dep_key, state ->
                  update_in(
                    state,
                    [
                      Access.key(:dependency_waiters),
                      Access.key(new_dep_key, MapSet.new())
                    ],
                    &MapSet.put(&1, waiter_id)
                  )
                end)
              end)
              |> then(fn state ->
                if MapSet.size(updated) == 0 do
                  update_in(
                    state,
                    [Access.key(:pending_dependencies)],
                    &Map.delete(&1, waiter_id)
                  )
                else
                  put_in(state, [Access.key(:pending_dependencies), waiter_id], updated)
                end
              end)
              |> notify_queue_dependencies(waiter_id, updated)

            :error ->
              state
          end
        end)

      :error ->
        state
    end
  end

  # Called when an input response is recorded. Resolves the {:input, id}
  # dependency for any executions that were waiting on this input.
  defp update_dependencies_on_input(state, input_id) do
    dependency_key = {:input, input_id}

    case Map.fetch(state.dependency_waiters, dependency_key) do
      {:ok, waiters} ->
        state =
          update_in(
            state,
            [Access.key(:dependency_waiters)],
            &Map.delete(&1, dependency_key)
          )

        Enum.reduce(waiters, state, fn waiter_id, state ->
          case Map.fetch(state.pending_dependencies, waiter_id) do
            {:ok, current} ->
              updated = MapSet.delete(current, dependency_key)

              if MapSet.size(updated) == 0 do
                send(self(), :tick)
              end

              state
              |> then(fn state ->
                if MapSet.size(updated) == 0 do
                  update_in(
                    state,
                    [Access.key(:pending_dependencies)],
                    &Map.delete(&1, waiter_id)
                  )
                else
                  put_in(state, [Access.key(:pending_dependencies), waiter_id], updated)
                end
              end)
              |> notify_queue_dependencies(waiter_id, updated)

            :error ->
              state
          end
        end)

      :error ->
        state
    end
  end

  defp choose_session(state, execution, requires) do
    target =
      state.targets
      |> Map.get(execution.module, %{})
      |> Map.get(execution.target)

    if target && target.type == execution.type do
      session_ids =
        Enum.filter(target.session_ids, fn session_id ->
          session = Map.fetch!(state.sessions, session_id)

          session.workspace_id == execution.workspace_id && session.connection &&
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

  defp choose_pool(state, execution, requires) do
    pools =
      state.pools
      |> Map.get(execution.workspace_id, %{})
      |> Map.filter(fn {_, pool} ->
        Map.get(pool, :state, :active) != :disabled &&
          pool.launcher && execution.module in pool.modules &&
          has_requirements?(merge_tag_sets(pool.provides, Map.get(pool, :accepts, %{})), requires) &&
          satisfies_accepts?(Map.get(pool, :accepts, %{}), requires)
      end)

    if Enum.any?(pools) do
      pools |> Map.values() |> Enum.map(& &1.id) |> Enum.random()
    end
  end

  defp reschedule_expire_waiters(state) do
    if state.expire_waiters_timer do
      Process.cancel_timer(state.expire_waiters_timer)
    end

    next_expire_at =
      state.waiting
      |> Map.values()
      |> Enum.flat_map(fn execution_waiting ->
        Enum.map(execution_waiting, fn {_, _, expire_at, _} -> expire_at end)
      end)
      |> Enum.reject(&is_nil/1)
      |> Enum.min(fn -> nil end)

    timer =
      if next_expire_at do
        Process.send_after(self(), :expire_waiters, next_expire_at, abs: true)
      end

    Map.put(state, :expire_waiters_timer, timer)
  end

  defp notify_waiting(state, execution_id) do
    # The waiting map uses external execution IDs as keys.
    # We need to find the external ID for this internal execution_id to pop the right entry.
    execution_ext_id =
      case Runs.get_execution_key(state.db, execution_id) do
        {:ok, {r, s, a}} -> execution_external_id(r, s, a)
        {:error, :not_found} -> nil
      end

    {execution_waiting, waiting} =
      if execution_ext_id do
        Map.pop(state.waiting, execution_ext_id)
      else
        {nil, state.waiting}
      end

    if execution_waiting do
      state =
        case resolve_result(state.db, execution_id) do
          {:pending, new_execution_id} ->
            # Find the external ID for the new execution
            new_ext_id =
              case Runs.get_execution_key(state.db, new_execution_id) do
                {:ok, {r, s, a}} -> execution_external_id(r, s, a)
              end

            waiting =
              Map.update(waiting, new_ext_id, execution_waiting, &(&1 ++ execution_waiting))

            Map.put(state, :waiting, waiting)

          {:ok, result} ->
            state = Map.put(state, :waiting, waiting)

            # Enrich value results with resolved references (asset metadata, execution metadata)
            result =
              case result do
                {:value, value} -> {:value, build_value(value, state.db)}
                other -> other
              end

            Enum.reduce(
              execution_waiting,
              state,
              fn {from_ext_id, request_id, _, _}, state ->
                # find_session_for_execution now uses external IDs
                case find_session_for_execution(state, from_ext_id) do
                  {:ok, session_id} ->
                    send_session(state, session_id, {:result, request_id, result})

                  :error ->
                    state
                end
              end
            )
        end

      reschedule_expire_waiters(state)
    else
      state
    end
  end

  # Finds the session for an execution by external execution ID
  defp find_session_for_execution(state, execution_ext_id) do
    state.sessions
    |> Map.keys()
    |> Enum.find(fn session_id ->
      session = Map.fetch!(state.sessions, session_id)

      MapSet.member?(session.starting, execution_ext_id) or
        MapSet.member?(session.executing, execution_ext_id)
    end)
    |> case do
      nil -> :error
      session_id -> {:ok, session_id}
    end
  end

  # Clean up waiting map entries and pending requests for an execution,
  # without sending an abort message to the worker.
  defp cleanup_execution(state, execution_ext_id) do
    # Clean up waiting map entries where this execution is the waiter,
    # and collect pending request IDs so we can send responses
    {state, pending_request_ids} =
      Enum.reduce(
        state.waiting,
        {state, []},
        fn {for_ext_id, execution_waiting}, {state, pending} ->
          {removed, remaining} =
            Enum.split_with(execution_waiting, fn {from_ext_id, _, _, _} ->
              from_ext_id == execution_ext_id
            end)

          pending =
            Enum.reduce(removed, pending, fn {_, request_id, _, _}, acc ->
              if request_id, do: [request_id | acc], else: acc
            end)

          state =
            Map.update!(state, :waiting, fn waiting ->
              if Enum.any?(remaining) do
                Map.put(waiting, for_ext_id, remaining)
              else
                Map.delete(waiting, for_ext_id)
              end
            end)

          {state, pending}
        end
      )

    # Send responses for any pending get_result requests so the worker
    # doesn't hang waiting for a reply that will never come
    case find_session_for_execution(state, execution_ext_id) do
      {:ok, session_id} ->
        Enum.reduce(pending_request_ids, state, fn request_id, state ->
          send_session(state, session_id, {:result, request_id, :suspended})
        end)

      :error ->
        state
    end
  end

  # Clean up an execution's state and send an abort message to the worker.
  defp abort_execution(state, execution_ext_id) do
    state = cleanup_execution(state, execution_ext_id)

    case find_session_for_execution(state, execution_ext_id) do
      {:ok, session_id} ->
        send_session(state, session_id, {:abort, execution_ext_id})

      :error ->
        Logger.warning("Couldn't locate session for execution #{execution_ext_id}. Ignoring.")
        state
    end
  end

  defp process_launcher_result(state, task_ref, result) do
    callback = Map.fetch!(state.launcher_tasks, task_ref)

    state
    |> callback.(result)
    |> Map.update!(:launcher_tasks, &Map.delete(&1, task_ref))
  end

  defp build_launcher_env(state, workspace_id, token, launcher) do
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

  defp call_launcher(state, launcher, fun, args, callback) do
    module =
      case launcher.type do
        :docker -> Coflux.DockerLauncher
        :process -> Coflux.ProcessLauncher
        :kubernetes -> Coflux.KubernetesLauncher
      end

    task = Task.Supervisor.async_nolink(Coflux.LauncherSupervisor, module, fun, args)

    put_in(state, [Access.key(:launcher_tasks), task.ref], callback)
  end

  defp update_worker_state(
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
    |> notify_listeners(
      {:pool, workspace_external_id(state, workspace_id), pool_name},
      {:worker_state, worker.external_id, worker_state}
    )
  end

  defp deactivate_worker(state, worker_id, error, logs \\ nil) do
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

    notify_listeners(
      state,
      {:pool, workspace_external_id(state, worker.workspace_id), worker.pool_name},
      {:worker_deactivated, worker.external_id, deactivated_at, error, logs}
    )
  end

  defp schedule_session_expiry(state, session_id, timeout_ms) do
    expiry_at = System.os_time(:millisecond) + timeout_ms
    state = put_in(state.session_expiries[session_id], expiry_at)
    reschedule_expire_sessions_timer(state)
  end

  defp cancel_session_expiry(state, session_id) do
    state = Map.update!(state, :session_expiries, &Map.delete(&1, session_id))
    reschedule_expire_sessions_timer(state)
  end

  defp reschedule_expire_sessions_timer(state) do
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

  defp maybe_schedule_idle_shutdown(state) do
    idle? = Enum.empty?(state.sessions) and Enum.empty?(state.listeners)

    cond do
      idle? and is_nil(state.idle_timer) ->
        ref = make_ref()
        timer = Process.send_after(self(), {:idle_shutdown, ref}, @idle_timeout_ms)
        %{state | idle_timer: {timer, ref}}

      not idle? and not is_nil(state.idle_timer) ->
        {timer, _ref} = state.idle_timer
        Process.cancel_timer(timer)
        %{state | idle_timer: nil}

      true ->
        state
    end
  end
end
