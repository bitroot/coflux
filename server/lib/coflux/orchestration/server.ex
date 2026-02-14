defmodule Coflux.Orchestration.Server do
  use GenServer, restart: :transient
  require Logger

  alias Coflux.Store.{Epoch, EpochIndex}
  alias Coflux.MapUtils

  alias Coflux.Orchestration.{
    Workspaces,
    Sessions,
    Runs,
    Results,
    Assets,
    CacheConfigs,
    TagSets,
    Workers,
    Manifests,
    Principals,
    EpochCopy
  }

  @default_activation_timeout_ms 600_000
  @default_reconnection_timeout_ms 30_000
  @connected_worker_poll_interval_ms 30_000
  @disconnected_worker_poll_interval_ms 5_000
  @worker_idle_timeout_ms 5_000
  @rotation_check_interval_ms 60_000
  @rotation_size_threshold_bytes 100 * 1024 * 1024

  defmodule State do
    defstruct project_id: nil,
              db: nil,
              epochs: nil,
              tick_timer: nil,
              suspend_timer: nil,
              expiry_timer: nil,

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

              # session_id -> %{external_id, connection, targets, queue, starting, executing, concurrency, workspace_id, provides, worker_id, last_idle_at, activated_at, activation_timeout, reconnection_timeout}
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

              # execution_external_id -> [{from_execution_external_id, request_id, suspend_at}]
              waiting: %{},

              # task_ref -> callback
              launcher_tasks: %{},

              # Coflux.Store.EpochIndex struct for Bloom filter lookups
              epoch_index: nil,

              # ref of running background index build task
              index_task: nil,

              # [epoch_id] awaiting Bloom filter build (FIFO)
              index_queue: []
  end

  def start_link(opts) do
    {project_id, opts} = Keyword.pop!(opts, :project_id)
    GenServer.start_link(__MODULE__, project_id, opts)
  end

  def init(project_id) do
    {:ok, epoch_index} = EpochIndex.load(project_id)

    # Crash recovery: scan for archived epoch files not in the index at all
    # (crash between rotate and writing placeholder)
    epochs_dir = Epoch.epochs_dir(project_id)
    File.mkdir_p!(epochs_dir)

    epoch_files =
      epochs_dir
      |> File.ls!()
      |> Enum.filter(&String.ends_with?(&1, ".sqlite"))
      |> Enum.sort()

    archived_files = Enum.drop(epoch_files, -1)
    indexed_epoch_ids = MapSet.new(epoch_index.entries, & &1.epoch_id)

    epoch_index =
      Enum.reduce(archived_files, epoch_index, fn file, idx ->
        epoch_id = Path.basename(file, ".sqlite")

        if MapSet.member?(indexed_epoch_ids, epoch_id) do
          idx
        else
          # Add placeholder with nil blooms
          now = System.os_time(:millisecond)
          EpochIndex.add_epoch(idx, epoch_id, now, nil, nil)
        end
      end)

    # Save if we added any placeholders
    if length(epoch_index.entries) != MapSet.size(indexed_epoch_ids) do
      :ok = EpochIndex.save(project_id, epoch_index)
    end

    # Determine which epochs need open DB handles (unindexed = nil blooms)
    unindexed_epoch_ids = EpochIndex.unindexed_epoch_ids(epoch_index)

    case Epoch.open(project_id, "orchestration", unindexed_epoch_ids) do
      {:ok, epochs} ->
        db = Epoch.active_db(epochs)

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
        {:ok, workspace_pools} = Workspaces.get_workspace_pools(state.db, workspace_id)
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

    state =
      Enum.reduce(
        active_sessions,
        state,
        fn {session_id, external_id, workspace_id, worker_id, provides_tag_set_id, concurrency,
            activation_timeout, reconnection_timeout, secret_hash, created_at, activated_at},
           state ->
          provides =
            if provides_tag_set_id do
              case TagSets.get_tag_set(state.db, provides_tag_set_id) do
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
            concurrency: concurrency,
            workspace_id: workspace_id,
            provides: provides,
            worker_id: worker_id,
            last_idle_at: activated_at || created_at,
            activated_at: activated_at,
            activation_timeout: activation_timeout,
            reconnection_timeout: reconnection_timeout
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

    # Schedule periodic epoch rotation check
    Process.send_after(self(), :check_rotation, @rotation_check_interval_ms)

    # Kick off background Bloom filter builds for any unindexed epochs
    state = maybe_start_index_build(state)

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
    {:reply, {:ok, result}, state}
  end

  def handle_call(:list_tokens, _from, state) do
    {:ok, tokens} = Principals.list_tokens(state.db)
    {:reply, {:ok, tokens}, state}
  end

  def handle_call({:revoke_token, token_id}, _from, state) do
    result = Principals.revoke_token(state.db, token_id)
    {:reply, result, state}
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
          state =
            state
            |> put_in([Access.key(:workspaces), workspace_id], workspace)
            |> put_in([Access.key(:workspace_names), workspace.name], workspace_id)
            |> put_in([Access.key(:workspace_external_ids), workspace.external_id], workspace_id)
            |> notify_listeners(:workspaces, {:workspace, workspace_id, workspace})
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
               Map.take(workspace, [:name, :base_id, :base_external_id, :state, :external_id])}
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
        {:ok, pools} ->
          {:reply, {:ok, pools}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:update_pool, workspace_external_id, pool_name, pool, access}, _from, state) do
    with {:ok, workspace_id, _} <-
           require_workspace(state, workspace_external_id, access) do
      case Workspaces.update_pool(state.db, workspace_id, pool_name, pool, access[:principal_id]) do
        {:ok, pool_id} ->
          state =
            state.workers
            |> Enum.reduce(state, fn {worker_id, worker}, state ->
              if worker.state == :active && worker.pool_name == pool_name do
                # TODO: only if pool has meaningfully changed?
                update_worker_state(state, worker_id, :draining, workspace_id, pool_name)
              else
                state
              end
            end)
            |> update_in([Access.key(:pools), Access.key(workspace_id, %{})], fn pools ->
              if pool do
                Map.put(pools, pool_name, Map.put(pool, :id, pool_id))
              else
                Map.delete(pools, pool_name)
              end
            end)
            |> notify_listeners(
              {:pool, workspace_external_id(state, workspace_id), pool_name},
              {:updated, pool}
            )
            |> notify_listeners(
              {:pools, workspace_external_id(state, workspace_id)},
              {:pool, pool_name, pool}
            )
            |> flush_notifications()

          {:reply, :ok, state}
      end
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
            state =
              state
              |> notify_listeners(
                {:modules, workspace_external_id(state, workspace_id)},
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
    concurrency = Keyword.get(opts, :concurrency, 0)
    activation_timeout = Keyword.get(opts, :activation_timeout, @default_activation_timeout_ms)

    reconnection_timeout =
      Keyword.get(opts, :reconnection_timeout, @default_reconnection_timeout_ms)

    with {:ok, workspace_id, _} <-
           require_workspace(state, workspace_external_id, access) do
      db_opts = [
        provides: provides,
        concurrency: concurrency,
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
            concurrency: concurrency,
            workspace_id: workspace_id,
            provides: provides,
            worker_id: nil,
            last_idle_at: now,
            activated_at: nil,
            activation_timeout: activation_timeout,
            reconnection_timeout: reconnection_timeout
          }

          state =
            state
            |> put_in([Access.key(:sessions), session_id], session)
            |> put_in([Access.key(:session_ids), external_session_id], session_id)
            |> schedule_session_expiry(session_id, activation_timeout)

          {:reply, {:ok, token}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
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
          {:session, session.external_id,
           %{
             connected: true,
             executions: session.starting |> MapSet.union(session.executing) |> Enum.count(),
             pool_name: get_in(state.workers, [session.worker_id, :pool_name])
           }}
        )

      state =
        case session.worker_id && Map.fetch(state.workers, session.worker_id) do
          {:ok, worker} ->
            state
            |> put_in(
              [Access.key(:workers), session.worker_id, Access.key(:session_id)],
              session_id
            )
            |> notify_listeners(
              {:pool, workspace_external_id(state, worker.workspace_id), worker.pool_name},
              {:worker_connected, worker.external_id, true}
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

  def handle_call({:declare_targets, external_id, targets}, _from, state) do
    session_id = Map.fetch!(state.session_ids, external_id)

    state =
      state
      |> assign_targets(targets, session_id)
      |> flush_notifications()

    send(self(), :tick)

    {:reply, :ok, state}
  end

  def handle_call({:start_run, module, target_name, type, arguments, access, opts}, _from, state) do
    workspace_external_id = Keyword.get(opts, :workspace)

    case require_workspace(state, workspace_external_id, access) do
      {:ok, workspace_id, _} ->
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
    arguments = Enum.map(arguments, &convert_value_to_internal_ids(state.db, &1))

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
         step_number: step_number,
         execution_id: _execution_id,
         attempt: attempt,
         created_at: created_at,
         cache_key: cache_key,
         memo_key: memo_key,
         memo_hit: memo_hit,
         child_added: child_added
       }} ->
        group_id = Keyword.get(opts, :group_id)
        cache = Keyword.get(opts, :cache)
        delay = Keyword.get(opts, :delay, 0)
        execute_after = if delay > 0, do: created_at + delay
        requires = Keyword.get(opts, :requires) || %{}

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
                 created_at: created_at,
                 arguments: arguments,
                 requires: requires
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

            state =
              state
              |> notify_listeners(
                {:modules, ws_ext_id},
                {:scheduled, module, execution_external_id, execute_at}
              )
              |> notify_listeners(
                {:module, module, ws_ext_id},
                {:scheduled, execution_external_id, target_name, run.external_id, step_number,
                 attempt, execute_after, created_at}
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
    # TODO: abort/cancel any running/scheduled retry? (for the same workspace) (and reference this retry?)
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
           Runs.get_execution_id(state.db, run_ext_id, step_number, attempt),
         {:ok, exec_workspace_id} <- Runs.get_workspace_id_for_execution(state.db, execution_id),
         :ok <- require_workspace_match(exec_workspace_id, workspace_id) do
      do_cancel_execution(state, execution_id, access[:principal_id])
    else
      {:error, :invalid_format} ->
        {:reply, {:error, :not_found}, state}

      {:ok, nil} ->
        {:reply, {:error, :not_found}, state}

      {:error, :workspace_invalid} ->
        {:reply, {:error, :not_found}, state}

      {:error, :forbidden} ->
        {:reply, {:error, :forbidden}, state}

      {:error, :workspace_mismatch} ->
        {:reply, {:error, :workspace_mismatch}, state}

      {:error, :not_found} ->
        {:reply, {:error, :not_found}, state}
    end
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
            {:executions, session.external_id,
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
            executions = session.starting |> MapSet.union(session.executing) |> Enum.count()

            notify_listeners(
              state,
              {:sessions, workspace_external_id(state, session.workspace_id)},
              {:executions, session.external_id, executions}
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
        {:get_result, execution_external_id, from_execution_external_id, timeout_ms, request_id},
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
              dependency = resolve_execution_ref(state.db, dep_ref_id)

              dep_ext_id = dependency.execution_external_id

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
              state =
                if timeout_ms == 0 do
                  {:ok, state} =
                    process_result(
                      state,
                      from_execution_id,
                      {:suspended, nil, [pending_execution_id]}
                    )

                  state
                else
                  now = System.monotonic_time(:millisecond)
                  suspend_at = if timeout_ms, do: now + timeout_ms

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
                      &[{from_execution_external_id, request_id, suspend_at} | &1]
                    )

                  if timeout_ms do
                    reschedule_next_suspend(state)
                  else
                    state
                  end
                end

              {:wait, state}

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
        if from_execution_external_id do
          from_execution_id = Map.fetch!(state.execution_ids, from_execution_external_id)
          {:ok, _} = Runs.record_asset_dependency(state.db, from_execution_id, asset_id)
        end

        {:reply, {:ok, name, entries}, state}

      {:error, :not_found} ->
        {:reply, {:error, :not_found}, state}
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

        # Collect all external execution IDs that are executing or starting
        all_executing_ext_ids =
          state.sessions
          |> Map.values()
          |> Enum.reduce(MapSet.new(), fn session, acc ->
            acc |> MapSet.union(session.executing) |> MapSet.union(session.starting)
          end)

        {:ok, executions} = Runs.get_unassigned_executions(state.db)
        # TODO: get/include assigned (pending) executions

        executions =
          Enum.reduce(executions, %{}, fn execution, executions ->
            ext_id =
              execution_external_id(
                execution.run_external_id,
                execution.step_number,
                execution.attempt
              )

            executions
            |> Map.put_new(execution.module, {MapSet.new(), %{}})
            |> Map.update!(execution.module, fn {module_executing, module_scheduled} ->
              if MapSet.member?(all_executing_ext_ids, ext_id) do
                {MapSet.put(module_executing, ext_id), module_scheduled}
              else
                {module_executing,
                 Map.put(
                   module_scheduled,
                   ext_id,
                   execution.execute_after || execution.created_at
                 )}
              end
            end)
          end)

        {:ok, ref, state} =
          add_listener(state, {:modules, workspace_external_id}, pid)

        {:reply, {:ok, manifests, executions, ref}, state}
    end
  end

  def handle_call({:subscribe_module, module, workspace_external_id, pid}, _from, state) do
    case resolve_workspace_external_id(state, workspace_external_id) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, _workspace_id} ->
        {:ok, executions} = Runs.get_module_executions(state.db, module)
        {:ok, ref, state} = add_listener(state, {:module, module, workspace_external_id}, pid)
        {:reply, {:ok, executions, ref}, state}
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
        pool = Map.get(state.pools[workspace_id], pool_name)
        {:ok, pool_workers} = Workers.get_pool_workers(state.db, pool_name)

        # TODO: include 'active' workers that aren't in this (potentially limited) list

        workers =
          Map.new(
            pool_workers,
            fn {worker_id, worker_external_id, starting_at, started_at, start_error, stopping_at,
                stopped_at, stop_error, deactivated_at, error} ->
              worker = Map.get(state.workers, worker_id)

              connected =
                if worker do
                  if worker.session_id do
                    case Map.fetch(state.sessions, worker.session_id) do
                      {:ok, session} -> !is_nil(session.connection)
                      :error -> false
                    end
                  else
                    false
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
                 state: if(worker, do: worker.state),
                 connected: connected
               }}
            end
          )

        {:ok, ref, state} = add_listener(state, {:pool, workspace_external_id, pool_name}, pid)
        {:reply, {:ok, pool, workers, ref}, state}

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
            executions =
              session.starting
              |> MapSet.union(session.executing)
              |> Enum.count()

            pool_name = get_in(state.workers, [session.worker_id, :pool_name])

            {session.external_id,
             %{
               connected: !is_nil(session.connection),
               executions: executions,
               pool_name: pool_name
             }}
          end)

        {:ok, ref, state} = add_listener(state, {:sessions, workspace_external_id}, pid)
        {:reply, {:ok, sessions, ref}, state}
    end
  end

  def handle_call(
        {:subscribe_workflow, module, target_name, workspace_external_id, pid},
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
           ),
         {:ok, runs} =
           Runs.get_target_runs(state.db, module, target_name, :workflow, workspace_id) do
      {:ok, ref, state} =
        add_listener(state, {:workflow, module, target_name, workspace_external_id}, pid)

      {:reply, {:ok, workflow, instruction, runs, ref}, state}
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:subscribe_run, external_run_id, pid}, _from, state) do
    case Runs.get_run_by_external_id(state.db, external_run_id) do
      {:ok, nil} ->
        {:reply, {:error, :not_found}, state}

      {:ok, run} ->
        parent =
          if run.parent_ref_id do
            resolve_execution_ref(state.db, run.parent_ref_id)
          end

        {:ok, steps} = Runs.get_run_steps(state.db, run.id)
        {:ok, run_executions} = Runs.get_run_executions(state.db, run.id)
        {:ok, run_dependencies} = Runs.get_run_dependencies(state.db, run.id)
        {:ok, run_children} = Runs.get_run_children(state.db, run.id)
        {:ok, groups} = Runs.get_groups_for_run(state.db, run.id)

        cache_configs =
          steps
          |> Enum.map(& &1.cache_config_id)
          |> Enum.reject(&is_nil/1)
          |> Enum.uniq()
          |> Enum.reduce(%{}, fn cache_config_id, cache_configs ->
            case CacheConfigs.get_cache_config(state.db, cache_config_id) do
              {:ok, cache_config} -> Map.put(cache_configs, cache_config_id, cache_config)
            end
          end)

        results =
          run_executions
          |> Enum.map(&elem(&1, 0))
          |> Enum.reduce(%{}, fn execution_id, results ->
            {result, completed_at, result_created_by} =
              case Results.get_result(state.db, execution_id) do
                {:ok, {result, completed_at, created_by}} ->
                  result = build_result(result, state.db)
                  {result, completed_at, created_by}

                {:ok, nil} ->
                  {nil, nil, nil}
              end

            Map.put(results, execution_id, {result, completed_at, result_created_by})
          end)

        steps =
          Map.new(steps, fn step ->
            {:ok, arguments} = Runs.get_step_arguments(state.db, step.id)
            arguments = Enum.map(arguments, &build_value(&1, state.db))

            requires =
              if step.requires_tag_set_id do
                case TagSets.get_tag_set(state.db, step.requires_tag_set_id) do
                  {:ok, requires} -> requires
                end
              else
                %{}
              end

            parent_execution_external_id =
              if step.parent_id do
                {:ok, {r, s, a}} = Runs.get_execution_key(state.db, step.parent_id)
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

                   {:ok, {r, s, a}} = Runs.get_execution_key(state.db, execution_id)
                   exec_external_id = execution_external_id(r, s, a)

                   workspace_external_id = state.workspaces[workspace_id].external_id

                   {result, completed_at, result_created_by} = Map.fetch!(results, execution_id)

                   execution_groups =
                     groups
                     |> Enum.filter(fn {e_id, _, _} -> e_id == execution_id end)
                     |> Map.new(fn {_, group_id, name} -> {group_id, name} end)

                   # TODO: load assets in one query
                   {:ok, asset_ids} = Results.get_assets_for_execution(state.db, execution_id)

                   assets =
                     asset_ids
                     |> Enum.map(&resolve_asset(state.db, &1))
                     |> Map.new(fn {external_id, name, total_count, total_size, entry} ->
                       {external_id, {name, total_count, total_size, entry}}
                     end)

                   # TODO: batch? get `get_dependencies` to resolve?
                   dependencies =
                     run_dependencies
                     |> Map.get(execution_id, [])
                     |> Map.new(fn dependency_ref_id ->
                       metadata = resolve_execution_ref(state.db, dependency_ref_id)
                       {metadata.execution_external_id, metadata}
                     end)

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
                      result: result,
                      result_created_by: result_created_by,
                      children: Map.get(run_children, execution_id, [])
                    }}
                 end)
             }}
          end)

        {:ok, ref, state} = add_listener(state, {:run, run.external_id}, pid)
        {:reply, {:ok, run, parent, steps, ref}, state}
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
    state = remove_listener(state, ref)
    {:noreply, state}
  end

  def handle_info(:expire_sessions, state) do
    now = System.os_time(:millisecond)

    {expired, remaining} =
      state.session_expiries
      |> Enum.split_with(fn {_, expiry_at} -> expiry_at <= now end)

    state = %{state | session_expiries: Map.new(remaining), expiry_timer: nil}

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
      |> reschedule_expiry_timer()
      |> flush_notifications()

    {:noreply, state}
  end

  def handle_info(:check_rotation, state) do
    db_size = Epoch.active_db_size(state.epochs)

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
      |> Enum.map(& &1.requires_tag_set_id)
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
              # TODO: choose session before resolving arguments?
              {:ok, arguments} = Runs.get_step_arguments(state.db, execution.step_id)

              if arguments_ready?(state.db, execution.wait_for, arguments) &&
                   dependencies_ready?(state.db, execution.execution_id) do
                requires =
                  if execution.requires_tag_set_id,
                    do: Map.fetch!(tag_sets, execution.requires_tag_set_id),
                    else: %{}

                if execution.type == :task || !execution.parent_id do
                  case choose_session(state, execution, requires) do
                    nil ->
                      {state, assigned, [execution | unassigned]}

                    session_id ->
                      {:ok, assigned_at} =
                        Runs.assign_execution(state.db, execution.execution_id, session_id)

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
                        |> send_session(
                          session_id,
                          {:execute, execution_external_id, execution.module, execution.target,
                           enriched_arguments, execution.run_external_id, workspace_external_id}
                        )

                      {state, [{execution, assigned_at} | assigned], unassigned}
                  end
                else
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
                                 delay_min: execution.retry_delay_min,
                                 delay_max: execution.retry_delay_max
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
              else
                {state, assigned, unassigned}
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

    assigned_groups =
      assigned
      |> Enum.group_by(fn {execution, _} -> execution.workspace_id end)
      |> Map.new(fn {workspace_id, executions} ->
        {workspace_id,
         executions
         |> Enum.group_by(
           fn {execution, _} -> execution.module end,
           fn {execution, _} ->
             execution_external_id(
               execution.run_external_id,
               execution.step_number,
               execution.attempt
             )
           end
         )
         |> Map.new(fn {k, v} -> {k, MapSet.new(v)} end)}
      end)

    state =
      Enum.reduce(assigned_groups, state, fn {workspace_id, workspace_executions}, state ->
        notify_listeners(
          state,
          {:modules, workspace_external_id(state, workspace_id)},
          {:assigned, workspace_executions}
        )
      end)

    state =
      Enum.reduce(assigned_groups, state, fn {workspace_id, workspace_executions}, state ->
        Enum.reduce(workspace_executions, state, fn {module, execution_ext_ids}, state ->
          module_executions =
            Enum.reduce(assigned, %{}, fn {execution, assigned_at}, module_executions ->
              ext_id =
                execution_external_id(
                  execution.run_external_id,
                  execution.step_number,
                  execution.attempt
                )

              if MapSet.member?(execution_ext_ids, ext_id) do
                Map.put(module_executions, ext_id, assigned_at)
              else
                module_executions
              end
            end)

          notify_listeners(
            state,
            {:module, module, workspace_external_id(state, workspace_id)},
            {:assigned, module_executions}
          )
        end)
      end)

    state =
      if Enum.any?(unassigned) do
        latest_pool_launch_at =
          state.workers
          |> Map.values()
          |> Enum.reduce(%{}, fn worker, latest ->
            Map.update(latest, worker.pool_id, worker.created_at, &max(&1, worker.created_at))
          end)

        unassigned
        |> Enum.group_by(& &1.workspace_id)
        |> Enum.reduce(state, fn {workspace_id, executions}, state ->
          executions
          |> Enum.map(fn execution ->
            requires =
              if execution.requires_tag_set_id,
                do: Map.fetch!(tag_sets, execution.requires_tag_set_id),
                else: %{}

            choose_pool(state, execution, requires)
          end)
          |> Enum.reject(&is_nil/1)
          |> Enum.uniq()
          |> Enum.filter(&(now - Map.get(latest_pool_launch_at, &1, 0) > 10_000))
          |> Enum.reduce(state, fn pool_id, state ->
            case Workers.create_worker(state.db, pool_id) do
              {:ok, worker_id, worker_external_id, created_at} ->
                {pool_name, pool} =
                  Enum.find(
                    state.pools[workspace_id],
                    &(elem(&1, 1).id == pool_id)
                  )

                # Create a session for the pool-launched worker
                activation_timeout =
                  Map.get(pool, :activation_timeout, @default_activation_timeout_ms)

                reconnection_timeout =
                  Map.get(pool, :reconnection_timeout, @default_reconnection_timeout_ms)

                session_opts = [
                  provides: pool.provides,
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
                  worker_id: worker_id,
                  last_idle_at: session_now,
                  activated_at: nil,
                  activation_timeout: activation_timeout,
                  reconnection_timeout: reconnection_timeout
                }

                state
                |> put_in([Access.key(:sessions), session_id], session)
                |> put_in([Access.key(:session_ids), external_id], session_id)
                |> schedule_session_expiry(session_id, activation_timeout)
                |> call_launcher(
                  pool.launcher,
                  :launch,
                  [
                    state.project_id,
                    state.workspaces[workspace_id].name,
                    token,
                    pool.modules,
                    Map.delete(pool.launcher, :type)
                  ],
                  fn state, result ->
                    {data, error} =
                      case result do
                        {:ok, {:ok, data}} -> {data, nil}
                        {:ok, {:error, error}} -> {nil, error}
                        :error -> {nil, "launcher_crashed"}
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
                  {:worker, worker_id, worker_external_id, created_at}
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

            {:ok, {:ok, false, error}} ->
              deactivate_worker(state, worker_id, error)

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

  def handle_info(:suspend, state) do
    now = System.monotonic_time(:millisecond)

    # waiting map now uses external IDs
    # suspended: from_ext_id -> [dependency_ext_id]
    suspended =
      Enum.reduce(
        state.waiting,
        %{},
        fn {execution_ext_id, execution_waiting}, suspended ->
          Enum.reduce(
            execution_waiting,
            suspended,
            fn {from_ext_id, _, suspend_at}, suspended ->
              if suspend_at && suspend_at <= now do
                Map.update(suspended, from_ext_id, [execution_ext_id], &[execution_ext_id | &1])
              else
                suspended
              end
            end
          )
        end
      )

    # Resolve external IDs to internal for process_result
    state =
      Enum.reduce(
        suspended,
        state,
        fn {from_ext_id, dependency_ext_ids}, state ->
          from_execution_id = Map.fetch!(state.execution_ids, from_ext_id)

          # Resolve dependency IDs to internal (may reference completed executions in old epochs)
          dependency_ids =
            Enum.flat_map(dependency_ext_ids, fn dep_ext_id ->
              case resolve_internal_execution_id(state, dep_ext_id) do
                {:ok, id} -> [id]
                {:error, :not_found} -> []
              end
            end)

          {:ok, state} =
            process_result(state, from_execution_id, {:suspended, nil, dependency_ids})

          state
        end
      )

    state =
      state
      |> reschedule_next_suspend()
      |> flush_notifications()

    {:noreply, state}
  end

  def handle_info({task_ref, {run_bloom, cache_bloom}}, state)
      when task_ref == state.index_task do
    Process.demonitor(task_ref, [:flush])
    [epoch_id | rest] = state.index_queue

    epoch_index = EpochIndex.update_blooms(state.epoch_index, epoch_id, run_bloom, cache_bloom)
    :ok = EpochIndex.save(state.project_id, epoch_index)
    epochs = Epoch.promote_to_indexed(state.epochs, epoch_id)

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

              state =
                if session.worker_id do
                  worker = Map.fetch!(state.workers, session.worker_id)

                  notify_listeners(
                    state,
                    {:pool, workspace_external_id(state, session.workspace_id), worker.pool_name},
                    {:worker_connected, worker.external_id, false}
                  )
                else
                  state
                end

              state

            :error ->
              state
          end

        state = flush_notifications(state)

        {:noreply, state}

      Map.has_key?(state.listeners, ref) ->
        state = remove_listener(state, ref)
        {:noreply, state}

      Map.has_key?(state.launcher_tasks, ref) ->
        state = process_launcher_result(state, ref, :error)
        {:noreply, state}

      ref == state.index_task ->
        # Drop failed epoch from queue; it remains unindexed and will be
        # retried on next server startup (via EpochIndex.unindexed_epoch_ids)
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
    Epoch.close(state.epochs)
  end

  # Private helper functions

  defp do_cancel_execution(state, execution_id, principal_id) do
    root_execution_id =
      case Results.get_result(state.db, execution_id) do
        {:ok, {{:spawned, spawned_execution_id}, _created_at, _created_by}} ->
          spawned_execution_id

        {:ok, _other} ->
          execution_id
      end

    {:ok, executions} = Runs.get_execution_descendants(state.db, root_execution_id)

    state =
      Enum.reduce(
        executions,
        state,
        fn {execution_id, module, assigned_at, completed_at}, state ->
          if !completed_at do
            # Only record created_by for the root execution (the one user explicitly cancelled)
            created_by = if execution_id == root_execution_id, do: principal_id, else: nil

            state =
              case record_and_notify_result(
                     state,
                     execution_id,
                     :cancelled,
                     module,
                     created_by
                   ) do
                {:ok, state} -> state
                {:error, :already_recorded} -> state
              end

            if assigned_at do
              # abort_execution now takes external ID
              case Runs.get_execution_key(state.db, execution_id) do
                {:ok, {r, s, a}} ->
                  abort_execution(state, execution_external_id(r, s, a))

                {:error, :not_found} ->
                  state
              end
            else
              state
            end
          else
            state
          end
        end
      )

    state = flush_notifications(state)

    {:reply, :ok, state}
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
           Enum.reject(execution_waiting, fn {from_ext_id, _, _} ->
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
            # TODO: check that worker workspace matches session workspace?
            state
            |> put_in([Access.key(:workers), session.worker_id, :session_id], nil)
            |> notify_listeners(
              {:pool, workspace_external_id(state, worker.workspace_id), worker.pool_name},
              {:worker_connected, worker.external_id, false}
            )

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

        state =
          state
          |> put_in([Access.key(:execution_ids), execution_external_id], execution_id)
          |> notify_listeners(
            {:workflow, module, target_name, ws_ext_id},
            {:run, external_run_id, created_at, principal}
          )
          |> notify_listeners(
            {:modules, ws_ext_id},
            {:scheduled, module, execution_external_id, execute_at}
          )
          |> notify_listeners(
            {:module, module, ws_ext_id},
            {:scheduled, execution_external_id, target_name, external_run_id, step_number,
             attempt, execute_after, created_at}
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
    dependency_ids = Keyword.get(opts, :dependency_ids, [])
    created_by = Keyword.get(opts, :created_by)

    # Convert internal dependency execution IDs to execution_ref IDs
    dependency_ref_ids =
      Enum.map(dependency_ids, fn dep_id ->
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
           created_by
         ) do
      {:ok, execution_id, attempt, created_at} ->
        {:ok, {run_module, run_target}} = Runs.get_run_target(state.db, run.id)

        execute_at = execute_after || created_at

        principal =
          case Principals.get_principal(state.db, created_by) do
            {:ok, {type, external_id}} -> %{type: type, external_id: external_id}
            {:ok, nil} -> nil
          end

        dependencies =
          Map.new(dependency_ref_ids, fn ref_id ->
            metadata = resolve_execution_ref(state.db, ref_id)
            {metadata.execution_external_id, metadata}
          end)

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
            {:scheduled, step.module, execution_external_id, execute_at}
          )
          |> notify_listeners(
            {:module, step.module, ws_ext_id},
            {:scheduled, execution_external_id, step.target, run.external_id, step.number,
             attempt, execute_after, created_at}
          )
          |> notify_listeners(
            {:targets, ws_ext_id},
            {:step, step.module, step.target, step.type, run.external_id, step.number, attempt}
          )

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
      {:error, _, _, _} -> true
      :abandoned -> true
      _ -> false
    end
  end

  defp workspace_external_id(state, workspace_id) do
    state.workspaces[workspace_id].external_id
  end

  defp do_rotate_epoch(state) do
    {:ok, new_epochs, old_epoch_id} = Epoch.rotate(state.epochs)
    new_db = Epoch.active_db(new_epochs)

    # Get the old DB from the unindexed list (it was just appended)
    {^old_epoch_id, old_db} = List.last(new_epochs.unindexed)
    id_mappings = EpochCopy.copy_config(old_db, new_db)

    # Write placeholder entry to EpochIndex (nil blooms — built in background)
    now = System.os_time(:millisecond)

    epoch_index =
      EpochIndex.add_epoch(state.epoch_index, old_epoch_id, now, nil, nil)

    :ok = EpochIndex.save(state.project_id, epoch_index)

    state
    |> Map.put(:epochs, new_epochs)
    |> Map.put(:db, new_db)
    |> Map.put(:epoch_index, epoch_index)
    |> Map.update!(:index_queue, &(&1 ++ [old_epoch_id]))
    |> remap_config_ids(id_mappings)
    |> copy_in_flight_runs()
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
      find_and_copy_run_from_archives(state, run_ext_id)
    end)

    # Repopulate execution_ids cache from the new active DB
    execution_ids =
      Enum.reduce(in_flight_ext_ids, %{}, fn ext_id, acc ->
        {:ok, run_ext_id, step_num, attempt} = parse_execution_external_id(ext_id)

        case Runs.get_execution_id(state.db, run_ext_id, step_num, attempt) do
          {:ok, {id}} when not is_nil(id) ->
            Map.put(acc, ext_id, id)

          _ ->
            acc
        end
      end)

    %{state | execution_ids: execution_ids}
  end

  defp maybe_start_index_build(%{index_task: nil, index_queue: [epoch_id | _]} = state) do
    project_id = state.project_id
    path = Path.join(Epoch.epochs_dir(project_id), "#{epoch_id}.sqlite")

    task =
      Task.Supervisor.async_nolink(Coflux.LauncherSupervisor, fn ->
        {:ok, db} = Exqlite.Sqlite3.open(path)

        try do
          EpochIndex.build_blooms_for_epoch(db)
        after
          Exqlite.Sqlite3.close(db)
        end
      end)

    %{state | index_task: task.ref}
  end

  defp maybe_start_index_build(state), do: state

  defp remap_config_ids(state, %{} = mappings) do
    ws_map = Map.get(mappings, :workspace_ids, %{})
    session_map = Map.get(mappings, :session_ids, %{})
    worker_map = Map.get(mappings, :worker_ids, %{})
    pool_map = Map.get(mappings, :pool_ids, %{})

    # Remap workspaces: rekey map, update base_id values
    workspaces =
      Map.new(state.workspaces, fn {old_id, workspace} ->
        new_id = Map.get(ws_map, old_id, old_id)

        new_base_id =
          if workspace.base_id,
            do: Map.get(ws_map, workspace.base_id, workspace.base_id),
            else: nil

        {new_id, %{workspace | base_id: new_base_id}}
      end)

    # Remap workspace_names: values are workspace IDs
    workspace_names =
      Map.new(state.workspace_names, fn {name, old_id} ->
        {name, Map.get(ws_map, old_id, old_id)}
      end)

    # Remap workspace_external_ids: values are workspace IDs
    workspace_external_ids =
      Map.new(state.workspace_external_ids, fn {ext_id, old_id} ->
        {ext_id, Map.get(ws_map, old_id, old_id)}
      end)

    # Remap pools: rekey by new workspace_id, pool values contain pool_definition_id etc.
    pools =
      Map.new(state.pools, fn {old_ws_id, ws_pools} ->
        new_ws_id = Map.get(ws_map, old_ws_id, old_ws_id)
        # Pool values within each workspace map contain internal IDs that may need remapping
        new_ws_pools =
          Map.new(ws_pools, fn {pool_name, pool} ->
            new_pool = %{pool | id: Map.get(pool_map, pool.id, pool.id)}
            {pool_name, new_pool}
          end)

        {new_ws_id, new_ws_pools}
      end)

    # Remap workers: rekey map, update pool_id, workspace_id, session_id
    workers =
      Map.new(state.workers, fn {old_id, worker} ->
        new_id = Map.get(worker_map, old_id, old_id)

        new_worker = %{
          worker
          | pool_id: Map.get(pool_map, worker.pool_id, worker.pool_id),
            workspace_id: Map.get(ws_map, worker.workspace_id, worker.workspace_id),
            session_id:
              if(worker.session_id,
                do: Map.get(session_map, worker.session_id, worker.session_id),
                else: nil
              )
        }

        {new_id, new_worker}
      end)

    # Remap worker_external_ids: values are worker IDs
    worker_external_ids =
      Map.new(state.worker_external_ids, fn {ext_id, old_id} ->
        {ext_id, Map.get(worker_map, old_id, old_id)}
      end)

    # Remap sessions: rekey map, update workspace_id, worker_id
    sessions =
      Map.new(state.sessions, fn {old_id, session} ->
        new_id = Map.get(session_map, old_id, old_id)

        new_session = %{
          session
          | workspace_id: Map.get(ws_map, session.workspace_id, session.workspace_id),
            worker_id:
              if(session.worker_id,
                do: Map.get(worker_map, session.worker_id, session.worker_id),
                else: nil
              )
        }

        {new_id, new_session}
      end)

    # Remap session_ids: values are session IDs
    session_ids =
      Map.new(state.session_ids, fn {ext_id, old_id} ->
        {ext_id, Map.get(session_map, old_id, old_id)}
      end)

    # Remap session_expiries: rekey by new session_id
    session_expiries =
      Map.new(state.session_expiries, fn {old_id, expiry} ->
        {Map.get(session_map, old_id, old_id), expiry}
      end)

    # Remap connections: values contain session_id
    connections =
      Map.new(state.connections, fn {ref, {pid, old_session_id}} ->
        {ref, {pid, Map.get(session_map, old_session_id, old_session_id)}}
      end)

    # Remap targets: session_ids in MapSets
    targets =
      Map.new(state.targets, fn {module, module_targets} ->
        new_module_targets =
          Map.new(module_targets, fn {target_name, target} ->
            new_session_ids =
              MapSet.new(target.session_ids, fn old_sid ->
                Map.get(session_map, old_sid, old_sid)
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

  defp find_and_copy_run_from_archives(state, run_external_id) do
    query_fn = fn archive_db ->
      case Coflux.Store.query_one(
             archive_db,
             "SELECT id FROM runs WHERE external_id = ?1",
             {run_external_id}
           ) do
        {:ok, {_id}} ->
          {:ok, remap, _visited} = EpochCopy.copy_run(archive_db, state.db, run_external_id)
          {:found, {:ok, remap}}

        {:ok, nil} ->
          :not_found
      end
    end

    bloom_fn = fn epoch_index ->
      EpochIndex.find_epochs_for_run(epoch_index, run_external_id)
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
            Runs.resolve_workspace_ids(archive_db, workspace_external_ids)

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
                    # Copy the value into active epoch
                    value_for_active = copy_value_to_active_epoch(archive_db, state.db, value)
                    # Create execution ref for the cached execution
                    {:ok, {run_ext, step_num, attempt}} =
                      Runs.get_execution_key(archive_db, archive_exec_id)

                    {:ok, ref_id} =
                      Runs.get_or_create_execution_ref(state.db, run_ext, step_num, attempt)

                    {:found, {:resolved, ref_id, value_for_active}}

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
            EpochIndex.find_epochs_for_cache_key(epoch_index, cache_key)
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

  # Copy a value (as loaded by Values.get_value_by_id) from source_db into target_db.
  # The value is already in the decoded form {:raw, data, refs} or {:blob, key, size, refs}.
  # We need to re-create execution_refs in the target and ensure blobs/fragments exist.
  defp copy_value_to_active_epoch(source_db, target_db, value) do
    case value do
      {:raw, data, references} ->
        {:raw, data, copy_value_refs_to_active(source_db, target_db, references)}

      {:blob, key, size, references} ->
        {:blob, key, size, copy_value_refs_to_active(source_db, target_db, references)}
    end
  end

  defp copy_value_refs_to_active(source_db, target_db, references) do
    Enum.map(references, fn
      {:execution_ref, old_ref_id} ->
        {:ok, {run_ext, step_num, attempt}} = Runs.get_execution_ref(source_db, old_ref_id)

        {:ok, new_ref_id} =
          Runs.get_or_create_execution_ref(target_db, run_ext, step_num, attempt)

        {:execution_ref, new_ref_id}

      other ->
        other
    end)
  end

  # Searches archived epochs across both tiers (unindexed, then indexed via Bloom).
  # `query_fn` receives an archive DB handle and returns `{:found, result}` or `:not_found`.
  # `bloom_fn` receives the epoch index and returns candidate epoch IDs.
  defp search_archived_epochs(state, query_fn, bloom_fn) do
    # Tier 1: Check unindexed DBs (always open, newest first)
    unindexed = Epoch.unindexed_dbs(state.epochs)

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
        candidate_epoch_ids =
          if state.epoch_index do
            bloom_fn.(state.epoch_index)
          else
            []
          end

        Enum.reduce_while(candidate_epoch_ids, :not_found, fn epoch_id, :not_found ->
          path = Path.join(Epoch.epochs_dir(state.project_id), "#{epoch_id}.sqlite")

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

    execution_external_id =
      execution_external_id(external_run_id, step_number, step_attempt)

    %{
      execution_external_id: execution_external_id,
      run_id: external_run_id,
      step_id: "#{external_run_id}:#{step_number}",
      step_number: step_number,
      attempt: step_attempt,
      module: module,
      target: target
    }
  end

  defp resolve_execution_ref(db, ref_id) do
    {:ok, {run_ext, step_num, attempt}} = Runs.get_execution_ref(db, ref_id)
    ext_id = execution_external_id(run_ext, step_num, attempt)

    # Try to get module/target if execution is in current epoch
    {module, target} =
      case Runs.get_execution_id(db, run_ext, step_num, attempt) do
        {:ok, {id}} when not is_nil(id) ->
          {:ok, {_, _, _, m, t}} = Runs.get_run_by_execution(db, id)
          {m, t}

        _ ->
          {nil, nil}
      end

    %{
      execution_external_id: ext_id,
      run_id: run_ext,
      step_id: "#{run_ext}:#{step_num}",
      step_number: step_num,
      attempt: attempt,
      module: module,
      target: target
    }
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

      {:execution_ref, ref_id} ->
        metadata = resolve_execution_ref(db, ref_id)
        {:execution, metadata.execution_external_id, metadata}

      {:asset, asset_id} ->
        {external_asset_id, name, total_count, total_size, entry} = resolve_asset(db, asset_id)
        {:asset, external_asset_id, {name, total_count, total_size, entry}}
    end)
  end

  defp with_internal_ids(db, references) do
    Enum.map(references, fn
      {:asset, external_id} ->
        {:ok, id} = Assets.get_asset_id(db, external_id)
        {:asset, id}

      {:execution, execution_external_id} ->
        {:ok, run_ext_id, step_num, attempt} =
          parse_execution_external_id(execution_external_id)

        {:ok, ref_id} =
          Runs.get_or_create_execution_ref(db, run_ext_id, step_num, attempt)

        {:execution_ref, ref_id}

      ref ->
        ref
    end)
  end

  defp convert_value_to_internal_ids(db, {:raw, data, refs}),
    do: {:raw, data, with_internal_ids(db, refs)}

  defp convert_value_to_internal_ids(db, {:blob, key, size, refs}),
    do: {:blob, key, size, with_internal_ids(db, refs)}

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
      {:error, _, _, _, retry_id} -> is_nil(retry_id)
      {:value, _} -> true
      {:abandoned, retry_id} -> is_nil(retry_id)
      :cancelled -> true
      {:suspended, _} -> false
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

      {:suspended, successor_id} ->
        successor = if successor_id, do: resolve_execution(db, successor_id)
        {:suspended, successor}

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

  # TODO: remove 'module' argument?
  defp record_and_notify_result(state, execution_id, result, module, created_by \\ nil) do
    {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)
    {:ok, successors} = Runs.get_result_successors(state.db, execution_id)
    {:ok, {r, s, a}} = Runs.get_execution_key(state.db, execution_id)
    execution_external_id = execution_external_id(r, s, a)

    result =
      case result do
        {:value, value} -> {:value, convert_value_to_internal_ids(state.db, value)}
        other -> other
      end

    case Results.record_result(state.db, execution_id, result, created_by) do
      {:ok, created_at} ->
        state = notify_waiting(state, execution_id)

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
          |> notify_listeners(
            {:modules, ws_ext_id},
            {:completed, module, execution_external_id}
          )
          |> notify_listeners(
            {:module, module, ws_ext_id},
            {:completed, execution_external_id}
          )

        # TODO: only if there's an execution waiting for this result?
        send(self(), :tick)

        {:ok, state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp process_result(state, execution_id, result) do
    case Results.has_result?(state.db, execution_id) do
      {:ok, true} ->
        {:ok, state}

      {:ok, false} ->
        {:ok, step} = Runs.get_step_for_execution(state.db, execution_id)
        {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)

        # Compute external ID for abort_execution (which now takes external IDs)
        execution_ext_id =
          case Runs.get_execution_key(state.db, execution_id) do
            {:ok, {r, s, a}} -> execution_external_id(r, s, a)
            {:error, :not_found} -> nil
          end

        {retry_id, state} =
          cond do
            match?({:suspended, _, _}, result) ->
              {:suspended, execute_after, dependency_ids} = result

              # TODO: limit the number of times a step can suspend? (or rate?)

              {:ok, retry_id, _, state} =
                rerun_step(state, step, workspace_id,
                  execute_after: execute_after,
                  dependency_ids: dependency_ids
                )

              state =
                if execution_ext_id, do: abort_execution(state, execution_ext_id), else: state

              {retry_id, state}

            result_retryable?(result) && step.retry_limit == -1 ->
              # Unlimited retries - random delay between min and max
              delay_ms =
                step.retry_delay_min +
                  :rand.uniform() * (step.retry_delay_max - step.retry_delay_min)

              execute_after = System.os_time(:millisecond) + delay_ms

              {:ok, retry_id, _, state} =
                rerun_step(state, step, workspace_id, execute_after: execute_after)

              {retry_id, state}

            result_retryable?(result) && step.retry_limit > 0 ->
              # Limited retries - check consecutive failures
              {:ok, result_types} =
                Runs.get_step_result_types(state.db, step.id, step.retry_limit + 1)

              consecutive_failures =
                result_types
                |> Enum.take_while(&(&1 in [0, 2]))
                |> Enum.count()

              if consecutive_failures <= step.retry_limit do
                # TODO: add jitter (within min/max delay)
                delay_ms =
                  step.retry_delay_min +
                    (consecutive_failures - 1) / step.retry_limit *
                      (step.retry_delay_max - step.retry_delay_min)

                execute_after = System.os_time(:millisecond) + delay_ms

                {:ok, retry_id, _, state} =
                  rerun_step(state, step, workspace_id, execute_after: execute_after)

                {retry_id, state}
              else
                {nil, state}
              end

            step.recurrent == 1 and match?({:value, _}, result) ->
              execute_after =
                if step.delay > 0 do
                  System.os_time(:millisecond) + step.delay
                end

              {:ok, _, _, state} =
                rerun_step(state, step, workspace_id, execute_after: execute_after)

              {nil, state}

            true ->
              {nil, state}
          end

        result =
          case result do
            {:error, type, message, frames} -> {:error, type, message, frames, retry_id}
            :abandoned -> {:abandoned, retry_id}
            {:suspended, _, _} -> {:suspended, retry_id}
            other -> other
          end

        state =
          case record_and_notify_result(
                 state,
                 execution_id,
                 result,
                 step.module
               ) do
            {:ok, state} -> state
            {:error, :already_recorded} -> state
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
          {:error, _, _, _, execution_id} when not is_nil(execution_id) ->
            resolve_result(db, execution_id)

          {:abandoned, execution_id} when not is_nil(execution_id) ->
            resolve_result(db, execution_id)

          # In-flight successor (follow chain)
          {:deferred, execution_id} when is_integer(execution_id) ->
            resolve_result(db, execution_id)

          {:cached, execution_id} when is_integer(execution_id) ->
            resolve_result(db, execution_id)

          {:suspended, execution_id} ->
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

  defp has_requirements?(provides, requires) do
    # TODO: case insensitive matching?
    Enum.all?(requires, fn {key, requires_values} ->
      (provides || %{})
      |> Map.get(key, [])
      |> Enum.any?(&(&1 in requires_values))
    end)
  end

  defp arguments_ready?(db, wait_for, arguments) do
    Enum.all?(wait_for, fn index ->
      references =
        case Enum.at(arguments, index) do
          {:raw, _, references} -> references
          {:blob, _, _, references} -> references
          nil -> []
        end

      Enum.all?(references, fn
        {:execution_ref, ref_id} ->
          {:ok, {run_ext, step_num, attempt}} = Runs.get_execution_ref(db, ref_id)

          case Runs.get_execution_id(db, run_ext, step_num, attempt) do
            {:ok, {execution_id}} when not is_nil(execution_id) ->
              case resolve_result(db, execution_id) do
                {:ok, _} -> true
                {:pending, _} -> false
              end

            _ ->
              false
          end

        {:fragment, _format, _blob_key, _size, _metadata} ->
          true

        {:asset, _asset_id} ->
          true
      end)
    end)
  end

  defp dependencies_ready?(db, execution_id) do
    # TODO: also check assets?
    case Runs.get_result_dependencies(db, execution_id) do
      {:ok, dependencies} ->
        Enum.all?(dependencies, fn {dependency_ref_id} ->
          {:ok, {run_ext, step_num, attempt}} = Runs.get_execution_ref(db, dependency_ref_id)

          case Runs.get_execution_id(db, run_ext, step_num, attempt) do
            {:ok, {dep_execution_id}} when not is_nil(dep_execution_id) ->
              case resolve_result(db, dep_execution_id) do
                {:ok, _} -> true
                {:pending, _} -> false
              end

            _ ->
              # Execution not in current epoch — cannot check
              false
          end
        end)
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
            has_requirements?(session.provides, requires)
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
        pool.launcher && execution.module in pool.modules &&
          has_requirements?(pool.provides, requires)
      end)

    if Enum.any?(pools) do
      pools |> Map.values() |> Enum.map(& &1.id) |> Enum.random()
    end
  end

  defp reschedule_next_suspend(state) do
    if state.suspend_timer do
      Process.cancel_timer(state.suspend_timer)
    end

    next_suspend_at =
      state.waiting
      |> Map.values()
      |> Enum.flat_map(fn execution_waiting ->
        Enum.map(execution_waiting, fn {_, _, suspend_at} -> suspend_at end)
      end)
      |> Enum.reject(&is_nil/1)
      |> Enum.max(fn -> nil end)

    timer =
      if next_suspend_at do
        Process.send_after(self(), :suspend, next_suspend_at, abs: true)
      end

    Map.put(state, :suspend_timer, timer)
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
              fn {from_ext_id, request_id, _}, state ->
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

      reschedule_next_suspend(state)
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

  # abort_execution now takes an external execution ID
  defp abort_execution(state, execution_ext_id) do
    # Clean up waiting map entries where this execution is the waiter
    state =
      Enum.reduce(
        state.waiting,
        state,
        fn {for_ext_id, execution_waiting}, state ->
          execution_waiting =
            Enum.reject(execution_waiting, fn {from_ext_id, _, _} ->
              from_ext_id == execution_ext_id
            end)

          Map.update!(state, :waiting, fn waiting ->
            if Enum.any?(execution_waiting) do
              Map.put(waiting, for_ext_id, execution_waiting)
            else
              Map.delete(waiting, for_ext_id)
            end
          end)
        end
      )

    # find_session_for_execution now takes external ID
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

  defp call_launcher(state, launcher, fun, args, callback) do
    module =
      case launcher.type do
        :docker -> Coflux.DockerLauncher
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

  defp deactivate_worker(state, worker_id, error) do
    {:ok, deactivated_at} = Workers.create_worker_deactivation(state.db, worker_id, error)

    {worker, state} = pop_in(state, [Access.key(:workers), worker_id])

    state = Map.update!(state, :worker_external_ids, &Map.delete(&1, worker.external_id))

    notify_listeners(
      state,
      {:pool, workspace_external_id(state, worker.workspace_id), worker.pool_name},
      {:worker_deactivated, worker.external_id, deactivated_at, error}
    )
  end

  defp schedule_session_expiry(state, session_id, timeout_ms) do
    expiry_at = System.os_time(:millisecond) + timeout_ms
    state = put_in(state.session_expiries[session_id], expiry_at)
    reschedule_expiry_timer(state)
  end

  defp cancel_session_expiry(state, session_id) do
    state = Map.update!(state, :session_expiries, &Map.delete(&1, session_id))
    reschedule_expiry_timer(state)
  end

  defp reschedule_expiry_timer(state) do
    if state.expiry_timer do
      Process.cancel_timer(state.expiry_timer)
    end

    case state.session_expiries |> Map.values() |> Enum.min(fn -> nil end) do
      nil ->
        %{state | expiry_timer: nil}

      next_expiry ->
        delay = max(0, next_expiry - System.os_time(:millisecond))
        timer = Process.send_after(self(), :expire_sessions, delay)
        %{state | expiry_timer: timer}
    end
  end
end
