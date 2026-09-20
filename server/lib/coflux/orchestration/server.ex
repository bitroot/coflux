defmodule Coflux.Orchestration.Server do
  alias Coflux.Events.{
    AssetDependencyRecorded,
    AssetPut,
    CheckpointsSet,
    ChildLinked,
    ExecutionScheduled,
    GroupCreated,
    InputResponded,
    InputSubmitted,
    ManifestRegistered,
    MetricDefined,
    ModuleArchived,
    PoolStateChanged,
    PoolUpdated,
    SessionConnected,
    SessionExecuting,
    StepArguments,
    StepCreated,
    StreamDependencyRecorded,
    SecretDeleted,
    SecretSet,
    TokenCreated,
    TokenRevoked,
    WorkspaceCreated,
    WorkspaceStateChanged,
    WorkspaceUpdated
  }

  alias Coflux.Scopes
  alias Coflux.Store.{Epochs, Index}

  alias Coflux.Orchestration.{
    Assets,
    Catalog,
    Checkpoints,
    Ids,
    Inputs,
    Manifests,
    Principals,
    Results,
    Runs,
    Sessions,
    Streams,
    TagSets,
    Values,
    Workers,
    Workspaces
  }

  alias Coflux.Orchestration.Server.{
    Archives,
    Cancellation,
    CatalogFlow,
    Commands,
    Dependencies,
    Effects,
    Fleet,
    Gates,
    InputFlow,
    Lifecycle,
    Listeners,
    Permissions,
    Resolve,
    Rotation,
    Scheduler,
    Scheduling,
    Snapshots,
    State,
    StreamDelivery,
    Waiters
  }

  use GenServer, restart: :transient
  require Logger

  @default_activation_timeout_ms 600_000
  @default_reconnection_timeout_ms 30_000
  @rotation_check_interval_ms 60_000
  @rotation_size_threshold_bytes 100 * 1024 * 1024

  def start_link(opts) do
    {project_id, opts} = Keyword.pop!(opts, :project_id)
    GenServer.start_link(__MODULE__, project_id, opts)
  end

  @impl true
  def init(project_id) do
    index_path = ["projects", project_id, "orchestration", "index.json"]

    {:ok, epoch_index} =
      Index.load(index_path, ["runs", "cache_keys", "idempotency_keys"])

    unindexed_epoch_ids = Index.unindexed_epoch_ids(epoch_index)
    archived_epoch_ids = Index.all_epoch_ids(epoch_index)

    case Epochs.open(project_id, "orchestration",
           unindexed_epoch_ids: unindexed_epoch_ids,
           archived_epoch_ids: archived_epoch_ids
         ) do
      {:ok, epochs} ->
        db = Epochs.active_db(epochs)

        # The admin store isn't an epoch: it holds what outlives them.
        {:ok, admin_db} = Coflux.Store.open(project_id, "admin")
        :ok = Coflux.Admin.Tokens.import_legacy(db, admin_db)

        state = %State{
          project_id: project_id,
          db: db,
          admin_db: admin_db,
          epochs: epochs,
          epoch_index: epoch_index,
          index_queue: unindexed_epoch_ids
        }

        send(self(), :tick)

        {:ok, state, {:continue, :setup}}
    end
  end

  @impl true
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
            stop_retry_at: nil,
            last_poll_at: nil,
            polling: false,
            poll_failures: 0,
            first_poll_failure_at: nil
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

    # Each owner restores what it holds from the database. Nothing here
    # knows the shape of another's state, so a new index is one module's
    # problem rather than this function's.
    state =
      state
      |> Fleet.load()
      |> State.load_indexes()
      |> Dependencies.rebuild()

    # Schedule periodic epoch rotation check
    Process.send_after(self(), :check_rotation, @rotation_check_interval_ms)

    # Kick off background Bloom filter builds for any unindexed epochs
    state = Rotation.maybe_start_index_build(state)

    # Schedule idle shutdown if no sessions or listeners yet
    state = Listeners.maybe_schedule_idle_shutdown(state)

    flush({:noreply, state})
  end

  # Every operation ends by flushing what it recorded: the callbacks below
  # do that once, here, so no individual clause can forget to. Flushing
  # before the reply is returned keeps the guarantee subscribers rely on -
  # that the events of an operation arrive before whatever the caller does
  # next in response to its reply.
  @impl true
  def handle_call(request, _from, state), do: flush(dispatch_call(request, state))

  @impl true
  def handle_cast(request, state), do: flush(dispatch_cast(request, state))

  @impl true
  def handle_info(message, state), do: flush(dispatch_info(message, state))

  defp flush(result) do
    case result do
      {:reply, reply, state} -> {:reply, reply, Effects.flush(state)}
      {:noreply, state} -> {:noreply, Effects.flush(state)}
      {:stop, reason, state} -> {:stop, reason, Effects.flush(state)}
    end
  end

  defp dispatch_call(:get_workspaces, state) do
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

  defp dispatch_call({:ensure_principal, external_id}, state) do
    {:ok, principal_id} = Principals.ensure_user(state.db, external_id)
    {:reply, {:ok, principal_id}, state}
  end

  # Token management

  defp dispatch_call({:check_token, token_hash}, state) do
    with {:ok, %{external_id: external_id, workspaces: workspaces}} <-
           Coflux.Admin.Tokens.check_token(state.admin_db, token_hash),
         {:ok, principal_id} <- Principals.ensure_token(state.db, external_id) do
      {:reply, {:ok, %{workspaces: workspaces, principal_id: principal_id}}, state}
    else
      {:error, :not_found} ->
        {:reply, {:error, :not_found}, state}
    end
  end

  defp dispatch_call({:create_token, name, principal_id, opts}, state) do
    created_by =
      case Principals.get_principal(state.db, principal_id) do
        {:ok, {type, external_id}} -> %{type: type, external_id: external_id}
        {:ok, nil} -> nil
      end

    {:ok, result} =
      Coflux.Admin.Tokens.create_token(state.admin_db, state.project_id, name, created_by, opts)

    # The token can act straight away, so give it its principal now rather
    # than on first use.
    {:ok, token_principal_id} = Principals.ensure_token(state.db, result.external_id)
    result = Map.put(result, :principal_id, token_principal_id)

    state =
      state
      |> Effects.emit(%TokenCreated{
        token: result.external_id,
        id: result.id,
        name: result.name,
        workspaces: result.workspaces,
        created_at: result.created_at,
        expires_at: result.expires_at,
        created_by: result.created_by
      })

    {:reply, {:ok, result}, state}
  end

  defp dispatch_call(:list_tokens, state) do
    {:ok, tokens} = Coflux.Admin.Tokens.list_tokens(state.admin_db)
    {:reply, {:ok, tokens}, state}
  end

  defp dispatch_call({:revoke_token, token_id}, state) do
    case Coflux.Admin.Tokens.revoke_token(state.admin_db, token_id) do
      {:ok, external_id} ->
        state = Effects.emit(state, %TokenRevoked{token: external_id})
        {:reply, {:ok, external_id}, state}

      error ->
        {:reply, error, state}
    end
  end

  # Secrets

  # A secret is set for one or more scopes, each stored on its own. Access
  # to every scope is checked before any is written, so a request that
  # isn't wholly allowed changes nothing.
  defp dispatch_call({:set_secret, scopes, name, value, access}, state) do
    with :ok <- check_secret_scope_access(access, scopes) do
      identity = principal_identity(state, access)

      Enum.reduce_while(scopes, {:reply, {:ok, []}, state}, fn scope,
                                                               {:reply, {:ok, secrets}, state} ->
        case Coflux.Admin.Secrets.set(
               state.admin_db,
               state.project_id,
               scope,
               name,
               value,
               identity
             ) do
          {:ok, secret} ->
            state =
              Effects.emit(state, %SecretSet{
                scope: secret.scope,
                name: secret.name,
                version: secret.version,
                created_at: secret.created_at,
                updated_at: secret.updated_at,
                updated_by: secret.updated_by
              })

            {:cont, {:reply, {:ok, secrets ++ [secret]}, state}}

          {:error, reason} ->
            {:halt, {:reply, {:error, reason}, state}}
        end
      end)
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  # Deleting is done scope by scope: it is not an error for a secret to be
  # absent from some of them, only from all of them.
  defp dispatch_call({:delete_secret, scopes, name, access}, state) do
    with :ok <- check_secret_scope_access(access, scopes) do
      {state, deleted} =
        Enum.reduce(scopes, {state, []}, fn scope, {state, deleted} ->
          case Coflux.Admin.Secrets.delete(state.admin_db, scope, name) do
            :ok ->
              {Effects.emit(state, %SecretDeleted{scope: scope, name: name}), deleted ++ [scope]}

            {:error, :not_found} ->
              {state, deleted}
          end
        end)

      if deleted == [] do
        {:reply, {:error, :not_found}, state}
      else
        {:reply, {:ok, deleted}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp dispatch_call({:get_token, external_id}, state) do
    case Coflux.Admin.Tokens.get_token_by_external_id(state.admin_db, external_id) do
      {:ok, nil} ->
        {:reply, {:ok, nil}, state}

      {:ok, token} ->
        # Whether the caller may revoke it is decided by principal id, and
        # a principal is local to this epoch, so the creator's identity is
        # given one here if it hasn't one yet.
        {:ok, created_by_principal_id} = Principals.ensure_identity(state.db, token.created_by)
        token = Map.put(token, :created_by_principal_id, created_by_principal_id)
        {:reply, {:ok, token}, state}
    end
  end

  # Workspace management

  defp dispatch_call({:create_workspace, name, base_external_id, access}, state) do
    with :ok <- Permissions.check_operator_access(access, name),
         {:ok, base_id} <- Permissions.resolve_optional_workspace(state, base_external_id) do
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
            |> Effects.emit(%WorkspaceCreated{
              workspace: workspace.external_id,
              name: workspace.name,
              base: base_external_id,
              state: workspace.state
            })

          {:reply, {:ok, workspace_id, workspace.external_id}, state}

        {:error, error} ->
          {:reply, {:error, error}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp dispatch_call({:update_workspace, workspace_external_id, updates, access}, state) do
    with {:ok, workspace_id} <-
           Permissions.resolve_workspace_external_id(state, workspace_external_id),
         :ok <- Permissions.check_operator_access(access, state.workspaces[workspace_id].name),
         :ok <- Permissions.check_rename_allowed(access, updates[:name]) do
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
            |> Effects.emit(%WorkspaceUpdated{
              workspace: workspace.external_id,
              name: workspace.name,
              base: base_external_id,
              state: workspace.state
            })

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

  defp dispatch_call({:pause_workspace, workspace_external_id, access}, state) do
    case Permissions.require_workspace(state, workspace_external_id, access) do
      {:ok, workspace_id, _} ->
        case Workspaces.pause_workspace(state.db, workspace_id, access[:principal_id]) do
          :ok ->
            state =
              state
              |> put_in([Access.key(:workspaces), workspace_id, Access.key(:state)], :paused)
              |> Effects.emit(%WorkspaceStateChanged{
                workspace: workspace_external_id,
                state: :paused
              })

            {:reply, :ok, state}
        end

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp dispatch_call({:resume_workspace, workspace_external_id, access}, state) do
    case Permissions.require_workspace(state, workspace_external_id, access) do
      {:ok, workspace_id, _} ->
        case Workspaces.resume_workspace(state.db, workspace_id, access[:principal_id]) do
          :ok ->
            state =
              state
              |> put_in([Access.key(:workspaces), workspace_id, Access.key(:state)], :active)
              |> Effects.emit(%WorkspaceStateChanged{
                workspace: workspace_external_id,
                state: :active
              })

            send(self(), :tick)

            {:reply, :ok, state}
        end

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp dispatch_call({:archive_workspace, workspace_external_id, access}, state) do
    case Permissions.require_workspace(state, workspace_external_id, access) do
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

                Fleet.remove_session(state, session_id)
              end)

            state =
              case Runs.get_pending_executions_for_workspace(state.db, workspace_id) do
                {:ok, executions} ->
                  Enum.reduce(executions, state, fn {execution_id, _run_id, module}, state ->
                    case Lifecycle.record_and_notify_result(
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
                  Fleet.update_worker_state(
                    state,
                    worker_id,
                    :draining,
                    workspace_id,
                    worker.pool_name
                  )
                else
                  state
                end
              end)
              |> put_in([Access.key(:workspaces), workspace_id, Access.key(:state)], :archived)
              |> Effects.emit(%WorkspaceStateChanged{
                workspace: workspace_external_id,
                state: :archived
              })
              |> Listeners.maybe_schedule_idle_shutdown()

            {:reply, :ok, state}

          {:error, error} ->
            {:reply, {:error, error}, state}
        end

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp dispatch_call({:get_pools, workspace_external_id}, state) do
    with {:ok, workspace_id, _} <- Permissions.require_workspace(state, workspace_external_id) do
      case Workspaces.get_workspace_pools(state.db, workspace_id) do
        {:ok, pools, hash} ->
          {:reply, {:ok, pools, hash}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp dispatch_call(
         {:update_pools, workspace_external_id, desired_pools, expected_hash, access},
         state
       ) do
    with {:ok, workspace_id, _} <-
           Permissions.require_workspace(state, workspace_external_id, access),
         :ok <-
           Enum.reduce_while(desired_pools, :ok, fn {_name, pool}, :ok ->
             case check_secret_references(state, workspace_id, pool[:launcher]) do
               :ok -> {:cont, :ok}
               error -> {:halt, error}
             end
           end) do
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
              # Pool names are per workspace: a pool of the same name in
              # another workspace is a different pool, and its workers are
              # not ours to drain.
              if worker.state == :active && worker.workspace_id == workspace_id &&
                   MapSet.member?(changed_pool_names, worker.pool_name) do
                Fleet.update_worker_state(
                  state,
                  worker_id,
                  :draining,
                  workspace_id,
                  worker.pool_name
                )
              else
                state
              end
            end)

          # Update in-memory pools (reload from DB to include :id and :state)
          ws_ext_id = State.workspace_external_id(state, workspace_id)

          {:ok, updated_pools, _hash} =
            Workspaces.get_workspace_pools(state.db, workspace_id)

          state =
            put_in(state, [Access.key(:pools), Access.key(workspace_id, %{})], updated_pools)

          # Send notifications only for changed pools
          state =
            Enum.reduce(changed_pool_names, state, fn name, state ->
              pool = Map.get(updated_pools, name)

              Effects.emit(state, %PoolUpdated{workspace: ws_ext_id, pool: name, definition: pool})
            end)

          {:reply, :ok, state}

        {:error, :conflict} ->
          {:reply, {:error, :conflict}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp dispatch_call(
         {:create_pool, workspace_external_id, pool_name, pool, access},
         state
       ) do
    with {:ok, workspace_id, _} <-
           Permissions.require_workspace(state, workspace_external_id, access),
         :ok <- check_secret_references(state, workspace_id, pool[:launcher]) do
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

          ws_ext_id = State.workspace_external_id(state, workspace_id)
          pool = Map.get(updated_pools, pool_name)

          state =
            state
            |> put_in([Access.key(:pools), Access.key(workspace_id, %{})], updated_pools)
            |> Effects.emit(%PoolUpdated{workspace: ws_ext_id, pool: pool_name, definition: pool})

          {:reply, :ok, state}

        {:error, :already_exists} ->
          {:reply, {:error, :already_exists}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp dispatch_call(
         {:update_pool, workspace_external_id, pool_name, pool_patch, access},
         state
       ) do
    with {:ok, workspace_id, _} <-
           Permissions.require_workspace(state, workspace_external_id, access),
         :ok <- check_secret_references(state, workspace_id, pool_patch[:launcher]) do
      case Workspaces.update_pool(
             state.db,
             workspace_id,
             pool_name,
             pool_patch,
             access[:principal_id]
           ) do
        {:ok, _pool_id, :updated} ->
          state =
            state.workers
            |> Enum.reduce(state, fn {worker_id, worker}, state ->
              if worker.state == :active && worker.workspace_id == workspace_id &&
                   worker.pool_name == pool_name do
                Fleet.update_worker_state(state, worker_id, :draining, workspace_id, pool_name)
              else
                state
              end
            end)

          # Reload pools from DB to get the merged result with :id and :state
          {:ok, updated_pools, _hash} =
            Workspaces.get_workspace_pools(state.db, workspace_id)

          ws_ext_id = State.workspace_external_id(state, workspace_id)
          pool = Map.get(updated_pools, pool_name)

          state =
            state
            |> put_in([Access.key(:pools), Access.key(workspace_id, %{})], updated_pools)
            |> Effects.emit(%PoolUpdated{workspace: ws_ext_id, pool: pool_name, definition: pool})

          {:reply, :ok, state}

        {:ok, _pool_id, :unchanged} ->
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

  defp dispatch_call({:disable_pool, workspace_external_id, pool_name, access}, state) do
    with {:ok, workspace_id, _} <-
           Permissions.require_workspace(state, workspace_external_id, access),
         :ok <- require_pool(state, workspace_id, pool_name) do
      :ok = Workspaces.disable_pool(state.db, workspace_id, pool_name, access[:principal_id])

      state =
        state
        |> put_in(
          [Access.key(:pools), Access.key!(workspace_id), Access.key!(pool_name), :state],
          :disabled
        )
        |> Effects.emit(%PoolStateChanged{
          workspace: workspace_external_id,
          pool: pool_name,
          state: :disabled
        })

      send(self(), :tick)

      {:reply, :ok, state}
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp dispatch_call({:enable_pool, workspace_external_id, pool_name, access}, state) do
    with {:ok, workspace_id, _} <-
           Permissions.require_workspace(state, workspace_external_id, access),
         :ok <- require_pool(state, workspace_id, pool_name) do
      :ok = Workspaces.enable_pool(state.db, workspace_id, pool_name, access[:principal_id])

      state =
        state
        |> put_in(
          [Access.key(:pools), Access.key!(workspace_id), Access.key!(pool_name), :state],
          :active
        )
        |> Effects.emit(%PoolStateChanged{
          workspace: workspace_external_id,
          pool: pool_name,
          state: :active
        })

      send(self(), :tick)

      {:reply, :ok, state}
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp dispatch_call({:stop_worker, workspace_external_id, worker_external_id, access}, state) do
    with {:ok, workspace_id, _} <-
           Permissions.require_workspace(state, workspace_external_id, access),
         {:ok, worker_id} <- Fleet.resolve_worker_external_id(state, worker_external_id),
         {:ok, worker} <- Fleet.lookup_worker(state, worker_id, workspace_id) do
      state =
        state
        |> Fleet.update_worker_state(
          worker_id,
          :draining,
          workspace_id,
          worker.pool_name,
          access[:principal_id]
        )

      send(self(), :tick)

      {:reply, :ok, state}
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp dispatch_call(
         {:resume_worker, workspace_external_id, worker_external_id, access},
         state
       ) do
    with {:ok, workspace_id, _} <-
           Permissions.require_workspace(state, workspace_external_id, access),
         {:ok, worker_id} <- Fleet.resolve_worker_external_id(state, worker_external_id),
         {:ok, worker} <- Fleet.lookup_worker(state, worker_id, workspace_id) do
      state =
        state
        |> Fleet.update_worker_state(
          worker_id,
          :active,
          workspace_id,
          worker.pool_name,
          access[:principal_id]
        )

      send(self(), :tick)

      {:reply, :ok, state}
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp dispatch_call(
         {:register_manifests, workspace_external_id, manifests, access},
         state
       ) do
    case Permissions.require_workspace(state, workspace_external_id, access) do
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
            ws_ext_id = State.workspace_external_id(state, workspace_id)

            # A module registered with no workflows is stored as archived.
            state =
              manifests
              |> Enum.reduce(state, fn {module, workflows}, state ->
                if workflows && map_size(workflows) > 0 do
                  Effects.emit(state, %ManifestRegistered{
                    workspace: ws_ext_id,
                    module: module,
                    workflows: workflows
                  })
                else
                  Effects.emit(state, %ModuleArchived{workspace: ws_ext_id, module: module})
                end
              end)

            {:reply, :ok, state}
        end
    end
  end

  defp dispatch_call({:archive_module, workspace_external_id, module_name, access}, state) do
    case Permissions.require_workspace(state, workspace_external_id, access) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, workspace_id, _} ->
        case Manifests.archive_module(state.db, workspace_id, module_name, access[:principal_id]) do
          :ok ->
            ws_ext_id = State.workspace_external_id(state, workspace_id)

            state =
              state
              |> Effects.emit(%ModuleArchived{workspace: ws_ext_id, module: module_name})

            {:reply, :ok, state}
        end
    end
  end

  defp dispatch_call({:get_manifests, workspace_external_id}, state) do
    case Permissions.require_workspace(state, workspace_external_id) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, workspace_id, _} ->
        {:ok, manifests} = Manifests.get_latest_manifests(state.db, workspace_id)
        {:reply, {:ok, manifests}, state}
    end
  end

  defp dispatch_call({:get_workflow, workspace_external_id, module, target_name}, state) do
    with {:ok, workspace_id, _} <- Permissions.require_workspace(state, workspace_external_id),
         {:ok, workflow} <-
           Manifests.get_latest_workflow(state.db, workspace_id, module, target_name) do
      {:reply, {:ok, workflow}, state}
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp dispatch_call({:create_session, workspace_external_id, access, opts}, state) do
    provides = Keyword.get(opts, :provides, %{})
    accepts = Keyword.get(opts, :accepts, %{})
    activation_timeout = Keyword.get(opts, :activation_timeout, @default_activation_timeout_ms)

    reconnection_timeout =
      Keyword.get(opts, :reconnection_timeout, @default_reconnection_timeout_ms)

    with {:ok, workspace_id, _} <-
           Permissions.require_workspace(state, workspace_external_id, access) do
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
            draining: false,
            workspace_id: workspace_id,
            provides: provides,
            accepts: accepts,
            worker_id: nil,
            last_idle_at: now,
            activated_at: nil,
            declared_at: nil,
            ready_deadline_at: nil,
            activation_timeout: activation_timeout,
            reconnection_timeout: reconnection_timeout,
            total_executions: 0
          }

          state =
            state
            |> put_in([Access.key(:sessions), session_id], session)
            |> put_in([Access.key(:session_ids), external_session_id], session_id)
            |> Fleet.schedule_session_expiry(session_id, activation_timeout)
            |> Listeners.maybe_schedule_idle_shutdown()

          {:reply, {:ok, token}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp dispatch_call({:verify_session, token}, state) do
    with {:ok, external_id, secret} <- Sessions.parse_token(token),
         {:ok, session_id} <- Map.fetch(state.session_ids, external_id),
         session = Map.fetch!(state.sessions, session_id),
         :ok <- Permissions.verify_session_secret(secret, session.secret_hash) do
      workspace = Map.fetch!(state.workspaces, session.workspace_id)
      {:reply, {:ok, %{type: :session, workspaces: [workspace.name], principal_id: nil}}, state}
    else
      _ -> {:reply, {:error, :session_invalid}, state}
    end
  end

  defp dispatch_call({:resume_session, token, workspace_external_id, pid}, state) do
    with {:ok, external_id, secret} <- Sessions.parse_token(token),
         {:ok, session_id} <- Map.fetch(state.session_ids, external_id),
         session = Map.fetch!(state.sessions, session_id),
         :ok <- Permissions.verify_session_secret(secret, session.secret_hash),
         {:ok, workspace_id, _} <- Permissions.require_workspace(state, workspace_external_id),
         :ok <- Permissions.require_workspace_match(session.workspace_id, workspace_id) do
      activated_at =
        if is_nil(session.activated_at) do
          {:ok, now} = Sessions.activate_session(state.db, session_id)
          now
        else
          session.activated_at
        end

      # Cancel any pending expiry (activation or reconnection)
      state = Fleet.cancel_session_expiry(state, session_id)

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

      # A worker that has connected but not yet said what it can run is
      # given until this deadline to do so, after which it is treated as
      # broken rather than idle (see `Scheduler`). Each connection gets a
      # fresh one; a session that has already declared keeps none.
      ready_deadline_at =
        if is_nil(session.declared_at) do
          System.os_time(:millisecond) + session.activation_timeout
        end

      state =
        state
        |> put_in([Access.key(:connections), ref], {pid, session_id})
        |> update_in(
          [Access.key(:sessions), session_id],
          &Map.merge(&1, %{
            connection: ref,
            queue: [],
            activated_at: activated_at,
            ready_deadline_at: ready_deadline_at
          })
        )

      state = Effects.emit(state, Fleet.session_event(state, session))

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

  defp dispatch_call({:declare_targets, external_id, targets, concurrency}, state) do
    session_id = Map.fetch!(state.session_ids, external_id)

    now = System.os_time(:millisecond)

    previous = Map.fetch!(state.sessions, session_id)

    state =
      state
      |> Fleet.assign_targets(targets, session_id)
      |> put_in([Access.key(:sessions), session_id, :concurrency], concurrency)
      |> put_in([Access.key(:sessions), session_id, :last_idle_at], now)
      # The worker has answered, so it is ready and the deadline for
      # answering no longer applies - even if it declared nothing, which
      # is an empty manifest rather than a broken worker.
      |> put_in([Access.key(:sessions), session_id, :declared_at], previous.declared_at || now)
      |> put_in([Access.key(:sessions), session_id, :ready_deadline_at], nil)

    # A pool that produces a working worker has no failures to back off
    # from, whatever its previous launches did.
    state =
      case previous.worker_id && Map.fetch(state.workers, previous.worker_id) do
        {:ok, worker} -> Map.update!(state, :pool_failures, &Map.delete(&1, worker.pool_id))
        _ -> state
      end

    session = Map.fetch!(state.sessions, session_id)

    state =
      state
      |> Effects.emit(Fleet.session_event(state, session))

    send(self(), :tick)

    {:reply, :ok, state}
  end

  defp dispatch_call({:session_draining, external_id}, state) do
    case Map.fetch(state.session_ids, external_id) do
      {:ok, session_id} ->
        state = put_in(state, [Access.key(:sessions), session_id, :draining], true)
        session = Map.fetch!(state.sessions, session_id)

        state =
          state
          |> Effects.emit(Fleet.session_event(state, session))

        {:reply, :ok, state}

      :error ->
        {:reply, :ok, state}
    end
  end

  defp dispatch_call({:start_run, module, target_name, type, arguments, access, opts}, state) do
    workspace_external_id = Keyword.get(opts, :workspace)

    with {:ok, workspace_id, _} <-
           Permissions.require_workspace(state, workspace_external_id, access),
         :ok <- validate_values_assets(state.db, arguments),
         {:ok, catalog_sequence} <-
           CatalogFlow.resolve_catalog_option(state, workspace_id, Keyword.get(opts, :catalog)) do
      client_key = Keyword.get(opts, :idempotency_key)
      ws_ext_id = State.workspace_external_id(state, workspace_id)

      case Archives.maybe_find_idempotent_run(state, client_key, ws_ext_id) do
        {:hit, ext_run_id, step_number, attempt} ->
          execution_external_id = Ids.execution(ext_run_id, step_number, attempt)
          {:reply, {:ok, ext_run_id, step_number, execution_external_id}, state}

        :miss ->
          opts =
            opts
            |> Keyword.delete(:catalog)
            |> Keyword.put(:catalog_sequence, catalog_sequence)

          opts =
            if client_key do
              hashed = Runs.build_idempotency_key(ws_ext_id, client_key)
              Keyword.put(opts, :idempotency_key, hashed)
            else
              opts
            end

          {:ok, external_run_id, step_number, _execution_id, state} =
            Scheduling.schedule_run(
              state,
              module,
              target_name,
              type,
              arguments,
              workspace_id,
              Keyword.put(opts, :created_by, access[:principal_id])
            )

          execution_external_id = Ids.execution(external_run_id, step_number, 1)

          send(self(), :tick)
          {:reply, {:ok, external_run_id, step_number, execution_external_id}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp dispatch_call(
         {:schedule_step, parent_external_id, module, target_name, type, arguments, opts},
         state
       ) do
    parent_id = Map.fetch!(state.execution_ids, parent_external_id)
    {:ok, parent_run_id} = Runs.get_execution_run_id(state.db, parent_id)
    {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, parent_id)
    {:ok, run} = Runs.get_run_by_id(state.db, parent_run_id)

    cache_workspace_ids = Permissions.get_cache_workspace_ids(state, workspace_id)
    arguments = Enum.map(arguments, &Values.normalize(&1))

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
         concurrency_key: concurrency_key,
         group_key: group_key,
         group_limit: group_limit,
         memo_key: memo_key,
         memo_hit: memo_hit,
         child_added: child_added
       }} ->
        # Compute and register pending dependencies for non-memoised executions
        {state, pending_dependencies, argument_dependencies, unresolved_dependencies} =
          if step_id && !memo_hit do
            wait_for = Keyword.get(opts, :wait_for) || []

            {pending_dependencies, _group} =
              pending =
              Dependencies.compute_pending_dependencies(state.db, execution_id, wait_for, step_id)

            state = Dependencies.register_pending_dependencies(state, execution_id, pending)

            {state, pending_dependencies,
             Dependencies.build_argument_dependencies(state.db, step_id, wait_for),
             Dependencies.unresolved_dependency_ids(state.db, execution_id)}
          else
            {state, MapSet.new(), %{}, MapSet.new()}
          end

        group_id = Keyword.get(opts, :group_id)
        cache = Keyword.get(opts, :cache)
        concurrency = Keyword.get(opts, :concurrency)
        concurrency_limit = if concurrency, do: concurrency.limit, else: 0
        retries = Keyword.get(opts, :retries)
        timeout = Keyword.get(opts, :timeout, 0)
        delay = Keyword.get(opts, :delay, 0)
        execute_after = if delay > 0, do: created_at + delay
        step_requires = Keyword.get(opts, :requires) || %{}

        run_requires = Resolve.tag_set(state.db, run.requires_tag_set_id)

        requires =
          run_requires
          |> Map.merge(step_requires)
          |> Map.reject(fn {_key, values} -> values == [] end)

        execution_external_id = Ids.execution(run.external_id, step_number, attempt)

        parent_execution_external_id =
          if parent_id do
            {:ok, {r, s, a}} = Runs.get_execution_key(state.db, parent_id)
            Ids.execution(r, s, a)
          end

        ws_ext_id = State.workspace_external_id(state, workspace_id)

        state =
          if !memo_hit do
            arguments = Enum.map(arguments, &Resolve.value(state.db, &1))

            recurrent = Keyword.get(opts, :recurrent, false)

            {:ok, checkpoints} =
              Checkpoints.get_effective(
                state.db,
                step_id,
                State.workspace_chain(state, workspace_id),
                attempt
              )

            {root_module, root_target} =
              State.get_run_workflow(state, run.external_id) ||
                raise "run_workflows missing entry for run #{run.external_id}"

            state
            |> Effects.emit(%StepCreated{
              run: run.external_id,
              step: step_number,
              module: module,
              target: target_name,
              type: type,
              parent: parent_execution_external_id,
              cache_config: cache,
              cache_key: cache_key,
              memo_key: memo_key,
              concurrency_key: concurrency_key,
              concurrency_limit: concurrency_limit,
              group_key: group_key,
              group_limit: group_limit,
              retries: retries,
              recurrent: recurrent,
              timeout: timeout,
              created_at: created_at,
              requires: step_requires
            })
            |> Effects.emit(%StepArguments{
              run: run.external_id,
              step: step_number,
              arguments: arguments
            })
            |> Effects.emit(%ExecutionScheduled{
              execution: execution_external_id,
              run: run.external_id,
              step: step_number,
              attempt: attempt,
              workspace: ws_ext_id,
              module: module,
              target: target_name,
              type: type,
              root_module: root_module,
              root_target: root_target,
              execute_after: execute_after,
              created_at: created_at,
              created_by: nil,
              requires: requires
            })
            |> Scheduling.emit_execution_detail(
              run.external_id,
              execution_external_id,
              argument_dependencies,
              Resolve.checkpoints(state.db, checkpoints),
              unresolved_dependencies
            )
          else
            state
          end

        state =
          if child_added do
            Effects.emit(state, %ChildLinked{
              run: run.external_id,
              parent: parent_execution_external_id,
              step: step_number,
              attempt: attempt,
              group: group_id
            })
          else
            state
          end

        state =
          if !memo_hit do
            {root_module, root_target} =
              State.get_run_workflow(state, run.external_id) ||
                raise "run_workflows missing entry for run #{run.external_id}"

            state =
              state
              |> State.track_run_execution(
                run.external_id,
                execution_id,
                root_module,
                root_target
              )
              |> Scheduling.emit_waiting(
                execution_external_id,
                run.external_id,
                ws_ext_id,
                Gates.describe(state.db, pending_dependencies)
              )

            send(self(), :tick)

            state
          else
            state
          end

        # Return extended metadata for log references
        execution_metadata = %{
          run_id: run.external_id,
          step_id: Ids.step(run.external_id, step_number),
          step_number: step_number,
          attempt: attempt,
          module: module,
          target: target_name
        }

        {:reply, {:ok, run.external_id, step_number, execution_external_id, execution_metadata},
         state}
    end
  end

  defp dispatch_call(
         {:register_group, parent_external_id, group_id, name, concurrency},
         state
       ) do
    parent_id = Map.fetch!(state.execution_ids, parent_external_id)
    {:ok, {run_external_id}} = Runs.get_external_run_id_for_execution(state.db, parent_id)

    case Runs.create_group(state.db, parent_id, group_id, name, concurrency) do
      :ok ->
        state =
          state
          |> Effects.emit(%GroupCreated{
            run: run_external_id,
            execution: parent_external_id,
            group: group_id,
            name: name,
            concurrency: concurrency
          })

        {:reply, :ok, state}
    end
  end

  defp dispatch_call({:rerun_step, step_id, workspace_external_id, access, opts}, state) do
    with {:ok, run_external_id, step_number} <- Ids.parse_step(step_id),
         {:ok, workspace_id, _} <-
           Permissions.require_workspace(state, workspace_external_id, access),
         {:ok, catalog_sequence} <-
           CatalogFlow.resolve_catalog_option(state, workspace_id, Keyword.get(opts, :catalog)),
         {:ok, run} when not is_nil(run) <-
           Archives.ensure_run_in_active_epoch(state, run_external_id),
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
           Permissions.is_workspace_ancestor?(state, base_workspace_id, workspace_id) do
        # A live attempt is cancelled and its streams closed, so the new
        # attempt starts fresh. A pending successor of a suspended attempt
        # never produced anything, so its cancellation leaves the paused
        # streams open and the new attempt continues them.
        state =
          Cancellation.cancel_active_step_executions(state, step.id, workspace_id,
            streams: :registered
          )

        {:ok, _execution_id, attempt, state} =
          Scheduling.rerun_step(state, step, workspace_id,
            created_by: access[:principal_id],
            catalog_sequence: catalog_sequence
          )

        execution_external_id = Ids.execution(run_external_id, step_number, attempt)

        {:reply, {:ok, execution_external_id, attempt}, state}
      else
        {:reply, {:error, :workspace_invalid}, state}
      end
    else
      {:error, :invalid} -> {:reply, {:error, :invalid}, state}
      {:error, :forbidden} -> {:reply, {:error, :forbidden}, state}
      {:error, :workspace_invalid} -> {:reply, {:error, :workspace_invalid}, state}
      {:error, :catalog_invalid} -> {:reply, {:error, :catalog_invalid}, state}
      {:error, :catalog_not_found} -> {:reply, {:error, :catalog_not_found}, state}
      {:error, :catalog_invisible} -> {:reply, {:error, :catalog_invisible}, state}
      {:ok, nil} -> {:reply, {:error, :not_found}, state}
    end
  end

  defp dispatch_call(
         {:cancel_execution, workspace_external_id, execution_external_id, access},
         state
       ) do
    with {:ok, workspace_id, _} <-
           Permissions.require_workspace(state, workspace_external_id, access),
         {:ok, run_ext_id, step_number, attempt} <-
           Ids.parse_execution(execution_external_id),
         {:ok, {execution_id}} <-
           Runs.get_execution_id(state.db, run_ext_id, step_number, attempt) do
      active_id = Cancellation.resolve_active_execution(state.db, execution_id)

      state = Cancellation.do_cancel_execution(state, active_id, workspace_id)
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

  defp dispatch_call(
         {:cancel, handles, workspace_external_id, _from_execution_external_id},
         state
       ) do
    case Map.fetch(state.workspace_external_ids, workspace_external_id) do
      :error ->
        {:reply, {:error, :workspace_not_found}, state}

      {:ok, workspace_id} ->
        if Enum.all?(handles, &Cancellation.cancellable_handle?/1) do
          state =
            Enum.reduce(handles, state, fn handle, state ->
              Cancellation.cancel_handle(state, handle, workspace_id)
            end)

          {:reply, :ok, state}
        else
          {:reply, {:error, :invalid_handle}, state}
        end
    end
  end

  defp dispatch_call({:execution_started, _external_execution_id, _metadata}, state) do
    {:reply, :ok, state}
  end

  defp dispatch_call({:define_metric, external_execution_id, key, definition}, state) do
    state =
      case Map.fetch(state.execution_ids, external_execution_id) do
        {:ok, execution_id} ->
          {:ok, _} = Runs.define_metric(state.db, execution_id, key, definition)

          # Notify the run topic
          case Runs.get_execution_run_id(state.db, execution_id) do
            {:ok, run_id} ->
              case Runs.get_run_by_id(state.db, run_id) do
                {:ok, run} ->
                  Effects.emit(state, %MetricDefined{
                    run: run.external_id,
                    execution: external_execution_id,
                    key: key,
                    definition: Scheduling.metric_definition(definition)
                  })

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

  defp dispatch_call({:record_heartbeats, executions, external_session_id}, state) do
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

        # Abandon executions that were executing but are no longer reported.
        # Keyed off `has_completion?` rather than `has_result?`: a value
        # result with streams still draining is still running from the
        # lifecycle's perspective, and we shouldn't synthesise a completion
        # until the worker actually stops reporting it.
        state =
          session.executing
          |> MapSet.difference(reported_ext_ids)
          |> Enum.reduce(state, fn ext_id, state ->
            execution_id = Map.fetch!(state.execution_ids, ext_id)

            case Results.has_completion?(state.db, execution_id) do
              {:ok, false} ->
                # Server-detected abandonment. No worker will send
                # notify_terminated for this execution.
                {:ok, state} = Lifecycle.process_result(state, execution_id, :abandoned)
                Lifecycle.complete_execution(state, execution_id)

              {:ok, true} ->
                state
            end
          end)

        # Executions the worker reported but the server has already
        # finalised (completion recorded) — tell the worker to abort
        # its local process. Mid-drain executions (result but no
        # completion) are legitimate and left alone.
        state =
          reported_ext_ids
          |> MapSet.difference(session.starting)
          |> MapSet.difference(session.executing)
          |> Enum.reduce(state, fn ext_id, state ->
            case Map.fetch(state.execution_ids, ext_id) do
              {:ok, execution_id} ->
                case Results.has_completion?(state.db, execution_id) do
                  {:ok, false} ->
                    state

                  {:ok, true} ->
                    Effects.command(state, session_id, Commands.abort(ext_id))
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
          |> Effects.emit(%SessionExecuting{
            workspace: State.workspace_external_id(state, session.workspace_id),
            session: session.external_id,
            executing: session.starting |> MapSet.union(session.executing) |> Enum.count()
          })

        {:reply, :ok, state}

      :error ->
        {:reply, {:error, :session_invalid}, state}
    end
  end

  defp dispatch_call({:notify_terminated, external_execution_ids}, state) do
    now = System.os_time(:millisecond)

    state =
      external_execution_ids
      |> Enum.reduce(state, fn ext_id, state ->
        # Finalize the execution — writes completion (plus creating any
        # successor) if it hasn't already been done. For worker-reported
        # error/timeout this runs the retry decision now. For executions
        # with no results row yet, falls back to the :abandoned path.
        state =
          case Map.fetch(state.execution_ids, ext_id) do
            {:ok, execution_id} ->
              state
              |> Lifecycle.complete_execution(execution_id)
              |> StreamDelivery.drop_execution_subscriptions(execution_id)

            :error ->
              state
          end

        # Remove from execution_ids cache
        state = Map.update!(state, :execution_ids, &Map.delete(&1, ext_id))

        # Remove from session's starting/executing (using external IDs directly)
        case State.session_for_execution(state, ext_id) do
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

            Effects.emit(state, %SessionExecuting{
              workspace: State.workspace_external_id(state, session.workspace_id),
              session: session.external_id,
              executing: executing
            })

          :error ->
            state
        end
      end)

    send(self(), :tick)

    {:reply, :ok, state}
  end

  defp dispatch_call({:record_result, execution_external_id, result}, state) do
    case Map.fetch(state.execution_ids, execution_external_id) do
      {:ok, execution_id} ->
        {result, abort?} = CatalogFlow.refuse_invalid_catalog_wait(result)

        case Lifecycle.process_result(state, execution_id, result) do
          {:ok, state} ->
            state =
              if abort?,
                do: Cancellation.abort_execution(state, execution_external_id),
                else: state

            {:reply, :ok, state}
        end

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  defp dispatch_call({:set_checkpoints, execution_external_id, set, reset}, state) do
    case Map.fetch(state.execution_ids, execution_external_id) do
      {:ok, execution_id} ->
        # Reject writes from an execution the server has already finalised. A
        # worker that was declared abandoned while still alive flushes its
        # buffer on reconnect; per-execution keying already makes those writes
        # unreadable (they land on an older attempt), but there's no reason to
        # mutate a terminated execution's history.
        case Results.has_completion?(state.db, execution_id) do
          {:ok, true} ->
            {:reply, {:error, :completed}, state}

          {:ok, false} ->
            {:ok, {step_id, workspace_id, attempt}} =
              Runs.get_execution_location(state.db, execution_id)

            set = Map.new(set, fn {name, value} -> {name, Values.normalize(value)} end)

            {:ok, _updated_at} =
              Checkpoints.apply_delta(
                state.db,
                execution_id,
                step_id,
                State.workspace_chain(state, workspace_id),
                attempt,
                set,
                reset
              )

            # Push the whole resolved snapshot rather than the delta — it's
            # what Studio renders, and carry-forward means the first write of
            # an execution materialises names it never touched. Only the
            # "after" side moves; what the execution started from is fixed.
            {:ok, checkpoints} =
              Checkpoints.get_effective_for_execution(state.db, execution_id)

            checkpoints = Resolve.checkpoints(state.db, checkpoints)

            {:ok, {run_external_id, _step_number, _attempt}} =
              Runs.get_execution_key(state.db, execution_id)

            state =
              state
              |> Effects.emit(%CheckpointsSet{
                run: run_external_id,
                execution: execution_external_id,
                checkpoints: checkpoints
              })

            {:reply, :ok, state}
        end

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  # A producer declares its k-th stream (`position`). The server decides
  # whether that continues a paused stream of the step — one left open by a
  # suspended execution — or opens a new one, and replies with the stream's
  # id, its step index and the head it should sequence from.
  defp dispatch_call(
         {:register_stream, execution_external_id, position, buffer, timeout_ms,
          session_external_id},
         state
       ) do
    with {:ok, execution_id} <-
           Map.fetch(state.execution_ids, execution_external_id) |> ok_or(:not_found),
         {:ok, false} <- Results.has_completion?(state.db, execution_id) do
      {:ok, {step_id, workspace_id, _attempt}} =
        Runs.get_execution_location(state.db, execution_id)

      {:ok, registration} =
        Streams.register(
          state.db,
          step_id,
          workspace_id,
          execution_id,
          position,
          buffer,
          timeout_ms
        )

      {:ok, stream} = Streams.get_stream(state.db, registration.id)

      external_id =
        Ids.stream(stream.run_external_id, stream.step_number, stream.index)

      state =
        if registration.created_at do
          # Resolve the session's external id to the internal one —
          # send_session (which delivers stream_demand) indexes by the
          # internal id.
          internal_session_id = Map.get(state.session_ids, session_external_id)

          state
          |> StreamDelivery.init_stream_producer(
            stream,
            execution_external_id,
            buffer,
            registration.head,
            internal_session_id
          )
          |> StreamDelivery.notify_stream_registered(
            stream,
            execution_id,
            registration,
            buffer,
            timeout_ms
          )
          # Lockstep (buffer=0) stays paused until a consumer attaches; a
          # larger buffer lets the producer pre-warm. A resuming producer
          # picks up whatever demand its subscribers have already built.
          |> StreamDelivery.refresh_stream_demand(stream.id)
        else
          # The same execution registering the same position again —
          # idempotent, nothing new to announce.
          state
        end

      {:reply, {:ok, %{id: external_id, index: stream.index, head: registration.head}}, state}
    else
      {:ok, true} -> {:reply, {:error, :completed}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp dispatch_call(
         {:append_stream_item, execution_external_id, index, sequence, value},
         state
       ) do
    # Appends are refused once the execution has a completion: after a
    # suspend the resuming execution owns the stream, and a still-alive
    # predecessor must not interleave with it.
    with {:ok, execution_id} <-
           Map.fetch(state.execution_ids, execution_external_id) |> ok_or(:not_found),
         {:ok, stream_id} <- StreamDelivery.resolve_step_stream(state.db, execution_id, index),
         {:ok, false} <- Results.has_completion?(state.db, execution_id),
         {:ok, created_at} <-
           Streams.append_item(
             state.db,
             stream_id,
             execution_id,
             sequence,
             Values.normalize(value)
           ) do
      # If we came out of a server restart with no in-memory producer
      # state for this stream, rebuild it now from the persisted config so
      # subsequent consumer advances can refresh demand. The appending
      # session is the producer.
      producer_session_id =
        case State.session_for_execution(state, execution_external_id) do
          {:ok, sid} -> sid
          :error -> nil
        end

      state =
        state
        |> StreamDelivery.ensure_stream_producer(stream_id, producer_session_id)
        |> StreamDelivery.push_stream_item(stream_id, sequence, value)
        |> StreamDelivery.notify_stream_item_appended(
          stream_id,
          execution_id,
          sequence,
          value,
          created_at
        )
        |> Dependencies.update_dependencies_on_stream(stream_id, sequence)

      {:reply, :ok, state}
    else
      {:ok, true} -> {:reply, {:error, :completed}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp dispatch_call({:close_stream, execution_external_id, index, close_spec}, state) do
    with {:ok, execution_id} <-
           Map.fetch(state.execution_ids, execution_external_id) |> ok_or(:not_found),
         {:ok, stream_id} <- StreamDelivery.resolve_step_stream(state.db, execution_id, index),
         {:ok, false} <- Results.has_completion?(state.db, execution_id) do
      {spec, reason, error} =
        case close_spec do
          nil ->
            {:complete, :complete, nil}

          :timeout ->
            {:timeout, :timeout, nil}

          {type, message, frames} ->
            {{:errored, type, message, frames}, :errored, {type, message, frames}}
        end

      case Streams.close_stream(state.db, stream_id, execution_id, spec) do
        {:ok, closed_at} ->
          state =
            state
            |> StreamDelivery.push_stream_closed(stream_id, reason, error)
            |> StreamDelivery.notify_stream_closed(
              stream_id,
              execution_id,
              reason,
              error,
              closed_at
            )
            |> Dependencies.update_dependencies_on_stream(stream_id, :closed)
            |> StreamDelivery.drop_stream_producer(stream_id)

          {:reply, :ok, state}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    else
      {:ok, true} -> {:reply, {:error, :completed}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp dispatch_call(
         {:subscribe_stream, session_external_id, subscription_id, consumer_execution_external_id,
          stream_external_id, from_sequence, stride, prefetch, progress},
         state
       ) do
    with {:ok, _session_id} <-
           Map.fetch(state.session_ids, session_external_id)
           |> ok_or(:session_not_found),
         {:ok, consumer_execution_id} <-
           Map.fetch(state.execution_ids, consumer_execution_external_id)
           |> ok_or(:consumer_not_found),
         # The stream's run may have been rotated into an older epoch —
         # resolving by id copies it forward if so.
         {:ok, stream_id} <- Archives.resolve_stream_id(state, stream_external_id),
         key = {consumer_execution_id, subscription_id},
         false <- Map.has_key?(state.stream_subscriptions, key) do
      # `progress` is nil for a fresh subscribe and carries the CLI's
      # cumulative counters when it's re-establishing a subscription after
      # a reconnect. The adapter's counters don't reset across a reconnect,
      # so we resume the accounting where the previous connection left off.
      # Starting from zero instead would over-grant the window by however
      # many items were in flight when the connection dropped; deriving
      # `acked_count` from `delivered` would under-grant it, and could
      # deadlock a consumer that has already drained its queue and so has
      # nothing left to ack.
      # The handler validates the shape, but this is worker-supplied JSON
      # reaching a GenServer whose crash would take down every in-flight
      # run in the project — so fall back rather than raise if anything
      # unexpected gets this far.
      {delivered, acked_count, acked_seq} =
        case progress do
          %{"delivered" => d, "acked_count" => a, "acked_seq" => s}
          when is_integer(d) and is_integer(a) and is_integer(s) ->
            {d, a, s}

          _ ->
            {0, 0, from_sequence - 1}
        end

      subscription = %{
        consumer_execution_external_id: consumer_execution_external_id,
        stream_id: stream_id,
        cursor: from_sequence,
        stride: stride,
        prefetch: prefetch,
        delivered: delivered,
        acked_count: acked_count,
        acked_seq: acked_seq,
        pending_close: nil
      }

      state =
        state
        |> Map.update!(:stream_subscriptions, &Map.put(&1, key, subscription))
        |> Map.update!(:stream_subscribers, fn m ->
          Map.update(m, stream_id, MapSet.new([key]), &MapSet.put(&1, key))
        end)

      # Post-restart recovery: producer state may be missing. The
      # producer's session isn't necessarily the one the subscribe came
      # from — look it up from the stream's current producer.
      state =
        StreamDelivery.ensure_stream_producer(
          state,
          stream_id,
          StreamDelivery.producer_session_id(state, stream_id)
        )

      # First subscriber (or a later one whose cursor exceeds the prior
      # max) may unblock the producer — recompute demand before pushing
      # backlog so any delivered items keep the credit maths honest.
      state = StreamDelivery.refresh_stream_demand(state, stream_id)

      # If the stream has already closed, record that as pending first so
      # the pump can emit it — but only once the backlog it's allowed to
      # send has actually been delivered. Pushing the closure eagerly
      # would land it ahead of a credit-limited backlog.
      state = StreamDelivery.mark_closed_if_closed(state, key)
      state = StreamDelivery.pump_subscription(state, key)

      # Record the subscribe as a lineage edge (consumer -> stream). Done
      # unconditionally on subscribe, independent of whether items end up
      # being read. Uses stream_refs so the edge survives epoch rotation.
      {:ok, stream_ref_id} = Streams.create_stream_ref_for(state.db, stream_id)

      {:ok, _} = Streams.record_dependency(state.db, consumer_execution_id, stream_ref_id)

      # Announced whether or not the insert was new: the row may already
      # exist as a wait recorded against this execution before it ran. The
      # topic merges, so re-announcing an edge it already holds is harmless.
      {:ok, {run_external_id}} =
        Runs.get_external_run_id_for_execution(state.db, consumer_execution_id)

      {:ok, {stream_run_ext_id, step_number, index, module, target}} =
        Streams.get_stream_ref(state.db, stream_ref_id)

      state =
        Effects.emit(state, %StreamDependencyRecorded{
          run: run_external_id,
          execution: consumer_execution_external_id,
          stream: Ids.stream(stream_run_ext_id, step_number, index),
          module: module,
          target: target,
          pending: false
        })

      {:reply, :ok, state}
    else
      true -> {:reply, {:error, :already_subscribed}, state}
      {:error, :not_found} -> {:reply, {:error, :stream_not_found}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  # Consumer reports progress. `count` and `sequence` are cumulative and
  # monotonic — `count` is how many items the consumer has finished
  # processing, `sequence` the highest sequence among them. Cumulative
  # (rather than incremental) so a retransmit after a reconnect is
  # idempotent, and so a dropped ack is corrected by the next one.
  #
  # Acking does two things: frees credit (allowing the pump to deliver
  # more), and advances the watermark the producer's buffer is measured
  # against.
  defp dispatch_call(
         {:ack_stream, consumer_execution_external_id, subscription_id, count, sequence},
         state
       ) do
    with {:ok, consumer_execution_id} <-
           Map.fetch(state.execution_ids, consumer_execution_external_id),
         key = {consumer_execution_id, subscription_id},
         {:ok, sub} <- Map.fetch(state.stream_subscriptions, key) do
      # Clamp rather than trust: an out-of-order or stale ack must never
      # move a watermark backwards, and `count` can't exceed what we
      # actually sent.
      acked_count = sub.acked_count |> max(count) |> min(sub.delivered)
      acked_seq = max(sub.acked_seq, sequence)

      state =
        update_in(
          state.stream_subscriptions[key],
          &%{&1 | acked_count: acked_count, acked_seq: acked_seq}
        )

      state = StreamDelivery.pump_subscription(state, key)
      state = StreamDelivery.refresh_stream_demand(state, sub.stream_id)

      {:reply, :ok, state}
    else
      :error -> {:reply, :ok, state}
    end
  end

  defp dispatch_call(
         {:unsubscribe_stream, session_external_id, consumer_execution_external_id,
          subscription_id},
         state
       ) do
    with {:ok, _session_id} <- Map.fetch(state.session_ids, session_external_id),
         {:ok, consumer_execution_id} <-
           Map.fetch(state.execution_ids, consumer_execution_external_id) do
      {:reply, :ok,
       StreamDelivery.drop_subscription(state, {consumer_execution_id, subscription_id})}
    else
      :error ->
        {:reply, :ok, state}
    end
  end

  defp dispatch_call(
         {:select, handles, from_execution_external_id, timeout_ms, suspend, cancel_remaining,
          request_id},
         state
       ) do
    case Archives.resolve_internal_execution_id(state, from_execution_external_id) do
      {:error, :not_found} ->
        {:reply, {:error, :execution_not_found}, state}

      {:ok, from_execution_id} ->
        # Process each handle: record dependency and determine status.
        # Each entry is {:ok, status} or {:error, reason}.
        {entries, state} =
          Enum.map_reduce(handles, state, fn handle, state ->
            Waiters.process_select_handle(
              state,
              handle,
              from_execution_id,
              from_execution_external_id
            )
          end)

        case Enum.find(entries, &match?({:error, _}, &1)) do
          {:error, reason} ->
            {:reply, {:error, reason}, state}

          nil ->
            # Find first already-resolved handle (earliest in input order wins)
            statuses = Enum.map(entries, fn {:ok, s} -> s end)

            resolved_index =
              Enum.find_index(statuses, fn
                {:resolved, _} -> true
                _ -> false
              end)

            cond do
              resolved_index != nil ->
                {:resolved, result} = Enum.at(statuses, resolved_index)

                state =
                  Waiters.maybe_cancel_remaining(
                    state,
                    statuses,
                    resolved_index,
                    cancel_remaining,
                    from_execution_external_id
                  )

                {:reply, {:ok, {resolved_index, result}}, state}

              timeout_ms == 0 && suspend ->
                dependency_keys =
                  Enum.map(statuses, fn {:pending, _waiting_key, dep_key} -> dep_key end)

                {:ok, state} =
                  Lifecycle.process_result(
                    state,
                    from_execution_id,
                    {:suspended, nil, dependency_keys}
                  )

                {:reply, {:ok, :suspended}, state}

              timeout_ms == 0 ->
                {:reply, {:ok, :timeout}, state}

              true ->
                now = System.monotonic_time(:millisecond)
                expire_at = if timeout_ms, do: now + timeout_ms

                waiting_keys =
                  Enum.map(statuses, fn {:pending, waiting_key, _} -> waiting_key end)

                state =
                  statuses
                  |> Enum.with_index()
                  |> Enum.reduce(state, fn {{:pending, waiting_key, _}, idx}, state ->
                    entry = %{
                      from_ext_id: from_execution_external_id,
                      request_id: request_id,
                      expire_at: expire_at,
                      suspend: suspend,
                      cancel_remaining: cancel_remaining,
                      handle_index: idx,
                      keys: waiting_keys
                    }

                    update_in(
                      state,
                      [Access.key(:waiting), Access.key(waiting_key, [])],
                      &[entry | &1]
                    )
                  end)

                state =
                  if timeout_ms do
                    Waiters.reschedule_expire_waiters(state)
                  else
                    state
                  end

                {:reply, :wait, state}
            end
        end
    end
  end

  defp dispatch_call({:put_asset, execution_external_id, name, entries}, state) do
    execution_id = Map.fetch!(state.execution_ids, execution_external_id)
    {:ok, {run_external_id}} = Runs.get_external_run_id_for_execution(state.db, execution_id)

    {:ok, asset_id, external_id, asset_name, total_count, total_size, entry} =
      Assets.get_or_create_asset(state.db, name, entries)

    :ok = Results.put_execution_asset(state.db, execution_id, asset_id)

    state =
      state
      |> Effects.emit(%AssetPut{
        run: run_external_id,
        execution: execution_external_id,
        asset: external_id,
        summary: {asset_name, total_count, total_size, entry}
      })

    asset_metadata = %{
      name: asset_name,
      total_count: total_count,
      total_size: total_size
    }

    {:reply, {:ok, external_id, asset_metadata}, state}
  end

  # An asset assembled from outside a run: the caller has already stored the
  # blobs and passes their keys, exactly as `put_asset` does. There is no
  # execution to record it against, so nothing is notified — the asset only
  # becomes visible once something references it (a catalog publish, a run
  # argument).
  defp dispatch_call({:create_asset, workspace_external_id, name, entries, access}, state) do
    case Permissions.require_workspace(state, workspace_external_id, access) do
      {:ok, _workspace_id, _workspace} ->
        {:ok, _asset_id, external_id, asset_name, total_count, total_size, _entry} =
          Assets.get_or_create_asset(state.db, name, entries)

        asset_metadata = %{
          name: asset_name,
          total_count: total_count,
          total_size: total_size
        }

        {:reply, {:ok, external_id, asset_metadata}, state}

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  # --- Catalog ---

  defp dispatch_call(
         {:catalog_publish, execution_external_id, path, value},
         state
       ) do
    with {:ok, execution_id} <-
           Archives.resolve_internal_execution_id(state, execution_external_id),
         :ok <- Catalog.validate_path(path),
         :ok <- validate_value_assets(state.db, value) do
      {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)
      chain = State.workspace_chain(state, workspace_id)
      {:ok, ref_id} = Runs.create_execution_ref_for(state.db, execution_id)
      {:ok, value_id} = Values.get_or_create_value(state.db, Values.normalize(value))

      {:ok, version, created?} =
        Catalog.publish(state.db, path, workspace_id, chain, value_id, ref_id, nil)

      state =
        if created?,
          do: CatalogFlow.notify_catalog_version(state, version),
          else: state

      {:reply, {:ok, version.number}, state}
    else
      {:error, :not_found} -> {:reply, {:error, :execution_not_found}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp dispatch_call({:catalog_get, execution_external_id, path, number}, state) do
    with {:ok, execution_id} <-
           Archives.resolve_internal_execution_id(state, execution_external_id),
         :ok <- Catalog.validate_path(path) do
      case CatalogFlow.lookup_catalog_version(state, execution_id, path, number) do
        {:ok, nil} ->
          {:reply, {:ok, nil}, state}

        {:ok, version} ->
          state =
            CatalogFlow.record_catalog_read(state, execution_id, execution_external_id, version)

          {:ok, value} = Values.get_value_by_id(state.db, version.value_id)
          reply = %{number: version.number, value: Resolve.value(state.db, value)}
          {:reply, {:ok, reply}, state}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    else
      {:error, :not_found} -> {:reply, {:error, :execution_not_found}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp dispatch_call({:catalog_list, workspace_external_id, prefix}, state) do
    case Permissions.resolve_workspace_external_id(state, workspace_external_id) do
      {:ok, workspace_id} ->
        chain = State.workspace_chain(state, workspace_id)
        {:ok, versions} = Catalog.list_heads(state.db, chain, prefix)
        {:reply, {:ok, Enum.map(versions, &Resolve.catalog_version(state.db, &1))}, state}

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp dispatch_call({:catalog_versions, workspace_external_id, path, limit, before}, state) do
    with {:ok, workspace_id} <-
           Permissions.resolve_workspace_external_id(state, workspace_external_id),
         :ok <- Catalog.validate_path(path) do
      chain = State.workspace_chain(state, workspace_id)
      {:ok, versions} = Catalog.list_versions(state.db, path, chain, limit, before)
      {:reply, {:ok, Enum.map(versions, &Resolve.catalog_version(state.db, &1))}, state}
    else
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  # A publish from outside a run — the API or CLI — attributed to a
  # principal rather than an execution.
  defp dispatch_call(
         {:publish_catalog, workspace_external_id, path, value, access},
         state
       ) do
    with {:ok, workspace_id, _workspace} <-
           Permissions.require_workspace(state, workspace_external_id, access),
         :ok <- Catalog.validate_path(path),
         :ok <- validate_value_assets(state.db, value) do
      chain = State.workspace_chain(state, workspace_id)

      created_by =
        case access do
          %{principal_id: id} -> id
          _ -> nil
        end

      {:ok, value_id} = Values.get_or_create_value(state.db, Values.normalize(value))

      {:ok, version, created?} =
        Catalog.publish(state.db, path, workspace_id, chain, value_id, nil, created_by)

      state = if created?, do: CatalogFlow.notify_catalog_version(state, version), else: state
      {:reply, {:ok, Resolve.catalog_version(state.db, version), created?}, state}
    else
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  defp dispatch_call({:get_asset, asset_external_id, from_execution_external_id}, state) do
    case Assets.get_asset_by_external_id(state.db, asset_external_id) do
      {:ok, asset_id, name, entries} ->
        state =
          if from_execution_external_id do
            from_execution_id = Map.fetch!(state.execution_ids, from_execution_external_id)
            {:ok, _} = Runs.record_asset_dependency(state.db, from_execution_id, asset_id)

            {:ok, {run_external_id}} =
              Runs.get_external_run_id_for_execution(state.db, from_execution_id)

            {^asset_external_id, asset_name, total_count, total_size, entry} =
              Resolve.asset(state.db, asset_id)

            Effects.emit(state, %AssetDependencyRecorded{
              run: run_external_id,
              execution: from_execution_external_id,
              asset: asset_external_id,
              summary: {asset_name, total_count, total_size, entry},
              pending: false
            })
          else
            state
          end

        {:reply, {:ok, name, entries}, state}

      {:error, :not_found} ->
        {:reply, {:error, :not_found}, state}
    end
  end

  # --- Input management ---

  defp dispatch_call(
         {:submit_input, execution_external_id, template, placeholders, schema_json, key, title,
          actions, initial, requires},
         state
       ) do
    execution_id = Map.fetch!(state.execution_ids, execution_external_id)
    {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)
    {:ok, run_id} = Runs.get_run_id_for_execution(state.db, execution_id)

    case InputFlow.validate_and_prepare_input(state, schema_json, initial) do
      {:error, reason} ->
        {:reply, {:error, reason}, state}

      {:ok, schema_id} ->
        now = System.system_time(:millisecond)

        # Get-or-create placeholder values
        placeholder_value_ids =
          Enum.map(placeholders, fn {placeholder, value} ->
            {:ok, value_id} = Values.get_or_create_value(state.db, Values.normalize(value))
            {placeholder, value_id}
          end)

        # Get-or-create prompt
        {:ok, prompt_id} = Inputs.get_or_create_prompt(state.db, template, placeholder_value_ids)

        # Get-or-create requires tag set
        requires_tag_set_id =
          if requires && map_size(requires) > 0 do
            {:ok, id} = TagSets.get_or_create_tag_set_id(state.db, requires)
            id
          end

        # Check for existing input by key (run-scoped, workspace-ancestry-aware)
        case InputFlow.find_or_create_input(
               state,
               key,
               run_id,
               workspace_id,
               execution_id,
               prompt_id,
               schema_id,
               title,
               actions,
               initial,
               requires_tag_set_id,
               now
             ) do
          {:error, reason} ->
            {:reply, {:error, reason}, state}

          {:ok, input_number, stored_title, _created} ->
            {:ok, {run_external_id}} =
              Runs.get_external_run_id_for_execution(state.db, execution_id)

            input_ext_id = Ids.input(run_external_id, input_number)

            state =
              Effects.emit(state, %InputSubmitted{
                run: run_external_id,
                execution: execution_external_id,
                input: input_ext_id,
                title: stored_title
              })

            {:reply, {:ok, input_ext_id}, state}
        end
    end
  end

  defp dispatch_call({:respond_input, input_external_id, value, access}, state) do
    case Archives.find_and_copy_input_from_archives(state, input_external_id) do
      {:ok, nil} ->
        {:reply, {:error, :not_found}, state}

      {:ok,
       {input_id, workspace_id, _key, _prompt_id, schema_id, _title, _actions, _initial,
        _requires_tag_set_id, _created_at, _run_id}} ->
        # Validate response against schema if one exists
        with :ok <- InputFlow.validate_input_response(state, schema_id, value) do
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
              # Notify select waiters for this input. Wrap the raw JSON value
              # in the tuple format so compose_value handles inputs and
              # executions uniformly.
              state =
                Waiters.notify_select_waiters(
                  state,
                  {:input, input_external_id},
                  {:value, {:raw, value, []}}
                )

              # Resolve input dependency for any suspended executions waiting on this input
              state = Dependencies.update_dependencies_on_input(state, input_id)

              # Notify run topic
              {:ok, run_external_id, _input_number} =
                Ids.parse_input(input_external_id)

              ws_ext_id = State.workspace_external_id(state, workspace_id)

              response = InputFlow.build_input_response(state.db, input_id)

              state =
                state
                |> Effects.emit(%InputResponded{
                  run: run_external_id,
                  workspace: ws_ext_id,
                  input: input_external_id,
                  response: response
                })

              {:reply, :ok, state}

            {:error, :already_responded} ->
              {:reply, {:error, :already_responded}, state}
          end
        else
          {:error, reason} ->
            {:reply, {:error, {:validation_failed, reason}}, state}
        end
    end
  end

  defp dispatch_call({:get_input, input_external_id}, state) do
    case Archives.read_input_from_active_or_archives(state, input_external_id) do
      {:ok, nil} ->
        {:reply, {:error, :not_found}, state}

      {:ok,
       {db, input_id, _workspace_id, key, prompt_id, schema_id, title, actions, initial,
        requires_tag_set_id, created_at}} ->
        details =
          InputFlow.build_input_details(
            db,
            input_id,
            key,
            prompt_id,
            schema_id,
            title,
            actions,
            initial,
            requires_tag_set_id,
            created_at
          )

        {:reply, {:ok, details}, state}
    end
  end

  defp dispatch_call({:dismiss_input, input_external_id, access}, state) do
    case Archives.find_and_copy_input_from_archives(state, input_external_id) do
      {:ok, nil} ->
        {:reply, {:error, :not_found}, state}

      {:ok,
       {input_id, workspace_id, _key, _prompt_id, _schema_id, _title, _actions, _initial,
        _requires_tag_set_id, _created_at, _run_id}} ->
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
            # Notify select waiters for this input (dismissed)
            state =
              Waiters.notify_select_waiters(state, {:input, input_external_id}, :dismissed)

            # Resolve input dependency for any suspended executions waiting on this input
            state = Dependencies.update_dependencies_on_input(state, input_id)

            # Notify topics
            {:ok, run_external_id, _input_number} =
              Ids.parse_input(input_external_id)

            ws_ext_id = State.workspace_external_id(state, workspace_id)

            response = InputFlow.build_input_response(state.db, input_id)

            state =
              state
              |> Effects.emit(%InputResponded{
                run: run_external_id,
                workspace: ws_ext_id,
                input: input_external_id,
                response: response
              })

            {:reply, :ok, state}

          {:error, :already_responded} ->
            {:reply, {:error, :already_responded}, state}
        end
    end
  end

  defp dispatch_call({:subscribe, key, opts, pid}, state) do
    case Snapshots.snapshot(state, key, opts) do
      {:ok, events} ->
        {:ok, ref, state} = Listeners.add_listener(state, key, pid)
        {:reply, {:ok, events, ref}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp dispatch_call({:get_run_details, external_run_id, request}, state) do
    case Archives.find_run(state, external_run_id, &Snapshots.build_run_details(&1, &2, request)) do
      {:ok, details} -> {:reply, {:ok, details}, state}
      :not_found -> {:reply, {:error, :not_found}, state}
    end
  end

  defp dispatch_call(:rotate_epoch, state) do
    state = Rotation.do_rotate_epoch(state)
    {:reply, :ok, state}
  end

  defp dispatch_cast({:unsubscribe, ref}, state) do
    Process.demonitor(ref, [:flush])

    state =
      state
      |> Listeners.remove_listener(ref)
      |> Listeners.maybe_schedule_idle_shutdown()

    {:noreply, state}
  end

  defp dispatch_info(:expire_sessions, state) do
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
            Fleet.remove_session(state, session_id)

          _ ->
            state
        end
      end)

    state =
      state
      |> Fleet.reschedule_expire_sessions_timer()
      |> Listeners.maybe_schedule_idle_shutdown()

    {:noreply, state}
  end

  defp dispatch_info({:idle_shutdown, ref}, state) do
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

  defp dispatch_info(:check_rotation, state) do
    db_size = Epochs.active_db_size(state.epochs)

    state =
      if db_size >= @rotation_size_threshold_bytes do
        Rotation.do_rotate_epoch(state)
      else
        state
      end

    Process.send_after(self(), :check_rotation, @rotation_check_interval_ms)
    {:noreply, state}
  end

  defp dispatch_info(:tick, state) do
    {:noreply, Scheduler.tick(state)}
  end

  defp dispatch_info(:expire_waiters, state) do
    now = System.monotonic_time(:millisecond)

    # Collect expired select waiters. Each waiter is registered under multiple
    # keys (one per handle), so we dedupe by request_id. For suspend waiters,
    # we collect the full set of dependency keys (for process_result); for
    # poll waiters, we just need the request_id once.
    expired =
      Enum.reduce(state.waiting, %{}, fn {_waiting_key, entries}, acc ->
        Enum.reduce(entries, acc, fn entry, acc ->
          if entry.expire_at && entry.expire_at <= now do
            Map.put_new(acc, entry.request_id, entry)
          else
            acc
          end
        end)
      end)

    {to_suspend, to_timeout} =
      Enum.reduce(expired, {[], []}, fn {_, entry}, {to_suspend, to_timeout} ->
        if entry.suspend do
          {[entry | to_suspend], to_timeout}
        else
          {to_suspend, [entry | to_timeout]}
        end
      end)

    # Remove expired poll (non-suspend) waiters from all keys they're
    # registered under. Suspend waiters are cleared when process_result →
    # abort_execution → cleanup_execution runs.
    state =
      Enum.reduce(to_timeout, state, fn entry, state ->
        Enum.reduce(entry.keys, state, fn key, state ->
          Waiters.remove_waiter_from_key(state, key, entry.request_id)
        end)
      end)

    # Send timeout responses to poll waiters
    state =
      Enum.reduce(to_timeout, state, fn entry, state ->
        case State.session_for_execution(state, entry.from_ext_id) do
          {:ok, session_id} ->
            Effects.command(state, session_id, Commands.result(entry.request_id, :timeout))

          :error ->
            state
        end
      end)

    # Suspend waiters: record suspension with the dependency keys they were
    # waiting on (so the execution can be resumed when any fires).
    state =
      Enum.reduce(to_suspend, state, fn entry, state ->
        from_execution_id = Map.fetch!(state.execution_ids, entry.from_ext_id)

        dependency_keys =
          Enum.flat_map(entry.keys, fn
            {:input, input_ext_id} ->
              with {:ok, run_ext_id, input_number} <- Ids.parse_input(input_ext_id),
                   {:ok, id} <-
                     Inputs.get_input_id_by_run_and_number(state.db, run_ext_id, input_number) do
                [{:input, id}]
              else
                _ -> []
              end

            {:execution, dep_ext_id} ->
              case Archives.resolve_internal_execution_id(state, dep_ext_id) do
                {:ok, id} -> [{:execution, id}]
                {:error, :not_found} -> []
              end

            {:catalog, _workspace_external_id, path, number} ->
              [{:catalog, path, number}]
          end)

        {:ok, state} =
          Lifecycle.process_result(
            state,
            from_execution_id,
            {:suspended, nil, dependency_keys}
          )

        state
      end)

    state =
      state
      |> Waiters.reschedule_expire_waiters()

    {:noreply, state}
  end

  defp dispatch_info(
         {task_ref, {run_bloom, cache_bloom, idempotency_bloom}},
         state
       )
       when task_ref == state.index_task do
    Process.demonitor(task_ref, [:flush])
    [epoch_id | rest] = state.index_queue

    epoch_index =
      Index.update_filters(state.epoch_index, epoch_id, %{
        "runs" => run_bloom,
        "cache_keys" => cache_bloom,
        "idempotency_keys" => idempotency_bloom
      })

    :ok = Index.save(epoch_index)
    epochs = Epochs.promote_to_indexed(state.epochs, epoch_id)

    state =
      %{state | epoch_index: epoch_index, epochs: epochs, index_task: nil, index_queue: rest}

    {:noreply, Rotation.maybe_start_index_build(state)}
  end

  defp dispatch_info({task_ref, result}, state) when is_map_key(state.launcher_tasks, task_ref) do
    state = Fleet.process_launcher_result(state, task_ref, {:ok, result})
    {:noreply, state}
  end

  defp dispatch_info({:DOWN, ref, :process, pid, _reason}, state) do
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
                |> Fleet.schedule_session_expiry(session_id, session.reconnection_timeout)
                |> Effects.emit(%SessionConnected{
                  workspace: State.workspace_external_id(state, session.workspace_id),
                  session: session.external_id,
                  connected: false
                })

              state

            :error ->
              state
          end

        {:noreply, state}

      Map.has_key?(state.listeners, ref) ->
        state =
          state
          |> Listeners.remove_listener(ref)
          |> Listeners.maybe_schedule_idle_shutdown()

        {:noreply, state}

      Map.has_key?(state.launcher_tasks, ref) ->
        state = Fleet.process_launcher_result(state, ref, :error)
        {:noreply, state}

      ref == state.index_task ->
        # Drop failed epoch from queue; it remains unindexed and will be
        # retried on next server startup (via Index.unindexed_epoch_ids)
        [failed | rest] = state.index_queue
        Logger.warning("index build failed for epoch #{failed} in project #{state.project_id}")
        state = %{state | index_task: nil, index_queue: rest}
        state = Rotation.maybe_start_index_build(state)
        {:noreply, state}

      true ->
        {:noreply, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    if state.idle_timer do
      {timer, _ref} = state.idle_timer
      Process.cancel_timer(timer)
    end

    if state.epochs do
      Epochs.close(state.epochs)
    end

    if state.admin_db do
      Coflux.Store.close(state.admin_db)
    end
  end

  # Private helper functions

  # A secret set for a scope is a secret for every workspace that scope
  # selects, so setting one takes a grant that contains the scope whole.
  # Holding one workspace inside it isn't enough - that is the difference
  # between `Scopes.covers?/2` and `Scopes.contains?/2`.
  defp check_secret_scope_access(nil, _scopes), do: :ok

  defp check_secret_scope_access(access, scopes) do
    case access[:workspaces] do
      :all ->
        :ok

      granted ->
        if Enum.all?(scopes, &Scopes.contains_any?(granted, &1)),
          do: :ok,
          else: {:error, :forbidden}
    end
  end

  defp principal_identity(state, access) do
    case Principals.get_principal(state.db, access && access[:principal_id]) do
      {:ok, {type, external_id}} -> %{type: type, external_id: external_id}
      {:ok, nil} -> nil
    end
  end

  # A pool naming a secret its workspace can't see would never launch, so
  # it is refused now rather than found out then.
  defp check_secret_references(state, workspace_id, launcher) when is_map(launcher) do
    workspace_name = state.workspaces[workspace_id].name
    Coflux.Admin.Secrets.check_references(state.admin_db, workspace_name, launcher)
  end

  defp check_secret_references(_state, _workspace_id, _launcher), do: :ok

  defp validate_values_assets(db, values) do
    Enum.reduce_while(values, :ok, fn value, :ok ->
      case validate_value_assets(db, value) do
        :ok -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  # A value can point at assets; make sure they exist before a `values_`
  # row is written against them.
  defp validate_value_assets(db, value) do
    references =
      case value do
        {:raw, _data, references} -> references
        {:blob, _key, _size, references} -> references
      end

    Enum.reduce_while(references, :ok, fn
      {:asset, external_id}, :ok ->
        case Assets.get_asset_id(db, external_id) do
          {:ok, _asset_id} -> {:cont, :ok}
          {:error, :not_found} -> {:halt, {:error, :asset_not_found}}
        end

      _reference, :ok ->
        {:cont, :ok}
    end)
  end

  # --- Catalog helpers ---

  defp ok_or({:ok, val}, _reason), do: {:ok, val}
  defp ok_or(:error, reason), do: {:error, reason}

  # Enabling or disabling a pool that doesn't exist must not conjure one:
  # a state-only entry has no launcher and no modules, and every reader of
  # `state.pools` assumes a pool has both.
  defp require_pool(state, workspace_id, pool_name) do
    if state.pools |> Map.get(workspace_id, %{}) |> Map.has_key?(pool_name) do
      :ok
    else
      {:error, :not_found}
    end
  end
end
