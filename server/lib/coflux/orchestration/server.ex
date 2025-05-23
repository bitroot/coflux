defmodule Coflux.Orchestration.Server do
  use GenServer, restart: :transient

  alias Coflux.Store
  alias Coflux.MapUtils

  alias Coflux.Orchestration.{
    Workspaces,
    Sessions,
    Runs,
    Results,
    CacheConfigs,
    TagSets,
    Agents,
    Manifests,
    Observations
  }

  @session_timeout_ms 5_000
  @sensor_rate_limit_ms 5_000
  @connected_agent_poll_interval_ms 30_000
  @disconnected_agent_poll_interval_ms 5_000
  @agent_idle_timeout_ms 5_000

  defmodule State do
    defstruct project_id: nil,
              db: nil,
              tick_timer: nil,

              # id -> %{name, base_id, state}
              workspaces: %{},

              # workspace_id -> %{pool_name -> pool}
              pools: %{},

              # name -> id
              workspace_names: %{},

              # agent_id -> %{created_at, pool_id, pool_name, workspace_id, state, data, session_id, stop_id, last_poll_at}
              agents: %{},

              # ref -> {pid, session_id}
              connections: %{},

              # session_id -> %{external_id, connection, targets, queue, starting, executing, expire_timer, concurrency, workspace_id, provides, agent_id, last_idle_at}
              sessions: %{},

              # external_id -> session_id
              session_ids: %{},

              # {module, target} -> %{type, session_ids}
              targets: %{},

              # ref -> topic
              listeners: %{},

              # topic -> %{ref -> pid}
              topics: %{},

              # topic -> [notification]
              notifications: %{},

              # execution_id -> [{session_id, request_id}]
              waiting: %{},

              # task_ref -> callback
              launcher_tasks: %{}
  end

  def start_link(opts) do
    {project_id, opts} = Keyword.pop!(opts, :project_id)
    GenServer.start_link(__MODULE__, project_id, opts)
  end

  def init(project_id) do
    case Store.open(project_id, "orchestration") do
      {:ok, db} ->
        state = %State{
          project_id: project_id,
          db: db
        }

        send(self(), :tick)

        {:ok, state, {:continue, :setup}}
    end
  end

  def handle_continue(:setup, state) do
    {:ok, workspaces} = Workspaces.get_all_workspaces(state.db)
    {:ok, agents} = Agents.get_active_agents(state.db)

    workspace_names =
      Map.new(workspaces, fn {workspace_id, workspace} ->
        {workspace.name, workspace_id}
      end)

    agents =
      Enum.reduce(
        agents,
        %{},
        fn {agent_id, created_at, pool_id, pool_name, workspace_id, state, data}, agents ->
          Map.put(agents, agent_id, %{
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
        pools: pools,
        agents: agents
      })

    {:ok, pending} = Runs.get_pending_assignments(state.db)

    state =
      Enum.reduce(pending, state, fn {execution_id}, state ->
        {:ok, state} = process_result(state, execution_id, :abandoned)
        state
      end)

    {:noreply, state}
  end

  def handle_call(:get_workspaces, _from, state) do
    workspaces =
      state.workspaces
      |> Enum.filter(fn {_, e} -> e.state != :archived end)
      |> Map.new(fn {workspace_id, workspace} ->
        {workspace_id, %{name: workspace.name, base_id: workspace.base_id}}
      end)

    {:reply, {:ok, workspaces}, state}
  end

  def handle_call({:create_workspace, name, base_id}, _from, state) do
    case Workspaces.create_workspace(state.db, name, base_id) do
      {:ok, workspace_id, workspace} ->
        state =
          state
          |> put_in([Access.key(:workspaces), workspace_id], workspace)
          |> put_in([Access.key(:workspace_names), workspace.name], workspace_id)
          |> notify_listeners(:workspaces, {:workspace, workspace_id, workspace})
          |> flush_notifications()

        {:reply, {:ok, workspace_id}, state}

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:update_workspace, workspace_id, updates}, _from, state) do
    # TODO: shut down/update pools
    case Workspaces.update_workspace(state.db, workspace_id, updates) do
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
            {:workspace, workspace_id, Map.take(workspace, [:name, :base_id, :state])}
          )
          |> flush_notifications()

        send(self(), :tick)

        # TODO: return updated?
        {:reply, :ok, state}

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:pause_workspace, workspace_id}, _from, state) do
    case Workspaces.pause_workspace(state.db, workspace_id) do
      :ok ->
        state =
          state
          |> put_in([Access.key(:workspaces), workspace_id, Access.key(:state)], :paused)
          |> notify_listeners(:workspaces, {:state, workspace_id, :paused})
          |> flush_notifications()

        {:reply, :ok, state}
    end
  end

  def handle_call({:resume_workspace, workspace_id}, _from, state) do
    case Workspaces.resume_workspace(state.db, workspace_id) do
      :ok ->
        state =
          state
          |> put_in([Access.key(:workspaces), workspace_id, Access.key(:state)], :active)
          |> notify_listeners(:workspaces, {:state, workspace_id, :active})
          |> flush_notifications()

        send(self(), :tick)

        {:reply, :ok, state}
    end
  end

  def handle_call({:archive_workspace, workspace_id}, _from, state) do
    case Workspaces.archive_workspace(state.db, workspace_id) do
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
          state.agents
          |> Enum.reduce(state, fn {agent_id, agent}, state ->
            if agent.workspace_id == workspace_id && agent.state == :active do
              update_agent_state(state, agent_id, :draining, workspace_id, agent.pool_name)
            else
              state
            end
          end)
          |> put_in([Access.key(:workspaces), workspace_id, Access.key(:state)], :archived)
          |> notify_listeners(:workspaces, {:state, workspace_id, :archived})
          |> flush_notifications()

        {:reply, :ok, state}

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:update_pool, workspace_name, pool_name, pool}, _from, state) do
    with {:ok, workspace_id, _} <- lookup_workspace_by_name(state, workspace_name) do
      case Workspaces.update_pool(state.db, workspace_id, pool_name, pool) do
        {:ok, pool_id} ->
          state =
            state.agents
            |> Enum.reduce(state, fn {agent_id, agent}, state ->
              if agent.state == :active && agent.pool_name == pool_name do
                # TODO: only if pool has meaningfully changed?
                update_agent_state(state, agent_id, :draining, workspace_id, pool_name)
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
            |> notify_listeners({:pool, workspace_id, pool_name}, {:updated, pool})
            |> notify_listeners({:pools, workspace_id}, {:pool, pool_name, pool})
            |> flush_notifications()

          {:reply, :ok, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:stop_agent, workspace_name, agent_id}, _from, state) do
    with {:ok, workspace_id, _} <- lookup_workspace_by_name(state, workspace_name),
         {:ok, agent} <- lookup_agent(state, agent_id, workspace_id) do
      state =
        state
        |> update_agent_state(agent_id, :draining, workspace_id, agent.pool_name)
        |> flush_notifications()

      send(self(), :tick)

      {:reply, :ok, state}
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:resume_agent, workspace_name, agent_id}, _from, state) do
    with {:ok, workspace_id, _} <- lookup_workspace_by_name(state, workspace_name),
         {:ok, agent} <- lookup_agent(state, agent_id, workspace_id) do
      state =
        state
        |> update_agent_state(agent_id, :active, workspace_id, agent.pool_name)
        |> flush_notifications()

      send(self(), :tick)

      {:reply, :ok, state}
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call(
        {:register_manifests, workspace_name, manifests},
        _from,
        state
      ) do
    case lookup_workspace_by_name(state, workspace_name) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, workspace_id, _} ->
        case Manifests.register_manifests(state.db, workspace_id, manifests) do
          :ok ->
            state =
              manifests
              |> Enum.reduce(state, fn {module, manifest}, state ->
                state =
                  Enum.reduce(manifest.workflows, state, fn {target_name, target}, state ->
                    notify_listeners(
                      state,
                      {:workflow, module, target_name, workspace_id},
                      {:target, target}
                    )
                  end)

                state =
                  Enum.reduce(manifest.sensors, state, fn {target_name, target}, state ->
                    notify_listeners(
                      state,
                      {:sensor, module, target_name, workspace_id},
                      {:target, target}
                    )
                  end)

                state
              end)
              |> notify_listeners(
                {:modules, workspace_id},
                {:manifests, manifests}
              )
              |> notify_listeners(
                {:targets, workspace_id},
                {:manifests,
                 Map.new(manifests, fn {module_name, targets} ->
                   {module_name,
                    %{
                      workflows: MapSet.new(Map.keys(targets.workflows)),
                      sensors: MapSet.new(Map.keys(targets.sensors))
                    }}
                 end)}
              )
              |> flush_notifications()

            {:reply, :ok, state}
        end
    end
  end

  def handle_call({:archive_module, workspace_name, module_name}, _from, state) do
    case lookup_workspace_by_name(state, workspace_name) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, workspace_id, _} ->
        case Manifests.archive_module(state.db, workspace_id, module_name) do
          :ok ->
            state =
              state
              |> notify_listeners(
                {:modules, workspace_id},
                {:manifest, module_name, nil}
              )
              |> flush_notifications()

            {:reply, :ok, state}
        end
    end
  end

  def handle_call({:get_workflow, workspace_name, module, target_name}, _from, state) do
    with {:ok, workspace_id, _} <- lookup_workspace_by_name(state, workspace_name),
         {:ok, workflow} <-
           Manifests.get_latest_workflow(state.db, workspace_id, module, target_name) do
      {:reply, {:ok, workflow}, state}
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call(
        {:start_session, workspace_name, agent_id, provides, concurrency, pid},
        _from,
        state
      ) do
    with {:ok, workspace_id, _} <- lookup_workspace_by_name(state, workspace_name),
         {:ok, agent} <- lookup_agent(state, agent_id, workspace_id) do
      case Sessions.start_session(state.db, workspace_id, provides, agent_id) do
        {:ok, session_id, external_session_id, now} ->
          ref = Process.monitor(pid)

          session = %{
            external_id: external_session_id,
            connection: ref,
            targets: %{},
            queue: [],
            starting: MapSet.new(),
            executing: MapSet.new(),
            expire_timer: nil,
            concurrency: concurrency,
            workspace_id: workspace_id,
            provides: provides,
            agent_id: agent_id,
            last_idle_at: now
          }

          state =
            state
            |> put_in([Access.key(:sessions), session_id], session)
            |> put_in([Access.key(:session_ids), external_session_id], session_id)
            |> put_in([Access.key(:connections), ref], {pid, session_id})
            |> notify_listeners(
              {:sessions, workspace_id},
              {:session, session_id,
               %{connected: true, executions: 0, pool_name: if(agent, do: agent.pool_name)}}
            )

          # TODO: check agent isn't already assigned to a (different (active?)) session?

          state =
            if agent do
              state
              |> put_in(
                [Access.key(:agents), agent_id, Access.key(:session_id)],
                session_id
              )
              |> notify_listeners(
                {:pool, workspace_id, agent.pool_name},
                {:agent_connected, agent_id, true}
              )
            else
              state
            end

          state = flush_notifications(state)

          {:reply, {:ok, external_session_id}, state}
      end
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:resume_session, external_session_id, pid}, _from, state) do
    case Map.fetch(state.session_ids, external_session_id) do
      {:ok, session_id} ->
        session = Map.fetch!(state.sessions, session_id)

        if session.expire_timer do
          Process.cancel_timer(session.expire_timer)
        end

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
            &Map.merge(&1, %{connection: ref, queue: []})
          )
          |> notify_listeners(
            {:sessions, session.workspace_id},
            {:session, session_id,
             %{
               connected: true,
               executions: session.starting |> MapSet.union(session.executing) |> Enum.count(),
               pool_name:
                 if(session.agent_id,
                   do: state.agents |> Map.fetch!(session.agent_id) |> Map.fetch!(:pool_name)
                 )
             }}
          )

        state =
          if session.agent_id do
            agent = Map.fetch!(state.agents, session.agent_id)
            # TODO: check that agent workspace matches session workspace?
            # TODO: check agent isn't assigned to a different session?
            state
            |> put_in(
              [Access.key(:agents), session.agent_id, Access.key(:session_id)],
              session_id
            )
            |> notify_listeners(
              {:pool, agent.workspace_id, agent.pool_name},
              {:agent_connected, session.agent_id, true}
            )
          else
            state
          end

        executions = MapSet.union(session.executing, session.starting)

        send(self(), :tick)

        state = flush_notifications(state)
        {:reply, {:ok, external_session_id, executions}, state}

      :error ->
        {:reply, {:error, :no_session}, state}
    end
  end

  def handle_call({:declare_targets, external_session_id, targets}, _from, state) do
    session_id = Map.fetch!(state.session_ids, external_session_id)

    state =
      state
      |> assign_targets(targets, session_id)
      |> flush_notifications()

    send(self(), :tick)

    {:reply, :ok, state}
  end

  def handle_call({:start_run, module, target_name, type, arguments, opts}, _from, state) do
    {:ok, parent} =
      case Keyword.get(opts, :parent_id) do
        nil ->
          {:ok, nil}

        parent_id ->
          {:ok, run_id} = Runs.get_execution_run_id(state.db, parent_id)
          {:ok, {run_id, parent_id}}
      end

    {:ok, workspace_id} =
      case parent do
        {_run_id, parent_id} ->
          Runs.get_workspace_id_for_execution(state.db, parent_id)

        nil ->
          workspace_name = Keyword.get(opts, :workspace)

          case lookup_workspace_by_name(state, workspace_name) do
            {:ok, workspace_id, _} -> {:ok, workspace_id}
            {:error, :workspace_invalid} -> {:ok, nil}
          end
      end

    if workspace_id do
      {:ok, external_run_id, external_step_id, execution_id, state} =
        schedule_run(state, module, target_name, type, arguments, workspace_id, opts)

      send(self(), :tick)
      state = flush_notifications(state)

      {:reply, {:ok, external_run_id, external_step_id, execution_id}, state}
    else
      {:reply, {:error, :workspace_invalid}, state}
    end
  end

  def handle_call(
        {:schedule_step, parent_id, module, target_name, type, arguments, opts},
        _from,
        state
      ) do
    {:ok, parent_run_id} = Runs.get_execution_run_id(state.db, parent_id)
    {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, parent_id)
    {:ok, run} = Runs.get_run_by_id(state.db, parent_run_id)

    cache_workspace_ids = get_cache_workspace_ids(state, workspace_id)

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
         external_step_id: external_step_id,
         execution_id: execution_id,
         attempt: attempt,
         created_at: created_at,
         cache_key: cache_key,
         memo_key: memo_key,
         memo_hit: memo_hit,
         child_added: child_added
       }} ->
        cache = Keyword.get(opts, :cache)
        execute_after = Keyword.get(opts, :execute_after)
        requires = Keyword.get(opts, :requires) || %{}

        state =
          if !memo_hit do
            arguments = Enum.map(arguments, &build_value(&1, state.db))

            state
            |> notify_listeners(
              {:run, run.id},
              {:step, external_step_id,
               %{
                 module: module,
                 target: target_name,
                 type: type,
                 parent_id: parent_id,
                 cache_config: cache,
                 cache_key: cache_key,
                 memo_key: memo_key,
                 created_at: created_at,
                 arguments: arguments,
                 requires: requires
               }, workspace_id}
            )
            |> notify_listeners(
              {:run, run.id},
              {:execution, external_step_id, attempt, execution_id, workspace_id, created_at,
               execute_after}
            )
          else
            state
          end

        state =
          if child_added do
            notify_listeners(
              state,
              {:run, run.id},
              {:child, parent_id, {external_step_id, attempt}}
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
                {:modules, workspace_id},
                {:scheduled, module, execution_id, execute_at}
              )
              |> notify_listeners(
                {:module, module, workspace_id},
                {:scheduled, execution_id, target_name, run.external_id, external_step_id,
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
            {:targets, workspace_id},
            {:step, module, target_name, type, run.external_id, external_step_id, attempt}
          )
          |> flush_notifications()

        {:reply, {:ok, run.external_id, external_step_id, execution_id}, state}
    end
  end

  def handle_call({:rerun_step, external_step_id, workspace_name}, _from, state) do
    # TODO: abort/cancel any running/scheduled retry? (for the same workspace) (and reference this retry?)
    case lookup_workspace_by_name(state, workspace_name) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, workspace_id, _} ->
        {:ok, step} = Runs.get_step_by_external_id(state.db, external_step_id)

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
          {:ok, execution_id, attempt, state} = rerun_step(state, step, workspace_id)
          state = flush_notifications(state)
          {:reply, {:ok, execution_id, attempt}, state}
        else
          {:reply, {:error, :workspace_invalid}, state}
        end
    end
  end

  def handle_call({:cancel_execution, execution_id}, _from, state) do
    execution_id =
      case Results.get_result(state.db, execution_id) do
        {:ok, {{:spawned, spawned_execution_id}, _created_at}} -> spawned_execution_id
        {:ok, _other} -> execution_id
      end

    {:ok, executions} = Runs.get_execution_descendants(state.db, execution_id)

    state =
      Enum.reduce(
        executions,
        state,
        fn {execution_id, module, assigned_at, completed_at}, state ->
          if !completed_at do
            state =
              case record_and_notify_result(
                     state,
                     execution_id,
                     :cancelled,
                     module
                   ) do
                {:ok, state} -> state
                {:error, :already_recorded} -> state
              end

            if assigned_at do
              abort_execution(state, execution_id)
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

  def handle_call({:record_heartbeats, executions, external_session_id}, _from, state) do
    # TODO: handle execution statuses?
    case Map.fetch(state.session_ids, external_session_id) do
      {:ok, session_id} ->
        session = Map.fetch!(state.sessions, session_id)

        execution_ids = executions |> Map.keys() |> MapSet.new()

        state =
          session.starting
          |> MapSet.intersection(execution_ids)
          |> Enum.reduce(state, fn execution_id, state ->
            update_in(state.sessions[session_id].starting, &MapSet.delete(&1, execution_id))
          end)

        state =
          session.executing
          |> MapSet.difference(execution_ids)
          |> Enum.reduce(state, fn execution_id, state ->
            case Results.has_result?(state.db, execution_id) do
              {:ok, false} ->
                {:ok, state} = process_result(state, execution_id, :abandoned)
                state

              {:ok, true} ->
                state
            end
          end)

        state =
          execution_ids
          |> MapSet.difference(session.starting)
          |> MapSet.difference(session.executing)
          |> Enum.reduce(state, fn execution_id, state ->
            case Results.has_result?(state.db, execution_id) do
              {:ok, false} ->
                state

              {:ok, true} ->
                send_session(state, session_id, {:abort, execution_id})
            end
          end)

        state =
          case Runs.record_hearbeats(state.db, executions) do
            {:ok, _created_at} ->
              put_in(state.sessions[session_id].executing, execution_ids)
          end

        session = Map.fetch!(state.sessions, session_id)

        state =
          state
          |> notify_listeners(
            {:sessions, session.workspace_id},
            {:executions, session_id,
             session.starting |> MapSet.union(session.executing) |> Enum.count()}
          )
          |> flush_notifications()

        {:reply, :ok, state}

      :error ->
        {:reply, {:error, :session_invalid}, state}
    end
  end

  def handle_call({:notify_terminated, execution_ids}, _from, state) do
    # TODO: record in database?

    now = System.os_time(:millisecond)

    state =
      execution_ids
      |> Enum.reduce(state, fn execution_id, state ->
        case find_session_for_execution(state, execution_id) do
          {:ok, session_id} ->
            state =
              update_in(state.sessions[session_id], fn session ->
                starting = MapSet.delete(session.starting, execution_id)
                executing = MapSet.delete(session.executing, execution_id)

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
              {:sessions, session.workspace_id},
              {:executions, session_id, executions}
            )

          :error ->
            state
        end
      end)
      |> flush_notifications()

    send(self(), :tick)

    {:reply, :ok, state}
  end

  def handle_call({:record_checkpoint, execution_id, arguments}, _from, state) do
    case Results.record_checkpoint(state.db, execution_id, arguments) do
      {:ok, _checkpoint_id, _attempt, _created_at} ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:record_result, execution_id, result}, _from, state) do
    case process_result(state, execution_id, result) do
      {:ok, state} ->
        state = flush_notifications(state)
        {:reply, :ok, state}
    end
  end

  def handle_call(
        {:get_result, execution_id, from_execution_id, external_session_id, request_id},
        _from,
        state
      ) do
    # TODO: check execution_id exists? (call resolve_result first?)

    state =
      if from_execution_id do
        {:ok, id} = Runs.record_result_dependency(state.db, from_execution_id, execution_id)

        if id do
          {:ok, run_id} = Runs.get_execution_run_id(state.db, from_execution_id)

          # TODO: only resolve if there are listeners to notify
          dependency = resolve_execution(state.db, execution_id)

          notify_listeners(
            state,
            {:run, run_id},
            {:result_dependency, from_execution_id, execution_id, dependency}
          )
        else
          state
        end
      else
        state
      end

    {result, state} =
      case resolve_result(state.db, execution_id) do
        {:pending, execution_id} ->
          session_id = Map.fetch!(state.session_ids, external_session_id)

          state =
            update_in(
              state,
              [Access.key(:waiting), Access.key(execution_id, [])],
              &[{session_id, request_id} | &1]
            )

          {:wait, state}

        {:ok, result} ->
          {{:ok, result}, state}
      end

    state = flush_notifications(state)

    {:reply, result, state}
  end

  def handle_call({:put_asset, execution_id, type, path, blob_key, size, metadata}, _from, state) do
    {:ok, run_id} = Runs.get_execution_run_id(state.db, execution_id)

    {:ok, asset_id} =
      Results.get_or_create_asset(state.db, execution_id, type, path, blob_key, size, metadata)

    asset = resolve_asset(state.db, asset_id)

    state =
      state
      |> notify_listeners({:run, run_id}, {:asset, execution_id, asset_id, asset})
      |> flush_notifications()

    {:reply, {:ok, asset_id}, state}
  end

  def handle_call({:get_asset, asset_id, opts}, _from, state) do
    from_execution_id = opts[:from_execution_id]
    load_metadata = opts[:load_metadata]

    case Results.get_asset_by_id(state.db, asset_id, load_metadata) do
      {:ok, {type, path, blob_key, _size, metadata}} ->
        if from_execution_id do
          {:ok, _} = Runs.record_asset_dependency(state.db, from_execution_id, asset_id)
        end

        {:reply, {:ok, type, path, blob_key, metadata}, state}

      {:ok, nil} ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:record_logs, execution_id, messages}, _from, state) do
    {:ok, run_id} = Runs.get_execution_run_id(state.db, execution_id)

    case Observations.record_logs(state.db, execution_id, messages) do
      :ok ->
        messages =
          Enum.map(messages, fn {timestamp, level, template, values} ->
            {execution_id, timestamp, level, template,
             Map.new(values, fn {k, v} -> {k, build_value(v, state.db)} end)}
          end)

        state =
          state
          |> notify_listeners({:logs, run_id}, {:messages, messages})
          |> notify_listeners({:run, run_id}, {:log_counts, execution_id, length(messages)})
          |> flush_notifications()

        {:reply, :ok, state}
    end
  end

  def handle_call({:subscribe_workspaces, pid}, _from, state) do
    {:ok, ref, state} = add_listener(state, :workspaces, pid)

    workspaces =
      Map.new(state.workspaces, fn {workspace_id, workspace} ->
        {workspace_id, Map.take(workspace, [:name, :base_id, :state])}
      end)

    {:reply, {:ok, workspaces, ref}, state}
  end

  def handle_call({:subscribe_modules, workspace_id, pid}, _from, state) do
    case lookup_workspace_by_id(state, workspace_id) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, _} ->
        {:ok, manifests} = Manifests.get_latest_manifests(state.db, workspace_id)

        executing =
          state.sessions
          |> Map.values()
          |> Enum.reduce(MapSet.new(), fn session, executing ->
            executing |> MapSet.union(session.executing) |> MapSet.union(session.starting)
          end)

        {:ok, executions} = Runs.get_unassigned_executions(state.db)
        # TODO: get/include assigned (pending) executions

        executions =
          Enum.reduce(executions, %{}, fn execution, executions ->
            executions
            |> Map.put_new(execution.module, {MapSet.new(), %{}})
            |> Map.update!(execution.module, fn {module_executing, module_scheduled} ->
              if Enum.member?(executing, execution.execution_id) do
                {MapSet.put(module_executing, execution.execution_id), module_scheduled}
              else
                {module_executing,
                 Map.put(
                   module_scheduled,
                   execution.execution_id,
                   execution.execute_after || execution.created_at
                 )}
              end
            end)
          end)

        {:ok, ref, state} =
          add_listener(state, {:modules, workspace_id}, pid)

        {:reply, {:ok, manifests, executions, ref}, state}
    end
  end

  def handle_call({:subscribe_module, module, workspace_id, pid}, _from, state) do
    case lookup_workspace_by_id(state, workspace_id) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, _} ->
        {:ok, executions} = Runs.get_module_executions(state.db, module)
        {:ok, ref, state} = add_listener(state, {:module, module, workspace_id}, pid)
        {:reply, {:ok, executions, ref}, state}
    end
  end

  def handle_call({:subscribe_pools, workspace_id, pid}, _from, state) do
    case lookup_workspace_by_id(state, workspace_id) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, _} ->
        # TODO: include non-active pools that contain active agents
        pools = Map.get(state.pools, workspace_id, %{})
        {:ok, ref, state} = add_listener(state, {:pools, workspace_id}, pid)
        {:reply, {:ok, pools, ref}, state}
    end
  end

  def handle_call({:subscribe_pool, workspace_id, pool_name, pid}, _from, state) do
    case lookup_workspace_by_id(state, workspace_id) do
      {:ok, _} ->
        pool = Map.get(state.pools[workspace_id], pool_name)
        {:ok, pool_agents} = Agents.get_pool_agents(state.db, pool_name)

        # TODO: include 'active' agents that aren't in this (potentially limited) list

        agents =
          Map.new(
            pool_agents,
            fn {agent_id, starting_at, started_at, start_error, stopping_at, stopped_at,
                stop_error, deactivated_at} ->
              agent = Map.get(state.agents, agent_id)

              connected =
                if agent do
                  if agent.session_id do
                    case Map.fetch(state.sessions, agent.session_id) do
                      {:ok, session} -> !is_nil(session.connection)
                      :error -> false
                    end
                  else
                    false
                  end
                end

              # TODO: include pool_id?
              {agent_id,
               %{
                 starting_at: starting_at,
                 started_at: started_at,
                 start_error: start_error,
                 stopping_at: stopping_at,
                 stopped_at: stopped_at,
                 stop_error: stop_error,
                 deactivated_at: deactivated_at,
                 state: if(agent, do: agent.state),
                 connected: connected
               }}
            end
          )

        {:ok, ref, state} = add_listener(state, {:pool, workspace_id, pool_name}, pid)
        {:reply, {:ok, pool, agents, ref}, state}

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:subscribe_sessions, workspace_id, pid}, _from, state) do
    case lookup_workspace_by_id(state, workspace_id) do
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, _} ->
        sessions =
          state.sessions
          |> Enum.filter(fn {_, session} ->
            session.workspace_id == workspace_id
          end)
          |> Map.new(fn {session_id, session} ->
            executions =
              session.starting
              |> MapSet.union(session.executing)
              |> Enum.count()

            pool_name =
              if session.agent_id do
                state.agents |> Map.fetch!(session.agent_id) |> Map.fetch!(:pool_name)
              end

            {session_id,
             %{
               connected: !is_nil(session.connection),
               executions: executions,
               pool_name: pool_name
             }}
          end)

        {:ok, ref, state} = add_listener(state, {:sessions, workspace_id}, pid)
        {:reply, {:ok, sessions, ref}, state}
    end
  end

  def handle_call(
        {:subscribe_workflow, module, target_name, workspace_id, pid},
        _from,
        state
      ) do
    with {:ok, _} <- lookup_workspace_by_id(state, workspace_id),
         {:ok, workflow} <-
           Manifests.get_latest_workflow(state.db, workspace_id, module, target_name),
         {:ok, instruction} <-
           if(workflow && workflow.instruction_id,
             do: Manifests.get_instruction(state.db, workflow.instruction_id),
             else: {:ok, nil}
           ),
         {:ok, runs} = Runs.get_target_runs(state.db, module, target_name, workspace_id) do
      {:ok, ref, state} =
        add_listener(state, {:workflow, module, target_name, workspace_id}, pid)

      {:reply, {:ok, workflow, instruction, runs, ref}, state}
    else
      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call(
        {:subscribe_sensor, module, target_name, workspace_id, pid},
        _from,
        state
      ) do
    with {:ok, _} <- lookup_workspace_by_id(state, workspace_id),
         {:ok, sensor} <-
           Manifests.get_latest_sensor(state.db, workspace_id, module, target_name),
         {:ok, instruction} <-
           if(sensor && sensor.instruction_id,
             do: Manifests.get_instruction(state.db, sensor.instruction_id),
             else: {:ok, nil}
           ),
         {:ok, runs} = Runs.get_target_runs(state.db, module, target_name, workspace_id) do
      {:ok, ref, state} =
        add_listener(state, {:sensor, module, target_name, workspace_id}, pid)

      {:reply, {:ok, sensor, instruction, runs, ref}, state}
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
          if run.parent_id do
            resolve_execution(state.db, run.parent_id)
          end

        {:ok, steps} = Runs.get_run_steps(state.db, run.id)
        {:ok, run_executions} = Runs.get_run_executions(state.db, run.id)
        {:ok, run_dependencies} = Runs.get_run_dependencies(state.db, run.id)
        {:ok, run_children} = Runs.get_run_children(state.db, run.id)
        {:ok, log_counts} = Observations.get_counts_for_run(state.db, run.id)

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
            {result, completed_at} =
              case Results.get_result(state.db, execution_id) do
                {:ok, {result, completed_at}} ->
                  result = build_result(result, state.db)
                  {result, completed_at}

                {:ok, nil} ->
                  {nil, nil}
              end

            Map.put(results, execution_id, {result, completed_at})
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

            {step.external_id,
             %{
               module: step.module,
               target: step.target,
               type: step.type,
               parent_id: step.parent_id,
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
                                created_at, assigned_at} ->
                   {result, completed_at} = Map.fetch!(results, execution_id)
                   # TODO: load assets in one query
                   {:ok, asset_ids} = Results.get_assets_for_execution(state.db, execution_id)
                   assets = Map.new(asset_ids, &{&1, resolve_asset(state.db, &1)})

                   # TODO: batch? get `get_dependencies` to resolve?
                   dependencies =
                     run_dependencies
                     |> Map.get(execution_id, [])
                     |> Map.new(fn dependency_id ->
                       {dependency_id, resolve_execution(state.db, dependency_id)}
                     end)

                   {attempt,
                    %{
                      execution_id: execution_id,
                      workspace_id: workspace_id,
                      created_at: created_at,
                      execute_after: execute_after,
                      assigned_at: assigned_at,
                      completed_at: completed_at,
                      assets: assets,
                      dependencies: dependencies,
                      result: result,
                      children: Map.get(run_children, execution_id, []),
                      log_count: Map.get(log_counts, execution_id, 0)
                    }}
                 end)
             }}
          end)

        {:ok, ref, state} = add_listener(state, {:run, run.id}, pid)
        {:reply, {:ok, run, parent, steps, ref}, state}
    end
  end

  def handle_call({:subscribe_logs, external_run_id, pid}, _from, state) do
    case Runs.get_run_by_external_id(state.db, external_run_id) do
      {:ok, run} ->
        case Observations.get_messages_for_run(state.db, run.id) do
          {:ok, messages} ->
            messages =
              Enum.map(messages, fn {execution_id, timestamp, level, template, values} ->
                {execution_id, timestamp, level, template,
                 Map.new(values, fn {k, v} -> {k, build_value(v, state.db)} end)}
              end)

            {:ok, ref, state} = add_listener(state, {:logs, run.id}, pid)
            {:reply, {:ok, ref, messages}, state}
        end
    end
  end

  def handle_call({:subscribe_targets, workspace_id, pid}, _from, state) do
    # TODO: indicate which are archived (only workflows/sensors)
    {:ok, workflows, sensors} =
      Manifests.get_all_targets_for_workspace(state.db, workspace_id)

    {:ok, steps} = Runs.get_steps_for_workspace(state.db, workspace_id)

    result =
      Enum.reduce(
        %{workflow: workflows, sensor: sensors},
        %{},
        fn {target_type, targets}, result ->
          Enum.reduce(targets, result, fn {module_name, target_names}, result ->
            Enum.reduce(target_names, result, fn target_name, result ->
              put_in(
                result,
                [Access.key(module_name, %{}), target_name],
                {target_type, nil}
              )
            end)
          end)
        end
      )

    result =
      Enum.reduce(
        steps,
        result,
        fn {module_name, target_name, target_type, run_external_id, step_external_id, attempt},
           result ->
          put_in(
            result,
            [Access.key(module_name, %{}), target_name],
            {target_type, {run_external_id, step_external_id, attempt}}
          )
        end
      )

    {:ok, ref, state} = add_listener(state, {:targets, workspace_id}, pid)
    {:reply, {:ok, result, ref}, state}
  end

  def handle_cast({:unsubscribe, ref}, state) do
    Process.demonitor(ref, [:flush])
    state = remove_listener(state, ref)
    {:noreply, state}
  end

  def handle_info({:expire_session, session_id}, state) do
    if state.sessions[session_id].connection do
      IO.puts("Ignoring session expire (#{inspect(session_id)})")
      {:noreply, state}
    else
      state =
        state
        |> remove_session(session_id)
        |> flush_notifications()

      {:noreply, state}
    end
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
      executions_due
      |> Enum.reverse()
      |> Enum.reduce(
        {state, [], []},
        fn
          execution, {state, assigned, unassigned} ->
            # TODO: support caching for other attempts?
            cached_execution_id =
              if execution.attempt == 1 && execution.cache_config_id do
                cache_workspace_ids = get_cache_workspace_ids(state, execution.workspace_id)
                cache = Map.fetch!(cache_configs, execution.cache_config_id)
                recorded_after = if cache.max_age, do: now - cache.max_age, else: 0

                case Runs.find_cached_execution(
                       state.db,
                       cache_workspace_ids,
                       execution.step_id,
                       execution.cache_key,
                       recorded_after
                     ) do
                  {:ok, cached_execution_id} ->
                    cached_execution_id
                end
              end

            if cached_execution_id do
              {:ok, state} =
                process_result(state, execution.execution_id, {:cached, cached_execution_id})

              {state, assigned, unassigned}
            else
              # TODO: choose session before resolving arguments?
              {:ok, arguments} =
                case Results.get_latest_checkpoint(state.db, execution.step_id) do
                  {:ok, nil} ->
                    Runs.get_step_arguments(state.db, execution.step_id)

                  {:ok, {checkpoint_id, _, _, _}} ->
                    Results.get_checkpoint_arguments(state.db, checkpoint_id)
                end

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

                      state =
                        state
                        |> update_in(
                          [Access.key(:sessions), session_id, :starting],
                          &MapSet.put(&1, execution.execution_id)
                        )
                        |> send_session(
                          session_id,
                          {:execute, execution.execution_id, execution.module,
                           execution.target, arguments}
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
                             if(execution.retry_limit > 0,
                               do: %{
                                 limit: execution.retry_limit,
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
      |> Enum.group_by(fn {execution, _assigned_at} -> execution.run_id end)
      |> Enum.reduce(state, fn {run_id, executions}, state ->
        assigned =
          Map.new(executions, fn {execution, assigned_at} ->
            {execution.execution_id, assigned_at}
          end)

        notify_listeners(state, {:run, run_id}, {:assigned, assigned})
      end)

    assigned_groups =
      assigned
      |> Enum.group_by(fn {execution, _} -> execution.workspace_id end)
      |> Map.new(fn {workspace_id, executions} ->
        {workspace_id,
         executions
         |> Enum.group_by(
           fn {execution, _} -> execution.module end,
           fn {execution, _} -> execution.execution_id end
         )
         |> Map.new(fn {k, v} -> {k, MapSet.new(v)} end)}
      end)

    state =
      Enum.reduce(assigned_groups, state, fn {workspace_id, workspace_executions}, state ->
        notify_listeners(
          state,
          {:modules, workspace_id},
          {:assigned, workspace_executions}
        )
      end)

    state =
      Enum.reduce(assigned_groups, state, fn {workspace_id, workspace_executions}, state ->
        Enum.reduce(workspace_executions, state, fn {module, execution_ids}, state ->
          module_executions =
            Enum.reduce(assigned, %{}, fn {execution, assigned_at}, module_executions ->
              if MapSet.member?(execution_ids, execution.execution_id) do
                Map.put(module_executions, execution.execution_id, assigned_at)
              else
                module_executions
              end
            end)

          notify_listeners(
            state,
            {:module, module, workspace_id},
            {:assigned, module_executions}
          )
        end)
      end)

    state =
      if Enum.any?(unassigned) do
        latest_pool_launch_at =
          state.agents
          |> Map.values()
          |> Enum.reduce(%{}, fn agent, latest ->
            Map.update(latest, agent.pool_id, agent.created_at, &max(&1, agent.created_at))
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
            case Agents.create_agent(state.db, pool_id) do
              {:ok, agent_id, created_at} ->
                {pool_name, pool} =
                  Enum.find(
                    state.pools[workspace_id],
                    &(elem(&1, 1).id == pool_id)
                  )

                state
                |> call_launcher(
                  pool.launcher,
                  :launch,
                  [
                    state.project_id,
                    state.workspaces[workspace_id].name,
                    agent_id,
                    pool.modules,
                    pool.provides,
                    Map.delete(pool.launcher, :type)
                  ],
                  fn state, result ->
                    {data, error} =
                      case result do
                        {:ok, {:ok, data}} -> {data, nil}
                        {:ok, {:error, error}} -> {nil, error}
                        :error -> {nil, nil}
                      end

                    {:ok, started_at} =
                      Agents.create_agent_launch_result(state.db, agent_id, data, error)

                    state =
                      state
                      |> put_in([Access.key(:agents), agent_id, Access.key(:data)], data)
                      |> notify_listeners(
                        {:pool, workspace_id, pool_name},
                        {:launch_result, agent_id, started_at, error}
                      )

                    state =
                      if error do
                        deactivate_agent(state, agent_id)
                      else
                        state
                      end

                    flush_notifications(state)
                  end
                )
                |> put_in([Access.key(:agents), agent_id], %{
                  created_at: created_at,
                  pool_id: pool_id,
                  pool_name: pool_name,
                  workspace_id: workspace_id,
                  state: :active,
                  data: nil,
                  session_id: nil,
                  stop_id: nil,
                  last_poll_at: nil
                })
                |> notify_listeners(
                  {:pool, workspace_id, pool_name},
                  {:agent, agent_id, created_at}
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
      state.agents
      |> Enum.filter(fn {_agent_id, agent} ->
        # TODO: don't poll if a poll is in progress?
        if agent.data do
          if is_nil(agent.last_poll_at) do
            true
          else
            connection =
              if agent.session_id && Map.has_key?(state.sessions, agent.session_id),
                do: state.sessions[agent.session_id].connection

            interval_ms =
              if connection,
                do: @connected_agent_poll_interval_ms,
                else: @disconnected_agent_poll_interval_ms

            now - agent.last_poll_at > interval_ms
          end
        else
          false
        end
      end)
      |> Enum.reduce(state, fn {agent_id, agent}, state ->
        {:ok, launcher} = Workspaces.get_launcher_for_pool(state.db, agent.pool_id)

        state
        |> call_launcher(launcher, :poll, [agent.data], fn state, result ->
          case result do
            {:ok, {:ok, true}} ->
              state

            {:ok, {:ok, false}} ->
              deactivate_agent(state, agent_id)

            :error ->
              # TODO: ?
              state
          end
        end)
        |> put_in([Access.key(:agents), agent_id, :last_poll_at], now)
      end)

    state =
      state.agents
      |> Enum.group_by(fn {_, agent} -> agent.pool_name end)
      |> Enum.flat_map(fn {_pool_name, agents} ->
        # TODO: consider min/max pool size
        Enum.filter(agents, fn {_agent_id, agent} ->
          # TODO: better way to check launched than checking existence of data?
          if agent.state == :active && agent.session_id && agent.data do
            session = Map.fetch!(state.sessions, agent.session_id)
            idle_time = now - session.last_idle_at

            if Enum.empty?(session.starting) && Enum.empty?(session.executing) &&
                 idle_time >= @agent_idle_timeout_ms do
              true
            end
          end
        end)
      end)
      |> Enum.reduce(state, fn {agent_id, agent}, state ->
        update_agent_state(state, agent_id, :draining, agent.workspace_id, agent.pool_name)
      end)

    state =
      state.agents
      |> Enum.filter(fn {_agent_id, agent} ->
        if agent.session_id do
          if agent.state == :draining && agent.data && !agent.stop_id do
            session = Map.fetch!(state.sessions, agent.session_id)
            Enum.empty?(session.starting) && Enum.empty?(session.executing)
          end
        else
          !is_nil(agent.data)
        end
      end)
      |> Enum.reduce(state, fn {agent_id, agent}, state ->
        {:ok, agent_stop_id, stopping_at} = Agents.create_agent_stop(state.db, agent_id)
        {:ok, launcher} = Workspaces.get_launcher_for_pool(state.db, agent.pool_id)

        state =
          state
          |> put_in([Access.key(:agents), agent_id, :stop_id], agent_stop_id)
          |> notify_listeners(
            {:pool, agent.workspace_id, agent.pool_name},
            {:agent_stopping, agent_id, stopping_at}
          )

        call_launcher(state, launcher, :stop, [agent.data], fn state, result ->
          case result do
            {:ok, :ok} ->
              {:ok, stopped_at} =
                Agents.create_agent_stop_result(state.db, agent_stop_id, nil)

              state
              |> notify_listeners(
                {:pool, agent.workspace_id, agent.pool_name},
                {:agent_stop_result, agent_id, stopped_at, nil}
              )

            :error ->
              # TODO: get error details
              error = %{}

              {:ok, _} =
                Agents.create_agent_stop_result(state.db, agent_stop_id, error)

              state =
                notify_listeners(
                  state,
                  {:pool, agent.workspace_id, agent.pool_name},
                  {:agent_stop_result, agent_id, nil, error}
                )

              # TODO: unset 'stop_id' of agent in state? (so it can be retried? but somehow limit rate?)
              state
          end
        end)
      end)

    delay_ms =
      [
        if(next_execute_after, do: trunc(next_execute_after) - System.os_time(:millisecond)),
        if(state.agents, do: 5_000)
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
              expire_timer =
                Process.send_after(self(), {:expire_session, session_id}, @session_timeout_ms)

              state =
                state
                |> update_in(
                  [Access.key(:sessions), session_id],
                  &Map.merge(&1, %{connection: nil, expire_timer: expire_timer})
                )
                |> notify_listeners(
                  {:sessions, session.workspace_id},
                  {:connected, session_id, false}
                )

              state =
                if session.agent_id do
                  pool_name = Map.fetch!(state.agents, session.agent_id).pool_name

                  notify_listeners(
                    state,
                    {:pool, session.workspace_id, pool_name},
                    {:agent_connected, session.agent_id, false}
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

      true ->
        {:noreply, state}
    end
  end

  def terminate(_reason, state) do
    Store.close(state.db)
  end

  defp lookup_workspace_by_name(state, workspace_name) do
    case Map.fetch(state.workspace_names, workspace_name) do
      {:ok, workspace_id} ->
        workspace = Map.fetch!(state.workspaces, workspace_id)

        if workspace.state != :archived do
          {:ok, workspace_id, workspace}
        else
          {:error, :workspace_invalid}
        end

      :error ->
        {:error, :workspace_invalid}
    end
  end

  defp lookup_workspace_by_id(state, workspace_id) do
    case Map.fetch(state.workspaces, workspace_id) do
      {:ok, workspace} ->
        # TODO: include workspace? Map.fetch!(state.workspaces, workspace_id)
        {:ok, workspace}

      :error ->
        {:error, :workspace_invalid}
    end
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

  defp lookup_agent(state, agent_id, expected_workspace_id) do
    if agent_id do
      case Map.fetch(state.agents, agent_id) do
        :error ->
          {:error, :no_agent}

        {:ok, agent} ->
          if agent.workspace_id != expected_workspace_id do
            {:error, :no_agent}
          else
            {:ok, agent}
          end
      end
    else
      {:ok, nil}
    end
  end

  defp remove_session(state, session_id) do
    {session, state} = pop_in(state.sessions[session_id])

    state =
      session.executing
      |> MapSet.union(session.starting)
      |> Enum.reduce(state, fn execution_id, state ->
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
        waiting
        |> Enum.map(fn {execution_id, execution_waiting} ->
          {execution_id,
           Enum.reject(execution_waiting, fn {s_id, _} ->
             s_id == session_id
           end)}
        end)
        |> Enum.reject(fn {_execution_id, execution_waiting} ->
          Enum.empty?(execution_waiting)
        end)
        |> Map.new()
      end)
      |> notify_listeners(
        {:sessions, session.workspace_id},
        {:session, session_id, nil}
      )

    state =
      if session.agent_id do
        case Map.fetch(state.agents, session.agent_id) do
          {:ok, agent} ->
            # TODO: check that agent workspace matches session workspace?
            state
            |> put_in([Access.key(:agents), session.agent_id, :session_id], nil)
            |> notify_listeners(
              {:pool, agent.workspace_id, agent.pool_name},
              {:agent_connected, session.agent_id, false}
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
              {execution.module, execution.target, execution.workspace_id,
               execution.defer_key}

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

    case Runs.schedule_run(
           state.db,
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
         external_run_id: external_run_id,
         external_step_id: external_step_id,
         execution_id: execution_id,
         attempt: attempt,
         created_at: created_at
       }} ->
        # TODO: neater way to get execute_after?
        execute_after = Keyword.get(opts, :execute_after)
        execute_at = execute_after || created_at

        state =
          state
          |> notify_listeners(
            case type do
              :workflow -> {:workflow, module, target_name, workspace_id}
              :sensor -> {:sensor, module, target_name, workspace_id}
            end,
            {:run, external_run_id, created_at}
          )
          |> notify_listeners(
            {:modules, workspace_id},
            {:scheduled, module, execution_id, execute_at}
          )
          |> notify_listeners(
            {:module, module, workspace_id},
            {:scheduled, execution_id, target_name, external_run_id, external_step_id, attempt,
             execute_after, created_at}
          )
          |> notify_listeners(
            {:targets, workspace_id},
            {:step, module, target_name, type, external_run_id, external_step_id, attempt}
          )

        {:ok, external_run_id, external_step_id, execution_id, state}
    end
  end

  defp rerun_step(state, step, workspace_id, opts \\ []) do
    execute_after = Keyword.get(opts, :execute_after, nil)
    dependency_ids = Keyword.get(opts, :dependency_ids, [])

    # TODO: only get run if needed for notify?
    {:ok, run} = Runs.get_run_by_id(state.db, step.run_id)

    case Runs.rerun_step(state.db, step.id, workspace_id, execute_after, dependency_ids) do
      {:ok, execution_id, attempt, created_at} ->
        {:ok, {run_module, run_target}} = Runs.get_run_target(state.db, run.id)

        execute_at = execute_after || created_at

        state =
          state
          |> notify_listeners(
            {:run, step.run_id},
            {:execution, step.external_id, attempt, execution_id, workspace_id, created_at,
             execute_after}
          )
          |> notify_listeners(
            {:modules, workspace_id},
            {:scheduled, step.module, execution_id, execute_at}
          )
          |> notify_listeners(
            {:module, step.module, workspace_id},
            {:scheduled, execution_id, step.target, run.external_id, step.external_id, attempt,
             execute_after, created_at}
          )
          |> notify_listeners(
            {:targets, workspace_id},
            {:step, step.module, step.target, step.type, run.external_id, step.external_id,
             attempt}
          )

        state =
          case step.type do
            :workflow ->
              notify_listeners(
                state,
                {:workflow, run_module, run_target, workspace_id},
                {:run, run.external_id, run.created_at}
              )

            :sensor ->
              notify_listeners(
                state,
                {:sensor, run_module, run_target, workspace_id},
                {:run, run.external_id, run.created_at}
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

  defp resolve_execution(db, execution_id) do
    {:ok, {external_run_id, external_step_id, step_attempt, module, target}} =
      Runs.get_run_by_execution(db, execution_id)

    %{
      run_id: external_run_id,
      step_id: external_step_id,
      attempt: step_attempt,
      module: module,
      target: target
    }
  end

  defp resolve_asset(db, asset_id) do
    {:ok, {type, path, blob_key, size, metadata}} =
      Results.get_asset_by_id(db, asset_id, true)

    %{
      type: type,
      path: path,
      metadata: metadata,
      blob_key: blob_key,
      size: size
    }
  end

  defp resolve_references(db, references) do
    Enum.map(references, fn
      {:fragment, format, blob_key, size, metadata} ->
        {:fragment, format, blob_key, size, metadata}

      {:execution, execution_id} ->
        {:execution, execution_id, resolve_execution(db, execution_id)}

      {:asset, asset_id} ->
        {:asset, asset_id, resolve_asset(db, asset_id)}
    end)
  end

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

      {type, execution_id} when type in [:deferred, :cached, :spawned] ->
        execution_result =
          case resolve_result(db, execution_id) do
            {:ok, execution_result} -> execution_result
            {:pending, _execution_id} -> nil
          end

        {type, resolve_execution(db, execution_id), build_result(execution_result, db)}

      nil ->
        nil
    end
  end

  # TODO: remove 'module' argument?
  defp record_and_notify_result(state, execution_id, result, module) do
    {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)
    {:ok, successors} = Runs.get_result_successors(state.db, execution_id)

    case Results.record_result(state.db, execution_id, result) do
      {:ok, created_at} ->
        state = notify_waiting(state, execution_id)

        final = is_result_final?(result)
        result = build_result(result, state.db)

        state =
          successors
          |> Enum.reduce(state, fn {run_id, successor_id}, state ->
            cond do
              successor_id == execution_id ->
                notify_listeners(
                  state,
                  {:run, run_id},
                  {:result, execution_id, result, created_at}
                )

              final ->
                notify_listeners(
                  state,
                  {:run, run_id},
                  # TODO: better name?
                  {:result_result, successor_id, result, created_at}
                )

              true ->
                state
            end
          end)
          |> notify_listeners(
            {:modules, workspace_id},
            {:completed, module, execution_id}
          )
          |> notify_listeners(
            {:module, module, workspace_id},
            {:completed, execution_id}
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

              state = abort_execution(state, execution_id)

              {retry_id, state}

            result_retryable?(result) && step.retry_limit > 0 ->
              {:ok, assignments} = Runs.get_step_assignments(state.db, step.id)
              attempts = Enum.count(assignments)

              if attempts <= step.retry_limit do
                # TODO: add jitter (within min/max delay)
                delay_s =
                  step.retry_delay_min +
                    (attempts - 1) / step.retry_limit *
                      (step.retry_delay_max - step.retry_delay_min)

                execute_after = System.os_time(:millisecond) + delay_s * 1000

                {:ok, retry_id, _, state} =
                  rerun_step(state, step, workspace_id, execute_after: execute_after)

                {retry_id, state}
              else
                {nil, state}
              end

            step.type == :sensor ->
              {:ok, assignments} = Runs.get_step_assignments(state.db, step.id)

              if Enum.all?(Map.values(assignments)) do
                now = System.os_time(:millisecond)

                last_assigned_at =
                  assignments |> Map.values() |> Enum.max(&>=/2, fn -> nil end)

                execute_after =
                  if last_assigned_at && now - last_assigned_at < @sensor_rate_limit_ms do
                    last_assigned_at + @sensor_rate_limit_ms
                  end

                {:ok, _, _, state} =
                  rerun_step(state, step, workspace_id, execute_after: execute_after)

                {nil, state}
              else
                {nil, state}
              end

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
          end

        {:ok, state}
    end
  end

  defp resolve_result(db, execution_id) do
    # TODO: check execution exists?
    case Results.get_result(db, execution_id) do
      {:ok, nil} ->
        {:pending, execution_id}

      {:ok, {result, _}} ->
        case result do
          {:error, _, _, _, execution_id} when not is_nil(execution_id) ->
            resolve_result(db, execution_id)

          {:abandoned, execution_id} when not is_nil(execution_id) ->
            resolve_result(db, execution_id)

          {:deferred, execution_id} ->
            resolve_result(db, execution_id)

          {:cached, execution_id} ->
            resolve_result(db, execution_id)

          {:suspended, execution_id} ->
            resolve_result(db, execution_id)

          {:spawned, execution_id} ->
            resolve_result(db, execution_id)

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
    if session.agent_id do
      agent = Map.fetch!(state.agents, session.agent_id)
      agent.state == :active
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
        {:execution, execution_id} ->
          case resolve_result(db, execution_id) do
            {:ok, _} -> true
            {:pending, _} -> false
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
        Enum.all?(dependencies, fn {dependency_id} ->
          case resolve_result(db, dependency_id) do
            {:ok, _} -> true
            {:pending, _} -> false
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

  defp notify_waiting(state, execution_id) do
    {execution_waiting, waiting} = Map.pop(state.waiting, execution_id)

    if execution_waiting do
      case resolve_result(state.db, execution_id) do
        {:pending, execution_id} ->
          waiting =
            Map.update(waiting, execution_id, execution_waiting, &(&1 ++ execution_waiting))

          Map.put(state, :waiting, waiting)

        {:ok, result} ->
          state = Map.put(state, :waiting, waiting)

          Enum.reduce(execution_waiting, state, fn {session_id, request_id}, state ->
            send_session(state, session_id, {:result, request_id, result})
          end)
      end
    else
      state
    end
  end

  defp find_session_for_execution(state, execution_id) do
    state.sessions
    |> Map.keys()
    |> Enum.find(fn session_id ->
      session = Map.fetch!(state.sessions, session_id)

      MapSet.member?(session.starting, execution_id) or
        MapSet.member?(session.executing, execution_id)
    end)
    |> case do
      nil -> :error
      session_id -> {:ok, session_id}
    end
  end

  defp abort_execution(state, execution_id) do
    case find_session_for_execution(state, execution_id) do
      {:ok, session_id} ->
        send_session(state, session_id, {:abort, execution_id})

      :error ->
        IO.puts("Couldn't locate session for execution #{execution_id}. Ignoring.")
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

  defp update_agent_state(state, agent_id, agent_state, workspace_id, pool_name) do
    :ok = Agents.create_agent_state(state.db, agent_id, agent_state)

    state
    |> put_in(
      [Access.key(:agents), agent_id, :state],
      agent_state
    )
    |> notify_listeners(
      {:pool, workspace_id, pool_name},
      {:agent_state, agent_id, agent_state}
    )
  end

  defp deactivate_agent(state, agent_id) do
    {:ok, deactivated_at} = Agents.create_agent_deactivation(state.db, agent_id)

    {agent, state} = pop_in(state, [Access.key(:agents), agent_id])

    notify_listeners(
      state,
      {:pool, agent.workspace_id, agent.pool_name},
      {:agent_deactivated, agent_id, deactivated_at}
    )
  end
end
