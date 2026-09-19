defmodule Coflux.Orchestration.Server.Snapshots do
  @moduledoc """
  The initial events for a subscription key: what a topic folds before it
  starts receiving live events.

  A snapshot must be *fold-equivalent* to the history: folding it gives the
  same model as folding every event ever routed to the key. Compaction is
  the loader's job (latest-state tables yield only the latest; completed
  executions are absent because scheduled-then-completed folds to absence),
  and every event is filled in full, exactly as the emit site would fill
  it.

  `snapshot/3` is the entry point. Most keys are answered by `load/3` from
  the active database; a run is answered by `run_snapshot/2`, which can
  run against an archived epoch's database too, since a run topic may be
  opened on a run that has long since rotated out.

  A run snapshot carries *structure* only - every step and attempt with
  its status, the links between them, and groups - and the per-execution
  detail is fetched separately, for the parts a topic actually shows,
  through `build_run_details/3`. That split is what keeps opening a large
  run cheap.
  """

  alias Coflux.Events.{
    AssetPut,
    CatalogPublished,
    CheckpointsInherited,
    CheckpointsSet,
    ChildLinked,
    CompletionRecorded,
    DependenciesPending,
    ExecutionAssigned,
    ExecutionScheduled,
    ExecutionWaiting,
    GroupCreated,
    InputActivated,
    InputDetails,
    InputResponded,
    InputSubmitted,
    ManifestRegistered,
    MetricDefined,
    PoolUpdated,
    ResultRecorded,
    RunCreated,
    RunOutcome,
    SessionConnected,
    SessionExecutions,
    StepArguments,
    StepCreated,
    StreamClosed,
    StreamItemAppended,
    StreamRegistered,
    SecretSet,
    TokenCreated,
    WorkerCreated,
    WorkerDeactivated,
    WorkerLaunchResult,
    WorkerStateChanged,
    WorkerStopResult,
    WorkerStopping,
    WorkspaceCreated
  }

  alias Coflux.Store.{Epochs, Index}

  alias Coflux.Orchestration.{
    Catalog,
    Checkpoints,
    Ids,
    Inputs,
    Manifests,
    Principals,
    Results,
    Runs,
    Streams,
    TagSets,
    Workers,
    Workspaces
  }

  alias Coflux.Orchestration.Server.{
    Archives,
    CatalogFlow,
    Dependencies,
    Fleet,
    Gates,
    InputFlow,
    Permissions,
    Resolve,
    Scheduling,
    State,
    StreamDelivery
  }

  @target_runs_archive_depth 5

  def load(state, {:queue, workspace_external_id}, _opts) do
    with {:ok, workspace_id} <- resolve_workspace(state, workspace_external_id) do
      gates = Gates.for_workspace(state, workspace_id)

      events =
        state
        |> active_executions(workspace_id)
        |> Enum.flat_map(fn {scheduled, assigned} ->
          [scheduled | List.wrap(assigned)] ++ waiting(scheduled, gates)
        end)

      {:ok, events}
    end
  end

  def load(state, {:modules, workspace_external_id}, _opts) do
    with {:ok, workspace_id} <- resolve_workspace(state, workspace_external_id) do
      manifests = manifest_events(state.db, workspace_id, workspace_external_id)

      executions =
        state
        |> active_executions(workspace_id)
        |> Enum.flat_map(fn {scheduled, assigned} -> [scheduled | List.wrap(assigned)] end)

      {:ok, manifests ++ executions}
    end
  end

  def load(state, {:workflow, module, target, workspace_external_id}, opts) do
    max_runs = Keyword.fetch!(opts, :max_runs)

    with {:ok, workspace_id} <- resolve_workspace(state, workspace_external_id) do
      manifest = manifest_event(state.db, workspace_id, workspace_external_id, module)
      runs = target_runs(state, module, target, workspace_id, max_runs)

      if (is_nil(manifest) or not Map.has_key?(manifest.workflows, target)) and runs == [] do
        {:error, :not_found}
      else
        run_events =
          Enum.flat_map(runs, fn {run, created_at, user_ext_id, token_ext_id, outcome} ->
            created = %RunCreated{
              run: run,
              workspace: workspace_external_id,
              root_module: module,
              root_target: target,
              type: :workflow,
              created_at: created_at,
              created_by: Principals.build(user_ext_id, token_ext_id)
            }

            if outcome do
              [
                created,
                %RunOutcome{
                  run: run,
                  workspace: workspace_external_id,
                  root_module: module,
                  root_target: target,
                  outcome: outcome
                }
              ]
            else
              [created]
            end
          end)

        executions =
          state
          |> active_executions(workspace_id, {module, target})
          |> Enum.flat_map(fn {scheduled, assigned} -> [scheduled | List.wrap(assigned)] end)

        {:ok, List.wrap(manifest) ++ run_events ++ executions}
      end
    end
  end

  def load(state, {:targets, workspace_external_id}, _opts) do
    with {:ok, workspace_id} <- resolve_workspace(state, workspace_external_id) do
      manifests = manifest_events(state.db, workspace_id, workspace_external_id)
      {:ok, rows} = Runs.get_latest_executions_for_workspace(state.db, workspace_id)
      tag_sets = tag_sets(state.db, rows)
      {:ok, manifests ++ Enum.map(rows, &scheduled_event(&1, tag_sets))}
    end
  end

  def load(state, {:manifests, workspace_external_id}, _opts) do
    with {:ok, workspace_id} <- resolve_workspace(state, workspace_external_id) do
      {:ok, manifest_events(state.db, workspace_id, workspace_external_id)}
    end
  end

  # Every unanswered input an in-flight execution is waiting on.
  def load(state, {:inputs, workspace_external_id}, _opts) do
    with {:ok, workspace_id} <- resolve_workspace(state, workspace_external_id) do
      {:ok, rows} = Inputs.get_inputs_for_workspace(state.db, workspace_id)

      events =
        Enum.map(rows, fn {_id, run_external_id, number, _workspace_id, _key, _prompt_id,
                           _schema_id, created_at, title, requires_tag_set_id, _response_type} ->
          %InputActivated{
            workspace: workspace_external_id,
            input: Ids.input(run_external_id, number),
            run: run_external_id,
            created_at: created_at,
            title: title,
            requires: Resolve.tag_set(state.db, requires_tag_set_id)
          }
        end)

      {:ok, events}
    end
  end

  def load(state, :secrets, _opts) do
    {:ok, secrets} = Coflux.Admin.Secrets.list(state.admin_db)

    {:ok,
     Enum.map(secrets, fn secret ->
       %SecretSet{
         scope: secret.scope,
         name: secret.name,
         version: secret.version,
         created_at: secret.created_at,
         updated_at: secret.updated_at,
         updated_by: secret.updated_by
       }
     end)}
  end

  # Revoked tokens are absent: created-then-revoked folds to absence.
  def load(state, :tokens, _opts) do
    {:ok, tokens} = Coflux.Admin.Tokens.list_tokens(state.admin_db)

    events =
      tokens
      |> Enum.reject(& &1.revoked_at)
      |> Enum.map(fn token ->
        %TokenCreated{
          token: token.external_id,
          id: token.id,
          name: token.name,
          workspaces: token.workspaces,
          created_at: token.created_at,
          expires_at: token.expires_at,
          created_by: token.created_by
        }
      end)

    {:ok, events}
  end

  # ---------------------------------------------------------------------------
  # Executions

  # Every execution in the workspace without a completion, as the events
  # that describe it: `{scheduled, assigned | nil}`. With a target, only
  # the executions of runs that started there - narrowed in SQL, since a
  # workspace can hold far more in flight than one workflow's share.
  defp active_executions(state, workspace_id, target \\ nil) do
    {:ok, rows} =
      case target do
        nil ->
          Runs.get_active_executions(state.db, workspace_id)

        {module, target} ->
          Runs.get_active_executions_for_target(state.db, workspace_id, module, target)
      end

    tag_sets = tag_sets(state.db, rows)

    Enum.map(rows, fn row ->
      {scheduled_event(row, tag_sets), if(row.assigned_at, do: struct(ExecutionAssigned, row))}
    end)
  end

  defp scheduled_event(row, tag_sets) do
    requires =
      tag_sets
      |> Map.get(row.run_requires_tag_set_id, %{})
      |> Map.merge(Map.get(tag_sets, row.step_requires_tag_set_id, %{}))
      |> Map.reject(fn {_key, values} -> values == [] end)

    struct(ExecutionScheduled, Map.put(row, :requires, requires))
  end

  defp waiting(scheduled, gates) do
    case Map.get(gates, scheduled.execution, []) do
      [] ->
        []

      gates ->
        [
          %ExecutionWaiting{
            execution: scheduled.execution,
            run: scheduled.run,
            workspace: scheduled.workspace,
            gates: gates
          }
        ]
    end
  end

  defp tag_sets(db, rows) do
    rows
    |> Enum.flat_map(&[&1.run_requires_tag_set_id, &1.step_requires_tag_set_id])
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Map.new(fn tag_set_id ->
      {:ok, tag_set} = TagSets.get_tag_set(db, tag_set_id)
      {tag_set_id, tag_set}
    end)
  end

  # ---------------------------------------------------------------------------
  # Manifests

  defp manifest_events(db, workspace_id, workspace_external_id) do
    {:ok, manifests} = Manifests.get_latest_manifests(db, workspace_id)
    instructions = instructions(db, Map.values(manifests))

    Enum.map(manifests, fn {module, workflows} ->
      %ManifestRegistered{
        workspace: workspace_external_id,
        module: module,
        workflows: with_instructions(workflows, instructions)
      }
    end)
  end

  defp manifest_event(db, workspace_id, workspace_external_id, module) do
    case Manifests.get_latest_manifest(db, workspace_id, module) do
      {:ok, nil} ->
        nil

      {:ok, workflows} ->
        %ManifestRegistered{
          workspace: workspace_external_id,
          module: module,
          workflows: with_instructions(workflows, instructions(db, [workflows]))
        }
    end
  end

  # The content of every instruction the manifests reference, in one
  # query rather than one per workflow.
  defp instructions(db, manifests) do
    ids =
      manifests
      |> Enum.flat_map(&Enum.map(&1, fn {_name, workflow} -> workflow.instruction_id end))
      |> Enum.reject(&is_nil/1)

    {:ok, contents} = Manifests.get_instructions(db, ids)
    contents
  end

  # The shape the API registers: instruction content in place of the stored id.
  defp with_instructions(workflows, instructions) do
    Map.new(workflows, fn {name, workflow} ->
      {instruction_id, workflow} = Map.pop(workflow, :instruction_id)
      content = if instruction_id, do: Map.get(instructions, instruction_id)
      {name, Map.put(workflow, :instruction, content)}
    end)
  end

  # ---------------------------------------------------------------------------
  # Runs

  # The workspace's most recent runs of a workflow, newest first, reaching
  # into archived epochs when the active one has fewer than `limit`.
  defp target_runs(state, module, target, workspace_id, limit) do
    {:ok, live_runs} =
      Runs.get_target_runs(state.db, module, target, :workflow, workspace_id, limit)

    runs = resolve_run_outcomes(state.db, live_runs)

    if length(runs) < limit do
      workspace_external_id = state.workspaces[workspace_id].external_id

      unindexed_map =
        state.epochs
        |> Epochs.unindexed_dbs()
        |> Map.new()

      indexed_epoch_ids = Index.indexed_epoch_ids(state.epoch_index, "runs")

      archives =
        (Map.keys(unindexed_map) ++ indexed_epoch_ids)
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
          archive_target_runs(state, archive, module, target, workspace_external_id, remaining)

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

  defp archive_target_runs(_state, {:open, db}, module, target, workspace_external_id, limit) do
    query_archive_target_runs(db, module, target, workspace_external_id, limit)
  end

  defp archive_target_runs(
         state,
         {:closed, epoch_id},
         module,
         target,
         workspace_external_id,
         limit
       ) do
    case Epochs.open_archive(state.epochs, epoch_id) do
      {:ok, db} ->
        try do
          query_archive_target_runs(db, module, target, workspace_external_id, limit)
        after
          Exqlite.Sqlite3.close(db)
        end

      {:error, _} ->
        []
    end
  end

  defp query_archive_target_runs(db, module, target, workspace_external_id, limit) do
    case Workspaces.get_workspace_id(db, workspace_external_id) do
      {:ok, workspace_id} when not is_nil(workspace_id) ->
        {:ok, runs} = Runs.get_target_runs(db, module, target, :workflow, workspace_id, limit)
        resolve_run_outcomes(db, runs)

      {:ok, nil} ->
        []
    end
  end

  # Swaps each run's initial execution id for the outcome it resolved to.
  # Done per database, since the completion (and any successor it handed
  # off to) lives in the same epoch as the run.
  defp resolve_run_outcomes(db, runs) do
    Enum.map(runs, fn {external_id, created_at, user_ext_id, token_ext_id, initial_execution_id} ->
      {external_id, created_at, user_ext_id, token_ext_id,
       Results.run_outcome(db, initial_execution_id)}
    end)
  end

  # ---------------------------------------------------------------------------

  defp resolve_workspace(state, external_id) do
    case Map.fetch(state.workspace_external_ids, external_id) do
      {:ok, workspace_id} -> {:ok, workspace_id}
      :error -> {:error, :workspace_invalid}
    end
  end

  def snapshot(state, {:run, run_external_id}, _opts) do
    case Archives.find_run(state, run_external_id, &run_snapshot/2) do
      {:ok, :not_found} -> {:error, :not_found}
      {:ok, events} -> {:ok, events}
      :not_found -> {:error, :not_found}
    end
  end

  def snapshot(state, :workspaces, _opts) do
    events =
      Enum.map(state.workspaces, fn {_workspace_id, workspace} ->
        %WorkspaceCreated{
          workspace: workspace.external_id,
          name: workspace.name,
          base: base_external_id(state, workspace),
          state: workspace.state
        }
      end)

    {:ok, events}
  end

  def snapshot(state, {:sessions, workspace_external_id}, _opts) do
    with {:ok, workspace_id} <-
           Permissions.resolve_workspace_external_id(state, workspace_external_id) do
      events =
        state.sessions
        |> Enum.filter(fn {_, session} -> session.workspace_id == workspace_id end)
        |> Enum.map(fn {_session_id, session} -> Fleet.session_event(state, session) end)

      {:ok, events}
    end
  end

  def snapshot(state, {:pools, workspace_external_id}, _opts) do
    with {:ok, workspace_id} <-
           Permissions.resolve_workspace_external_id(state, workspace_external_id) do
      events =
        state.pools
        |> Map.get(workspace_id, %{})
        |> Enum.map(fn {name, pool} ->
          %PoolUpdated{workspace: workspace_external_id, pool: name, definition: pool}
        end)

      {:ok, events}
    end
  end

  def snapshot(state, {:pool, workspace_external_id, pool_name}, _opts) do
    with {:ok, workspace_id} <-
           Permissions.resolve_workspace_external_id(state, workspace_external_id) do
      pool = state.pools |> Map.get(workspace_id, %{}) |> Map.get(pool_name)
      {:ok, pool_workers} = Workers.get_pool_workers(state.db, workspace_id, pool_name)

      if is_nil(pool) and pool_workers == [] do
        {:error, :not_found}
      else
        definition =
          if pool,
            do: [
              %PoolUpdated{workspace: workspace_external_id, pool: pool_name, definition: pool}
            ],
            else: []

        workers =
          Enum.flat_map(pool_workers, fn {worker_id, worker_external_id, starting_at, started_at,
                                          start_error, stopping_at, stopped_at, stop_error,
                                          deactivated_at, error, logs, total_executions} ->
            worker = Map.get(state.workers, worker_id)

            session =
              if worker && worker.session_id do
                case Map.fetch(state.sessions, worker.session_id) do
                  {:ok, session} -> session
                  :error -> nil
                end
              end

            session_external_id = session && session.external_id

            # So a subscriber joining now sees the same connection state
            # a subscriber watching all along would have.
            [
              %WorkerCreated{
                workspace: workspace_external_id,
                pool: pool_name,
                worker: worker_external_id,
                created_at: starting_at,
                session: session_external_id
              }
            ] ++
              if(started_at || start_error,
                do: [
                  %WorkerLaunchResult{
                    workspace: workspace_external_id,
                    pool: pool_name,
                    worker: worker_external_id,
                    started_at: started_at,
                    error: start_error
                  }
                ],
                else: []
              ) ++
              if(stopping_at,
                do: [
                  %WorkerStopping{
                    workspace: workspace_external_id,
                    pool: pool_name,
                    worker: worker_external_id,
                    stopping_at: stopping_at
                  }
                ],
                else: []
              ) ++
              if(stopped_at || stop_error,
                do: [
                  %WorkerStopResult{
                    workspace: workspace_external_id,
                    pool: pool_name,
                    worker: worker_external_id,
                    stopped_at: stopped_at,
                    error: stop_error
                  }
                ],
                else: []
              ) ++
              if(deactivated_at,
                do: [
                  %WorkerDeactivated{
                    workspace: workspace_external_id,
                    pool: pool_name,
                    worker: worker_external_id,
                    deactivated_at: deactivated_at,
                    error: error,
                    logs: logs
                  }
                ],
                else: []
              ) ++
              [
                %WorkerStateChanged{
                  workspace: workspace_external_id,
                  pool: pool_name,
                  worker: worker_external_id,
                  state: if(worker, do: worker.state)
                },
                %SessionExecutions{
                  workspace: workspace_external_id,
                  session: session_external_id,
                  worker: worker_external_id,
                  pool: pool_name,
                  executions: total_executions
                }
              ] ++
              if(session,
                do: [
                  %SessionConnected{
                    workspace: workspace_external_id,
                    session: session.external_id,
                    connected: !is_nil(session.connection)
                  }
                ],
                else: []
              )
          end)

        {:ok, definition ++ workers}
      end
    end
  end

  def snapshot(state, {:catalog, workspace_external_id}, _opts) do
    with {:ok, workspace_id} <-
           Permissions.resolve_workspace_external_id(state, workspace_external_id) do
      chain = State.workspace_chain(state, workspace_id)
      {:ok, versions} = Catalog.list_heads(state.db, chain, nil)

      {:ok, Enum.map(versions, &CatalogFlow.catalog_published(state, &1))}
    end
  end

  def snapshot(state, {:input, input_external_id}, _opts) do
    case Archives.read_input_from_active_or_archives(state, input_external_id) do
      {:ok, nil} ->
        {:error, :not_found}

      {:ok,
       {db, input_id, workspace_id, key, prompt_id, schema_id, title, actions, initial,
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

        {:ok, workspace_external_id} = Workspaces.get_workspace_external_id(db, workspace_id)
        {:ok, run_external_id, _number} = Ids.parse_input(input_external_id)

        events =
          [
            %InputDetails{
              workspace: workspace_external_id,
              input: input_external_id,
              key: details.key,
              template: details.template,
              placeholders: details.placeholders,
              schema: details.schema,
              initial: details.initial,
              title: details.title,
              actions: details.actions,
              requires: details.requires,
              created_at: details.created_at
            }
          ] ++
            if(details.response,
              do: [
                %InputResponded{
                  run: run_external_id,
                  workspace: workspace_external_id,
                  input: input_external_id,
                  response: details.response
                }
              ],
              else: []
            ) ++
            if(Inputs.has_active_dependency?(db, input_id),
              do: [
                %InputActivated{
                  workspace: workspace_external_id,
                  input: input_external_id,
                  run: run_external_id,
                  created_at: created_at,
                  title: title,
                  requires: details.requires
                }
              ],
              else: []
            )

        {:ok, events}
    end
  end

  def snapshot(state, {:stream, stream_external_id}, _opts) do
    with {:ok, stream_id} <- Archives.resolve_stream_id(state, stream_external_id),
         {:ok, stream} <- Streams.get_stream(state.db, stream_id),
         {:ok, registrations} <- Streams.get_registrations(state.db, stream_id),
         {:ok, {items, _total_count}} <-
           Streams.get_stream_tail(state.db, stream_id) do
      {:ok, workspace_external_id} =
        Workspaces.get_workspace_external_id(state.db, stream.workspace_id)

      registered =
        Enum.map(registrations, fn {_execution_id, attempt, buffer, timeout_ms, _created_at} ->
          %StreamRegistered{
            run: stream.run_external_id,
            step: stream.step_number,
            index: stream.index,
            stream: stream_external_id,
            workspace: workspace_external_id,
            module: stream.module,
            target: stream.target,
            position: stream.position,
            attempt: attempt,
            buffer: buffer,
            timeout_ms: timeout_ms,
            opened_at: stream.created_at
          }
        end)

      appended =
        Enum.map(items, fn {sequence, value, attempt, created_at} ->
          %StreamItemAppended{
            stream: stream_external_id,
            sequence: sequence,
            value: Resolve.value(state.db, value),
            attempt: attempt,
            created_at: created_at
          }
        end)

      closed =
        case StreamDelivery.build_stream_topic_closure(state, stream_id) do
          nil ->
            []

          closure ->
            [
              %StreamClosed{
                run: stream.run_external_id,
                step: stream.step_number,
                index: stream.index,
                stream: stream_external_id,
                reason: closure.reason,
                error: closure.error,
                attempt: closure.attempt,
                closed_at: closure.closedAt
              }
            ]
        end

      {:ok, registered ++ appended ++ closed}
    else
      {:error, _reason} -> {:error, :not_found}
    end
  end

  def snapshot(state, key, opts), do: load(state, key, opts)

  defp base_external_id(state, workspace) do
    if workspace.base_id do
      case Map.fetch(state.workspaces, workspace.base_id) do
        {:ok, base} -> base.external_id
        :error -> nil
      end
    end
  end

  # The structure of a run as events: the run, its steps, every execution
  # (with its groups, assignment and completion) and every child link, in
  # an order a fold accepts. Detail is left to `build_run_details`.
  defp run_snapshot(db, run) do
    {:ok, steps} = Runs.get_run_steps(db, run.id)

    # Everything below is keyed off the initial step, and a run without
    # one has nothing a run topic could show, so it reads as missing
    # rather than crashing the subscribe.
    case Enum.find(steps, &is_nil(&1.parent_id)) do
      nil -> :not_found
      initial -> run_snapshot(db, run, steps, initial)
    end
  end

  defp run_snapshot(db, run, steps, initial) do
    {:ok, run_executions} = Runs.get_run_executions(db, run.id)
    {:ok, run_children} = Runs.get_run_children(db, run.id)
    {:ok, groups} = Runs.get_groups_for_run(db, run.id)
    {:ok, completions} = Results.get_run_completions(db, run.id)
    {:ok, workspaces} = Workspaces.get_all_workspaces(db)

    steps_by_id = Map.new(steps, &{&1.id, &1})
    run_requires = Resolve.tag_set(db, run.requires_tag_set_id)

    external_ids =
      Map.new(run_executions, fn {execution_id, step_id, attempt, _, _, _, _, _, _} ->
        {execution_id,
         Ids.execution(run.external_id, Map.fetch!(steps_by_id, step_id).number, attempt)}
      end)

    groups_by_execution =
      groups
      |> Enum.group_by(&elem(&1, 0))
      |> Map.new(fn {execution_id, rows} ->
        {execution_id,
         Enum.map(rows, fn {_, group_id, name, concurrency} -> {group_id, name, concurrency} end)}
      end)

    cache_configs = Resolve.cache_configs(db, steps)

    executions =
      Enum.sort_by(run_executions, fn {_, step_id, attempt, _, _, _, _, _, _} ->
        {Map.fetch!(steps_by_id, step_id).number, attempt}
      end)

    # The workspace of the earliest execution of the initial step; nil if
    # the step has yet to be scheduled in any of them.
    initial_workspace =
      executions
      |> Enum.filter(fn {_, step_id, _, _, _, _, _, _, _} -> step_id == initial.id end)
      |> Enum.min_by(fn {_, _, _, _, _, created_at, _, _, _} -> created_at end, fn -> nil end)
      |> then(fn
        nil ->
          nil

        {_, _, _, workspace_id, _, _, _, _, _} ->
          Map.fetch!(workspaces, workspace_id).external_id
      end)

    created = %RunCreated{
      run: run.external_id,
      workspace: initial_workspace,
      root_module: initial.module,
      root_target: initial.target,
      type: initial.type,
      created_at: run.created_at,
      created_by: run.created_by,
      parent: if(run.parent_ref_id, do: Resolve.execution_ref(db, run.parent_ref_id)),
      requires: run_requires
    }

    step_events =
      steps
      |> Enum.sort_by(& &1.number)
      |> Enum.map(fn step ->
        # A step's parent is an execution of this run, except for a step
        # copied in from an archive, which is looked up.
        parent =
          if step.parent_id do
            Map.get_lazy(external_ids, step.parent_id, fn ->
              {:ok, {r, s, a}} = Runs.get_execution_key(db, step.parent_id)
              Ids.execution(r, s, a)
            end)
          end

        %StepCreated{
          run: run.external_id,
          step: step.number,
          module: step.module,
          target: step.target,
          type: step.type,
          parent: parent,
          cache_config:
            if(step.cache_config_id, do: Map.fetch!(cache_configs, step.cache_config_id)),
          cache_key: step.cache_key,
          memo_key: step.memo_key,
          concurrency_key: step.concurrency_key,
          concurrency_limit: step.concurrency_limit,
          group_key: step.group_key,
          group_limit: step.group_limit,
          retries: Scheduling.step_retries(step),
          recurrent: step.recurrent == 1 or step.recurrent == true,
          timeout: step.timeout,
          created_at: step.created_at,
          requires: Resolve.tag_set(db, step.requires_tag_set_id)
        }
      end)

    execution_events =
      Enum.flat_map(executions, fn {execution_id, step_id, attempt, workspace_id, execute_after,
                                    created_at, assigned_at, created_by_user_ext_id,
                                    created_by_token_ext_id} ->
        step = Map.fetch!(steps_by_id, step_id)
        execution = Map.fetch!(external_ids, execution_id)
        workspace = Map.fetch!(workspaces, workspace_id).external_id

        requires =
          run_requires
          |> Map.merge(Resolve.tag_set(db, step.requires_tag_set_id))
          |> Map.reject(fn {_key, values} -> values == [] end)

        scheduled =
          %ExecutionScheduled{
            execution: execution,
            run: run.external_id,
            step: step.number,
            attempt: attempt,
            workspace: workspace,
            module: step.module,
            target: step.target,
            type: step.type,
            root_module: initial.module,
            root_target: initial.target,
            execute_after: execute_after,
            created_at: created_at,
            created_by: Principals.build(created_by_user_ext_id, created_by_token_ext_id),
            requires: requires
          }

        groups =
          groups_by_execution
          |> Map.get(execution_id, [])
          |> Enum.map(fn {group_id, name, concurrency} ->
            %GroupCreated{
              run: run.external_id,
              execution: execution,
              group: group_id,
              name: name,
              concurrency: concurrency
            }
          end)

        assigned =
          if assigned_at do
            [
              %ExecutionAssigned{
                execution: execution,
                run: run.external_id,
                step: step.number,
                attempt: attempt,
                workspace: workspace,
                module: step.module,
                target: step.target,
                type: step.type,
                root_module: initial.module,
                root_target: initial.target,
                assigned_at: assigned_at
              }
            ]
          else
            []
          end

        completion =
          case Map.get(completions, execution_id) do
            nil ->
              []

            {kind, successor, completion_at} ->
              [
                %CompletionRecorded{
                  execution: execution,
                  run: run.external_id,
                  step: step.number,
                  attempt: attempt,
                  workspace: workspace,
                  module: step.module,
                  target: step.target,
                  type: step.type,
                  root_module: initial.module,
                  root_target: initial.target,
                  kind: kind,
                  successor: Resolve.successor(successor),
                  completed_at: completion_at
                }
              ]
          end

        [scheduled | groups ++ assigned ++ completion]
      end)

    child_events =
      Enum.flat_map(run_children, fn {parent_id, links} ->
        Enum.map(links, fn {step_number, attempt, group_id} ->
          %ChildLinked{
            run: run.external_id,
            parent: Map.fetch!(external_ids, parent_id),
            step: step_number,
            attempt: attempt,
            group: group_id
          }
        end)
      end)

    [created | step_events] ++ execution_events ++ child_events
  end

  def build_run_details(db, run, %{executions: execution_keys, steps: step_numbers}) do
    {:ok, steps} = Runs.get_run_steps(db, run.id)
    {:ok, run_executions} = Runs.get_run_executions(db, run.id)
    steps_by_id = Map.new(steps, &{&1.id, &1})
    steps_by_number = Map.new(steps, &{&1.number, &1})

    executions_by_key =
      Map.new(run_executions, fn {execution_id, step_id, attempt, workspace_id, _, _, _, _, _} ->
        {{Map.fetch!(steps_by_id, step_id).number, attempt}, {execution_id, workspace_id}}
      end)

    requested =
      Enum.flat_map(execution_keys, fn {number, attempt} ->
        case Map.fetch(executions_by_key, {number, attempt}) do
          {:ok, {execution_id, workspace_id}} ->
            [{execution_id, workspace_id, Map.fetch!(steps_by_number, number), attempt}]

          :error ->
            []
        end
      end)

    requested_ids = MapSet.new(requested, &elem(&1, 0))
    requested? = fn execution_id -> MapSet.member?(requested_ids, execution_id) end

    executions =
      if requested == [] do
        []
      else
        {:ok, run_dependencies} = Runs.get_run_dependencies(db, run.id)
        {:ok, run_stream_dependencies} = Streams.get_run_dependencies(db, run.id)
        {:ok, run_metric_defs} = Runs.get_run_metric_definitions(db, run.id)
        {:ok, run_input_deps} = Inputs.get_input_dependencies_for_run(db, run.id)
        {:ok, run_submitted_inputs} = Inputs.get_submitted_inputs_for_run(db, run.id)
        {:ok, run_asset_deps} = Runs.get_asset_dependencies_for_run(db, run.id)
        {:ok, run_catalog_reads} = Catalog.get_reads_for_run(db, run.id)
        {:ok, run_catalog_waits} = Catalog.get_waits_for_run(db, run.id)
        {:ok, run_catalog_publishes} = Catalog.get_publishes_for_run(db, run.external_id)

        # Resolving a checkpoint needs the workspace chain of the execution
        # reading it. Resolved from `db` rather than `state` because this
        # also runs against archived epochs, which remap workspace ids.
        workspace_chains =
          requested
          |> Enum.map(&elem(&1, 1))
          |> Enum.uniq()
          |> Map.new(fn workspace_id ->
            {:ok, chain} = Workspaces.get_workspace_chain(db, workspace_id)
            {workspace_id, chain}
          end)

        submitted_inputs_by_execution =
          run_submitted_inputs
          |> Enum.filter(fn {execution_id, _, _, _, _} -> requested?.(execution_id) end)
          |> Enum.group_by(
            fn {execution_id, _run_ext_id, _input_number, _title, _response_type} ->
              execution_id
            end,
            fn {_execution_id, run_ext_id, input_number, title, response_type} ->
              {Ids.input(run_ext_id, input_number),
               %{
                 title: title,
                 status:
                   if(response_type, do: InputFlow.decode_input_response_type(response_type))
               }}
            end
          )
          |> Map.new(fn {execution_id, inputs} -> {execution_id, Map.new(inputs)} end)

        input_deps_by_execution =
          run_input_deps
          |> Enum.filter(fn row -> requested?.(elem(row, 0)) end)
          |> Enum.group_by(
            fn {execution_id, _run_ext_id, _input_number, _key, _prompt_id, _title, _created_at,
                _response_type, _response_value, _responded_at, _created_by} ->
              execution_id
            end,
            fn {_execution_id, run_ext_id, input_number, _key, _prompt_id, title, _created_at,
                response_type, _response_value, _responded_at, _response_created_by} ->
              {Ids.input(run_ext_id, input_number),
               {:input, title,
                if(response_type, do: InputFlow.decode_input_response_type(response_type))}}
            end
          )
          |> Map.new(fn {execution_id, deps} -> {execution_id, Map.new(deps)} end)

        asset_deps_by_execution =
          run_asset_deps
          |> Enum.filter(fn {execution_id, _asset_id} -> requested?.(execution_id) end)
          |> Enum.group_by(
            fn {execution_id, _asset_id} -> execution_id end,
            fn {_execution_id, asset_id} ->
              {external_id, name, total_count, total_size, entry} = Resolve.asset(db, asset_id)
              {external_id, {:asset, {name, total_count, total_size, entry}}}
            end
          )
          |> Map.new(fn {execution_id, deps} -> {execution_id, Map.new(deps)} end)

        catalog_reads_by_execution =
          run_catalog_reads
          |> Enum.filter(fn {execution_id, _version} -> requested?.(execution_id) end)
          |> Enum.group_by(
            fn {execution_id, _version} -> execution_id end,
            fn {_execution_id, version} ->
              {Ids.catalog_version(version.path, version.number),
               {:catalog, Resolve.catalog_version(db, version)}}
            end
          )
          |> Map.new(fn {execution_id, deps} -> {execution_id, Map.new(deps)} end)

        catalog_waits_by_execution =
          run_catalog_waits
          |> Enum.filter(fn {execution_id, _path, _number} -> requested?.(execution_id) end)
          |> Enum.group_by(
            fn {execution_id, _path, _number} -> execution_id end,
            fn {_execution_id, path, number} ->
              {Ids.catalog_wait(path, number), {:catalog_wait, path, number}}
            end
          )
          |> Map.new(fn {execution_id, deps} -> {execution_id, Map.new(deps)} end)

        # Publishes are recorded against the publishing execution's ref, so
        # they come keyed by `{step number, attempt}` rather than by id.
        requested_keys =
          MapSet.new(requested, fn {_execution_id, _workspace_id, step, attempt} ->
            {step.number, attempt}
          end)

        catalog_publishes_by_attempt =
          run_catalog_publishes
          |> Enum.filter(fn {key, _version} -> MapSet.member?(requested_keys, key) end)
          |> Enum.group_by(
            fn {key, _version} -> key end,
            fn {_key, version} ->
              {Ids.catalog_version(version.path, version.number),
               Resolve.catalog_version(db, version)}
            end
          )
          |> Map.new(fn {key, versions} -> {key, Map.new(versions)} end)

        metric_definitions_by_execution =
          run_metric_defs
          |> Enum.filter(fn row -> requested?.(elem(row, 0)) end)
          |> Enum.group_by(
            fn {execution_id, _, _, _, _, _, _, _, _, _, _} -> execution_id end,
            fn {_, key, group, group_units, group_lower, group_upper, scale, units, progress,
                lower, upper} ->
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

        Enum.flat_map(requested, fn {execution_id, workspace_id, step, attempt} ->
          ext_id = Ids.execution(run.external_id, step.number, attempt)

          {result, result_at, completed_at, result_created_by, final?} =
            case Results.get_result(db, execution_id) do
              {:ok, {result, result_at, completion_at, created_by}} ->
                {Resolve.result(db, result), result_at, completion_at, created_by,
                 Resolve.final_result?(result)}

              {:ok, nil} ->
                {nil, nil, nil, nil, false}
            end

          {:ok, asset_ids} = Results.get_assets_for_execution(db, execution_id)

          assets =
            Enum.map(asset_ids, fn asset_id ->
              {external_id, name, total_count, total_size, entry} = Resolve.asset(db, asset_id)

              %AssetPut{
                run: run.external_id,
                execution: ext_id,
                asset: external_id,
                summary: {name, total_count, total_size, entry}
              }
            end)

          result_deps =
            run_dependencies
            |> Map.get(execution_id, [])
            |> Map.new(fn dependency_ref_id ->
              {dep_ext_id, _module, _target} =
                execution = Resolve.execution_ref(db, dependency_ref_id)

              {dep_ext_id, {:result, execution}}
            end)

          stream_deps =
            run_stream_dependencies
            |> Map.get(execution_id, [])
            |> Map.new(fn stream_ref_id ->
              {:ok, {stream_run_ext_id, step_number, index, module, target}} =
                Streams.get_stream_ref(db, stream_ref_id)

              id = Ids.stream(stream_run_ext_id, step_number, index)
              {id, {:stream, id, module, target}}
            end)

          dependencies =
            [
              Dependencies.build_argument_dependencies(db, step.id, step.wait_for),
              result_deps,
              stream_deps,
              Map.get(input_deps_by_execution, execution_id, %{}),
              Map.get(asset_deps_by_execution, execution_id, %{}),
              Map.get(catalog_reads_by_execution, execution_id, %{}),
              Map.get(catalog_waits_by_execution, execution_id, %{})
            ]
            |> Enum.reduce(%{}, &Map.merge(&2, &1))

          # Nothing is outstanding for an execution that has finished: it
          # isn't waiting on anything any more, whatever state its
          # dependencies are in.
          pending =
            if completed_at,
              do: MapSet.new(),
              else: Dependencies.unresolved_dependency_ids(db, execution_id)

          dependency_events =
            Enum.map(
              dependencies,
              &Scheduling.dependency_event(run.external_id, ext_id, pending, &1)
            )

          published =
            catalog_publishes_by_attempt
            |> Map.get({step.number, attempt}, %{})
            |> Enum.map(fn {_key, version} ->
              %CatalogPublished{run: run.external_id, execution: ext_id, version: version}
            end)

          {:ok, workspace_external_id} = Workspaces.get_workspace_external_id(db, workspace_id)

          inputs =
            submitted_inputs_by_execution
            |> Map.get(execution_id, %{})
            |> Enum.flat_map(fn {input_ext_id, %{title: title, status: status}} ->
              submitted = %InputSubmitted{
                run: run.external_id,
                execution: ext_id,
                input: input_ext_id,
                title: title
              }

              if status do
                {:ok, input_run_ext_id, number} = Ids.parse_input(input_ext_id)

                {:ok, input_id} =
                  Inputs.get_input_id_by_run_and_number(db, input_run_ext_id, number)

                [
                  submitted,
                  %InputResponded{
                    run: input_run_ext_id,
                    workspace: workspace_external_id,
                    input: input_ext_id,
                    response: InputFlow.build_input_response(db, input_id)
                  }
                ]
              else
                [submitted]
              end
            end)

          results =
            if result do
              [
                %ResultRecorded{
                  run: run.external_id,
                  execution: ext_id,
                  result: result,
                  result_at: result_at,
                  created_by: result_created_by,
                  final: final?
                }
              ]
            else
              []
            end

          metrics =
            metric_definitions_by_execution
            |> Map.get(execution_id, %{})
            |> Enum.map(fn {key, definition} ->
              %MetricDefined{
                run: run.external_id,
                execution: ext_id,
                key: key,
                definition: definition
              }
            end)

          {:ok, {checkpoints_before, checkpoints_after}} =
            Checkpoints.get_execution_snapshots(
              db,
              execution_id,
              step.id,
              Map.fetch!(workspace_chains, workspace_id),
              attempt
            )

          checkpoints = [
            %CheckpointsInherited{
              run: run.external_id,
              execution: ext_id,
              checkpoints: Resolve.checkpoints(db, checkpoints_before)
            },
            %CheckpointsSet{
              run: run.external_id,
              execution: ext_id,
              checkpoints: Resolve.checkpoints(db, checkpoints_after)
            }
          ]

          [%DependenciesPending{run: run.external_id, execution: ext_id, pending: pending}] ++
            dependency_events ++
            assets ++ published ++ inputs ++ results ++ metrics ++ checkpoints
        end)
      end

    requested_steps =
      Enum.flat_map(step_numbers, fn number ->
        case Map.fetch(steps_by_number, number) do
          {:ok, step} -> [step]
          :error -> []
        end
      end)

    streams_by_step =
      if requested_steps == [] do
        %{}
      else
        {:ok, run_streams} = Streams.get_streams_for_run(db, run.id)
        step_ids = MapSet.new(requested_steps, & &1.id)

        run_streams
        |> Enum.filter(&MapSet.member?(step_ids, &1.step_id))
        |> Enum.group_by(& &1.step_id)
      end

    step_events =
      Enum.flat_map(requested_steps, fn step ->
        {:ok, arguments} = Runs.get_step_arguments(db, step.id)

        streams =
          streams_by_step
          |> Map.get(step.id, [])
          |> Enum.flat_map(&stream_events(db, run.external_id, step, &1))

        [
          %StepArguments{
            run: run.external_id,
            step: step.number,
            arguments: Enum.map(arguments, &Resolve.value(db, &1))
          }
          | streams
        ]
      end)

    executions ++ step_events
  end

  # A stream as the events that opened, resumed and closed it.
  defp stream_events(db, run_external_id, step, stream) do
    {:ok, registrations} = Streams.get_registrations(db, stream.id)
    {:ok, workspace_external_id} = Workspaces.get_workspace_external_id(db, stream.workspace_id)
    ext_id = Ids.stream(stream.run_external_id, stream.step_number, stream.index)

    registered =
      Enum.map(registrations, fn {_execution_id, attempt, buffer, timeout_ms, _created_at} ->
        %StreamRegistered{
          run: run_external_id,
          step: stream.step_number,
          index: stream.index,
          stream: ext_id,
          workspace: workspace_external_id,
          position: stream.position,
          attempt: attempt,
          buffer: buffer,
          timeout_ms: timeout_ms,
          opened_at: stream.created_at,
          module: step.module,
          target: step.target
        }
      end)

    closed =
      if stream.closed_at do
        {reason, error} =
          Resolve.closure_reason(db, stream.reason, stream.error, stream.closed_by)

        {:ok, {_r, _s, attempt}} = Runs.get_execution_key(db, stream.closed_by)

        [
          %StreamClosed{
            run: run_external_id,
            step: stream.step_number,
            index: stream.index,
            stream: ext_id,
            reason: if(reason, do: Atom.to_string(reason)),
            error: StreamDelivery.encode_stream_error_summary(error),
            attempt: attempt,
            closed_at: stream.closed_at
          }
        ]
      else
        []
      end

    registered ++ closed
  end
end
