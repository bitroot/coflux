defmodule Coflux.Orchestration.Server.Scheduling do
  @moduledoc """
  Creating work: starting a run, scheduling a step, and re-running one.

  Every execution enters the system through here, and each entry does the
  same three things - write the rows, work out what the execution is
  waiting on, and announce it - which is why they share
  `emit_execution_detail/6`. An execution is announced complete with its
  dependencies, checkpoints and pending set, so a topic that hears about
  it never has to ask a follow-up question.

  Re-running is the involved case. A new attempt inherits the step's
  checkpoints as of the attempt it follows, re-derives its dependencies
  against the state of the world now rather than then, and may need to
  cancel a successor that never started - keeping that attempt's paused
  streams alive, because the new attempt continues them.

  `split_executions/2` is the other half: given everything queued, it
  decides what is due, what is waiting for its `execute_after`, and what
  defers onto an identical execution already in flight.
  """

  require Logger

  alias Coflux.Events.{
    AssetDependencyRecorded,
    CatalogRead,
    CatalogWaitRecorded,
    CheckpointsInherited,
    CheckpointsSet,
    DependenciesPending,
    ExecutionScheduled,
    ExecutionWaiting,
    InputDependencyRecorded,
    ResultDependencyRecorded,
    RunCreated,
    StreamDependencyRecorded
  }

  alias Coflux.Orchestration.{Catalog, Checkpoints, Ids, Inputs, Principals, Runs, Streams}

  alias Coflux.Orchestration.Server.{
    Archives,
    Dependencies,
    Effects,
    Gates,
    Lifecycle,
    Permissions,
    Resolve,
    State
  }

  def split_executions(executions, now) do
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

  def schedule_run(state, module, target_name, type, arguments, workspace_id, opts) do
    cache_workspace_ids = Permissions.get_cache_workspace_ids(state, workspace_id)
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
        delay_ms = Keyword.get(opts, :delay_ms, 0)
        execute_after = if delay_ms > 0, do: created_at + delay_ms

        principal =
          case Principals.get_principal(state.db, created_by) do
            {:ok, {type, external_id}} -> %{type: type, external_id: external_id}
            {:ok, nil} -> nil
          end

        execution_external_id =
          Ids.execution(external_run_id, step_number, attempt)

        ws_ext_id = State.workspace_external_id(state, workspace_id)

        # Compute and register pending dependencies
        wait_for = Keyword.get(opts, :wait_for) || []
        requires = Keyword.get(opts, :requires) || %{}

        {state, pending_dependencies} =
          if step_id do
            {pending_dependencies, _group} =
              pending =
              Dependencies.compute_pending_dependencies(state.db, execution_id, wait_for, step_id)

            state = Dependencies.register_pending_dependencies(state, execution_id, pending)

            {state, pending_dependencies}
          else
            {state, MapSet.new()}
          end

        state =
          state
          |> put_in([Access.key(:execution_ids), execution_external_id], execution_id)
          |> State.track_run_execution(external_run_id, execution_id, module, target_name)
          |> Effects.emit(%RunCreated{
            run: external_run_id,
            workspace: ws_ext_id,
            root_module: module,
            root_target: target_name,
            type: type,
            created_at: created_at,
            created_by: principal,
            parent:
              case Keyword.get(opts, :parent_id) do
                nil -> nil
                parent_id -> Resolve.execution(state.db, parent_id)
              end,
            requires: requires
          })
          |> Effects.emit(%ExecutionScheduled{
            execution: execution_external_id,
            run: external_run_id,
            step: step_number,
            attempt: attempt,
            workspace: ws_ext_id,
            module: module,
            target: target_name,
            type: type,
            root_module: module,
            root_target: target_name,
            execute_after: execute_after,
            created_at: created_at,
            created_by: principal,
            requires: requires
          })
          |> emit_waiting(
            execution_external_id,
            external_run_id,
            ws_ext_id,
            Gates.describe(state.db, pending_dependencies)
          )

        {:ok, external_run_id, step_number, execution_id, state}
    end
  end

  def rerun_step(state, step, workspace_id, opts) do
    execute_after = Keyword.get(opts, :execute_after, nil)
    dependency_keys = Keyword.get(opts, :dependency_keys, [])
    created_by = Keyword.get(opts, :created_by)
    catalog_sequence = Keyword.get(opts, :catalog_sequence)

    # Separate execution, input and stream dependencies.
    #
    # A stream wait arrives naming the stream externally, and is persisted
    # against a *stream ref* — the same indirection subscription lineage
    # uses, so the edge survives epoch rotation. A stream that can't be
    # resolved is dropped rather than recorded: gating on it would strand
    # the successor forever.
    {exec_deps, input_deps, stream_waits, catalog_waits} =
      Enum.reduce(dependency_keys, {[], [], [], []}, fn
        {:execution, id}, {execs, inputs, streams, catalog} ->
          {[id | execs], inputs, streams, catalog}

        {:input, id}, {execs, inputs, streams, catalog} ->
          {execs, [id | inputs], streams, catalog}

        {:stream, external_id, sequence}, {execs, inputs, streams, catalog} ->
          case Archives.resolve_stream_id(state, external_id) do
            {:ok, stream_id} ->
              case Streams.create_stream_ref_for(state.db, stream_id) do
                {:ok, ref_id} -> {execs, inputs, [{ref_id, sequence} | streams], catalog}
                {:error, :not_found} -> {execs, inputs, streams, catalog}
              end

            {:error, :not_found} ->
              {execs, inputs, streams, catalog}
          end

        {:catalog, path, number}, {execs, inputs, streams, catalog} ->
          {execs, inputs, streams, [{path, number} | catalog]}
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
           input_dependency_ids: input_deps,
           stream_waits: stream_waits,
           created_by: created_by,
           catalog_sequence: catalog_sequence
         ) do
      {:ok, execution_id, attempt, created_at} ->
        # A catalog wait is keyed by path rather than by a ref, so it is
        # written directly. Before the gate is computed, since it reads
        # these rows.
        Enum.each(catalog_waits, fn {path, number} ->
          :ok = Catalog.record_wait(state.db, execution_id, path, number)
        end)

        {run_module, run_target} =
          case State.get_run_workflow(state, run.external_id) do
            {_, _} = workflow ->
              workflow

            nil ->
              {:ok, workflow} = Runs.get_run_target(state.db, run.id)
              workflow
          end

        state =
          State.track_run_execution(state, run.external_id, execution_id, run_module, run_target)

        principal =
          case Principals.get_principal(state.db, created_by) do
            {:ok, {type, external_id}} -> %{type: type, external_id: external_id}
            {:ok, nil} -> nil
          end

        dependencies =
          Map.new(dependency_ref_ids, fn ref_id ->
            {ext_id, _module, _target} = execution = Resolve.execution_ref(state.db, ref_id)
            {ext_id, {:result, execution}}
          end)

        # Compute and register pending dependencies
        {pending_dependencies, _group} =
          pending =
          Dependencies.compute_pending_dependencies(
            state.db,
            execution_id,
            step.wait_for || [],
            step.id
          )

        state = Dependencies.register_pending_dependencies(state, execution_id, pending)

        dependencies =
          Map.merge(
            Dependencies.build_argument_dependencies(state.db, step.id, step.wait_for),
            dependencies
          )

        unresolved_dependencies = Dependencies.unresolved_dependency_ids(state.db, execution_id)

        step_requires = Resolve.tag_set(state.db, step.requires_tag_set_id)
        run_requires = Resolve.tag_set(state.db, run.requires_tag_set_id)

        requires =
          run_requires
          |> Map.merge(step_requires)
          |> Map.reject(fn {_key, values} -> values == [] end)

        execution_external_id =
          Ids.execution(run.external_id, step.number, attempt)

        ws_ext_id = State.workspace_external_id(state, workspace_id)

        {:ok, checkpoints} =
          Checkpoints.get_effective(
            state.db,
            step.id,
            State.workspace_chain(state, workspace_id),
            attempt
          )

        state =
          state
          |> put_in([Access.key(:execution_ids), execution_external_id], execution_id)
          |> Effects.emit(%ExecutionScheduled{
            execution: execution_external_id,
            run: run.external_id,
            step: step.number,
            attempt: attempt,
            workspace: ws_ext_id,
            module: step.module,
            target: step.target,
            type: step.type,
            root_module: run_module,
            root_target: run_target,
            execute_after: execute_after,
            created_at: created_at,
            created_by: principal,
            requires: requires
          })
          |> emit_execution_detail(
            run.external_id,
            execution_external_id,
            dependencies,
            Resolve.checkpoints(state.db, checkpoints),
            unresolved_dependencies
          )
          |> emit_waiting(
            execution_external_id,
            run.external_id,
            ws_ext_id,
            Gates.describe(state.db, pending_dependencies)
          )

        # Announce the stream waits `rerun_step` recorded. They are lineage
        # edges against an execution that hasn't run yet, so without this
        # nothing announces them until it subscribes — and it may never get
        # that far. A topic opened later reads them from the snapshot.
        state =
          Enum.reduce(stream_waits, state, fn {stream_ref_id, _sequence}, state ->
            {:ok, {stream_run_ext_id, stream_step_number, index, module, target}} =
              Streams.get_stream_ref(state.db, stream_ref_id)

            stream_id = Ids.stream(stream_run_ext_id, stream_step_number, index)

            Effects.emit(state, %StreamDependencyRecorded{
              run: run.external_id,
              execution: execution_external_id,
              stream: stream_id,
              module: module,
              target: target,
              pending: MapSet.member?(unresolved_dependencies, stream_id)
            })
          end)

        state =
          Enum.reduce(catalog_waits, state, fn {path, number}, state ->
            Effects.emit(state, %CatalogWaitRecorded{
              run: run.external_id,
              execution: execution_external_id,
              path: path,
              number: number,
              pending: MapSet.member?(unresolved_dependencies, Ids.catalog_wait(path, number))
            })
          end)

        # Notify run topic about input dependencies for this execution
        state =
          Enum.reduce(input_deps, state, fn input_id, state ->
            case Inputs.get_input_by_id(state.db, input_id) do
              {:ok,
               {run_ext_id, input_number, _key, _prompt_id, _schema_id, input_title, _actions,
                _requires_tag_set_id, _created_at}} ->
                input_ext_id = Ids.input(run_ext_id, input_number)

                response_type =
                  case Inputs.get_input_response(state.db, input_id) do
                    {:ok, nil} -> nil
                    {:ok, {:value, _, _, _}} -> :value
                    {:ok, {:dismissed, _, _}} -> :dismissed
                    {:ok, {:cancelled, _, _}} -> :cancelled
                  end

                Effects.emit(state, %InputDependencyRecorded{
                  run: run.external_id,
                  execution: execution_external_id,
                  input: input_ext_id,
                  title: input_title,
                  response: response_type,
                  pending: MapSet.member?(unresolved_dependencies, input_ext_id)
                })

              _ ->
                state
            end
          end)

        # Re-running the initial step reopens the run's outcome.
        state =
          case step.type do
            :workflow ->
              Lifecycle.emit_run_outcome(
                state,
                ws_ext_id,
                run.external_id,
                run_module,
                run_target
              )

            _other ->
              state
          end

        send(self(), :tick)

        {:ok, execution_id, attempt, state}
    end
  end

  def step_retries(%{retry_limit: 0}), do: nil

  def step_retries(step) do
    %{
      limit: if(step.retry_limit == -1, do: nil, else: step.retry_limit),
      backoff_min_ms: step.retry_backoff_min_ms,
      backoff_max_ms: step.retry_backoff_max_ms
    }
  end

  # How one of an execution's dependencies is announced. Every kind the
  # dependency map can hold is here, so the live emit and the snapshot
  # rebuild describe a dependency the same way.
  def dependency_event(run_external_id, execution_external_id, pending, {id, dependency}) do
    case dependency do
      {:result, execution} ->
        %ResultDependencyRecorded{
          run: run_external_id,
          execution: execution_external_id,
          dependency: execution,
          pending: MapSet.member?(pending, id)
        }

      {:input, title, status} ->
        %InputDependencyRecorded{
          run: run_external_id,
          execution: execution_external_id,
          input: id,
          title: title,
          response: status,
          pending: MapSet.member?(pending, id)
        }

      {:asset, summary} ->
        %AssetDependencyRecorded{
          run: run_external_id,
          execution: execution_external_id,
          asset: id,
          summary: summary,
          pending: MapSet.member?(pending, id)
        }

      {:stream, stream_id, module, target} ->
        %StreamDependencyRecorded{
          run: run_external_id,
          execution: execution_external_id,
          stream: stream_id,
          module: module,
          target: target,
          pending: MapSet.member?(pending, id)
        }

      {:catalog, version} ->
        %CatalogRead{
          run: run_external_id,
          execution: execution_external_id,
          version: version
        }

      {:catalog_wait, path, number} ->
        %CatalogWaitRecorded{
          run: run_external_id,
          execution: execution_external_id,
          path: path,
          number: number,
          pending: MapSet.member?(pending, id)
        }
    end
  end

  # The detail of one execution as it is announced when it is created.
  def emit_execution_detail(
        state,
        run_external_id,
        execution_external_id,
        dependencies,
        checkpoints,
        unresolved
      ) do
    state =
      Enum.reduce(dependencies, state, fn dependency, state ->
        Effects.emit(
          state,
          dependency_event(run_external_id, execution_external_id, unresolved, dependency)
        )
      end)

    # Nothing has run yet, so what the execution will start from is also
    # what it currently holds.
    state
    |> Effects.emit(%CheckpointsInherited{
      run: run_external_id,
      execution: execution_external_id,
      checkpoints: checkpoints
    })
    |> Effects.emit(%CheckpointsSet{
      run: run_external_id,
      execution: execution_external_id,
      checkpoints: checkpoints
    })
    |> Effects.emit(%DependenciesPending{
      run: run_external_id,
      execution: execution_external_id,
      pending: unresolved
    })
  end

  def metric_definition(definition) do
    %{
      group: Map.get(definition, "group"),
      group_units: Map.get(definition, "group_units"),
      group_lower: Map.get(definition, "group_lower"),
      group_upper: Map.get(definition, "group_upper"),
      scale: Map.get(definition, "scale"),
      units: Map.get(definition, "units"),
      progress: Map.get(definition, "progress", false),
      lower: Map.get(definition, "lower"),
      upper: Map.get(definition, "upper")
    }
  end

  # The detail of the executions named by `{step number, attempt}`, and the
  # arguments and streams of the named steps, as events. The row scans are
  # over the whole run (they're indexed by it and cheap); everything that
  # resolves a value, ref or asset is done only for what was asked for.
  def build_streams_config(nil, nil), do: nil

  def build_streams_config(buffer, timeout_ms) do
    map = %{buffer: if(buffer == -1, do: nil, else: buffer)}
    if timeout_ms != nil, do: Map.put(map, :timeout_ms, timeout_ms), else: map
  end

  def emit_waiting(state, _execution, _run, _workspace, []), do: state

  def emit_waiting(state, execution, run, workspace, gates) do
    Effects.emit(state, %ExecutionWaiting{
      execution: execution,
      run: run,
      workspace: workspace,
      gates: gates
    })
  end
end
