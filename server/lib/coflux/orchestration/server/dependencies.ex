defmodule Coflux.Orchestration.Server.Dependencies do
  @moduledoc """
  What an execution is waiting on, and what is holding it back.

  Two gates decide whether a queued execution may be assigned, and this
  module owns the ledgers behind both.

  **Dependencies.** `pending_dependencies` maps an execution to the set of
  things it is still waiting on - a result, an input, a stream position, a
  catalog entry - and `dependency_waiters` is the reverse index, so a
  result that lands can find who cares in one lookup. Two further indexes
  are derived from the first and rebuilt with it: `stream_dependency_keys`
  (waits by stream, so an append need not scan) and `dependency_groups`
  (the any-of sets a suspended select recorded, where one member clearing
  releases the execution from all of them).

  **Concurrency.** `concurrency_permits` records which execution holds a
  permit for which key; an execution holds one from the moment its
  assignment is written until its completion is. `concurrency_gated`
  records the executions the last scheduler pass left waiting on a permit,
  along with the queue entry that was published for each, so a subscriber
  joining mid-gate sees what existing subscribers already have.

  None of this is authoritative. The pending set is derivable from the
  database (`compute_pending_dependencies/4` does exactly that) and the
  permits are derivable from assignments without completions
  (`load_concurrency_permits/1`), which is why a restart or an epoch
  rotation can rebuild both rather than having to carry them across. Where
  the ledger and the database disagree, the database is right.

  The same question is answered twice here, deliberately:
  `pending_dependencies` is the assignment gate, computed once and amended
  as dependencies clear, while `unresolved_dependency_ids/2` re-derives
  the same thing per dependency for display. They are kept apart because
  the gate must be cheap and the display must be current.
  """

  require Logger

  alias Coflux.Events.{DependenciesPending, ExecutionWaiting}

  alias Coflux.Orchestration.{Catalog, Ids, Inputs, Results, Runs, Streams, Workspaces}

  alias Coflux.Orchestration.Server.{Commands, Effects, Gates, Resolve, State, StreamDelivery}

  # Send a notification with the current pending dependencies for an execution.
  # The queue gets the gates themselves, so it can say what an execution is
  # waiting on; the run only needs to know which of the dependencies it
  # already lists are still unresolved.
  def notify_pending_dependencies(state, execution_id, pending_dependency_ids) do
    case Runs.get_execution_key(state.db, execution_id) do
      {:ok, {r, s, a}} ->
        execution_ext_id = Ids.execution(r, s, a)

        gates =
          Gates.describe(state.db, pending_dependency_ids) ++
            Map.get(state.concurrency_gated, execution_id, [])

        {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)
        ws_ext_id = State.workspace_external_id(state, workspace_id)

        state
        |> Effects.emit(%ExecutionWaiting{
          execution: execution_ext_id,
          run: r,
          workspace: ws_ext_id,
          gates: gates
        })
        |> Effects.emit(%DependenciesPending{
          run: r,
          execution: execution_ext_id,
          pending: unresolved_dependency_ids(state.db, execution_id)
        })

      {:error, _} ->
        state
    end
  end

  # Rebuild the concurrency ledger from the database: every execution with
  # an assignment and no completion holds a permit for each key it declares.
  def load_concurrency_permits(state) do
    {:ok, permits} = Runs.get_held_concurrency_permits(state.db)

    Map.put(
      state,
      :concurrency_permits,
      Map.new(permits, fn {execution_id, workspace_id, concurrency_key, group_key} ->
        {execution_id,
         %{
           workspace_id: workspace_id,
           keys: Enum.reject([concurrency_key, group_key], &is_nil/1)
         }}
      end)
    )
  end

  # An execution can be subject to two limits at once: its task's (a blob
  # key hashed from namespace and arguments) and its group's (a string
  # naming the parent execution), so the two never collide in the counts.
  # Each is judged against its own limit; admission needs room in all.
  def execution_requirements(execution) do
    [
      {:concurrency, execution.concurrency_key, execution.concurrency_limit},
      {:group, execution.group_key, execution.group_limit}
    ]
    |> Enum.reject(fn {_type, key, limit} -> is_nil(key) or limit == 0 end)
    |> Enum.map(fn {type, key, limit} -> %{type: type, key: key, limit: limit} end)
  end

  # The keys an execution holds while it runs — whichever of the two it
  # declares. Empty for an execution under no limit, which never enters
  # the ledger.
  def permit_keys(execution) do
    Enum.reject([execution.concurrency_key, execution.group_key], &is_nil/1)
  end

  # Permits are counted per {workspace, key}. Workspaces are scheduled
  # independently — each has its own workers, pools and defer keys — and
  # inheritance only shares results, so a limit is scoped the same way: a
  # derived workspace neither waits behind its base nor holds it up.
  #
  # %{{workspace_id, key} => count}, computed once per tick.
  def concurrency_held_counts(state) do
    Enum.reduce(state.concurrency_permits, %{}, fn {_execution_id, permit}, counts ->
      Enum.reduce(permit.keys, counts, fn key, counts ->
        Map.update(counts, concurrency_scope(permit.workspace_id, key), 1, &(&1 + 1))
      end)
    end)
  end

  def concurrency_scope(workspace_id, key), do: {workspace_id, key}

  # Empty when the execution declares no limit, or when there's room under
  # every limit it declares. Otherwise one queue-topic dependency map per
  # limit that lacks room, each naming the current holders — so an
  # execution gated on both its task and its group reports both, and
  # neither is reserved while waiting on the other.
  #
  # The limit compared against is the execution's own, not the holders' —
  # targets sharing a namespace may each declare a different one, and each
  # is judged by what it asked for.
  def concurrency_gate(state, counts, execution) do
    execution
    |> execution_requirements()
    |> Enum.reject(fn requirement ->
      scope = concurrency_scope(execution.workspace_id, requirement.key)
      Map.get(counts, scope, 0) < requirement.limit
    end)
    |> Enum.map(fn requirement ->
      scope = concurrency_scope(execution.workspace_id, requirement.key)

      holders =
        state.concurrency_permits
        |> Enum.filter(fn {_execution_id, permit} ->
          requirement.key in permit.keys and
            concurrency_scope(permit.workspace_id, requirement.key) == scope
        end)
        |> Enum.map(fn {holder_id, _permit} ->
          case Runs.get_execution_key(state.db, holder_id) do
            {:ok, {r, s, a}} -> Ids.execution(r, s, a)
            {:error, _} -> nil
          end
        end)
        |> Enum.reject(&is_nil/1)

      build_gate(requirement, holders)
    end)
  end

  def build_gate(%{type: :concurrency, key: key, limit: limit}, holders) do
    %{type: "concurrency", key: build_concurrency_key(key), limit: limit, holders: holders}
  end

  # The group key is readable as it stands — "<parent execution>/<group>" —
  # so it's sent verbatim, split into its parts for the queue's benefit.
  # The group's name isn't carried: the run topic has it, and resolving it
  # here would need a join the spawned-run case can't make.
  def build_gate(%{type: :group, key: key, limit: limit}, holders) do
    [parent, group] = String.split(key, "/", parts: 2)

    %{
      type: "group",
      key: key,
      parent: parent,
      group: String.to_integer(group),
      limit: limit,
      holders: holders
    }
  end

  # Rendered like the run topic's cacheKey — a short hex prefix, enough to
  # tell two pools apart at a glance.
  def build_concurrency_key(key) do
    key |> Base.encode16(case: :lower) |> String.slice(0, 10)
  end

  def grant_concurrency_permit(state, execution) do
    case permit_keys(execution) do
      [] ->
        state

      keys ->
        put_in(state, [Access.key(:concurrency_permits), execution.execution_id], %{
          workspace_id: execution.workspace_id,
          keys: keys
        })
    end
  end

  # A no-op for an execution that never held one (never assigned, or
  # declaring no limit), which is every completion but a holder's.
  def release_concurrency_permit(state, execution_id) do
    if Map.has_key?(state.concurrency_permits, execution_id) do
      # A completion that only frees a permit doesn't necessarily tick —
      # the result-time tick has already happened by the time a draining
      # execution completes — so whatever was gated on it would otherwise
      # wait for the next unrelated tick.
      send(self(), :tick)

      state
      |> Map.put(:concurrency_permits, Map.delete(state.concurrency_permits, execution_id))
      |> Map.put(:concurrency_gated, Map.delete(state.concurrency_gated, execution_id))
    else
      Map.put(state, :concurrency_gated, Map.delete(state.concurrency_gated, execution_id))
    end
  end

  def increment_concurrency_count(counts, execution) do
    Enum.reduce(permit_keys(execution), counts, fn key, counts ->
      Map.update(counts, concurrency_scope(execution.workspace_id, key), 1, &(&1 + 1))
    end)
  end

  # Emit queue-topic dependency updates for executions that have just become
  # gated, or have just stopped being. Transitions only — re-sending an
  # unchanged gate every tick would be pure noise on a busy queue.
  def update_concurrency_gates(state, gated_now) do
    changed =
      Enum.uniq(Map.keys(gated_now) ++ Map.keys(state.concurrency_gated))
      |> Enum.reject(&(Map.get(gated_now, &1) == Map.get(state.concurrency_gated, &1)))

    state = Map.put(state, :concurrency_gated, gated_now)

    Enum.reduce(changed, state, fn execution_id, state ->
      notify_queue_dependencies(state, execution_id)
    end)
  end

  # The queue's answer to "why isn't this running?": whatever the execution
  # is waiting on, plus each concurrency gate that's holding it back.
  def notify_queue_dependencies(state, execution_id) do
    with {:ok, {r, s, a}} <- Runs.get_execution_key(state.db, execution_id),
         {:ok, workspace_id} <- Runs.get_workspace_id_for_execution(state.db, execution_id) do
      Effects.emit(state, %ExecutionWaiting{
        execution: Ids.execution(r, s, a),
        run: r,
        workspace: State.workspace_external_id(state, workspace_id),
        gates: Gates.for_execution(state, execution_id)
      })
    else
      _ -> state
    end
  end

  # Initialize pending_dependencies and dependency_waiters for all existing unassigned executions.
  def initialize_pending_dependencies(state) do
    {:ok, executions} = Runs.get_unassigned_executions(state.db)

    Enum.reduce(executions, state, fn execution, state ->
      pending =
        compute_pending_dependencies(
          state.db,
          execution.execution_id,
          execution.wait_for,
          execution.step_id
        )

      register_pending_dependencies(state, execution.execution_id, pending)
    end)
  end

  # The execution references carried by the arguments named in `wait_for`,
  # as {run_external_id, step_number, attempt}. These gate the execution
  # before it ever runs, but live in the step's arguments rather than the
  # dependency table, so nothing else surfaces them.
  def argument_reference_keys(db, step_id, wait_for) do
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
      |> Enum.flat_map(fn
        {:execution, run_ext, step_num, attempt} -> [{run_ext, step_num, attempt}]
        _ -> []
      end)
      |> Enum.uniq()
    else
      []
    end
  end

  # Those same references, shaped as run topic dependencies.
  def build_argument_dependencies(db, step_id, wait_for) do
    db
    |> argument_reference_keys(step_id, wait_for)
    |> Map.new(fn {run_ext, step_num, attempt} ->
      ext_id = Ids.execution(run_ext, step_num, attempt)

      {module, target} =
        case Runs.get_module_target(db, run_ext, step_num, attempt) do
          {:ok, {m, t}} -> {m, t}
          {:ok, nil} -> {nil, nil}
        end

      {ext_id, {:result, {ext_id, module, target}}}
    end)
  end

  # Whether the execution these coordinates name has produced a result,
  # following redirects (a suspend's successor, a spawn's target) the way
  # the assignment gate does.
  def execution_result_pending?(db, run_ext, step_num, attempt) do
    case Runs.get_execution_id(db, run_ext, step_num, attempt) do
      {:ok, {execution_id}} when not is_nil(execution_id) ->
        match?({:pending, _}, Results.resolve(db, execution_id))

      _ ->
        false
    end
  end

  # Which of an execution's dependencies are still outstanding, keyed as the
  # run topic's dependency map is. Related to `pending_dependencies` but not
  # the same thing: that's the assignment gate, computed once and amended as
  # dependencies clear, whereas this is re-derived per dependency for
  # display. A completed execution reports nothing - it isn't waiting on
  # anything any more, whatever state its dependencies are in.
  #
  # Same two classes as the gate: argument references are outstanding
  # individually, and the recorded dependencies form an any-of group, so a
  # single met member means none of them is outstanding.
  def unresolved_dependency_ids(db, execution_id) do
    {:ok, step} = Runs.get_step_for_execution(db, execution_id)

    argument_ids =
      db
      |> argument_reference_keys(step.id, step.wait_for)
      |> Enum.filter(fn {run_ext, step_num, attempt} ->
        execution_result_pending?(db, run_ext, step_num, attempt)
      end)
      |> MapSet.new(fn {run_ext, step_num, attempt} ->
        Ids.execution(run_ext, step_num, attempt)
      end)

    result_members =
      case Runs.get_result_dependencies(db, execution_id) do
        {:ok, dependencies} ->
          Enum.map(dependencies, fn {ref_id} ->
            {:ok, {run_ext, step_num, attempt, _, _}} = Runs.get_execution_ref(db, ref_id)

            {Ids.execution(run_ext, step_num, attempt),
             execution_result_pending?(db, run_ext, step_num, attempt)}
          end)
      end

    input_members =
      case Runs.get_input_dependencies(db, execution_id) do
        {:ok, deps} ->
          Enum.map(deps, fn {input_id} ->
            {:ok, run_ext, number} = Inputs.get_input_run_and_number(db, input_id)
            {Ids.input(run_ext, number), !Inputs.is_input_responded?(db, input_id)}
          end)
      end

    stream_members =
      case Streams.get_wait_dependencies(db, execution_id) do
        {:ok, waits} ->
          Enum.map(waits, fn {stream_ref_id, sequence} ->
            {:ok, {run_ext, step_number, index, _module, _target}} =
              Streams.get_stream_ref(db, stream_ref_id)

            pending? =
              case resolve_stream_ref_id(db, stream_ref_id) do
                {:ok, stream_id} -> !stream_reached?(db, stream_id, sequence)
                {:error, :not_found} -> false
              end

            {Ids.stream(run_ext, step_number, index), pending?}
          end)
      end

    catalog_members =
      case Catalog.get_waits(db, execution_id) do
        {:ok, []} ->
          []

        {:ok, waits} ->
          {:ok, workspace_id} = Runs.get_workspace_id_for_execution(db, execution_id)
          {:ok, chain} = Workspaces.get_workspace_chain(db, workspace_id)

          Enum.map(waits, fn {path, number} ->
            {Ids.catalog_wait(path, number),
             match?({:ok, nil}, Catalog.get_next(db, path, chain, number))}
          end)
      end

    members = result_members ++ input_members ++ stream_members ++ catalog_members

    group_ids =
      if Enum.any?(members, fn {_id, pending?} -> !pending? end),
        do: MapSet.new(),
        else: MapSet.new(members, fn {id, _pending?} -> id end)

    MapSet.union(argument_ids, group_ids)
  end

  # Compute the set of dependency keys the given execution is waiting on.
  #
  # Two classes, combined as `all arguments AND any of the recorded group`:
  #
  #   * Argument references (`wait_for`) — every one must resolve before the
  #     step's arguments make sense.
  #   * Dependencies recorded on the execution row — result, input, stream
  #     and catalog waits, which only a suspended select (or a suspended
  #     stream consumer) writes. Select is first-wins, so the successor
  #     wakes when ANY of them is met: if one already is, the whole group
  #     is dropped here; otherwise every unmet one is registered and
  #     `dependency_groups` remembers they go together.
  #
  # Returns `{pending, group}`: the keys to gate on, and the subset of them
  # that form the any-of group.
  def compute_pending_dependencies(db, execution_id, wait_for, step_id) do
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

    # Each recorded dependency, as {:met | key}. A key is one still to wait
    # for; :met is one that has already resolved, which is enough on its
    # own to release the group.
    result_dependencies =
      case Runs.get_result_dependencies(db, execution_id) do
        {:ok, dependencies} ->
          Enum.map(dependencies, fn {dependency_ref_id} ->
            {:ok, {run_ext, step_num, attempt, _, _}} =
              Runs.get_execution_ref(db, dependency_ref_id)

            case Runs.get_execution_id(db, run_ext, step_num, attempt) do
              {:ok, {dependency_execution_id}} when not is_nil(dependency_execution_id) ->
                case Results.resolve(db, dependency_execution_id) do
                  {:ok, _} -> :met
                  {:pending, pending_id} -> {:execution, pending_id}
                end

              _ ->
                # Gone (a pruned epoch, say). Treated as met rather than
                # stranding the execution.
                :met
            end
          end)
      end

    input_dependencies =
      case Runs.get_input_dependencies(db, execution_id) do
        {:ok, deps} ->
          Enum.map(deps, fn {input_id} ->
            if Inputs.is_input_responded?(db, input_id), do: :met, else: {:input, input_id}
          end)
      end

    # Only rows with a sequence are waits; the rest of the table is
    # subscription lineage. This runs solely for executions that have not
    # been assigned yet, which is what makes it safe for the sequence to
    # stay on the row after the gate clears — a completed execution's row
    # is never read back here.
    stream_dependencies =
      case Streams.get_wait_dependencies(db, execution_id) do
        {:ok, waits} ->
          Enum.map(waits, fn {stream_ref_id, sequence} ->
            case resolve_stream_ref_id(db, stream_ref_id) do
              {:ok, stream_id} ->
                if stream_reached?(db, stream_id, sequence),
                  do: :met,
                  else: {:stream, stream_id, sequence}

              {:error, :not_found} ->
                :met
            end
          end)
      end

    # A catalog wait is met once the path, as seen from the execution's
    # workspace chain, holds a version numbered above the one recorded.
    catalog_dependencies =
      case Catalog.get_waits(db, execution_id) do
        {:ok, []} ->
          []

        {:ok, waits} ->
          {:ok, workspace_id} = Runs.get_workspace_id_for_execution(db, execution_id)
          {:ok, chain} = Workspaces.get_workspace_chain(db, workspace_id)

          Enum.map(waits, fn {path, number} ->
            case Catalog.get_next(db, path, chain, number) do
              {:ok, nil} -> {:catalog, workspace_id, path, number}
              {:ok, _version} -> :met
            end
          end)
      end

    recorded =
      result_dependencies ++ input_dependencies ++ stream_dependencies ++ catalog_dependencies

    group =
      if Enum.any?(recorded, &(&1 == :met)),
        do: MapSet.new(),
        else: MapSet.new(recorded)

    {MapSet.union(argument_dependencies, group), group}
  end

  # A stream wait is met once the stream holds the sequence, or can never
  # hold it because it closed.
  def stream_reached?(db, stream_id, sequence) do
    case Streams.get_head(db, stream_id) do
      {:ok, head} -> head >= sequence || Streams.closed?(db, stream_id)
    end
  end

  def resolve_stream_ref_id(db, stream_ref_id) do
    case Streams.get_stream_ref(db, stream_ref_id) do
      {:ok, {run_external_id, step_number, index, _module, _target}} ->
        Streams.get_stream_id_by_key(db, run_external_id, step_number, index)

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  # Walk references and collect tagged dependency keys that are still pending.
  def collect_pending_execution_ids(references, db, seen) do
    Enum.reduce(references, MapSet.new(), fn
      {:execution, run_ext, step_num, attempt}, acc ->
        case Runs.get_execution_id(db, run_ext, step_num, attempt) do
          {:ok, {execution_id}} when not is_nil(execution_id) ->
            if MapSet.member?(seen, execution_id) do
              acc
            else
              case Results.resolve(db, execution_id) do
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

      {:input, _external_id}, acc ->
        acc
    end)
  end

  # Register an execution's pending dependencies in state.
  # Only adds entries if there are actual pending dependencies.
  def register_pending_dependencies(state, execution_id, {dependencies, group}) do
    if MapSet.size(dependencies) == 0 do
      state
    else
      state =
        state
        |> put_in([Access.key(:pending_dependencies), execution_id], dependencies)
        |> then(fn state ->
          if MapSet.size(group) > 0,
            do: put_in(state, [Access.key(:dependency_groups), execution_id], group),
            else: state
        end)

      Enum.reduce(dependencies, state, fn dependency_id, state ->
        state
        |> update_in(
          [Access.key(:dependency_waiters), Access.key(dependency_id, MapSet.new())],
          &MapSet.put(&1, execution_id)
        )
        |> index_stream_dependency(dependency_id)
      end)
    end
  end

  # Stream waits get a secondary index, keyed by stream. Appends are hot,
  # and without it every appended item would have to scan the whole
  # dependency_waiters map to find out whether anything was waiting; with
  # it the check is one map lookup that almost always misses.
  def index_stream_dependency(state, {:stream, stream_id, _sequence} = key) do
    was_waiting = stream_has_waiters?(state, stream_id)

    state =
      update_in(
        state,
        [Access.key(:stream_dependency_keys), Access.key(stream_id, MapSet.new())],
        &MapSet.put(&1, key)
      )

    # First waiter: the producer's idle countdown stops. A consumer's nap
    # is not the producer being idle — the mirror of the existing rule
    # that a suspended producer's own pause doesn't count against it.
    state = if was_waiting, do: state, else: set_stream_timer_paused(state, stream_id, true)

    # The wait counts as demand (see refresh_stream_demand_for), and the
    # producer may be blocked on exactly that.
    StreamDelivery.refresh_stream_demand(state, stream_id)
  end

  def index_stream_dependency(state, _key), do: state

  def unindex_stream_dependency(state, {:stream, stream_id, _sequence} = key) do
    state =
      update_in(
        state,
        [Access.key(:stream_dependency_keys), Access.key(stream_id, MapSet.new())],
        &MapSet.delete(&1, key)
      )

    if MapSet.size(state.stream_dependency_keys[stream_id] || MapSet.new()) == 0 do
      state
      |> update_in([Access.key(:stream_dependency_keys)], &Map.delete(&1, stream_id))
      |> set_stream_timer_paused(stream_id, false)
    else
      state
    end
  end

  def unindex_stream_dependency(state, _key), do: state

  def stream_has_waiters?(state, stream_id) do
    MapSet.size(Map.get(state.stream_dependency_keys, stream_id, MapSet.new())) > 0
  end

  # Tell the producer's worker to stop or restart the stream's idle
  # countdown. Enforcement is worker-side, so this is the only way to say
  # it. A producer with no live session has no timer to pause.
  #
  # Deliberately resolved from the database rather than from
  # `stream_producers`: that map only exists to track demand, so a stream
  # with `buffer=nil` has no entry at all — and an unbuffered producer is
  # exactly what you pair with a suspending consumer, so it is the case
  # that most needs this. Infrequent enough for the lookup not to matter:
  # once when the first waiter arrives, once when the last one clears.
  def set_stream_timer_paused(state, stream_id, paused) do
    with execution_external_id when is_binary(execution_external_id) <-
           Resolve.stream_producer(state.db, stream_id),
         {:ok, session_id} <- State.session_for_execution(state, execution_external_id),
         {:ok, stream} <- Streams.get_stream(state.db, stream_id) do
      Effects.command(
        state,
        session_id,
        Commands.stream_timer_pause(execution_external_id, stream.index, paused)
      )
    else
      _ -> state
    end
  end

  # Remove an execution from the dependency tracking (when assigned or completed).
  def unregister_pending_dependencies(state, execution_id) do
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
              state
              |> update_in(
                [Access.key(:dependency_waiters)],
                &Map.delete(&1, dependency_id)
              )
              |> unindex_stream_dependency(dependency_id)
            else
              state
            end
          end)

        state
        |> update_in([Access.key(:pending_dependencies)], &Map.delete(&1, execution_id))
        |> update_in([Access.key(:dependency_groups)], &Map.delete(&1, execution_id))

      :error ->
        state
    end
  end

  # Apply the removal of `dependency_key` from `waiter_id`'s pending set,
  # with `new_pending` (a redirect's replacement keys, usually empty) taking
  # its place. If the key belonged to the waiter's any-of group and nothing
  # replaces it, the group is met: every other member is dropped too, and
  # the execution is scheduled. A redirected member stays in the group under
  # its new key.
  def remove_pending_dependency(state, waiter_id, dependency_key, new_pending) do
    case Map.fetch(state.pending_dependencies, waiter_id) do
      {:ok, current} ->
        group = Map.get(state.dependency_groups, waiter_id, MapSet.new())
        in_group? = MapSet.member?(group, dependency_key)

        {updated, group} =
          cond do
            in_group? and MapSet.size(new_pending) == 0 ->
              {MapSet.difference(MapSet.delete(current, dependency_key), group), MapSet.new()}

            in_group? ->
              {current |> MapSet.delete(dependency_key) |> MapSet.union(new_pending),
               group |> MapSet.delete(dependency_key) |> MapSet.union(new_pending)}

            true ->
              {current |> MapSet.delete(dependency_key) |> MapSet.union(new_pending), group}
          end

        # Keys released along with the one that cleared no longer have this
        # waiter behind them.
        released = MapSet.difference(current, MapSet.put(updated, dependency_key))

        state =
          Enum.reduce(released, state, fn key, state ->
            state
            |> update_in(
              [Access.key(:dependency_waiters), Access.key(key, MapSet.new())],
              &MapSet.delete(&1, waiter_id)
            )
            |> then(fn state ->
              if MapSet.size(state.dependency_waiters[key] || MapSet.new()) == 0 do
                state
                |> update_in([Access.key(:dependency_waiters)], &Map.delete(&1, key))
                |> unindex_stream_dependency(key)
              else
                state
              end
            end)
          end)

        state =
          Enum.reduce(new_pending, state, fn new_dep_key, state ->
            state
            |> update_in(
              [Access.key(:dependency_waiters), Access.key(new_dep_key, MapSet.new())],
              &MapSet.put(&1, waiter_id)
            )
            |> index_stream_dependency(new_dep_key)
          end)

        if MapSet.size(updated) == 0 do
          send(self(), :tick)
        end

        state
        |> then(fn state ->
          if MapSet.size(updated) == 0 do
            state
            |> update_in([Access.key(:pending_dependencies)], &Map.delete(&1, waiter_id))
            |> update_in([Access.key(:dependency_groups)], &Map.delete(&1, waiter_id))
          else
            state
            |> put_in([Access.key(:pending_dependencies), waiter_id], updated)
            |> then(fn state ->
              if MapSet.size(group) > 0,
                do: put_in(state, [Access.key(:dependency_groups), waiter_id], group),
                else:
                  update_in(state, [Access.key(:dependency_groups)], &Map.delete(&1, waiter_id))
            end)
          end
        end)
        |> notify_pending_dependencies(waiter_id, updated)

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
  def update_dependencies_on_result(state, execution_id) do
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
          case Results.resolve(state.db, execution_id) do
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
          remove_pending_dependency(state, waiter_id, dependency_key, new_pending)
        end)

      :error ->
        state
    end
  end

  # Called when a stream gains an item, or closes. Wakes any execution that
  # suspended mid-iteration and is gated on this stream.
  #
  # `head` is the highest sequence now available, or `:closed` — a closed
  # stream will never reach the sequence anyone is still waiting for, so
  # every waiter on it is released rather than stranded. The consumer
  # re-subscribes at its checkpoint cursor and sees the closure.
  def update_dependencies_on_stream(state, stream_id, head) do
    case Map.fetch(state.stream_dependency_keys, stream_id) do
      {:ok, keys} ->
        keys
        |> Enum.filter(fn {:stream, _stream_id, sequence} ->
          head == :closed || head >= sequence
        end)
        |> Enum.reduce(state, &clear_dependency_key(&2, &1))

      :error ->
        state
    end
  end

  # Called when an input response is recorded. Resolves the {:input, id}
  # dependency for any executions that were waiting on this input.
  def update_dependencies_on_input(state, input_id) do
    clear_dependency_key(state, {:input, input_id})
  end

  # Drop one dependency key: forget its waiter set, and take the key out of
  # each waiter's pending set, scheduling any execution that has nothing
  # left to wait for.
  def clear_dependency_key(state, dependency_key) do
    case Map.fetch(state.dependency_waiters, dependency_key) do
      {:ok, waiters} ->
        state =
          state
          |> update_in(
            [Access.key(:dependency_waiters)],
            &Map.delete(&1, dependency_key)
          )
          |> unindex_stream_dependency(dependency_key)

        Enum.reduce(waiters, state, fn waiter_id, state ->
          remove_pending_dependency(state, waiter_id, dependency_key, MapSet.new())
        end)

      :error ->
        state
    end
  end

  @doc """
  Re-derives both ledgers from the database, discarding whatever was
  held. Used at boot and after an epoch rotation - neither ledger is
  authoritative, so rebuilding is always safe and is the only thing a
  rotation has to do about them.
  """
  def rebuild(state) do
    state
    |> Map.put(:pending_dependencies, %{})
    |> Map.put(:stream_dependency_keys, %{})
    |> Map.put(:dependency_waiters, %{})
    |> Map.put(:dependency_groups, %{})
    |> Map.put(:concurrency_gated, %{})
    |> Map.put(:concurrency_permits, %{})
    |> initialize_pending_dependencies()
    |> load_concurrency_permits()
  end
end
