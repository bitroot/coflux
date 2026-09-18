defmodule Coflux.RunView do
  @moduledoc """
  A run's structure, and a projection of it for the run topics.

  The structure is what's needed to decide what to show: steps, their
  attempts (with status), the child links from executions to steps, and
  groups. The detail of each execution (result, dependencies, assets, and
  so on) and of each step (arguments, streams) is held alongside, but only
  once loaded: a snapshot carries structure alone, and detail is fetched
  for what the projection shows (`missing_details/3`, `put_details/2`).
  The projection follows one rule: from the root
  step, expand one attempt per step (pinned, else the latest), show that
  attempt's ungrouped children and one member of each group (pinned, else
  the first), and summarise the rest of each group by branch status. A step
  only reachable through an attempt that isn't expanded isn't shown.

  Status roll-ups are kept incrementally so the cost of a notification is
  proportional to the depth of the tree rather than the size of the run:
  `branch` memoises each step's branch status, and `child_counts` and
  `group_counts` count the branch statuses of an execution's children so a
  step's status can be recomputed without walking them.

  `apply/2` returns the updated view with the *effects* of the notification:
  which steps' projected entries may have changed (`dirty`), which steps'
  branch status changed (`branches`), and the structural `events` (a new
  step, a new attempt, a new child link) that can change which steps are
  shown.
  """

  alias Coflux.RunView.Format

  @branch_priority [:running, :assigning, :errored, :aborted, :suspended]

  @empty_pins %{steps: %{}, groups: %{}}
  @empty_effects %{dirty: MapSet.new(), branches: MapSet.new(), events: []}

  defstruct run: nil,
            workspace_ids: [],
            initial: nil,
            root: :initial,
            pins: @empty_pins,
            steps: %{},
            executions: %{},
            attempts: %{},
            children: %{},
            child_index: %{},
            group_first: %{},
            parents: %{},
            branch: %{},
            child_counts: %{},
            group_counts: %{},
            input_status: %{},
            input_submissions: %{},
            input_dependents: %{}

  # ---------------------------------------------------------------------------
  # Building

  @doc """
  Builds a view from a run snapshot (the `steps` map `subscribe_run`
  returns). Executions outside `workspace_ids` are left out, as are child
  links from them; steps are kept whatever workspace their attempts are in,
  since a later attempt may be in a shown workspace. A snapshot may carry
  detail (a step's `arguments`, an execution's `result` and so on) or
  structure only; what's missing is reported by `missing_details/3`.
  """
  def new(run, steps, workspace_ids, opts \\ []) do
    view = %__MODULE__{
      run: run,
      workspace_ids: workspace_ids,
      root: Keyword.get(opts, :root, :initial),
      pins: Keyword.get(opts, :pins, @empty_pins)
    }

    view =
      Enum.reduce(steps, view, fn {number, step}, view ->
        put_step(view, number, snapshot_step(number, step))
      end)

    view =
      Enum.reduce(steps, view, fn {number, step}, view ->
        Enum.reduce(step.executions, view, fn {attempt, execution}, view ->
          execution = snapshot_execution(number, attempt, execution)

          if execution.workspace_id in workspace_ids,
            do: insert_execution(view, execution),
            else: view
        end)
      end)

    view =
      Enum.reduce(steps, view, fn {_number, step}, view ->
        Enum.reduce(step.executions, view, fn {_attempt, execution}, view ->
          if Map.has_key?(view.executions, execution.execution_id) do
            Enum.reduce(execution.children, view, fn {step, attempt, group_id}, view ->
              insert_link(view, execution.execution_id, step, attempt, group_id)
            end)
          else
            view
          end
        end)
      end)

    view
    |> Map.put(:initial, find_initial(view))
    |> init_branches()
    |> init_counts()
    |> index_inputs()
  end

  defp snapshot_step(number, step) do
    %{
      number: number,
      module: step.module,
      target: step.target,
      type: step.type,
      parent_id: step.parent_id,
      cache_config: step.cache_config,
      cache_key: step.cache_key,
      memo_key: step.memo_key,
      concurrency_key: step.concurrency_key,
      concurrency_limit: step.concurrency_limit,
      group_key: step.group_key,
      group_limit: step.group_limit,
      retries: snapshot_retries(step),
      recurrent: step.recurrent == 1 or step.recurrent == true,
      timeout: step.timeout,
      created_at: step.created_at,
      arguments: Map.get(step, :arguments),
      requires: step.requires,
      streams: snapshot_streams(Map.get(step, :streams))
    }
  end

  defp snapshot_streams(nil), do: nil

  defp snapshot_streams(streams) do
    Map.new(streams, fn {index, stream} ->
      {index,
       %{
         id: stream.id,
         index: stream.index,
         position: stream.position,
         workspace_id: stream.workspace_id,
         buffer: stream.buffer,
         timeout_ms: stream.timeout_ms,
         opened_at: stream.opened_at,
         attempts: stream.attempts,
         closed_at: stream.closed_at,
         closed_by: stream.closed_by,
         reason: if(stream.reason, do: Atom.to_string(stream.reason)),
         error: Format.stream_error(stream.error)
       }}
    end)
  end

  defp snapshot_retries(%{retry_limit: 0}), do: nil

  defp snapshot_retries(step) do
    %{
      limit: if(step.retry_limit == -1, do: nil, else: step.retry_limit),
      backoff_min: step.retry_backoff_min,
      backoff_max: step.retry_backoff_max
    }
  end

  defp snapshot_execution(step, attempt, execution) do
    base = %{
      id: execution.execution_id,
      step: step,
      attempt: attempt,
      workspace_id: execution.workspace_id,
      created_at: execution.created_at,
      created_by: execution.created_by,
      execute_after: execution.execute_after,
      assigned_at: execution.assigned_at,
      completed_at: execution.completed_at,
      completion: execution.completion,
      groups: execution.groups,
      loaded: false,
      assets: %{},
      dependencies: %{},
      pending: MapSet.new(),
      inputs: %{},
      result: nil,
      result_at: nil,
      result_created_by: nil,
      inner_result: nil,
      metrics: %{},
      checkpoints: %{before: %{}, after: %{}}
    }

    if Map.has_key?(execution, :result), do: apply_detail(base, execution), else: base
  end

  # Detail as `build_run_details` shapes it, replacing whatever the
  # execution held: the database is authoritative at the moment it was
  # read, and any notification still queued re-applies on top.
  defp apply_detail(execution, detail) do
    %{
      execution
      | loaded: true,
        assets: detail.assets,
        dependencies: detail.dependencies,
        pending: Map.get(detail, :pending_dependencies, MapSet.new()),
        inputs: Map.get(detail, :inputs, %{}),
        result: detail.result,
        result_at: detail.result_at,
        result_created_by: detail.result_created_by,
        inner_result: nil,
        metrics: detail.metric_definitions,
        checkpoints: detail.checkpoints
    }
  end

  defp find_initial(view) do
    view.steps
    |> Map.values()
    |> Enum.reject(& &1.parent_id)
    |> Enum.min_by(& &1.created_at, fn -> nil end)
    |> case do
      nil -> nil
      step -> step.number
    end
  end

  defp init_branches(view) do
    Enum.reduce(Map.keys(view.steps), view, fn step, view ->
      {view, _status} = ensure_branch(view, step, MapSet.new())
      view
    end)
  end

  # Memoised, bottom-up. A step reached again while it's being computed (a
  # cycle through memo links) contributes nothing to its own status.
  defp ensure_branch(view, step, visiting) do
    case Map.fetch(view.branch, step) do
      {:ok, status} ->
        {view, status}

      :error ->
        cond do
          MapSet.member?(visiting, step) ->
            {view, nil}

          true ->
            case latest_execution(view, step) do
              nil ->
                {view, nil}

              execution ->
                visiting = MapSet.put(visiting, step)

                {view, statuses} =
                  Enum.reduce(child_steps(view, execution.id), {view, []}, fn child,
                                                                              {view, statuses} ->
                    {view, status} = ensure_branch(view, child, visiting)
                    {view, [status | statuses]}
                  end)

                status = combine(own_status(execution), statuses)
                {%{view | branch: Map.put(view.branch, step, status)}, status}
            end
        end
    end
  end

  defp init_counts(view) do
    Enum.reduce(view.child_index, view, fn {execution_id, children}, view ->
      Enum.reduce(children, view, fn {step, group_id}, view ->
        move_count(view, execution_id, group_id, nil, Map.get(view.branch, step))
      end)
    end)
  end

  defp index_inputs(view) do
    Enum.reduce(view.executions, view, fn {_id, execution}, view ->
      index_execution_inputs(view, execution)
    end)
  end

  defp index_execution_inputs(view, execution) do
    view =
      Enum.reduce(execution.inputs, view, fn {input_id, input}, view ->
        view
        |> index_input(:input_submissions, input_id, execution.id)
        |> record_input_status(input_id, input.status)
      end)

    Enum.reduce(execution.dependencies, view, fn
      {input_id, {:input, _title, status}}, view ->
        view
        |> index_input(:input_dependents, input_id, execution.id)
        |> record_input_status(input_id, status)

      _other, view ->
        view
    end)
  end

  # ---------------------------------------------------------------------------
  # Detail, loaded for what's shown

  @doc """
  What `build_run_details` needs to be asked for before the given steps can
  be projected: each step whose arguments aren't loaded, and (unless
  `executions?` is false) each step's expanded execution if its detail
  isn't loaded. Shaped as the request `Orchestration.get_run_details` takes.
  """
  def missing_details(view, steps, executions? \\ true) do
    Enum.reduce(steps, %{executions: [], steps: []}, fn number, request ->
      request =
        case Map.get(view.steps, number) do
          %{arguments: nil} -> %{request | steps: [number | request.steps]}
          _ -> request
        end

      case executions? && expanded_execution(view, number) do
        %{loaded: false, attempt: attempt} ->
          %{request | executions: [{number, attempt} | request.executions]}

        _ ->
          request
      end
    end)
  end

  def empty_request?(%{executions: [], steps: []}), do: true
  def empty_request?(_request), do: false

  @doc "Merges a `get_run_details` reply into the view."
  def put_details(view, %{executions: executions, steps: steps}) do
    view =
      Enum.reduce(executions, view, fn {execution_id, detail}, view ->
        case Map.fetch(view.executions, execution_id) do
          {:ok, execution} ->
            execution = apply_detail(execution, detail)
            view |> store(execution) |> index_execution_inputs(execution)

          :error ->
            view
        end
      end)

    Enum.reduce(steps, view, fn {number, detail}, view ->
      case Map.fetch(view.steps, number) do
        {:ok, step} ->
          put_step(view, number, %{
            step
            | arguments: detail.arguments,
              streams: snapshot_streams(detail.streams)
          })

        :error ->
          view
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # Structure primitives (no effects, no roll-ups)

  defp put_step(view, number, step) do
    %{view | steps: Map.put(view.steps, number, step)}
  end

  defp insert_execution(view, execution) do
    %{
      view
      | executions: Map.put(view.executions, execution.id, execution),
        attempts:
          Map.update(
            view.attempts,
            execution.step,
            %{execution.attempt => execution.id},
            &Map.put(&1, execution.attempt, execution.id)
          )
    }
  end

  # A link is kept once per (parent execution, step): a parent that
  # re-submits a step it already links to (a memo hit on the same target)
  # doesn't get a second entry.
  defp insert_link(view, parent_id, step, attempt, group_id) do
    index = Map.get(view.child_index, parent_id, %{})

    if Map.has_key?(index, step) do
      view
    else
      link = %{step: step, attempt: attempt, group_id: group_id}

      group_first =
        if group_id,
          do: Map.put_new(view.group_first, {parent_id, group_id}, step),
          else: view.group_first

      %{
        view
        | children: Map.update(view.children, parent_id, [link], &(&1 ++ [link])),
          child_index: Map.put(view.child_index, parent_id, Map.put(index, step, group_id)),
          parents: Map.update(view.parents, step, [parent_id], &(&1 ++ [parent_id])),
          group_first: group_first
      }
    end
  end

  defp index_input(view, field, input_id, execution_id) do
    Map.update!(view, field, fn index ->
      Map.update(index, input_id, MapSet.new([execution_id]), &MapSet.put(&1, execution_id))
    end)
  end

  defp record_input_status(view, _input_id, nil), do: view

  defp record_input_status(view, input_id, status) do
    %{view | input_status: Map.put(view.input_status, input_id, status)}
  end

  # ---------------------------------------------------------------------------
  # Status

  defp own_status(%{completion: nil, assigned_at: nil}), do: :assigning
  defp own_status(%{completion: nil}), do: :running
  defp own_status(%{completion: %{kind: kind}}), do: completion_status(kind)

  defp completion_status(kind)
       when kind in ["succeeded", "recurred", "stream_errored", "stream_timeout"],
       do: :completed

  defp completion_status("errored"), do: :errored

  defp completion_status(kind) when kind in ["abandoned", "cancelled", "crashed", "timeout"],
    do: :aborted

  defp completion_status("suspended"), do: :suspended
  defp completion_status(kind) when kind in ["deferred", "cached", "spawned"], do: :deferred

  # The roll-up the graph shows for a branch: anything still going wins,
  # then anything that went wrong; otherwise it's done.
  defp combine(own, child_statuses) do
    Enum.find(@branch_priority, :completed, fn status ->
      status == own or status in child_statuses
    end)
  end

  defp compute_branch(view, step) do
    case latest_execution(view, step) do
      nil ->
        nil

      execution ->
        statuses =
          view.child_counts
          |> Map.get(execution.id, %{})
          |> Enum.filter(fn {_status, count} -> count > 0 end)
          |> Enum.map(&elem(&1, 0))

        combine(own_status(execution), statuses)
    end
  end

  defp move_count(view, parent_id, group_id, from, to) do
    view = %{view | child_counts: shift_in(view.child_counts, parent_id, from, to)}

    if group_id do
      %{view | group_counts: shift_in(view.group_counts, {parent_id, group_id}, from, to)}
    else
      view
    end
  end

  defp shift_in(counts, key, from, to) do
    Map.put(counts, key, shift(Map.get(counts, key, %{}), from, to))
  end

  defp shift(counts, from, to) do
    counts
    |> then(fn counts -> if from, do: Map.update(counts, from, 0, &(&1 - 1)), else: counts end)
    |> then(fn counts -> if to, do: Map.update(counts, to, 1, &(&1 + 1)), else: counts end)
  end

  # Recomputes a step's branch status and, if it changed, moves the counts
  # on every parent execution that links to it and continues upward from
  # each parent that is its step's latest attempt.
  defp recompute_branch(view, step, effects, visited) do
    new = compute_branch(view, step)
    old = Map.get(view.branch, step)

    if new == old do
      {view, effects}
    else
      view = %{view | branch: Map.put(view.branch, step, new)}
      effects = %{effects | branches: MapSet.put(effects.branches, step)}
      visited = MapSet.put(visited, step)

      Enum.reduce(Map.get(view.parents, step, []), {view, effects}, fn parent_id,
                                                                       {view, effects} ->
        group_id = view.child_index[parent_id][step]
        view = move_count(view, parent_id, group_id, old, new)
        parent = view.executions[parent_id]
        effects = dirty(effects, parent.step)

        if latest(view, parent.step) == parent.attempt and
             not MapSet.member?(visited, parent.step) do
          recompute_branch(view, parent.step, effects, visited)
        else
          {view, effects}
        end
      end)
    end
  end

  defp status_changed(view, execution_id, effects) do
    execution = view.executions[execution_id]
    effects = dirty(effects, execution.step)

    if latest(view, execution.step) == execution.attempt do
      recompute_branch(view, execution.step, effects, MapSet.new())
    else
      {view, effects}
    end
  end

  # ---------------------------------------------------------------------------
  # Notifications

  def apply_all(view, notifications) do
    {view, effects} =
      Enum.reduce(notifications, {view, @empty_effects}, fn notification, {view, effects} ->
        {view, more} = __MODULE__.apply(view, notification)
        {view, merge_effects(effects, more)}
      end)

    {view, %{effects | events: Enum.reverse(effects.events)}}
  end

  defp merge_effects(a, b) do
    %{
      dirty: MapSet.union(a.dirty, b.dirty),
      branches: MapSet.union(a.branches, b.branches),
      events: b.events ++ a.events
    }
  end

  defp dirty(effects, step), do: %{effects | dirty: MapSet.put(effects.dirty, step)}
  defp event(effects, event), do: %{effects | events: [event | effects.events]}

  def apply(view, {:step, number, step, _workspace_external_id}) do
    if Map.has_key?(view.steps, number) do
      {view, @empty_effects}
    else
      step = %{
        number: number,
        module: step.module,
        target: step.target,
        type: step.type,
        parent_id: step.parent_id,
        cache_config: step.cache_config,
        cache_key: step.cache_key,
        memo_key: step.memo_key,
        concurrency_key: step.concurrency_key,
        concurrency_limit: step.concurrency_limit,
        group_key: step.group_key,
        group_limit: step.group_limit,
        retries: step.retries,
        recurrent: step.recurrent == true,
        timeout: step.timeout,
        created_at: step.created_at,
        arguments: step.arguments,
        requires: step.requires,
        streams: %{}
      }

      {put_step(view, number, step), event(@empty_effects, {:step, number})}
    end
  end

  def apply(
        view,
        {:execution, step, attempt, execution_id, workspace_id, created_at, execute_after,
         dependencies, created_by, checkpoints, pending}
      ) do
    execution = %{
      id: execution_id,
      step: step,
      attempt: attempt,
      workspace_id: workspace_id,
      created_at: created_at,
      created_by: created_by,
      execute_after: execute_after,
      assigned_at: nil,
      result_at: nil,
      completed_at: nil,
      completion: nil,
      groups: %{},
      loaded: true,
      assets: %{},
      dependencies:
        Map.new(dependencies, fn {id, dependency} -> {id, tag_result(dependency)} end),
      pending: pending,
      inputs: %{},
      result: nil,
      result_created_by: nil,
      inner_result: nil,
      metrics: %{},
      # Nothing has run yet, so what the execution will start from is also
      # what it currently holds.
      checkpoints: %{before: checkpoints, after: checkpoints}
    }

    cond do
      execution.workspace_id not in view.workspace_ids ->
        {view, @empty_effects}

      not Map.has_key?(view.steps, step) ->
        {view, @empty_effects}

      Map.has_key?(view.executions, execution_id) ->
        {view, @empty_effects}

      true ->
        # Reported whether or not the step had an attempt before: a first
        # attempt in a shown workspace can bring an already-linked step
        # into view, a later one can change which subtree is shown.
        view = insert_execution(view, execution)
        effects = @empty_effects |> dirty(step) |> event({:attempt, step})

        if latest(view, step) == attempt do
          recompute_branch(view, step, effects, MapSet.new())
        else
          {view, effects}
        end
    end
  end

  def apply(view, {:child, parent_id, {step, attempt, group_id}}) do
    cond do
      not Map.has_key?(view.executions, parent_id) ->
        {view, @empty_effects}

      not Map.has_key?(view.steps, step) ->
        {view, @empty_effects}

      Map.has_key?(Map.get(view.child_index, parent_id, %{}), step) ->
        {view, @empty_effects}

      true ->
        view = insert_link(view, parent_id, step, attempt, group_id)
        parent = view.executions[parent_id]

        effects =
          @empty_effects
          |> dirty(parent.step)
          |> event({:link, parent_id, step, group_id})

        case Map.get(view.branch, step) do
          nil ->
            {view, effects}

          status ->
            view = move_count(view, parent_id, group_id, nil, status)

            if latest(view, parent.step) == parent.attempt do
              recompute_branch(view, parent.step, effects, MapSet.new())
            else
              {view, effects}
            end
        end
    end
  end

  def apply(view, {:assigned, assigned}) do
    Enum.reduce(assigned, {view, @empty_effects}, fn {execution_id, assigned_at},
                                                     {view, effects} ->
      case Map.fetch(view.executions, execution_id) do
        {:ok, execution} ->
          view = store(view, %{execution | assigned_at: assigned_at})
          {view, more} = status_changed(view, execution_id, @empty_effects)
          {view, merge_effects(effects, more)}

        :error ->
          {view, effects}
      end
    end)
  end

  def apply(view, {:completion, execution_id, kind, successor, completed_at}) do
    case Map.fetch(view.executions, execution_id) do
      {:ok, execution} ->
        # Nothing is outstanding once the execution has finished, whatever
        # state its dependencies are in - the rule the snapshot applies too.
        view =
          store(view, %{
            execution
            | completed_at: completed_at,
              completion: %{kind: Atom.to_string(kind), successor: successor},
              pending: MapSet.new()
          })

        status_changed(view, execution_id, @empty_effects)

      :error ->
        {view, @empty_effects}
    end
  end

  def apply(view, {:result, execution_id, result, result_at, created_by}) do
    update_execution(view, execution_id, fn execution ->
      %{execution | result: result, result_at: result_at, result_created_by: created_by}
    end)
  end

  # The result of a deferred/cached/spawned execution's target, shown nested
  # inside the redirecting result.
  def apply(view, {:result_result, execution_id, result, _created_at, created_by}) do
    update_execution(view, execution_id, fn execution ->
      %{execution | inner_result: {result, created_by}}
    end)
  end

  def apply(view, {:metric_defined, execution_id, key, definition}) do
    update_execution(view, execution_id, fn execution ->
      metric = %{
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

      %{execution | metrics: Map.put(execution.metrics, key, metric)}
    end)
  end

  def apply(view, {:group, execution_id, group_id, name, concurrency}) do
    update_execution(view, execution_id, fn execution ->
      %{
        execution
        | groups: Map.put(execution.groups, group_id, %{name: name, concurrency: concurrency})
      }
    end)
  end

  def apply(view, {:asset, execution_id, asset_id, asset}) do
    update_execution(view, execution_id, fn execution ->
      %{execution | assets: Map.put(execution.assets, asset_id, asset)}
    end)
  end

  # Which dependencies the execution is still waiting on. Sent as the whole
  # set rather than a delta: a result that redirects (to a suspend's
  # successor, say) clears a dependency keyed by the execution originally
  # referenced, so there's no dependable one-to-one between what resolved
  # and which entry it releases.
  def apply(view, {:pending_dependencies, execution_id, pending}) do
    update_execution(view, execution_id, fn execution -> %{execution | pending: pending} end)
  end

  def apply(view, {:result_dependency, execution_id, dependency_id, dependency, pending}) do
    update_execution(view, execution_id, fn execution ->
      execution
      |> put_dependency(dependency_id, tag_result(dependency))
      |> set_pending(dependency_id, pending)
    end)
  end

  def apply(view, {:stream_dependency, execution_id, stream_id, module, target, pending}) do
    update_execution(view, execution_id, fn execution ->
      execution
      |> put_dependency(stream_id, {:stream, stream_id, module, target})
      |> set_pending(stream_id, pending)
    end)
  end

  def apply(view, {:asset_dependency, execution_id, asset_id, asset, pending}) do
    update_execution(view, execution_id, fn execution ->
      execution
      |> put_dependency(asset_id, {:asset, asset})
      |> set_pending(asset_id, pending)
    end)
  end

  def apply(view, {:input_dependency, execution_id, input_id, title, response_type, pending}) do
    {view, effects} =
      update_execution(view, execution_id, fn execution ->
        execution
        |> put_dependency(input_id, {:input, title, response_type})
        |> set_pending(input_id, pending)
      end)

    view =
      if Map.has_key?(view.executions, execution_id) do
        view
        |> index_input(:input_dependents, input_id, execution_id)
        |> record_input_status(input_id, response_type)
      else
        view
      end

    {view, effects}
  end

  def apply(view, {:input_submitted, execution_id, input_id, title}) do
    status = Map.get(view.input_status, input_id)

    {view, effects} =
      update_execution(view, execution_id, fn execution ->
        %{
          execution
          | inputs: Map.put(execution.inputs, input_id, %{title: title, status: status})
        }
      end)

    view =
      if Map.has_key?(view.executions, execution_id),
        do: index_input(view, :input_submissions, input_id, execution_id),
        else: view

    {view, effects}
  end

  def apply(view, {:input_response, input_id, response_type}) do
    view = record_input_status(view, input_id, response_type)

    {view, effects} =
      view.input_submissions
      |> Map.get(input_id, MapSet.new())
      |> Enum.reduce({view, @empty_effects}, fn execution_id, {view, effects} ->
        {view, more} =
          update_execution(view, execution_id, fn execution ->
            %{
              execution
              | inputs:
                  Map.update!(execution.inputs, input_id, &Map.put(&1, :status, response_type))
            }
          end)

        {view, merge_effects(effects, more)}
      end)

    view.input_dependents
    |> Map.get(input_id, MapSet.new())
    |> Enum.reduce({view, effects}, fn execution_id, {view, effects} ->
      {view, more} =
        update_execution(view, execution_id, fn execution ->
          case Map.fetch(execution.dependencies, input_id) do
            {:ok, {:input, title, _status}} ->
              put_dependency(execution, input_id, {:input, title, response_type})

            _ ->
              execution
          end
        end)

      {view, merge_effects(effects, more)}
    end)
  end

  def apply(view, {:checkpoints, execution_id, checkpoints}) do
    # Only the "after" side moves — what the execution started from was
    # fixed when it was created.
    update_execution(view, execution_id, fn execution ->
      %{execution | checkpoints: %{execution.checkpoints | after: checkpoints}}
    end)
  end

  # An execution registered on one of the step's streams — opening it, or
  # resuming it after a suspend (`continued`).
  def apply(view, {:stream_registered, step, index, info}) do
    cond do
      info.workspace_id not in view.workspace_ids ->
        {view, @empty_effects}

      not Map.has_key?(view.steps, step) ->
        {view, @empty_effects}

      true ->
        update_stream(view, step, index, fn
          nil ->
            %{
              id: info.id,
              index: index,
              position: info.position,
              workspace_id: info.workspace_id,
              buffer: info.buffer,
              timeout_ms: info.timeout_ms,
              opened_at: info.opened_at,
              attempts: [info.attempt],
              closed_at: nil,
              closed_by: nil,
              reason: nil,
              error: nil
            }

          stream ->
            attempts =
              if info.attempt in stream.attempts,
                do: stream.attempts,
                else: stream.attempts ++ [info.attempt]

            %{stream | attempts: attempts, buffer: info.buffer, timeout_ms: info.timeout_ms}
        end)
    end
  end

  def apply(view, {:stream_closed, step, index, reason, error, attempt, closed_at}) do
    if get_in(view.steps, [step, :streams]) && get_in(view.steps, [step, :streams, index]) do
      update_stream(view, step, index, fn stream ->
        %{stream | closed_at: closed_at, closed_by: attempt, reason: reason, error: error}
      end)
    else
      {view, @empty_effects}
    end
  end

  defp update_execution(view, execution_id, fun) do
    case Map.fetch(view.executions, execution_id) do
      {:ok, execution} ->
        {store(view, fun.(execution)), dirty(@empty_effects, execution.step)}

      :error ->
        {view, @empty_effects}
    end
  end

  defp update_stream(view, step, index, fun) do
    view =
      update_in(view.steps[step].streams, fn streams ->
        streams = streams || %{}
        Map.put(streams, index, fun.(Map.get(streams, index)))
      end)

    {view, dirty(@empty_effects, step)}
  end

  defp store(view, execution) do
    %{view | executions: Map.put(view.executions, execution.id, execution)}
  end

  defp put_dependency(execution, id, dependency) do
    %{execution | dependencies: Map.put(execution.dependencies, id, dependency)}
  end

  defp set_pending(execution, id, true),
    do: %{execution | pending: MapSet.put(execution.pending, id)}

  defp set_pending(execution, id, _),
    do: %{execution | pending: MapSet.delete(execution.pending, id)}

  # Result dependencies arrive both tagged (from the snapshot, and argument
  # dependencies) and as a bare execution reference (from notifications).
  defp tag_result({:result, _execution} = dependency), do: dependency
  defp tag_result({_ext_id, _module, _target} = execution), do: {:result, execution}

  # ---------------------------------------------------------------------------
  # Reading the structure

  def latest(view, step) do
    case Map.get(view.attempts, step) do
      nil -> nil
      attempts when map_size(attempts) == 0 -> nil
      attempts -> attempts |> Map.keys() |> Enum.max()
    end
  end

  defp latest_execution(view, step) do
    case latest(view, step) do
      nil -> nil
      attempt -> view.executions[view.attempts[step][attempt]]
    end
  end

  @doc "The attempt of `step` that's expanded: the pinned one, else the latest."
  def expanded(view, step) do
    case Map.get(view.pins.steps, step) do
      nil -> latest(view, step)
      attempt -> if Map.has_key?(Map.get(view.attempts, step, %{}), attempt), do: attempt
    end
  end

  def expanded_execution(view, step) do
    case expanded(view, step) do
      nil -> nil
      attempt -> view.executions[view.attempts[step][attempt]]
    end
  end

  def branch_status(view, step), do: Map.get(view.branch, step)

  defp child_steps(view, execution_id) do
    view.child_index |> Map.get(execution_id, %{}) |> Map.keys()
  end

  @doc "The step shown for a group: the pinned member, else the first."
  def chosen_member(view, execution_id, group_id) do
    Map.get(view.pins.groups, {execution_id, group_id}) ||
      Map.get(view.group_first, {execution_id, group_id})
  end

  @doc "The child links of an execution that the projection shows."
  def visible_children(view, execution_id) do
    view.children
    |> Map.get(execution_id, [])
    |> Enum.filter(fn
      %{group_id: nil} -> true
      %{group_id: group_id, step: step} -> chosen_member(view, execution_id, group_id) == step
    end)
  end

  @doc "Whether a link from `parent_id` to `step` is one the projection shows."
  def link_visible?(view, parent_id, step, group_id) do
    case Map.fetch(view.executions, parent_id) do
      {:ok, parent} ->
        expanded(view, parent.step) == parent.attempt and
          (is_nil(group_id) or chosen_member(view, parent_id, group_id) == step)

      :error ->
        false
    end
  end

  def root_step(view) do
    case view.root do
      :initial -> view.initial
      :none -> nil
      {:step, step} -> step
    end
  end

  @doc "The steps the projection shows, from the root."
  def visible_steps(view) do
    case root_step(view) do
      nil -> MapSet.new()
      step -> reachable(view, step, MapSet.new())
    end
  end

  @doc """
  The steps reached from `step` through expanded attempts and shown
  children, not counting those in `seen` (or anything only reachable through
  them).
  """
  def reachable(view, step, seen) do
    MapSet.difference(traverse(view, step, seen), seen)
  end

  defp traverse(view, step, seen) do
    cond do
      MapSet.member?(seen, step) ->
        seen

      not Map.has_key?(view.steps, step) ->
        seen

      true ->
        case expanded_execution(view, step) do
          nil ->
            seen

          execution ->
            seen = MapSet.put(seen, step)

            Enum.reduce(visible_children(view, execution.id), seen, fn link, seen ->
              traverse(view, link.step, seen)
            end)
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Selection

  @doc """
  What pinning `execution_id` changes about the default view. Walks up from
  the execution (through the most recently created parent at each hop, as
  the graph does) and finds the topmost place the pinned path leaves the
  default: a pinned attempt that isn't the step's latest, or a pinned group
  member that isn't the group's first.

  Returns `nil` for an unknown execution. Otherwise a map with `root`
  (`{:step, number}` at which the view diverges, or `:none` when the
  execution is in the default view already), `parent` (the hop from the
  default view into the root, if any), `pins`, and `path` (the steps on the
  pinned path, for deciding whether a change touches it).
  """
  def selection(view, execution_id) do
    case Map.fetch(view.executions, execution_id) do
      :error ->
        nil

      {:ok, execution} ->
        hops = climb(view, execution.step, [], MapSet.new([execution.step]))

        pins =
          Enum.reduce(hops, %{steps: %{execution.step => execution.attempt}, groups: %{}}, fn
            {parent_id, group_id, step}, pins ->
              parent = view.executions[parent_id]
              steps = Map.put(pins.steps, parent.step, parent.attempt)

              groups =
                if group_id,
                  do: Map.put(pins.groups, {parent_id, group_id}, step),
                  else: pins.groups

              %{steps: steps, groups: groups}
          end)

        {root, parent} = divergence(view, hops, execution)

        %{
          root: root,
          parent: parent,
          pins: pins,
          path: MapSet.new(Map.keys(pins.steps))
        }
    end
  end

  # Hops from the top of the run down to `step`: `{parent execution, group,
  # child step}`. Stops at a step with no shown parent, or one already on
  # the path (a cycle through memo links).
  defp climb(view, step, hops, seen) do
    case most_recent_parent(view, step) do
      nil ->
        hops

      parent_id ->
        parent = view.executions[parent_id]

        if MapSet.member?(seen, parent.step) do
          hops
        else
          group_id = view.child_index[parent_id][step]

          climb(
            view,
            parent.step,
            [{parent_id, group_id, step} | hops],
            MapSet.put(seen, parent.step)
          )
        end
    end
  end

  defp most_recent_parent(view, step) do
    view.parents
    |> Map.get(step, [])
    |> Enum.max_by(fn parent_id -> view.executions[parent_id].created_at end, fn -> nil end)
  end

  defp divergence(view, hops, execution) do
    result =
      Enum.reduce_while(hops, nil, fn {parent_id, group_id, step}, above ->
        parent = view.executions[parent_id]

        cond do
          latest(view, parent.step) != parent.attempt ->
            {:halt, {:diverged, parent.step, above}}

          group_id != nil and Map.get(view.group_first, {parent_id, group_id}) != step ->
            {:halt, {:diverged, step, %{execution_id: parent_id, group_id: group_id}}}

          true ->
            {:cont, %{execution_id: parent_id, group_id: group_id}}
        end
      end)

    case result do
      {:diverged, step, parent} ->
        {{:step, step}, parent}

      above ->
        if latest(view, execution.step) != execution.attempt,
          do: {{:step, execution.step}, above},
          else: {:none, nil}
    end
  end

  def with_selection(view, selection) do
    %{view | root: selection.root, pins: selection.pins}
  end

  @doc "Whether any of the structural events could change a selection's path."
  def touches?(view, events, path) do
    Enum.any?(events, fn
      {:attempt, step} ->
        MapSet.member?(path, step)

      {:link, parent_id, step, _group_id} ->
        MapSet.member?(path, step) or MapSet.member?(path, view.executions[parent_id].step)

      _ ->
        false
    end)
  end

  # ---------------------------------------------------------------------------
  # Projection

  def step_key(view, step), do: Format.step_key(view.run.external_id, step)

  @doc "The projected `steps` map: every visible step."
  def project(view) do
    view
    |> visible_steps()
    |> Map.new(fn step -> {step_key(view, step), project_step(view, step)} end)
  end

  def project_step(view, number) do
    step = view.steps[number]
    attempts = Map.get(view.attempts, number, %{})

    executions =
      case expanded(view, number) do
        nil ->
          %{}

        attempt ->
          %{
            Integer.to_string(attempt) =>
              project_execution(view, view.executions[attempts[attempt]])
          }
      end

    %{
      stepNumber: number,
      module: step.module,
      target: step.target,
      type: step.type,
      parentId: step.parent_id,
      cacheConfig: Format.cache_config(step.cache_config),
      cacheKey: Format.key(step.cache_key),
      memoKey: Format.key(step.memo_key),
      concurrency: Format.concurrency(step),
      group: Format.group(step),
      retries: Format.retries(step.retries),
      recurrent: step.recurrent,
      timeout: step.timeout,
      createdAt: step.created_at,
      arguments: Enum.map(step.arguments || [], &Format.value/1),
      requires: step.requires,
      streams: Format.streams(step.streams || %{}, view.workspace_ids),
      attempts:
        Map.new(attempts, fn {attempt, execution_id} ->
          {Integer.to_string(attempt), attempt_summary(view.executions[execution_id])}
        end),
      executions: executions
    }
  end

  defp attempt_summary(execution) do
    %{
      executionId: execution.id,
      workspaceId: execution.workspace_id,
      createdAt: execution.created_at,
      executeAfter: execution.execute_after,
      assignedAt: execution.assigned_at,
      completedAt: execution.completed_at,
      completion: execution.completion
    }
  end

  defp project_execution(view, execution) do
    result = Format.result(execution.result, execution.result_created_by)

    result =
      case {result, execution.inner_result} do
        {%{result: _}, {inner, created_by}} ->
          %{result | result: Format.result(inner, created_by)}

        _ ->
          result
      end

    %{
      executionId: execution.id,
      workspaceId: execution.workspace_id,
      createdAt: execution.created_at,
      createdBy: Format.principal(execution.created_by),
      executeAfter: execution.execute_after,
      assignedAt: execution.assigned_at,
      resultAt: execution.result_at,
      completedAt: execution.completed_at,
      completion: execution.completion,
      groups:
        Map.new(execution.groups, fn {group_id, group} ->
          {Integer.to_string(group_id),
           %{
             name: group.name,
             concurrency: group.concurrency,
             members: Format.members(Map.get(view.group_counts, {execution.id, group_id}, %{}))
           }}
        end),
      assets:
        Map.new(execution.assets, fn {asset_id, asset} -> {asset_id, Format.asset(asset)} end),
      dependencies: Format.dependencies(execution.dependencies, execution.pending),
      children:
        view
        |> visible_children(execution.id)
        |> Enum.map(&Format.child(&1, view.run.external_id)),
      inputs: execution.inputs,
      result: result,
      metrics: Map.new(execution.metrics, fn {key, metric} -> {key, Format.metric(metric)} end),
      checkpoints: Format.checkpoints(execution.checkpoints.before, execution.checkpoints.after)
    }
  end

  @doc """
  Every step with an attempt in a shown workspace, as structure only: the
  step's identity, and each attempt summarised with its complete list of
  children. Nothing is collapsed and nothing needs detail, so it's what the
  whole-run pages (timeline, logs) work from.
  """
  def project_structure(view) do
    Map.new(view.attempts, fn {number, _attempts} ->
      {step_key(view, number), project_structure_step(view, number)}
    end)
  end

  def project_structure_step(view, number) do
    step = view.steps[number]

    %{
      stepNumber: number,
      module: step.module,
      target: step.target,
      type: step.type,
      parentId: step.parent_id,
      createdAt: step.created_at,
      attempts:
        Map.new(Map.get(view.attempts, number, %{}), fn {attempt, execution_id} ->
          execution = view.executions[execution_id]

          children =
            view.children
            |> Map.get(execution_id, [])
            |> Enum.map(&Format.child(&1, view.run.external_id))

          {Integer.to_string(attempt), Map.put(attempt_summary(execution), :children, children)}
        end)
    }
  end

  @doc "The members of a group, in the order they were linked, with branch status."
  def group_members(view, execution_id, group_id) do
    view.children
    |> Map.get(execution_id, [])
    |> Enum.filter(&(&1.group_id == group_id))
    |> Enum.map(&group_member(view, &1))
  end

  # A member is listed with its latest attempt (what selecting it opens)
  # rather than the attempt it was first linked as, and with its arguments,
  # which is how the popover labels it.
  def group_member(view, link) do
    step = view.steps[link.step]

    %{
      stepId: step_key(view, link.step),
      attempt: latest(view, link.step),
      module: step.module,
      target: step.target,
      arguments: Enum.map(step.arguments || [], &Format.value/1),
      status: member_status(view, link.step),
      createdAt: step.created_at
    }
  end

  def member_status(view, step) do
    case Map.get(view.branch, step) do
      nil -> nil
      status -> Format.branch_status(status)
    end
  end
end
