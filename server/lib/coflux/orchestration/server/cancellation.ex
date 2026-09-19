defmodule Coflux.Orchestration.Server.Cancellation do
  @moduledoc """
  Stopping work that is still in flight.

  Cancelling an execution cancels everything it caused: the descendants
  are walked first, so nothing is left running with no parent to report
  to. Each one gets a cancelled result, and a worker currently running it
  is told to abort - the result is recorded either way, so an execution
  whose worker has already gone is still cancelled rather than left
  pending.

  A cancelled *handle* is narrower: a worker selecting over several things
  can give up on some of them. Only some kinds can be cancelled that way -
  an execution it spawned, or an input it raised - because only those have
  something to stop; a stream position or a catalog version is somebody
  else's to produce.
  """

  require Logger

  alias Coflux.Events.{InputResponded}

  alias Coflux.Orchestration.{Ids, Inputs, Results, Runs}

  alias Coflux.Orchestration.Server.{
    Archives,
    Commands,
    Dependencies,
    Effects,
    InputFlow,
    Lifecycle,
    State,
    Waiters
  }

  # Follow the spawned result chain to find the currently-active execution.
  # When execution A spawns B (which spawns C, etc.), we need to find the
  # execution that doesn't yet have a result.
  def resolve_active_execution(db, execution_id) do
    case Results.get_result(db, execution_id) do
      {:ok, {{:spawned, successor_id}, _created_at, _completion_created_at, _created_by}} ->
        resolve_active_execution(db, successor_id)

      _ ->
        execution_id
    end
  end

  # Cancel a single execution: record :cancelled, abort if assigned, cancel descendants.
  #
  # `streams: :step` (the default) closes every open stream of the step in
  # the workspace, so a consumer waiting on a paused stream is released when
  # the pending successor is cancelled. `streams: :registered` closes only
  # the streams the cancelled execution itself produced into — used by
  # re-run, where cancelling a never-started successor must leave the
  # paused stream for the new attempt to continue.
  def do_cancel_execution(state, execution_id, workspace_id, opts \\ []) do
    # Write the completion row (kind = cancelled) and fire notifications.
    # The result row is left untouched: if the worker already produced a
    # value, it stays; otherwise nothing is recorded. UI shows "cancelled"
    # via the completion kind, with any prior result visible in the
    # sidebar.
    state =
      case Lifecycle.record_and_notify_result(state, execution_id, :cancelled, nil, nil, opts) do
        {:ok, state} -> state
        {:error, :already_recorded} -> state
        {:error, :already_completed} -> state
      end

    # Close any open streams so iterating consumers stop waiting. Any
    # subsequent `append_item` from the producer will fail with `:closed`,
    # signalling the worker to stop. Recorded as :lifecycle — consumers
    # derive the ExecutionCancelled error from the recorded result.
    state =
      Lifecycle.close_open_streams(
        state,
        execution_id,
        :lifecycle,
        Keyword.get(opts, :streams, :step)
      )

    state =
      case Runs.get_execution_key(state.db, execution_id) do
        {:ok, {r, s, a}} ->
          abort_execution(state, Ids.execution(r, s, a))

        {:error, :not_found} ->
          state
      end

    cancel_descendants(state, execution_id, workspace_id)
  end

  # Only an execution or an input can be cancelled. A catalog entry is a
  # select handle with nothing pending behind it, and anything else is
  # malformed; a request naming either is refused before any handle in it
  # is acted on, since cancellation is meant to be all-or-nothing.
  def cancellable_handle?(%{"type" => type, "id" => id})
      when type in ["execution", "input"] and is_binary(id),
      do: true

  def cancellable_handle?(_handle), do: false

  # Dispatch a single handle cancellation. Used by the unified cancel RPC
  # and by maybe_cancel_remaining on select.
  def cancel_handle(state, %{"type" => "execution", "id" => ext_id}, workspace_id) do
    case Archives.resolve_internal_execution_id(state, ext_id) do
      {:ok, execution_id} ->
        active_id = resolve_active_execution(state.db, execution_id)
        do_cancel_execution(state, active_id, workspace_id)

      {:error, :not_found} ->
        state
    end
  end

  def cancel_handle(state, %{"type" => "input", "id" => ext_id}, _workspace_id) do
    do_cancel_input(state, ext_id)
  end

  # Mark an input as cancelled. Parallels dismiss_input but with a distinct
  # terminal state; notifies select waiters with :cancelled, resumes any
  # suspended executions, and notifies topic subscribers.
  def do_cancel_input(state, input_external_id) do
    case Archives.find_and_copy_input_from_archives(state, input_external_id) do
      {:ok, nil} ->
        state

      {:ok,
       {input_id, workspace_id, _key, _prompt_id, _schema_id, _title, _actions, _initial,
        _requires_tag_set_id, _created_at, _run_id}} ->
        now = System.system_time(:millisecond)

        case Inputs.record_input_response(
               state.db,
               input_id,
               Inputs.type_cancelled(),
               nil,
               now,
               nil
             ) do
          {:ok, true} ->
            state =
              Waiters.notify_select_waiters(state, {:input, input_external_id}, :cancelled)

            state = Dependencies.update_dependencies_on_input(state, input_id)

            {:ok, run_external_id, _input_number} =
              Ids.parse_input(input_external_id)

            ws_ext_id = State.workspace_external_id(state, workspace_id)

            response = InputFlow.build_input_response(state.db, input_id)

            state
            |> Effects.emit(%InputResponded{
              run: run_external_id,
              workspace: ws_ext_id,
              input: input_external_id,
              response: response
            })

          {:error, :already_responded} ->
            state
        end
    end
  end

  # Cancel all active (unresolved) executions for a step in a workspace.
  def cancel_active_step_executions(state, step_id, workspace_id, opts) do
    {:ok, active_execution_ids} =
      Runs.get_active_execution_ids_for_step(state.db, step_id, workspace_id)

    Enum.reduce(active_execution_ids, state, fn exec_id, state ->
      do_cancel_execution(state, exec_id, workspace_id, opts)
    end)
  end

  # Cancel all descendant executions of a given execution (excludes the
  # execution itself). Follows both step parent-child links and spawned
  # result chains via the recursive CTE in get_execution_descendants.
  def cancel_descendants(state, execution_id, workspace_id) do
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

  # Clean up an execution's state and send an abort message to the worker.
  # If the execution has already terminated (completion recorded), there's
  # nothing to abort - skip silently. Only warn when an actively-running
  # execution unexpectedly has no session.
  def abort_execution(state, execution_ext_id) do
    state = Waiters.cleanup_execution(state, execution_ext_id)

    case State.session_for_execution(state, execution_ext_id) do
      {:ok, session_id} ->
        Effects.command(state, session_id, Commands.abort(execution_ext_id))

      :error ->
        already_completed? =
          case Map.fetch(state.execution_ids, execution_ext_id) do
            {:ok, execution_id} ->
              case Results.has_completion?(state.db, execution_id) do
                {:ok, done?} -> done?
              end

            :error ->
              # No internal id mapped - execution is long gone (e.g. cache
              # rotation cleared the cache). Treat as terminated.
              true
          end

        unless already_completed? do
          Logger.warning("Couldn't locate session for execution #{execution_ext_id}. Ignoring.")
        end

        state
    end
  end
end
