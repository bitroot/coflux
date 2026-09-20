defmodule Coflux.Orchestration.Server.Lifecycle do
  @moduledoc """
  How an execution ends, and everything that follows from it.

  An ending is recorded in two steps, and the gap between them is
  deliberate. The *result* says what the execution produced; the
  *completion* says the lifecycle is over and, if anything takes over, what
  does. They are separate because an execution can have produced a value
  while its streams are still draining - running, for the queue's purposes,
  with a result already recorded - and because whether an error stands is
  not known until the retry decision is made, which happens on the
  completion.

  Most endings write both at once. The ones that do not are those where a
  worker is still involved: the worker reports the result, and the
  completion is written when it reports termination.

  An ending may hand off. A retry, a deferral, a cache hit, a spawn, a
  suspension and a recurrence all name a successor, and the original's
  result resolves to whatever the successor produced. That is why a result
  is not final until nothing is taking over from it, and why a run's
  outcome is recomputed rather than read off its initial execution.

  Every completion funnels through `fire_completion_notification/3`, which
  is why the concurrency permit is released there: suspension, retry
  backoff, recurrence, cancellation and abandonment all write one, so all
  of them release.
  """

  require Logger

  alias Coflux.Events.{CompletionRecorded, InputDeactivated, ResultRecorded, RunOutcome}

  alias Coflux.Orchestration.{Errors, Ids, Inputs, Principals, Results, Runs, Streams, Values}

  alias Coflux.Orchestration.Server.{
    Cancellation,
    CatalogFlow,
    Dependencies,
    Effects,
    Identity,
    Resolve,
    Scheduling,
    State,
    StreamDelivery,
    Waiters
  }

  def result_retryable?(result) do
    case result do
      {:error, _, _, _, false} -> false
      {:error, _, _, _, _} -> true
      :abandoned -> true
      :crashed -> true
      :timeout -> true
      _ -> false
    end
  end

  def record_and_notify_result(
        state,
        execution_id,
        result,
        _module,
        created_by \\ nil,
        opts \\ []
      ) do
    result =
      case result do
        {:value, value} -> {:value, Values.normalize(value)}
        other -> other
      end

    case Results.record_result(state.db, execution_id, result, created_by) do
      {:ok, timestamp} ->
        state = fire_result_notifications(state, execution_id, result, timestamp, created_by)

        # For result shapes that write the completion synchronously
        # (cancelled / abandoned / timeout / deferred / cached / spawned /
        # suspended / recurred), fire the completion-time notifications
        # now — queue removal, run-topic `completion` update, waiter
        # wake-ups. For value/error the completion is written later via
        # complete_execution, which fires these itself.
        #
        # Terminal completions imply streams closed: any streams this
        # execution left open are closed here (after the completion row is
        # written, so derive_lifecycle_info resolves the real reason for
        # live subscribers) — otherwise abandoned/timed-out producers leave
        # consumers blocked forever waiting for a close that never comes.
        state =
          if writes_completion_immediately?(result) do
            state
            |> maybe_close_open_streams(result, execution_id, Keyword.get(opts, :streams, :step))
            |> fire_completion_notification(execution_id, timestamp)
          else
            state
          end

        {:ok, state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Which of the immediately-completing shapes should close the step's
  # open streams. Deliberately the same set that derive_lifecycle_info
  # can name — closing a stream whose reason we can't derive would push a
  # nil reason, which consumers coerce to a clean "complete" and silently
  # accept as a truncated stream.
  #
  # :deferred / :cached / :spawned never reach here with open streams
  # (the execution was superseded before its body ran, so it appended
  # nothing).
  #
  # :suspended is deliberately *not* included: a suspend pauses the
  # step's streams, and the execution that resumes the step continues
  # them, so consumers see one unbroken sequence. :recurred *is*
  # included — a recurrent iteration finishing is a completion, and the
  # next iteration opens its own streams.
  def maybe_close_open_streams(state, result, execution_id, scope) do
    if closes_streams_on_completion?(result) do
      close_open_streams(state, execution_id, :lifecycle, scope)
    else
      state
    end
  end

  def closes_streams_on_completion?(:cancelled), do: true
  def closes_streams_on_completion?({:abandoned, _}), do: true
  def closes_streams_on_completion?({:crashed, _}), do: true
  def closes_streams_on_completion?({:timeout, _}), do: true
  def closes_streams_on_completion?({:recurred, _}), do: true
  def closes_streams_on_completion?(_), do: false

  def writes_completion_immediately?(:cancelled), do: true
  def writes_completion_immediately?({:abandoned, _}), do: true
  def writes_completion_immediately?({:crashed, _}), do: true
  def writes_completion_immediately?({:timeout, _}), do: true
  def writes_completion_immediately?({:suspended, _}), do: true
  def writes_completion_immediately?({:recurred, _}), do: true
  def writes_completion_immediately?({:deferred, _}), do: true
  def writes_completion_immediately?({:deferred, _, _}), do: true
  def writes_completion_immediately?({:cached, _}), do: true
  def writes_completion_immediately?({:cached, _, _}), do: true
  def writes_completion_immediately?({:spawned, _}), do: true
  def writes_completion_immediately?({:spawned, _, _}), do: true
  def writes_completion_immediately?(_), do: false

  def fire_result_notifications(state, execution_id, result, result_at, created_by) do
    {:ok, {r, s, a}} = Runs.get_execution_key(state.db, execution_id)
    execution_external_id = Ids.execution(r, s, a)

    state =
      state
      |> Waiters.notify_waiting(execution_id)
      |> Dependencies.update_dependencies_on_result(execution_id)
      |> Dependencies.unregister_pending_dependencies(execution_id)

    # Cancellation after a value result was already recorded: the value
    # stays authoritative for consumer resolution (a consumer that saw
    # the value before the cancel must keep seeing it). The run-topic
    # `:result` notification was already fired at value-record time, so
    # re-firing it with `:cancelled` would clobber the value in the UI
    # and in any dependent executions' nested result state. The
    # `:completion` notification carries the cancelled status via its
    # kind field, which is what the UI reads for the badge.
    skip_topic_notifications =
      result == :cancelled and has_value_result?(state.db, execution_id)

    state =
      if skip_topic_notifications do
        state
      else
        final = Resolve.final_result?(result)
        built_result = Resolve.result(state.db, result)

        principal =
          case Principals.get_principal(state.db, created_by) do
            {:ok, {type, external_id}} -> %{type: type, external_id: external_id}
            {:ok, nil} -> nil
          end

        Effects.emit(state, %ResultRecorded{
          run: r,
          execution: execution_external_id,
          result: built_result,
          result_at: result_at,
          created_by: principal,
          final: final
        })
      end

    # TODO: only if there's an execution waiting for this result?
    send(self(), :tick)

    state
  end

  def has_value_result?(db, execution_id) do
    case Results.get_result_payload(db, execution_id) do
      {:ok, {:value, _}} -> true
      _ -> false
    end
  end

  # Notify input-topic subscribers when any input this execution depended on
  # has now become inactive. `has_active_dependency?` keys off the completion
  # row, so this must run at completion time rather than result time —
  # calling it any earlier would always see "still active".
  def notify_input_deactivations(state, execution_id) do
    case Inputs.get_input_dependencies_for_execution(state.db, execution_id) do
      {:ok, deps} ->
        Enum.reduce(deps, state, fn {input_id, input_ws_id}, state ->
          if Inputs.has_active_dependency?(state.db, input_id) do
            state
          else
            {:ok, run_ext_id, input_number} =
              Inputs.get_input_run_and_number(state.db, input_id)

            input_ext_id = Ids.input(run_ext_id, input_number)
            # Route :inputs topic notification to the INPUT's workspace
            # (matching :input_dependency_active in the resolve_input
            # handler) — these differ when an execution in a child
            # workspace resolved an input created in a parent.
            input_ws_ext_id = State.workspace_external_id(state, input_ws_id)

            Effects.emit(state, %InputDeactivated{workspace: input_ws_ext_id, input: input_ext_id})
          end
        end)

      _ ->
        state
    end
  end

  # Write the completion row and fire completion-time notifications. Called
  # from notify_terminated (or the abandonment/crash paths). Decides any
  # retry/successor from the persisted result row at this point rather than
  # carrying a decision forward from result-record time — so the decision
  # survives server restarts and epoch rotation.
  def complete_execution(state, execution_id) do
    case Results.has_completion?(state.db, execution_id) do
      {:ok, true} ->
        state

      {:ok, false} ->
        case Results.get_result_payload(state.db, execution_id) do
          {:ok, {:value, _}} ->
            finalize_success_completion(state, execution_id)

          {:ok, {:error, type, message, frames, retryable}} ->
            finalize_error_completion(
              state,
              execution_id,
              {type, message, frames, retryable}
            )

          {:ok, nil} ->
            handle_crashed(state, execution_id)
        end
    end
  end

  # Value result + drain: dispatch on stream closure outcomes.
  #   * any stream this execution closed `:errored` → `:stream_errored`
  #     (retried)
  #   * else any stream it closed `:timeout` → `:stream_timeout` (not
  #     retried, not cacheable)
  #   * else `:succeeded`
  # `close_open_streams` runs first so any of the step's streams still
  # open get a `:complete` row — the step finished, so they're done. That
  # doesn't influence the dispatch; only the explicit `:errored` /
  # `:timeout` reasons do.
  def finalize_success_completion(state, execution_id) do
    # The adapter exits only once every stream it produces has closed, and
    # its stream_close messages precede notify_terminated on the wire. So a
    # stream this execution registered that is still open now means the
    # process died mid-drain: a crash with a truncated stream, not a
    # success with leftovers. (Paused streams from an earlier attempt are
    # not this execution's registrations, and are closed below as usual.)
    {:ok, still_producing} = Streams.get_open_stream_ids_for_execution(state.db, execution_id)

    if still_producing == [] do
      finalize_drained_completion(state, execution_id)
    else
      finalize_crashed_mid_drain(state, execution_id)
    end
  end

  # Like handle_crashed, but the value result was already recorded and
  # notified — only the completion (with the step's retry decision) and the
  # stream closures are outstanding. Closing after the completion row is
  # written lets derive_lifecycle_info report :crashed to consumers.
  def finalize_crashed_mid_drain(state, execution_id) do
    {:ok, step} = Runs.get_step_for_execution(state.db, execution_id)
    {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)

    {retry_id, _recurred?, state} =
      decide_and_create_successor(state, execution_id, step, workspace_id, :crashed)

    case Results.record_completion(state.db, execution_id, :crashed, successor_id: retry_id) do
      {:ok, completion_at} ->
        state = close_open_streams(state, execution_id)
        fire_completion_notification(state, execution_id, completion_at)

      {:error, :already_completed} ->
        state
    end
  end

  def finalize_drained_completion(state, execution_id) do
    state = close_open_streams(state, execution_id, :complete)

    {:ok, summary} = Streams.get_closure_summary_for_execution(state.db, execution_id)

    cond do
      not is_nil(summary.errored) ->
        finalize_stream_errored_completion(state, execution_id, summary.errored)

      summary.timed_out ->
        finalize_stream_timeout_completion(state, execution_id)

      true ->
        case Results.record_completion(state.db, execution_id, :succeeded) do
          {:ok, completion_at} ->
            fire_completion_notification(state, execution_id, completion_at)

          {:error, :already_completed} ->
            state
        end
    end
  end

  # A stream owned by this execution closed with an error, but the function
  # body returned a value. Promote to `:stream_errored`: drives the retry
  # policy and excludes the execution from cache lookups. The value result
  # stays untouched in `results` — the execution's "result" remains the
  # value (the stream reference). The stream's error info is surfaced via
  # the streams panel; the completion kind alone tells the UI to render
  # this as a failure-with-value (mirrors `do_cancel_execution`).
  def finalize_stream_errored_completion(state, execution_id, error_id) do
    {:ok, step} = Runs.get_step_for_execution(state.db, execution_id)
    {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)
    {:ok, {type, message, frames}} = Errors.get_by_id(state.db, error_id)

    {retry_id, _recurred?, state} =
      decide_and_create_successor(
        state,
        execution_id,
        step,
        workspace_id,
        {:error, type, message, frames, nil}
      )

    case Results.record_completion(state.db, execution_id, :stream_errored,
           successor_id: retry_id
         ) do
      {:ok, completion_at} ->
        fire_completion_notification(state, execution_id, completion_at)

      {:error, :already_completed} ->
        state
    end
  end

  # A stream owned by this execution closed via idle timeout. The execution
  # itself succeeded; promote to `:stream_timeout` to exclude it from cache
  # lookups (consumer-shaped cache contents would be wrong) without
  # surfacing as a failure or triggering a retry.
  def finalize_stream_timeout_completion(state, execution_id) do
    case Results.record_completion(state.db, execution_id, :stream_timeout) do
      {:ok, completion_at} ->
        fire_completion_notification(state, execution_id, completion_at)

      {:error, :already_completed} ->
        state
    end
  end

  # Error result: decide retry now (so the successor decision lands on
  # the persisted completion row, not in transient in-memory state) and
  # re-fire the :result notification with the retry link filled in.
  def finalize_error_completion(state, execution_id, {type, message, frames, retryable}) do
    {:ok, step} = Runs.get_step_for_execution(state.db, execution_id)
    {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)

    {retry_id, _recurred?, state} =
      decide_and_create_successor(
        state,
        execution_id,
        step,
        workspace_id,
        {:error, type, message, frames, retryable}
      )

    case Results.record_completion(state.db, execution_id, :errored, successor_id: retry_id) do
      {:ok, completion_at} ->
        # Close streams only after the completion row exists, so
        # derive_lifecycle_info resolves the real reason (:errored, with
        # the producer's error) for live subscribers — closing first
        # would push a nil reason, which consumers coerce to a clean
        # "complete" and silently accept the truncated stream.
        state = close_open_streams(state, execution_id)

        # Re-fire :result on the run topic so the error entry in the UI
        # picks up the newly-created retry successor. We only need to do
        # this when retry_id changed from nil (there was no successor at
        # initial :result time) to something.
        state =
          if retry_id do
            fire_result_notifications(
              state,
              execution_id,
              {:error, type, message, frames, retry_id, retryable},
              nil,
              nil
            )
          else
            state
          end

        fire_completion_notification(state, execution_id, completion_at)

      {:error, :already_completed} ->
        # A completion row already exists, so derive_lifecycle_info still
        # resolves a real reason. Close streams here too — otherwise this
        # branch strands every consumer of a stream the producer left
        # open, waiting for a close that nothing else emits.
        close_open_streams(state, execution_id)
    end
  end

  # No results row exists for this execution but notify_terminated has
  # arrived — the worker terminated without reporting. Decide retry, write
  # completion (no results row), fire notifications.
  def handle_crashed(state, execution_id) do
    {:ok, step} = Runs.get_step_for_execution(state.db, execution_id)
    {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)

    # Decide retry as if this were an abandoned-like failure. result_retryable?
    # treats :crashed as retryable so the step's retry policy applies.
    {retry_id, _recurred?, state} =
      decide_and_create_successor(state, execution_id, step, workspace_id, :crashed)

    case Results.record_completion(state.db, execution_id, :crashed, successor_id: retry_id) do
      {:ok, completion_at} ->
        # Streams that had been appended to before the worker died need to
        # be closed so consumers don't wait forever. Closed after the
        # completion row is written so derive_lifecycle_info resolves the
        # real reason (:crashed) for live subscribers, rather than a nil
        # reason that consumers would coerce to a clean "complete".
        state = close_open_streams(state, execution_id)

        # Result-time notifications weren't fired (no results row was ever
        # written), so fire them now alongside the completion notification.
        state =
          fire_result_notifications(state, execution_id, {:crashed, retry_id}, nil, nil)

        fire_completion_notification(state, execution_id, completion_at)

      {:error, :already_completed} ->
        # As in finalize_error_completion: a completion row exists, so the
        # reason is derivable and any streams the dead worker left open
        # still need closing or their consumers wait forever.
        close_open_streams(state, execution_id)
    end
  end

  # Closes the step's open streams on behalf of `execution_id`, and pushes
  # a `stream_closed` notification to every active subscriber. Streams
  # already closed by the producer (clean or errored) are left untouched.
  #
  # `scope` picks which streams: `:step` (default) is every open stream
  # of the execution's step in its workspace — including paused streams
  # left by an earlier suspended attempt, which nothing else would close.
  # `:registered` is only the streams this execution produced into; used
  # when a re-run cancels a pending successor so the paused streams
  # survive for the new attempt.
  #
  # `spec` is how the closure is recorded: `:lifecycle` (the reason is
  # derived on read from the closing execution's completion), `:timeout`
  # or `:complete`.
  def close_open_streams(state, execution_id, spec \\ :lifecycle, scope \\ :step) do
    {:ok, stream_ids} =
      case scope do
        :step ->
          {:ok, {step_id, workspace_id, _attempt}} =
            Runs.get_execution_location(state.db, execution_id)

          Streams.get_open_stream_ids_for_step(state.db, step_id, workspace_id)

        :registered ->
          Streams.get_open_stream_ids_for_execution(state.db, execution_id)
      end

    {push_reason, push_error} =
      case spec do
        :lifecycle -> Resolve.lifecycle_info(state.db, execution_id)
        :timeout -> {:timeout, nil}
        :complete -> {:complete, nil}
      end

    Enum.reduce(stream_ids, state, fn stream_id, state ->
      case Streams.close_stream(state.db, stream_id, execution_id, spec) do
        {:ok, closed_at} ->
          state
          |> StreamDelivery.push_stream_closed(stream_id, push_reason, push_error)
          |> StreamDelivery.notify_stream_closed(
            stream_id,
            execution_id,
            push_reason,
            push_error,
            closed_at
          )
          |> Dependencies.update_dependencies_on_stream(stream_id, :closed)
          |> StreamDelivery.drop_stream_producer(stream_id)

        {:error, :already_closed} ->
          state
      end
    end)
  end

  def fire_completion_notification(state, execution_id, completion_at) do
    # Every completion funnels through here, whoever wrote it, so this is
    # the one place a permit needs releasing. Suspension, retry backoff,
    # recurrence, cancellation and abandonment all write a completion, so
    # all of them release; the successor re-acquires when it's next
    # admitted.
    state = Dependencies.release_concurrency_permit(state, execution_id)

    # Error results become resolvable only once the completion lands (the
    # retry decision is on the completion). Any waiters parked at result
    # time because the result was still pending need to be re-evaluated now.
    state = Waiters.notify_waiting(state, execution_id)

    # Resolved from the database, before `untrack_run_execution` below:
    # untracking the last in-flight execution deletes the run's
    # `run_workflows` entry, which would otherwise supply the root target.
    # An execution whose identity can't be resolved (no run, workspace or
    # root step to name it by) has nothing a topic could be told about, so
    # the rest is skipped rather than crashing the project's orchestration.
    case Identity.execution(state.db, execution_id) do
      {:ok, identity} ->
        emit_completion(state, execution_id, identity, completion_at)

      {:error, :not_found} ->
        Logger.warning(
          "Couldn't resolve identity of execution #{execution_id}; skipping completion notification."
        )

        state
    end
  end

  def emit_completion(state, execution_id, identity, completion_at) do
    %{run: r, workspace: ws_ext_id} = identity

    {kind, successor} =
      case Results.get_completion(state.db, execution_id) do
        {:ok, {kind_atom, successor_id, successor_ref_id, _, _}} ->
          {kind_atom, build_completion_successor(state.db, successor_id, successor_ref_id)}

        {:ok, nil} ->
          {nil, nil}
      end

    state
    # Queue / workflow-list bookkeeping is completion-driven: the
    # execution is considered "done" for the queue and the module-level
    # running-workflow tracker only once the completion is recorded.
    # An execution with a value result but no completion (streams still
    # draining) continues to show up as running.
    |> then(fn state ->
      case State.untrack_run_execution(state, r, execution_id) do
        {{root_module, root_target}, state} ->
          maybe_emit_run_outcome(state, ws_ext_id, r, root_module, root_target, execution_id)

        {nil, state} ->
          state
      end
    end)
    |> Effects.emit(%CompletionRecorded{
      execution: identity.execution,
      run: identity.run,
      step: identity.step,
      attempt: identity.attempt,
      workspace: identity.workspace,
      module: identity.module,
      target: identity.target,
      type: identity.type,
      root_module: identity.root_module,
      root_target: identity.root_target,
      kind: kind,
      successor: successor,
      completed_at: completion_at
    })
    |> notify_input_deactivations(execution_id)
  end

  # Tell the workflow topic how the run turned out. The run's outcome comes
  # from its initial execution, so it can move either when that execution
  # completes, or when a later one does - a handed-off (deferred/cached/
  # spawned) initial execution resolves to its successor's outcome. Rather
  # than tracking which executions the initial one handed off to, this
  # recomputes whenever the run has nothing left in flight, plus whenever the
  # initial execution itself completes.
  def maybe_emit_run_outcome(state, ws_ext_id, run_external_id, module, target, execution_id) do
    {:ok, initial?} = Runs.initial_execution?(state.db, execution_id)
    idle? = !Map.has_key?(state.run_workflows, run_external_id)

    if initial? or idle? do
      emit_run_outcome(state, ws_ext_id, run_external_id, module, target)
    else
      state
    end
  end

  def emit_run_outcome(state, ws_ext_id, run_external_id, module, target) do
    {:ok, initial_execution_id} = Runs.get_initial_execution_id(state.db, run_external_id)

    Effects.emit(state, %RunOutcome{
      run: run_external_id,
      workspace: ws_ext_id,
      root_module: module,
      root_target: target,
      outcome: Results.run_outcome(state.db, initial_execution_id)
    })
  end

  # Shape the successor on a completion for the run topic. Same-epoch
  # integer ids get resolved to their external form; cross-epoch refs go
  # out as their resolved run/step/attempt triple.
  def build_completion_successor(_db, nil, nil), do: nil

  def build_completion_successor(db, successor_id, nil) when is_integer(successor_id) do
    case Runs.get_execution_key(db, successor_id) do
      {:ok, {r, s, a}} -> %{type: "execution", id: Ids.execution(r, s, a)}
      _ -> nil
    end
  end

  def build_completion_successor(db, nil, successor_ref_id) when is_integer(successor_ref_id) do
    {ext_id, _module, _target} = Resolve.execution_ref(db, successor_ref_id)
    %{type: "execution", id: ext_id}
  end

  def process_result(state, execution_id, result, created_by \\ nil) do
    {:ok, has_result?} = Results.has_result?(state.db, execution_id)
    {:ok, has_completion?} = Results.has_completion?(state.db, execution_id)

    cond do
      # Already completed (e.g. cancelled, then the session died before the
      # worker acknowledged): nothing to record — proceeding would create a
      # spurious retry and then trip the completions UNIQUE constraint.
      has_completion? ->
        {:ok, state}

      has_result? ->
        # Mid-drain: the value result is recorded and the completion is
        # pending while the execution's streams drain.
        cond do
          # A wall-clock timeout here means the drain was cut short. Close
          # the remaining open streams as :timeout so complete_execution
          # promotes the completion to :stream_timeout — otherwise the
          # kill would land as a clean :succeeded with silently truncated
          # streams.
          result == :timeout ->
            {:ok, close_open_streams(state, execution_id, :timeout)}

          # The worker's session went away mid-drain. The value stands, but
          # whatever it was still producing into is truncated, so this is an
          # abandonment, not a success: write the completion as :abandoned
          # (with the step's retry decision, as for any abandoned execution)
          # and close what it left open, so consumers see :abandoned rather
          # than a clean "complete" — and the truncated run isn't cached.
          result == :abandoned ->
            {:ok, step} = Runs.get_step_for_execution(state.db, execution_id)
            {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)

            {retry_id, _recurred?, state} =
              decide_and_create_successor(state, execution_id, step, workspace_id, :abandoned)

            case Results.record_completion(state.db, execution_id, :abandoned,
                   successor_id: retry_id,
                   created_by: created_by
                 ) do
              {:ok, completion_at} ->
                state = close_open_streams(state, execution_id)
                {:ok, fire_completion_notification(state, execution_id, completion_at)}

              {:error, :already_completed} ->
                {:ok, state}
            end

          # A generator-bodied producer suspends from inside its body,
          # after its value (the stream handle) was recorded. Write the
          # completion so the successor is scheduled and the streams stay
          # paused for it. The run topic's `:result` isn't re-fired: the
          # value stands, and the completion carries the suspension.
          match?({:suspended, _, _}, result) ->
            {:ok, step} = Runs.get_step_for_execution(state.db, execution_id)
            {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)

            {successor_id, _recurred?, state} =
              decide_and_create_successor(state, execution_id, step, workspace_id, result)

            case Results.record_completion(state.db, execution_id, :suspended,
                   successor_id: successor_id,
                   created_by: created_by
                 ) do
              {:ok, completion_at} ->
                {:ok, fire_completion_notification(state, execution_id, completion_at)}

              {:error, :already_completed} ->
                {:ok, state}
            end

          true ->
            {:ok, state}
        end

      true ->
        {:ok, step} = Runs.get_step_for_execution(state.db, execution_id)
        {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)

        # Retry decisions for error results are deferred to
        # complete_execution — they need to survive server restart, and
        # the error payload is already persisted on the results row so
        # the decision can be reconstructed from there. Every other shape
        # (value with recurrent → :recurred, suspended, abandoned, crashed,
        # timeout) still needs its successor decided here so the
        # compat-shim-written completion carries the correct link.
        {retry_id, recurred?, state} =
          if match?({:error, _, _, _, _}, result) do
            {nil, false, state}
          else
            decide_and_create_successor(state, execution_id, step, workspace_id, result)
          end

        result = transform_result_with_successor(result, retry_id, recurred?)

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
            {:error, :already_completed} -> state
          end

        # Cancel descendant executions for timeouts and cancellations
        state =
          if match?({:timeout, _}, result) or result == :cancelled do
            Cancellation.cancel_descendants(state, execution_id, workspace_id)
          else
            state
          end

        {:ok, state}
    end
  end

  def decide_and_create_successor(state, execution_id, step, workspace_id, result) do
    execution_ext_id =
      case Runs.get_execution_key(state.db, execution_id) do
        {:ok, {r, s, a}} -> Ids.execution(r, s, a)
        {:error, :not_found} -> nil
      end

    cond do
      match?({:suspended, _, _}, result) ->
        {:suspended, execute_after, dependency_keys} = result
        dependency_keys = CatalogFlow.resolve_catalog_waits(state, execution_id, dependency_keys)

        # TODO: limit the number of times a step can suspend? (or rate?)

        {:ok, retry_id, _, state} =
          Scheduling.rerun_step(state, step, workspace_id,
            execute_after: execute_after,
            dependency_keys: dependency_keys
          )

        state =
          if execution_ext_id do
            Cancellation.abort_execution(state, execution_ext_id)
          else
            state
          end

        {retry_id, false, state}

      result_retryable?(result) && step.retry_limit == -1 ->
        # Unlimited retries - random delay between min and max
        delay_ms =
          step.retry_backoff_min_ms +
            :rand.uniform() * (step.retry_backoff_max_ms - step.retry_backoff_min_ms)

        execute_after = System.os_time(:millisecond) + delay_ms

        {:ok, retry_id, _, state} =
          Scheduling.rerun_step(state, step, workspace_id, execute_after: execute_after)

        {retry_id, false, state}

      result_retryable?(result) && step.retry_limit > 0 ->
        # Limited retries - check consecutive failures. Exclude the current
        # execution so this works whether or not its completion has been
        # written yet. Failure kinds are errored/abandoned/crashed/timeout —
        # the same set the retry predicate uses.
        {:ok, rows} =
          Runs.get_step_completion_kinds(state.db, step.id, step.retry_limit + 2)

        failure_kinds = Results.failure_kinds()

        consecutive_failures =
          rows
          |> Enum.reject(fn {id, _kind} -> id == execution_id end)
          |> Enum.take_while(fn {_id, kind} -> kind in failure_kinds end)
          |> Enum.count()

        if consecutive_failures < step.retry_limit do
          # TODO: add jitter (within min/max delay)
          delay_ms =
            step.retry_backoff_min_ms +
              consecutive_failures / max(step.retry_limit - 1, 1) *
                (step.retry_backoff_max_ms - step.retry_backoff_min_ms)

          execute_after = System.os_time(:millisecond) + delay_ms

          {:ok, retry_id, _, state} =
            Scheduling.rerun_step(state, step, workspace_id, execute_after: execute_after)

          {retry_id, false, state}
        else
          {nil, false, state}
        end

      step.recurrent == 1 and match?({:value, {:raw, nil, []}}, result) ->
        # Null return from recurrent step: schedule next iteration via :recurred
        execute_after =
          if step.delay_ms > 0 do
            System.os_time(:millisecond) + step.delay_ms
          end

        {:ok, retry_id, _, state} =
          Scheduling.rerun_step(state, step, workspace_id, execute_after: execute_after)

        {retry_id, true, state}

      step.recurrent == 1 and match?({:value, _}, result) ->
        # Non-null return from recurrent step: stop recurrence
        {nil, false, state}

      true ->
        {nil, false, state}
    end
  end

  def transform_result_with_successor(result, retry_id, recurred?) do
    case result do
      {:error, type, message, frames, retryable} ->
        {:error, type, message, frames, retry_id, retryable}

      :abandoned ->
        {:abandoned, retry_id}

      :crashed ->
        {:crashed, retry_id}

      :timeout ->
        {:timeout, retry_id}

      {:suspended, _, _} ->
        {:suspended, retry_id}

      {:value, _} when recurred? ->
        {:recurred, retry_id}

      other ->
        other
    end
  end
end
