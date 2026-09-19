defmodule Coflux.Orchestration.Server.Waiters do
  @moduledoc """
  Calls parked mid-flight, and what wakes them.

  A worker that selects over several handles - results, inputs, stream
  positions, catalog versions - makes one call and blocks. Rather than
  hold the caller in a process, the reply address is parked in `waiting`,
  keyed by the *thing* being waited for, and answered when that thing
  resolves. One request appears under every key it is waiting on; select
  is first-wins, so whichever resolves first answers the call and the
  others are unparked.

  Keys name things by external id only, because this map is carried across
  an epoch rotation exactly as it is and rotation reassigns internal ids.
  A new kind of key must be handled in five places: here, in the input
  response and dismissal paths, in the expiry sweep, and when an execution
  is cleaned up.

  A parked call can end three ways: something resolves it, its deadline
  passes, or the execution that made it goes away. The last is why
  `cleanup_execution/2` lives here - an aborted execution's parked calls
  must be answered, or its worker waits for a reply that will never come.
  """

  alias Coflux.Events.{InputActivated, InputDependencyRecorded, ResultDependencyRecorded}

  alias Coflux.Orchestration.{Catalog, Ids, Inputs, Results, Runs}

  alias Coflux.Orchestration.Server.{
    Archives,
    Cancellation,
    CatalogFlow,
    Commands,
    Dependencies,
    Effects,
    Resolve,
    State
  }

  def reschedule_expire_waiters(state) do
    if state.expire_waiters_timer do
      Process.cancel_timer(state.expire_waiters_timer)
    end

    next_expire_at =
      state.waiting
      |> Map.values()
      |> Enum.flat_map(fn entries ->
        Enum.map(entries, & &1.expire_at)
      end)
      |> Enum.reject(&is_nil/1)
      |> Enum.min(fn -> nil end)

    timer =
      if next_expire_at do
        Process.send_after(self(), :expire_waiters, next_expire_at, abs: true)
      end

    Map.put(state, :expire_waiters_timer, timer)
  end

  def notify_waiting(state, execution_id) do
    execution_ext_id =
      case Runs.get_execution_key(state.db, execution_id) do
        {:ok, {r, s, a}} -> Ids.execution(r, s, a)
        {:error, :not_found} -> nil
      end

    if execution_ext_id do
      old_key = {:execution, execution_ext_id}

      case Map.get(state.waiting, old_key) do
        nil ->
          state

        _entries ->
          case Results.resolve(state.db, execution_id) do
            {:pending, new_execution_id} ->
              # Execution was replaced by a spawned one — migrate waiters from
              # the old key to the new one, updating their keys lists.
              new_ext_id =
                case Runs.get_execution_key(state.db, new_execution_id) do
                  {:ok, {r, s, a}} -> Ids.execution(r, s, a)
                end

              new_key = {:execution, new_ext_id}
              migrate_select_waiters(state, old_key, new_key)

            {:ok, result} ->
              result =
                case result do
                  {:value, value} -> {:value, Resolve.value(state.db, value)}
                  other -> other
                end

              notify_select_waiters(state, old_key, result)
          end
      end
    else
      state
    end
  end

  # Process a single handle for select: record the dependency and determine
  # current status. Returns one of:
  #   {:ok, {:resolved, result}}
  #       where result is {:value, _} | {:error, ...} | :cancelled | :dismissed | ...
  #   {:ok, {:pending, waiting_key, dependency_key}}
  #       waiting_key identifies this handle in state.waiting
  #       dependency_key is used when suspending (for process_result)
  #   {:error, reason}
  def process_select_handle(
        state,
        %{"type" => "execution", "id" => execution_external_id},
        from_execution_id,
        from_execution_external_id
      ) do
    case Archives.resolve_internal_execution_id(state, execution_external_id) do
      {:error, :not_found} ->
        {{:error, :not_found}, state}

      {:ok, execution_id} ->
        {:ok, dep_ref_id} = Runs.create_execution_ref_for(state.db, execution_id)
        {:ok, id} = Runs.record_result_dependency(state.db, from_execution_id, dep_ref_id)

        state =
          if id do
            {:ok, {run_external_id}} =
              Runs.get_external_run_id_for_execution(state.db, from_execution_id)

            Effects.emit(state, %ResultDependencyRecorded{
              run: run_external_id,
              execution: from_execution_external_id,
              dependency: Resolve.execution_ref(state.db, dep_ref_id),
              pending: match?({:pending, _}, Results.resolve(state.db, execution_id))
            })
          else
            state
          end

        case Results.resolve(state.db, execution_id) do
          {:ok, result} ->
            result =
              case result do
                {:value, value} -> {:value, Resolve.value(state.db, value)}
                other -> other
              end

            {{:ok, {:resolved, result}}, state}

          {:pending, pending_execution_id} ->
            pending_ext_id =
              case Runs.get_execution_key(state.db, pending_execution_id) do
                {:ok, {r, s, a}} -> Ids.execution(r, s, a)
              end

            {
              {:ok, {:pending, {:execution, pending_ext_id}, {:execution, pending_execution_id}}},
              state
            }
        end
    end
  end

  # A stream handle asks one question: has the stream reached this
  # sequence (or closed, so it never will)? A consumer can't answer it
  # itself — its queue is fed asynchronously, so an empty one means
  # "nothing has arrived yet", not "the stream has nothing".
  #
  # Resolving carries no value: the item reaches the consumer through the
  # subscription it already holds. The answer only says "there is
  # something, stop waiting".
  def process_select_handle(
        state,
        %{"type" => "stream", "id" => stream_external_id} = handle,
        _from_execution_id,
        _from_execution_external_id
      ) do
    sequence = Map.get(handle, "sequence", 0)

    case Archives.resolve_stream_id(state, stream_external_id) do
      {:error, :not_found} ->
        {{:error, :not_found}, state}

      {:ok, stream_id} ->
        if Dependencies.stream_reached?(state.db, stream_id, sequence) do
          {{:ok, {:resolved, :available}}, state}
        else
          {{:ok,
            {:pending, {:stream, stream_external_id}, {:stream, stream_external_id, sequence}}},
           state}
        end
    end
  end

  # A catalog handle asks for the first version at `path` numbered above a
  # position, as seen from the caller's workspace, and resolves with that
  # version's number (recorded as a read). Without an explicit `number`
  # the position is the caller's own view of the path — the head as of its
  # snapshot, plus its run's writes — so the wait means "anything newer
  # than what `current()` gives me", and an execution's own publish never
  # wakes it. Either way what comes *after* the position ignores the pin:
  # this is the wait side, and seeing past the snapshot is the point.
  #
  # The waiting key carries the caller's workspace so a publish can check
  # visibility per key — by external id, like every waiting key, since the
  # map outlives an epoch rotation and internal ids don't. The dependency
  # key recorded on a suspended successor doesn't need it, since the
  # successor's workspace is known.
  def process_select_handle(
        state,
        %{"type" => "catalog", "path" => path} = handle,
        from_execution_id,
        from_execution_external_id
      ) do
    case Catalog.validate_path(path) do
      :ok ->
        number =
          case Map.get(handle, "number") do
            nil ->
              case CatalogFlow.lookup_catalog_version(state, from_execution_id, path, nil) do
                {:ok, nil} -> 0
                {:ok, version} -> version.number
              end

            number ->
              number
          end

        {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, from_execution_id)
        chain = State.workspace_chain(state, workspace_id)

        case Catalog.get_next(state.db, path, chain, number) do
          {:ok, nil} ->
            waiting_key =
              {:catalog, State.workspace_external_id(state, workspace_id), path, number}

            {{:ok, {:pending, waiting_key, {:catalog, path, number}}}, state}

          {:ok, version} ->
            state =
              CatalogFlow.record_catalog_read(
                state,
                from_execution_id,
                from_execution_external_id,
                version
              )

            {{:ok, {:resolved, {:value, {:raw, version.number, []}}}}, state}
        end

      {:error, :invalid_path} ->
        {{:error, :invalid_path}, state}
    end
  end

  def process_select_handle(
        state,
        %{"type" => "input", "id" => input_external_id},
        from_execution_id,
        from_execution_external_id
      ) do
    case Archives.find_and_copy_input_from_archives(state, input_external_id) do
      {:ok, nil} ->
        {{:error, :input_not_found}, state}

      {:ok,
       {input_id, workspace_id, _key, _prompt_id, _schema_id, title, _actions, _initial,
        requires_tag_set_id, created_at, _run_id}} ->
        now = System.system_time(:millisecond)
        input_response = Inputs.get_input_response(state.db, input_id)

        {:ok, is_new} =
          Inputs.record_input_dependency(state.db, from_execution_id, input_id, now)

        state =
          if is_new do
            {:ok, {run_external_id}} =
              Runs.get_external_run_id_for_execution(state.db, from_execution_id)

            response_type =
              case input_response do
                {:ok, nil} -> nil
                {:ok, {:value, _, _, _}} -> :value
                {:ok, {:dismissed, _, _}} -> :dismissed
                {:ok, {:cancelled, _, _}} -> :cancelled
              end

            ws_ext_id = State.workspace_external_id(state, workspace_id)
            requires = Resolve.tag_set(state.db, requires_tag_set_id)

            state
            |> Effects.emit(%InputDependencyRecorded{
              run: run_external_id,
              execution: from_execution_external_id,
              input: input_external_id,
              title: title,
              response: response_type,
              pending: is_nil(response_type)
            })
            |> Effects.emit(%InputActivated{
              workspace: ws_ext_id,
              input: input_external_id,
              run: elem(Ids.parse_input(input_external_id), 1),
              created_at: created_at,
              title: title,
              requires: requires
            })
          else
            state
          end

        case input_response do
          {:ok, nil} ->
            {
              {:ok, {:pending, {:input, input_external_id}, {:input, input_id}}},
              state
            }

          {:ok, {:value, value, _created_at, _created_by}} ->
            # Input values are raw decoded JSON; wrap in the value tuple format
            # so compose_value treats them uniformly with execution values.
            wrapped = {:raw, value, []}
            {{:ok, {:resolved, {:value, wrapped}}}, state}

          {:ok, {:dismissed, _created_at, _created_by}} ->
            {{:ok, {:resolved, :dismissed}}, state}

          {:ok, {:cancelled, _created_at, _created_by}} ->
            {{:ok, {:resolved, :cancelled}}, state}
        end
    end
  end

  # When cancel_remaining is true and a handle resolves, cancel all
  # non-winner handles (executions via do_cancel_execution, inputs by
  # marking them cancelled).
  def maybe_cancel_remaining(state, _statuses, _winner_idx, false, _from_ext_id),
    do: state

  def maybe_cancel_remaining(
        state,
        statuses,
        winner_idx,
        true,
        from_execution_external_id
      ) do
    {:ok, from_execution_id} =
      Archives.resolve_internal_execution_id(state, from_execution_external_id)

    {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, from_execution_id)

    statuses
    |> Enum.with_index()
    |> Enum.reject(fn {_, idx} -> idx == winner_idx end)
    |> Enum.reduce(state, fn
      {{:pending, {:execution, ext_id}, _}, _idx}, state ->
        Cancellation.cancel_handle(state, %{"type" => "execution", "id" => ext_id}, workspace_id)

      {{:pending, {:input, ext_id}, _}, _idx}, state ->
        Cancellation.cancel_handle(state, %{"type" => "input", "id" => ext_id}, workspace_id)

      {_, _}, state ->
        state
    end)
  end

  # Pop all select waiters registered under `waiting_key` and serve each one
  # with `result`. Removes the waiters from any other keys they're
  # registered under and cancels remaining executions when requested.
  def notify_select_waiters(state, waiting_key, result) do
    {entries, waiting} = Map.pop(state.waiting, waiting_key, [])
    state = Map.put(state, :waiting, waiting)

    state =
      Enum.reduce(entries, state, fn entry, state ->
        serve_select_entry(state, entry, waiting_key, result)
      end)

    reschedule_expire_waiters(state)
  end

  # Serve a single waiter entry: clean it up from its other registration
  # keys, optionally cancel non-winner executions, and notify the waiter's
  # session.
  def serve_select_entry(state, entry, winner_key, result) do
    state =
      entry.keys
      |> Enum.reject(&(&1 == winner_key))
      |> Enum.reduce(state, fn key, state ->
        remove_waiter_from_key(state, key, entry.request_id)
      end)

    state =
      if entry.cancel_remaining do
        cancel_other_execution_keys(state, entry, winner_key)
      else
        state
      end

    case State.session_for_execution(state, entry.from_ext_id) do
      {:ok, session_id} ->
        Effects.command(
          state,
          session_id,
          Commands.result(entry.request_id, {entry.handle_index, result})
        )

      :error ->
        state
    end
  end

  def remove_waiter_from_key(state, key, request_id) do
    update_in(state, [Access.key(:waiting)], fn waiting ->
      case Map.fetch(waiting, key) do
        {:ok, entries} ->
          remaining = Enum.reject(entries, &(&1.request_id == request_id))

          if remaining == [] do
            Map.delete(waiting, key)
          else
            Map.put(waiting, key, remaining)
          end

        :error ->
          waiting
      end
    end)
  end

  def cancel_other_execution_keys(state, entry, winner_key) do
    {:ok, from_execution_id} =
      Archives.resolve_internal_execution_id(state, entry.from_ext_id)

    {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, from_execution_id)

    entry.keys
    |> Enum.reject(&(&1 == winner_key))
    |> Enum.reduce(state, fn
      {:execution, ext_id}, state ->
        case Archives.resolve_internal_execution_id(state, ext_id) do
          {:ok, execution_id} ->
            Cancellation.do_cancel_execution(state, execution_id, workspace_id)

          {:error, :not_found} ->
            state
        end

      {:input, _}, state ->
        state

      {:catalog, _, _, _}, state ->
        state
    end)
  end

  # Move all waiters from `old_key` to `new_key`, updating each entry's
  # `keys` list. Used when an execution is replaced by a spawned one.
  def migrate_select_waiters(state, old_key, new_key) do
    {entries, waiting} = Map.pop(state.waiting, old_key, [])

    migrated =
      Enum.map(entries, fn entry ->
        Map.update!(entry, :keys, fn keys ->
          Enum.map(keys, fn
            ^old_key -> new_key
            other -> other
          end)
        end)
      end)

    waiting =
      Map.update(waiting, new_key, migrated, &(&1 ++ migrated))

    state = Map.put(state, :waiting, waiting)

    # Also update other key entries that reference old_key in their keys list
    # (waiters registered under multiple keys, one of which was old_key).
    update_in(state, [Access.key(:waiting)], fn waiting ->
      Map.new(waiting, fn {key, entries} ->
        if key == new_key do
          {key, entries}
        else
          updated =
            Enum.map(entries, fn entry ->
              if old_key in entry.keys do
                Map.update!(entry, :keys, fn keys ->
                  Enum.map(keys, fn
                    ^old_key -> new_key
                    other -> other
                  end)
                end)
              else
                entry
              end
            end)

          {key, updated}
        end
      end)
    end)
  end

  # Finds the session for an execution by external execution ID

  # Clean up waiting map entries and pending requests for an execution,
  # without sending an abort message to the worker.
  def cleanup_execution(state, execution_ext_id) do
    # Remove all select waiters where this execution is the waiter, deduping
    # by request_id since each waiter may be registered under multiple keys.
    removed_by_request =
      Enum.reduce(state.waiting, %{}, fn {_key, entries}, acc ->
        Enum.reduce(entries, acc, fn entry, acc ->
          if entry.from_ext_id == execution_ext_id do
            Map.put_new(acc, entry.request_id, entry)
          else
            acc
          end
        end)
      end)

    state =
      Map.update!(state, :waiting, fn waiting ->
        waiting
        |> Enum.map(fn {key, entries} ->
          {key, Enum.reject(entries, &(&1.from_ext_id == execution_ext_id))}
        end)
        |> Enum.reject(fn {_key, entries} -> entries == [] end)
        |> Map.new()
      end)

    # Send responses for any pending select requests so the worker doesn't
    # hang waiting for a reply that will never come. Since the execution is
    # being cleaned up (abort/suspend), we send :timeout to let the client
    # know the wait is over — the process will be killed separately.
    case State.session_for_execution(state, execution_ext_id) do
      {:ok, session_id} ->
        Enum.reduce(removed_by_request, state, fn {_, entry}, state ->
          Effects.command(
            state,
            session_id,
            Commands.result(entry.request_id, :timeout)
          )
        end)

      :error ->
        state
    end
  end

  # --- Producer flow control ---
  #
end
