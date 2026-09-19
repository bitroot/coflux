defmodule Coflux.Orchestration.Server.Rotation do
  @moduledoc """
  Closing the active epoch and opening a new one.

  When the active database grows past its threshold it is archived and a
  fresh one takes over. Everything still in flight has to survive that,
  and the three kinds of state survive it differently:

    * **Rows** are copied forward. A run with anything still running is
      copied into the new epoch, because what happens next will write
      against it, and writes only go to the active database.

    * **Accelerations** are rebuilt, not carried: the dependency ledger
      and the concurrency permits are re-derived from the rows that were
      just copied. This is the same path the server takes at boot, and it
      is why neither index has to be correct across a rotation - only
      re-derivable.

    * **In-memory-only state** is captured before the switch and restored
      after it, because nothing can rebuild it. Stream subscriptions and
      producer credit are the ones that matter: a consumer mid-stream must
      not have its subscription dropped because the database underneath
      it changed.

  Internal ids are reassigned by the copy, so anything holding one is
  remapped; anything keyed by external id needs no attention, which is why
  parked calls are keyed that way.
  """

  alias Coflux.Store.{Bloom, Epochs, Index}

  alias Coflux.Orchestration.{Epoch, Ids, Runs}

  alias Coflux.Orchestration.Server.{Archives, Dependencies, StreamDelivery}

  def do_rotate_epoch(state) do
    epoch_id = Epochs.next_epoch_id(state.epochs)

    # Write placeholder entry to index first (null value)
    epoch_index = Index.add_epoch(state.epoch_index, epoch_id, System.os_time(:millisecond))
    :ok = Index.save(epoch_index)

    # Live stream state (subscriptions, producers) is keyed by internal
    # ids, which the copy below reassigns. Capture it by external id from
    # the old database first, and rebuild it once the runs are copied.
    stream_state = StreamDelivery.capture(state)

    # Now rotate
    {:ok, new_epochs, old_db} = Epochs.rotate(state.epochs, epoch_id)
    new_db = Epochs.active_db(new_epochs)

    id_mappings = Epoch.copy_config(old_db, new_db)

    state
    |> Map.put(:epochs, new_epochs)
    |> Map.put(:db, new_db)
    |> Map.put(:epoch_index, epoch_index)
    |> Map.update!(:index_queue, &(&1 ++ [epoch_id]))
    |> remap_config_ids(id_mappings)
    |> copy_in_flight_runs()
    # Each owner is asked to deal with the new database itself: internal
    # ids were reassigned by the copy, so what can be re-derived is
    # rebuilt and what cannot is restored from the capture above.
    |> StreamDelivery.restore(stream_state)
    |> Dependencies.rebuild()
    |> maybe_start_index_build()
  end

  def copy_in_flight_runs(state) do
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
        {:ok, run_ext_id, _step, _attempt} = Ids.parse_execution(ext_id)
        run_ext_id
      end)
      |> Enum.uniq()

    # Copy each run from archives into the active epoch
    Enum.each(run_ext_ids, fn run_ext_id ->
      {:ok, _remap} = Archives.find_and_copy_run_from_archives(state, run_ext_id)
    end)

    # Repopulate execution_ids cache from the new active DB
    execution_ids =
      Map.new(in_flight_ext_ids, fn ext_id ->
        {:ok, run_ext_id, step_num, attempt} = Ids.parse_execution(ext_id)
        {:ok, {id}} = Runs.get_execution_id(state.db, run_ext_id, step_num, attempt)
        {ext_id, id}
      end)

    %{state | execution_ids: execution_ids}
  end

  def maybe_start_index_build(%{index_task: nil, index_queue: [epoch_id | _]} = state) do
    epochs = state.epochs

    task =
      Task.Supervisor.async_nolink(Coflux.LauncherSupervisor, fn ->
        {:ok, db} = Epochs.open_archive(epochs, epoch_id)

        try do
          build_blooms_for_epoch(db)
        after
          Exqlite.Sqlite3.close(db)
        end
      end)

    %{state | index_task: task.ref}
  end

  def maybe_start_index_build(state), do: state

  def build_blooms_for_epoch(db) do
    {:ok, run_ids} = Runs.get_all_run_external_ids(db)
    runs = Bloom.new(max(100, length(run_ids)))
    runs = Enum.reduce(run_ids, runs, &Bloom.add(&2, &1))

    {:ok, cache_keys_list} = Runs.get_all_cache_keys(db)
    cache_keys = Bloom.new(max(100, length(cache_keys_list)))
    cache_keys = Enum.reduce(cache_keys_list, cache_keys, &Bloom.add(&2, &1))

    {:ok, idemp_keys_list} = Runs.get_all_idempotency_keys(db)
    idempotency_keys = Bloom.new(max(100, length(idemp_keys_list)))
    idempotency_keys = Enum.reduce(idemp_keys_list, idempotency_keys, &Bloom.add(&2, &1))

    {runs, cache_keys, idempotency_keys}
  end

  def remap_config_ids(state, %{} = mappings) do
    ws_map = Map.get(mappings, :workspace_ids, %{})
    session_map = Map.get(mappings, :session_ids, %{})
    worker_map = Map.get(mappings, :worker_ids, %{})
    pool_map = Map.get(mappings, :pool_ids, %{})

    # Remap workspaces: rekey map, update base_id values
    workspaces =
      Map.new(state.workspaces, fn {old_id, workspace} ->
        new_id = Map.fetch!(ws_map, old_id)

        new_base_id =
          if workspace.base_id,
            do: Map.fetch!(ws_map, workspace.base_id),
            else: nil

        {new_id, %{workspace | base_id: new_base_id}}
      end)

    # Remap workspace_names: values are workspace IDs
    workspace_names =
      Map.new(state.workspace_names, fn {name, old_id} ->
        {name, Map.fetch!(ws_map, old_id)}
      end)

    # Remap workspace_external_ids: values are workspace IDs
    workspace_external_ids =
      Map.new(state.workspace_external_ids, fn {ext_id, old_id} ->
        {ext_id, Map.fetch!(ws_map, old_id)}
      end)

    # Remap pools: rekey by new workspace_id, pool values contain pool_definition_id etc.
    pools =
      Map.new(state.pools, fn {old_ws_id, ws_pools} ->
        new_ws_id = Map.fetch!(ws_map, old_ws_id)

        new_ws_pools =
          Map.new(ws_pools, fn {pool_name, pool} ->
            new_pool = %{pool | id: Map.fetch!(pool_map, pool.id)}
            {pool_name, new_pool}
          end)

        {new_ws_id, new_ws_pools}
      end)

    # Remap pool_failures: keyed by pool ID. A pool that no longer exists
    # in the new epoch has nothing left to back off from.
    pool_failures =
      state.pool_failures
      |> Enum.filter(fn {old_id, _} -> Map.has_key?(pool_map, old_id) end)
      |> Map.new(fn {old_id, failures} -> {Map.fetch!(pool_map, old_id), failures} end)

    # Remap workers: rekey map, update pool_id, workspace_id, session_id
    workers =
      Map.new(state.workers, fn {old_id, worker} ->
        new_id = Map.fetch!(worker_map, old_id)

        new_worker = %{
          worker
          | pool_id: Map.fetch!(pool_map, worker.pool_id),
            workspace_id: Map.fetch!(ws_map, worker.workspace_id),
            session_id:
              if(worker.session_id,
                do: Map.fetch!(session_map, worker.session_id),
                else: nil
              )
        }

        {new_id, new_worker}
      end)

    # Remap worker_external_ids: values are worker IDs
    worker_external_ids =
      Map.new(state.worker_external_ids, fn {ext_id, old_id} ->
        {ext_id, Map.fetch!(worker_map, old_id)}
      end)

    # Remap sessions: rekey map, update workspace_id, worker_id
    sessions =
      Map.new(state.sessions, fn {old_id, session} ->
        new_id = Map.fetch!(session_map, old_id)

        new_session = %{
          session
          | workspace_id: Map.fetch!(ws_map, session.workspace_id),
            worker_id:
              if(session.worker_id,
                do: Map.fetch!(worker_map, session.worker_id),
                else: nil
              )
        }

        {new_id, new_session}
      end)

    # Remap session_ids: values are session IDs
    session_ids =
      Map.new(state.session_ids, fn {ext_id, old_id} ->
        {ext_id, Map.fetch!(session_map, old_id)}
      end)

    # Remap session_expiries: rekey by new session_id
    session_expiries =
      Map.new(state.session_expiries, fn {old_id, expiry} ->
        {Map.fetch!(session_map, old_id), expiry}
      end)

    # Remap connections: values contain session_id
    connections =
      Map.new(state.connections, fn {ref, {pid, old_session_id}} ->
        {ref, {pid, Map.fetch!(session_map, old_session_id)}}
      end)

    # Remap targets: session_ids in MapSets
    targets =
      Map.new(state.targets, fn {module, module_targets} ->
        new_module_targets =
          Map.new(module_targets, fn {target_name, target} ->
            new_session_ids =
              MapSet.new(target.session_ids, fn old_sid ->
                Map.fetch!(session_map, old_sid)
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
        pool_failures: pool_failures,
        workers: workers,
        worker_external_ids: worker_external_ids,
        sessions: sessions,
        session_ids: session_ids,
        session_expiries: session_expiries,
        connections: connections,
        targets: targets
    }
  end
end
