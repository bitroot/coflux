defmodule Coflux.Orchestration.Epoch do
  @moduledoc """
  Copies data between epoch databases during rotation and copy-on-reference.
  """

  alias Coflux.Orchestration.Runs

  import Coflux.Store

  @doc """
  Copy config entities from old_db to new_db with latest-state-only semantics.
  Returns a map of ID remappings: %{workspace_ids: %{old→new}, session_ids: %{old→new}, ...}
  """
  def copy_config(old_db, new_db) do
    with_transaction(new_db, fn ->
      # 1. Copy workspaces
      workspace_ids = copy_workspaces(old_db, new_db)

      # Copy latest workspace_names per workspace
      copy_latest_workspace_names(old_db, new_db, workspace_ids)

      # Copy latest workspace_states per workspace
      copy_latest_workspace_states(old_db, new_db, workspace_ids)

      # Copy latest workspace_bases per workspace
      copy_latest_workspace_bases(old_db, new_db, workspace_ids)

      # 2. Copy pools (ensures pool_definitions and launchers on demand)
      pool_ids = copy_pools(old_db, new_db, workspace_ids)

      # 3. Copy active workers (remap pool_id)
      worker_ids = copy_active_workers(old_db, new_db, pool_ids)

      # Copy latest worker_states
      copy_latest_worker_states(old_db, new_db, worker_ids)

      # 4. Copy active sessions (ensures tag_sets on demand)
      session_ids =
        copy_active_sessions(
          old_db,
          new_db,
          workspace_ids,
          worker_ids
        )

      # Copy session_activations for active sessions
      copy_session_activations(old_db, new_db, session_ids)

      # 5. Copy latest workspace_manifests and their workflows
      #    (ensures manifests, parameter_sets, instructions, cache_configs, tag_sets on demand)
      copy_latest_workspace_manifests(old_db, new_db, workspace_ids)

      %{
        workspace_ids: workspace_ids,
        session_ids: session_ids,
        worker_ids: worker_ids,
        pool_ids: pool_ids
      }
    end)
  end

  @doc """
  Copy a run and all its data from source_db to target_db.
  Follows cross-run references recursively.
  Returns ID remapping for the copied data.
  """
  def copy_run(source_db, target_db, run_external_id) do
    with_transaction(target_db, fn ->
      {:ok, remap, _visited} =
        do_copy_run(source_db, target_db, run_external_id, MapSet.new())

      remap
    end)
  end

  defp do_copy_run(source_db, target_db, run_external_id, visited) do
    if MapSet.member?(visited, run_external_id) do
      {:ok, %{}, visited}
    else
      visited = MapSet.put(visited, run_external_id)

      case query_one(
             source_db,
             """
             SELECT id, external_id, parent_ref_id, idempotency_key, created_at, created_by
             FROM runs
             WHERE external_id = ?1
             """,
             {run_external_id}
           ) do
        {:ok, nil} ->
          {:ok, %{}, visited}

        {:ok, {old_run_id, ext_id, parent_ref_id, idempotency_key, created_at, created_by}} ->
          # Check if already exists in target
          case query_one(target_db, "SELECT id FROM runs WHERE external_id = ?1", {ext_id}) do
            {:ok, {_existing_id}} ->
              {:ok, %{}, visited}

            {:ok, nil} ->
              # parent_ref_id is an execution_ref — copy it to target
              new_parent_ref_id =
                if parent_ref_id do
                  ensure_execution_ref(source_db, target_db, parent_ref_id)
                end

              # Insert the run
              {:ok, new_run_id} =
                insert_one(target_db, :runs, %{
                  external_id: ext_id,
                  parent_ref_id: new_parent_ref_id,
                  idempotency_key: idempotency_key,
                  created_at: created_at,
                  created_by: ensure_principal(source_db, target_db, created_by)
                })

              # Copy steps and executions interleaved (ordered by step number).
              # Each step's parent_id references an execution from an earlier step,
              # so by processing in order and inserting executions after each step,
              # the parent execution is always available.
              {:ok, steps} =
                query(
                  source_db,
                  """
                  SELECT
                    id, number, run_id, parent_id, module, target, type, priority,
                    wait_for, cache_config_id, cache_key, defer_key, memo_key,
                    retry_limit, retry_delay_min, retry_delay_max, recurrent, delay,
                    requires_tag_set_id, created_at
                  FROM steps
                  WHERE run_id = ?1
                  ORDER BY number
                  """,
                  {old_run_id}
                )

              {step_ids, execution_ids} =
                Enum.reduce(steps, {%{}, %{}}, fn {old_step_id, number, _run_id, step_parent_id,
                                                   module, target, type, priority, wait_for,
                                                   cache_config_id, cache_key, defer_key,
                                                   memo_key, retry_limit, retry_delay_min,
                                                   retry_delay_max, recurrent, delay,
                                                   requires_tag_set_id, step_created_at},
                                                  {step_acc, exec_acc} ->
                  # steps.parent_id is same-run internal — strict remap
                  new_parent_id =
                    if step_parent_id, do: Map.fetch!(exec_acc, step_parent_id)

                  {:ok, new_step_id} =
                    insert_one(target_db, :steps, %{
                      number: number,
                      run_id: new_run_id,
                      parent_id: new_parent_id,
                      module: module,
                      target: target,
                      type: type,
                      priority: priority,
                      wait_for: wait_for,
                      cache_config_id: ensure_cache_config(source_db, target_db, cache_config_id),
                      cache_key: if(cache_key, do: {:blob, cache_key}),
                      defer_key: if(defer_key, do: {:blob, defer_key}),
                      memo_key: if(memo_key, do: {:blob, memo_key}),
                      retry_limit: retry_limit,
                      retry_delay_min: retry_delay_min,
                      retry_delay_max: retry_delay_max,
                      recurrent: recurrent,
                      delay: delay,
                      requires_tag_set_id:
                        ensure_tag_set(source_db, target_db, requires_tag_set_id),
                      created_at: step_created_at
                    })

                  # Copy step_arguments
                  {:ok, args} =
                    query(
                      source_db,
                      "SELECT position, value_id FROM step_arguments WHERE step_id = ?1 ORDER BY position",
                      {old_step_id}
                    )

                  Enum.each(args, fn {position, value_id} ->
                    new_value_id = ensure_value(source_db, target_db, value_id)

                    {:ok, _} =
                      insert_one(target_db, :step_arguments, %{
                        step_id: new_step_id,
                        position: position,
                        value_id: new_value_id
                      })
                  end)

                  # Copy executions for this step
                  {:ok, execs} =
                    query(
                      source_db,
                      """
                      SELECT id, attempt, workspace_id, execute_after, created_at, created_by
                      FROM executions
                      WHERE step_id = ?1
                      """,
                      {old_step_id}
                    )

                  exec_acc =
                    Enum.reduce(execs, exec_acc, fn {old_exec_id, attempt, workspace_id,
                                                     execute_after, exec_created_at,
                                                     exec_created_by},
                                                    acc ->
                      {:ok, new_exec_id} =
                        insert_one(target_db, :executions, %{
                          step_id: new_step_id,
                          attempt: attempt,
                          workspace_id: remap_workspace_id(source_db, target_db, workspace_id),
                          execute_after: execute_after,
                          created_at: exec_created_at,
                          created_by: ensure_principal(source_db, target_db, exec_created_by)
                        })

                      Map.put(acc, old_exec_id, new_exec_id)
                    end)

                  {Map.put(step_acc, old_step_id, new_step_id), exec_acc}
                end)

              # Copy assignments
              Enum.each(execution_ids, fn {old_exec_id, new_exec_id} ->
                case query_one(
                       source_db,
                       "SELECT session_id, created_at FROM assignments WHERE execution_id = ?1",
                       {old_exec_id}
                     ) do
                  {:ok, {session_id, assign_created_at}} ->
                    new_session_id =
                      remap_session_id(source_db, target_db, session_id)

                    # Session may no longer exist (expired and not copied during rotation)
                    if new_session_id do
                      {:ok, _} =
                        insert_one(target_db, :assignments, %{
                          execution_id: new_exec_id,
                          session_id: new_session_id,
                          created_at: assign_created_at
                        })
                    end

                  {:ok, nil} ->
                    :ok
                end
              end)

              # Copy results
              visited =
                Enum.reduce(execution_ids, visited, fn {old_exec_id, new_exec_id}, visited ->
                  case query_one(
                         source_db,
                         """
                         SELECT type, error_id, value_id, successor_id, successor_ref_id, created_at, created_by
                         FROM results
                         WHERE execution_id = ?1
                         """,
                         {old_exec_id}
                       ) do
                    {:ok,
                     {type, error_id, value_id, successor_id, successor_ref_id, result_created_at,
                      result_created_by}} ->
                      new_error_id = if error_id, do: ensure_error(source_db, target_db, error_id)

                      {new_value_id, new_successor_id, new_successor_ref_id, visited} =
                        copy_result_successor(
                          source_db,
                          target_db,
                          type,
                          value_id,
                          successor_id,
                          successor_ref_id,
                          execution_ids,
                          visited
                        )

                      new_value_id =
                        cond do
                          new_value_id -> new_value_id
                          value_id && type == 1 -> ensure_value(source_db, target_db, value_id)
                          true -> nil
                        end

                      {:ok, _} =
                        insert_one(target_db, :results, %{
                          execution_id: new_exec_id,
                          type: type,
                          error_id: new_error_id,
                          value_id: new_value_id,
                          successor_id: new_successor_id,
                          successor_ref_id: new_successor_ref_id,
                          created_at: result_created_at,
                          created_by: ensure_principal(source_db, target_db, result_created_by)
                        })

                      visited

                    {:ok, nil} ->
                      visited
                  end
                end)

              # Copy children — same-run internal IDs
              {:ok, children} =
                query(
                  source_db,
                  """
                  SELECT c.parent_id, c.child_id, c.group_id, c.created_at
                  FROM children AS c
                  INNER JOIN executions AS e ON e.id = c.parent_id
                  INNER JOIN steps AS s ON s.id = e.step_id
                  WHERE s.run_id = ?1
                  """,
                  {old_run_id}
                )

              Enum.each(children, fn {old_parent, old_child, group_id, child_created_at} ->
                new_parent = Map.fetch!(execution_ids, old_parent)
                new_child = Map.fetch!(execution_ids, old_child)

                {:ok, _} =
                  insert_one(
                    target_db,
                    :children,
                    %{
                      parent_id: new_parent,
                      child_id: new_child,
                      group_id: group_id,
                      created_at: child_created_at
                    },
                    on_conflict: "DO NOTHING"
                  )
              end)

              # Copy per-execution data in a single pass
              Enum.each(execution_ids, fn {old_exec_id, new_exec_id} ->
                copy_execution_groups(source_db, target_db, old_exec_id, new_exec_id)
                copy_execution_heartbeats(source_db, target_db, old_exec_id, new_exec_id)
                copy_execution_result_deps(source_db, target_db, old_exec_id, new_exec_id)
                copy_execution_assets(source_db, target_db, old_exec_id, new_exec_id)
                copy_execution_asset_deps(source_db, target_db, old_exec_id, new_exec_id)
              end)

              {:ok,
               %{
                 run_ids: %{old_run_id => new_run_id},
                 execution_ids: execution_ids,
                 step_ids: step_ids
               }, visited}
          end
      end
    end
  end

  # Per-execution copy helpers

  defp copy_execution_groups(source_db, target_db, old_exec_id, new_exec_id) do
    {:ok, groups} =
      query(source_db, "SELECT group_id, name FROM groups WHERE execution_id = ?1", {old_exec_id})

    Enum.each(groups, fn {group_id, name} ->
      {:ok, _} =
        insert_one(
          target_db,
          :groups,
          %{execution_id: new_exec_id, group_id: group_id, name: name},
          on_conflict: "DO NOTHING"
        )
    end)
  end

  defp copy_execution_heartbeats(source_db, target_db, old_exec_id, new_exec_id) do
    {:ok, heartbeats} =
      query(
        source_db,
        "SELECT status, created_at FROM heartbeats WHERE execution_id = ?1",
        {old_exec_id}
      )

    Enum.each(heartbeats, fn {status, created_at} ->
      {:ok, _} =
        insert_one(target_db, :heartbeats, %{
          execution_id: new_exec_id,
          status: status,
          created_at: created_at
        })
    end)
  end

  defp copy_execution_result_deps(source_db, target_db, old_exec_id, new_exec_id) do
    {:ok, deps} =
      query(
        source_db,
        "SELECT dependency_ref_id, created_at FROM result_dependencies WHERE execution_id = ?1",
        {old_exec_id}
      )

    Enum.each(deps, fn {old_dep_ref_id, created_at} ->
      new_dep_ref_id = ensure_execution_ref(source_db, target_db, old_dep_ref_id)

      {:ok, _} =
        insert_one(
          target_db,
          :result_dependencies,
          %{execution_id: new_exec_id, dependency_ref_id: new_dep_ref_id, created_at: created_at},
          on_conflict: "DO NOTHING"
        )
    end)
  end

  defp copy_execution_assets(source_db, target_db, old_exec_id, new_exec_id) do
    {:ok, assets} =
      query(
        source_db,
        "SELECT asset_id, created_at FROM execution_assets WHERE execution_id = ?1",
        {old_exec_id}
      )

    Enum.each(assets, fn {asset_id, created_at} ->
      new_asset_id = ensure_asset(source_db, target_db, asset_id)

      {:ok, _} =
        insert_one(
          target_db,
          :execution_assets,
          %{execution_id: new_exec_id, asset_id: new_asset_id, created_at: created_at},
          on_conflict: "DO NOTHING"
        )
    end)
  end

  defp copy_execution_asset_deps(source_db, target_db, old_exec_id, new_exec_id) do
    {:ok, asset_deps} =
      query(
        source_db,
        "SELECT asset_id, created_at FROM asset_dependencies WHERE execution_id = ?1",
        {old_exec_id}
      )

    Enum.each(asset_deps, fn {asset_id, created_at} ->
      new_asset_id = ensure_asset(source_db, target_db, asset_id)

      {:ok, _} =
        insert_one(
          target_db,
          :asset_dependencies,
          %{execution_id: new_exec_id, asset_id: new_asset_id, created_at: created_at},
          on_conflict: "DO NOTHING"
        )
    end)
  end

  # Private helpers — config copy

  defp copy_workspaces(old_db, new_db) do
    {:ok, rows} = query(old_db, "SELECT id, external_id FROM workspaces")

    Enum.reduce(rows, %{}, fn {old_id, external_id}, acc ->
      {:ok, new_id} = insert_one(new_db, :workspaces, %{external_id: external_id})
      Map.put(acc, old_id, new_id)
    end)
  end

  defp copy_latest_workspace_names(old_db, new_db, workspace_ids) do
    Enum.each(workspace_ids, fn {old_ws_id, new_ws_id} ->
      case query_one(
             old_db,
             """
             SELECT name, created_at, created_by
             FROM workspace_names
             WHERE workspace_id = ?1
             ORDER BY created_at DESC
             LIMIT 1
             """,
             {old_ws_id}
           ) do
        {:ok, nil} ->
          :ok

        {:ok, {name, created_at, created_by}} ->
          {:ok, _} =
            insert_one(new_db, :workspace_names, %{
              workspace_id: new_ws_id,
              name: name,
              created_at: created_at,
              created_by: ensure_principal(old_db, new_db, created_by)
            })
      end
    end)
  end

  defp copy_latest_workspace_states(old_db, new_db, workspace_ids) do
    Enum.each(workspace_ids, fn {old_ws_id, new_ws_id} ->
      case query_one(
             old_db,
             """
             SELECT state, created_at, created_by
             FROM workspace_states
             WHERE workspace_id = ?1
             ORDER BY created_at DESC
             LIMIT 1
             """,
             {old_ws_id}
           ) do
        {:ok, nil} ->
          :ok

        {:ok, {state, created_at, created_by}} ->
          {:ok, _} =
            insert_one(new_db, :workspace_states, %{
              workspace_id: new_ws_id,
              state: state,
              created_at: created_at,
              created_by: ensure_principal(old_db, new_db, created_by)
            })
      end
    end)
  end

  defp copy_latest_workspace_bases(old_db, new_db, workspace_ids) do
    Enum.each(workspace_ids, fn {old_ws_id, new_ws_id} ->
      case query_one(
             old_db,
             """
             SELECT base_workspace_id, created_at, created_by
             FROM workspace_bases
             WHERE workspace_id = ?1
             ORDER BY created_at DESC
             LIMIT 1
             """,
             {old_ws_id}
           ) do
        {:ok, nil} ->
          :ok

        {:ok, {base_ws_id, created_at, created_by}} ->
          new_base_ws_id = if base_ws_id, do: Map.fetch!(workspace_ids, base_ws_id)

          {:ok, _} =
            insert_one(new_db, :workspace_bases, %{
              workspace_id: new_ws_id,
              base_workspace_id: new_base_ws_id,
              created_at: created_at,
              created_by: ensure_principal(old_db, new_db, created_by)
            })
      end
    end)
  end

  defp copy_pools(old_db, new_db, workspace_ids) do
    old_ws_ids = Map.keys(workspace_ids)

    if old_ws_ids == [] do
      %{}
    else
      copy_pools_query(old_db, new_db, workspace_ids, old_ws_ids)
    end
  end

  defp copy_pools_query(old_db, new_db, workspace_ids, old_ws_ids) do
    placeholders = Enum.map_join(1..length(old_ws_ids), ", ", &"?#{&1}")

    {:ok, rows} =
      query(
        old_db,
        """
        SELECT id, external_id, workspace_id, name,
          pool_definition_id, created_at, created_by
        FROM pools
        WHERE pool_definition_id IS NOT NULL
          AND workspace_id IN (#{placeholders})
        """,
        List.to_tuple(old_ws_ids)
      )

    Enum.reduce(rows, %{}, fn {old_id, ext_id, old_ws_id, name, old_pd_id, created_at, created_by},
                              acc ->
      new_ws_id = Map.fetch!(workspace_ids, old_ws_id)
      new_pd_id = ensure_pool_definition(old_db, new_db, old_pd_id)

      {:ok, new_id} =
        insert_one(new_db, :pools, %{
          external_id: ext_id,
          workspace_id: new_ws_id,
          name: name,
          pool_definition_id: new_pd_id,
          created_at: created_at,
          created_by: ensure_principal(old_db, new_db, created_by)
        })

      Map.put(acc, old_id, new_id)
    end)
  end

  defp copy_active_workers(old_db, new_db, pool_ids) do
    # Only copy workers that haven't been deactivated
    {:ok, rows} =
      query(old_db, """
        SELECT w.id, w.external_id, w.pool_id, w.created_at
        FROM workers AS w
        LEFT JOIN worker_deactivations AS wd ON wd.worker_id = w.id
        WHERE wd.worker_id IS NULL
      """)

    Enum.reduce(rows, %{}, fn {old_id, ext_id, old_pool_id, created_at}, acc ->
      new_pool_id = Map.fetch!(pool_ids, old_pool_id)

      {:ok, new_id} =
        insert_one(new_db, :workers, %{
          external_id: ext_id,
          pool_id: new_pool_id,
          created_at: created_at
        })

      # Copy worker_launch_results
      case query_one(
             old_db,
             "SELECT data, error, created_at FROM worker_launch_results WHERE worker_id = ?1",
             {old_id}
           ) do
        {:ok, {data, error, lr_created_at}} ->
          {:ok, _} =
            insert_one(new_db, :worker_launch_results, %{
              worker_id: new_id,
              data: if(data, do: {:blob, data}),
              error: error,
              created_at: lr_created_at
            })

        {:ok, nil} ->
          :ok
      end

      Map.put(acc, old_id, new_id)
    end)
  end

  defp copy_latest_worker_states(old_db, new_db, worker_ids) do
    Enum.each(worker_ids, fn {old_id, new_id} ->
      case query_one(
             old_db,
             "SELECT state, created_at, created_by FROM worker_states WHERE worker_id = ?1 ORDER BY created_at DESC LIMIT 1",
             {old_id}
           ) do
        {:ok, {state, created_at, created_by}} ->
          {:ok, _} =
            insert_one(new_db, :worker_states, %{
              worker_id: new_id,
              state: state,
              created_at: created_at,
              created_by: ensure_principal(old_db, new_db, created_by)
            })

        {:ok, nil} ->
          :ok
      end
    end)
  end

  defp copy_active_sessions(old_db, new_db, workspace_ids, worker_ids) do
    # Only copy sessions that haven't expired
    {:ok, rows} =
      query(old_db, """
        SELECT s.id, s.external_id, s.workspace_id, s.worker_id, s.provides_tag_set_id,
               s.concurrency, s.activation_timeout, s.reconnection_timeout, s.secret_hash,
               s.created_at, s.created_by
        FROM sessions AS s
        LEFT JOIN session_expirations AS se ON se.session_id = s.id
        WHERE se.session_id IS NULL
      """)

    Enum.reduce(rows, %{}, fn {old_id, ext_id, old_ws_id, old_worker_id, old_tag_set_id,
                               concurrency, activation_timeout, reconnection_timeout, secret_hash,
                               created_at, created_by},
                              acc ->
      new_ws_id = Map.fetch!(workspace_ids, old_ws_id)
      new_worker_id = if old_worker_id, do: Map.fetch!(worker_ids, old_worker_id)
      new_tag_set_id = ensure_tag_set(old_db, new_db, old_tag_set_id)
      new_created_by = ensure_principal(old_db, new_db, created_by)

      {:ok, new_id} =
        insert_one(new_db, :sessions, %{
          external_id: ext_id,
          workspace_id: new_ws_id,
          worker_id: new_worker_id,
          provides_tag_set_id: new_tag_set_id,
          concurrency: concurrency,
          activation_timeout: activation_timeout,
          reconnection_timeout: reconnection_timeout,
          secret_hash: if(secret_hash, do: {:blob, secret_hash}),
          created_at: created_at,
          created_by: new_created_by
        })

      Map.put(acc, old_id, new_id)
    end)
  end

  defp copy_session_activations(old_db, new_db, session_ids) do
    Enum.each(session_ids, fn {old_id, new_id} ->
      case query_one(
             old_db,
             "SELECT created_at FROM session_activations WHERE session_id = ?1",
             {old_id}
           ) do
        {:ok, {created_at}} ->
          {:ok, _} =
            insert_one(new_db, :session_activations, %{session_id: new_id, created_at: created_at})

        {:ok, nil} ->
          :ok
      end
    end)
  end

  defp copy_latest_workspace_manifests(old_db, new_db, workspace_ids) do
    Enum.each(workspace_ids, fn {old_ws_id, new_ws_id} ->
      # Get latest manifest per module for this workspace
      {:ok, rows} =
        query(
          old_db,
          """
            SELECT wm.module, wm.manifest_id, wm.created_at, wm.created_by
            FROM workspace_manifests AS wm
            INNER JOIN (
              SELECT workspace_id, module, MAX(created_at) AS max_created_at
              FROM workspace_manifests
              WHERE workspace_id = ?1
              GROUP BY module
            ) AS latest ON wm.workspace_id = latest.workspace_id AND wm.module = latest.module AND wm.created_at = latest.max_created_at
            WHERE wm.workspace_id = ?1
          """,
          {old_ws_id}
        )

      Enum.each(rows, fn {module, old_manifest_id, created_at, created_by} ->
        new_manifest_id = ensure_manifest(old_db, new_db, old_manifest_id)

        {:ok, _} =
          insert_one(new_db, :workspace_manifests, %{
            workspace_id: new_ws_id,
            module: module,
            manifest_id: new_manifest_id,
            created_at: created_at,
            created_by: ensure_principal(old_db, new_db, created_by)
          })
      end)
    end)
  end

  # Ensure a value exists in target_db, copying from source_db if needed
  defp ensure_value(source_db, target_db, value_id) do
    {:ok, {hash, content, blob_id}} =
      query_one!(
        source_db,
        "SELECT hash, content, blob_id FROM values_ WHERE id = ?1",
        {value_id}
      )

    case query_one(target_db, "SELECT id FROM values_ WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {existing_id}} ->
        existing_id

      {:ok, nil} ->
        new_blob_id = if blob_id, do: ensure_blob(source_db, target_db, blob_id)

        {:ok, new_id} =
          insert_one(target_db, :values_, %{
            hash: {:blob, hash},
            content: if(content, do: {:blob, content}),
            blob_id: new_blob_id
          })

        # Copy value_references
        {:ok, refs} =
          query(
            source_db,
            "SELECT position, fragment_id, execution_ref_id, asset_id FROM value_references WHERE value_id = ?1",
            {value_id}
          )

        Enum.each(refs, fn {position, fragment_id, execution_ref_id, asset_id} ->
          new_fragment_id = if fragment_id, do: ensure_fragment(source_db, target_db, fragment_id)
          new_asset_id = if asset_id, do: ensure_asset(source_db, target_db, asset_id)

          new_execution_ref_id =
            if execution_ref_id,
              do: ensure_execution_ref(source_db, target_db, execution_ref_id)

          {:ok, _} =
            insert_one(
              target_db,
              :value_references,
              %{
                value_id: new_id,
                position: position,
                fragment_id: new_fragment_id,
                execution_ref_id: new_execution_ref_id,
                asset_id: new_asset_id
              },
              on_conflict: "DO NOTHING"
            )
        end)

        new_id
    end
  end

  defp ensure_blob(source_db, target_db, blob_id) do
    {:ok, {key, size}} =
      query_one!(source_db, "SELECT key, size FROM blobs WHERE id = ?1", {blob_id})

    case query_one(target_db, "SELECT id FROM blobs WHERE key = ?1", {key}) do
      {:ok, {existing_id}} ->
        existing_id

      {:ok, nil} ->
        {:ok, new_id} = insert_one(target_db, :blobs, %{key: key, size: size})
        new_id
    end
  end

  defp ensure_fragment(source_db, target_db, fragment_id) do
    {:ok, {hash, format_id, blob_id}} =
      query_one!(
        source_db,
        "SELECT hash, format_id, blob_id FROM fragments WHERE id = ?1",
        {fragment_id}
      )

    case query_one(target_db, "SELECT id FROM fragments WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {existing_id}} ->
        existing_id

      {:ok, nil} ->
        new_blob_id = ensure_blob(source_db, target_db, blob_id)
        new_format_id = ensure_fragment_format(source_db, target_db, format_id)

        {:ok, new_id} =
          insert_one(target_db, :fragments, %{
            hash: {:blob, hash},
            format_id: new_format_id,
            blob_id: new_blob_id
          })

        # Copy fragment_metadata
        {:ok, metadata} =
          query(
            source_db,
            "SELECT key, value FROM fragment_metadata WHERE fragment_id = ?1",
            {fragment_id}
          )

        Enum.each(metadata, fn {key, value} ->
          {:ok, _} =
            insert_one(
              target_db,
              :fragment_metadata,
              %{fragment_id: new_id, key: key, value: value},
              on_conflict: "DO NOTHING"
            )
        end)

        new_id
    end
  end

  defp ensure_error(source_db, target_db, error_id) do
    {:ok, {hash, type, message}} =
      query_one!(source_db, "SELECT hash, type, message FROM errors WHERE id = ?1", {error_id})

    case query_one(target_db, "SELECT id FROM errors WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {existing_id}} ->
        existing_id

      {:ok, nil} ->
        {:ok, new_id} =
          insert_one(target_db, :errors, %{hash: {:blob, hash}, type: type, message: message})

        # Copy error_frames
        {:ok, frames} =
          query(
            source_db,
            "SELECT depth, file, line, name, code FROM error_frames WHERE error_id = ?1",
            {error_id}
          )

        Enum.each(frames, fn {depth, file, line, name, code} ->
          {:ok, _} =
            insert_one(
              target_db,
              :error_frames,
              %{error_id: new_id, depth: depth, file: file, line: line, name: name, code: code},
              on_conflict: "DO NOTHING"
            )
        end)

        new_id
    end
  end

  defp ensure_asset(source_db, target_db, asset_id) do
    {:ok, {ext_id, name, hash}} =
      query_one!(
        source_db,
        "SELECT external_id, name, hash FROM assets WHERE id = ?1",
        {asset_id}
      )

    case query_one(target_db, "SELECT id FROM assets WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {existing_id}} ->
        existing_id

      {:ok, nil} ->
        {:ok, new_id} =
          insert_one(target_db, :assets, %{external_id: ext_id, name: name, hash: {:blob, hash}})

        # Copy asset_entries and their metadata
        {:ok, entries} =
          query(
            source_db,
            "SELECT entry_id, path, blob_id FROM asset_entries WHERE asset_id = ?1",
            {asset_id}
          )

        Enum.each(entries, fn {old_entry_id, path, blob_id} ->
          new_blob_id = ensure_blob(source_db, target_db, blob_id)

          {:ok, new_entry_id} =
            insert_one(
              target_db,
              :asset_entries,
              %{asset_id: new_id, path: path, blob_id: new_blob_id},
              on_conflict: "DO NOTHING"
            )

          if new_entry_id do
            {:ok, metadata} =
              query(
                source_db,
                "SELECT key, value FROM asset_entry_metadata WHERE asset_entry_id = ?1",
                {old_entry_id}
              )

            Enum.each(metadata, fn {key, value} ->
              {:ok, _} =
                insert_one(
                  target_db,
                  :asset_entry_metadata,
                  %{asset_entry_id: new_entry_id, key: key, value: value},
                  on_conflict: "DO NOTHING"
                )
            end)
          end
        end)

        new_id
    end
  end

  # Remap helpers: resolve config-level IDs from source epoch to target epoch
  # via stable identifiers (external_id, hash).

  defp remap_workspace_id(_source_db, _target_db, nil), do: nil

  defp remap_workspace_id(source_db, target_db, old_id) do
    case query_one(source_db, "SELECT external_id FROM workspaces WHERE id = ?1", {old_id}) do
      {:ok, {ext_id}} ->
        case query_one(target_db, "SELECT id FROM workspaces WHERE external_id = ?1", {ext_id}) do
          {:ok, {new_id}} -> new_id
          {:ok, nil} -> nil
        end

      {:ok, nil} ->
        nil
    end
  end

  defp ensure_principal(_source_db, _target_db, nil), do: nil

  defp ensure_principal(source_db, target_db, old_id) do
    case query_one!(
           source_db,
           "SELECT user_external_id, token_id FROM principals WHERE id = ?1",
           {old_id}
         ) do
      {:ok, {user_ext_id, nil}} ->
        # User principal — find or create by user_external_id
        case query_one(
               target_db,
               "SELECT id FROM principals WHERE user_external_id = ?1",
               {user_ext_id}
             ) do
          {:ok, {existing_id}} ->
            existing_id

          {:ok, nil} ->
            {:ok, new_id} =
              insert_one(target_db, :principals, %{user_external_id: user_ext_id})

            new_id
        end

      {:ok, {nil, token_id}} ->
        # Token principal — ensure the token first, then find or create principal
        new_token_id = ensure_token(source_db, target_db, token_id)

        case query_one(target_db, "SELECT id FROM principals WHERE token_id = ?1", {new_token_id}) do
          {:ok, {existing_id}} ->
            existing_id

          {:ok, nil} ->
            {:ok, new_id} =
              insert_one(target_db, :principals, %{token_id: new_token_id})

            new_id
        end
    end
  end

  defp ensure_token(source_db, target_db, old_id) do
    {:ok, {ext_id, token_hash, name, workspaces, created_by, created_at, expires_at}} =
      query_one!(
        source_db,
        """
        SELECT external_id, token_hash, name, workspaces,
          created_by, created_at, expires_at
        FROM tokens
        WHERE id = ?1
        """,
        {old_id}
      )

    case query_one(target_db, "SELECT id FROM tokens WHERE external_id = ?1", {ext_id}) do
      {:ok, {existing_id}} ->
        existing_id

      {:ok, nil} ->
        new_created_by = ensure_principal(source_db, target_db, created_by)

        {:ok, new_id} =
          insert_one(target_db, :tokens, %{
            external_id: ext_id,
            token_hash: token_hash,
            name: name,
            workspaces: workspaces,
            created_by: new_created_by,
            created_at: created_at,
            expires_at: expires_at
          })

        new_id
    end
  end

  defp ensure_fragment_format(source_db, target_db, format_id) do
    {:ok, {name}} =
      query_one!(source_db, "SELECT name FROM fragment_formats WHERE id = ?1", {format_id})

    case query_one(target_db, "SELECT id FROM fragment_formats WHERE name = ?1", {name}) do
      {:ok, {existing_id}} ->
        existing_id

      {:ok, nil} ->
        {:ok, new_id} = insert_one(target_db, :fragment_formats, %{name: name})
        new_id
    end
  end

  defp ensure_tag_set(_source_db, _target_db, nil), do: nil

  defp ensure_tag_set(source_db, target_db, old_id) do
    {:ok, {hash}} =
      query_one!(source_db, "SELECT hash FROM tag_sets WHERE id = ?1", {old_id})

    case query_one(target_db, "SELECT id FROM tag_sets WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {existing_id}} ->
        existing_id

      {:ok, nil} ->
        {:ok, new_id} = insert_one(target_db, :tag_sets, %{hash: {:blob, hash}})

        {:ok, items} =
          query(source_db, "SELECT key, value FROM tag_set_items WHERE tag_set_id = ?1", {old_id})

        Enum.each(items, fn {key, value} ->
          {:ok, _} =
            insert_one(target_db, :tag_set_items, %{
              tag_set_id: new_id,
              key: key,
              value: value
            })
        end)

        new_id
    end
  end

  defp ensure_parameter_set(source_db, target_db, old_id) do
    {:ok, {hash}} =
      query_one!(source_db, "SELECT hash FROM parameter_sets WHERE id = ?1", {old_id})

    case query_one(target_db, "SELECT id FROM parameter_sets WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {existing_id}} ->
        existing_id

      {:ok, nil} ->
        {:ok, new_id} = insert_one(target_db, :parameter_sets, %{hash: {:blob, hash}})

        {:ok, items} =
          query(
            source_db,
            "SELECT position, name, default_, annotation FROM parameter_set_items WHERE parameter_set_id = ?1 ORDER BY position",
            {old_id}
          )

        Enum.each(items, fn {position, name, default_, annotation} ->
          {:ok, _} =
            insert_one(target_db, :parameter_set_items, %{
              parameter_set_id: new_id,
              position: position,
              name: name,
              default_: default_,
              annotation: annotation
            })
        end)

        new_id
    end
  end

  defp ensure_instruction(_source_db, _target_db, nil), do: nil

  defp ensure_instruction(source_db, target_db, old_id) do
    {:ok, {hash, content}} =
      query_one!(source_db, "SELECT hash, content FROM instructions WHERE id = ?1", {old_id})

    case query_one(target_db, "SELECT id FROM instructions WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {existing_id}} ->
        existing_id

      {:ok, nil} ->
        {:ok, new_id} =
          insert_one(target_db, :instructions, %{hash: {:blob, hash}, content: content})

        new_id
    end
  end

  defp ensure_cache_config(_source_db, _target_db, nil), do: nil

  defp ensure_cache_config(source_db, target_db, old_id) do
    {:ok, {hash, params, max_age, namespace, version}} =
      query_one!(
        source_db,
        "SELECT hash, params, max_age, namespace, version FROM cache_configs WHERE id = ?1",
        {old_id}
      )

    case query_one(target_db, "SELECT id FROM cache_configs WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {existing_id}} ->
        existing_id

      {:ok, nil} ->
        {:ok, new_id} =
          insert_one(target_db, :cache_configs, %{
            hash: {:blob, hash},
            params: params,
            max_age: max_age,
            namespace: namespace,
            version: version
          })

        new_id
    end
  end

  defp ensure_manifest(_source_db, _target_db, nil), do: nil

  defp ensure_manifest(source_db, target_db, old_id) do
    {:ok, {hash}} =
      query_one!(source_db, "SELECT hash FROM manifests WHERE id = ?1", {old_id})

    case query_one(target_db, "SELECT id FROM manifests WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {existing_id}} ->
        existing_id

      {:ok, nil} ->
        {:ok, new_id} = insert_one(target_db, :manifests, %{hash: {:blob, hash}})

        # Copy workflows for this manifest
        {:ok, workflows} =
          query(
            source_db,
            """
            SELECT name, parameter_set_id, instruction_id, wait_for,
              cache_config_id, defer_params, delay, retry_limit,
              retry_delay_min, retry_delay_max, recurrent, requires_tag_set_id
            FROM workflows
            WHERE manifest_id = ?1
            """,
            {old_id}
          )

        Enum.each(workflows, fn {name, ps_id, instr_id, wait_for, cc_id, defer_params, delay,
                                 retry_limit, retry_delay_min, retry_delay_max, recurrent,
                                 rts_id} ->
          {:ok, _} =
            insert_one(target_db, :workflows, %{
              manifest_id: new_id,
              name: name,
              parameter_set_id: ensure_parameter_set(source_db, target_db, ps_id),
              instruction_id: ensure_instruction(source_db, target_db, instr_id),
              wait_for: wait_for,
              cache_config_id: ensure_cache_config(source_db, target_db, cc_id),
              defer_params: defer_params,
              delay: delay,
              retry_limit: retry_limit,
              retry_delay_min: retry_delay_min,
              retry_delay_max: retry_delay_max,
              recurrent: recurrent,
              requires_tag_set_id: ensure_tag_set(source_db, target_db, rts_id)
            })
        end)

        new_id
    end
  end

  defp ensure_launcher(_source_db, _target_db, nil), do: nil

  defp ensure_launcher(source_db, target_db, old_id) do
    {:ok, {hash, type, config}} =
      query_one!(source_db, "SELECT hash, type, config FROM launchers WHERE id = ?1", {old_id})

    case query_one(target_db, "SELECT id FROM launchers WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {existing_id}} ->
        existing_id

      {:ok, nil} ->
        {:ok, new_id} =
          insert_one(target_db, :launchers, %{hash: {:blob, hash}, type: type, config: config})

        new_id
    end
  end

  defp ensure_pool_definition(_source_db, _target_db, nil), do: nil

  defp ensure_pool_definition(source_db, target_db, old_id) do
    {:ok, {hash, launcher_id, provides_tag_set_id}} =
      query_one!(
        source_db,
        "SELECT hash, launcher_id, provides_tag_set_id FROM pool_definitions WHERE id = ?1",
        {old_id}
      )

    case query_one(
           target_db,
           "SELECT id FROM pool_definitions WHERE hash = ?1",
           {{:blob, hash}}
         ) do
      {:ok, {existing_id}} ->
        existing_id

      {:ok, nil} ->
        {:ok, new_id} =
          insert_one(target_db, :pool_definitions, %{
            hash: {:blob, hash},
            launcher_id: ensure_launcher(source_db, target_db, launcher_id),
            provides_tag_set_id: ensure_tag_set(source_db, target_db, provides_tag_set_id)
          })

        # Copy pool_definition_modules
        {:ok, modules} =
          query(
            source_db,
            "SELECT pattern FROM pool_definition_modules WHERE pool_definition_id = ?1",
            {old_id}
          )

        Enum.each(modules, fn {pattern} ->
          {:ok, _} =
            insert_one(target_db, :pool_definition_modules, %{
              pool_definition_id: new_id,
              pattern: pattern
            })
        end)

        new_id
    end
  end

  defp remap_session_id(_source_db, _target_db, nil), do: nil

  defp remap_session_id(source_db, target_db, old_id) do
    case query_one(source_db, "SELECT external_id FROM sessions WHERE id = ?1", {old_id}) do
      {:ok, {ext_id}} ->
        case query_one(target_db, "SELECT id FROM sessions WHERE external_id = ?1", {ext_id}) do
          {:ok, {new_id}} -> new_id
          {:ok, nil} -> nil
        end

      {:ok, nil} ->
        nil
    end
  end

  # Copy an execution_ref from source_db to target_db by its stable triple.
  defp ensure_execution_ref(source_db, target_db, old_ref_id) do
    {:ok, {run_ext_id, step_num, attempt, module, target}} =
      query_one!(
        source_db,
        "SELECT run_external_id, step_number, attempt, module, target FROM execution_refs WHERE id = ?1",
        {old_ref_id}
      )

    {:ok, ref_id} =
      Runs.get_or_create_execution_ref(target_db, run_ext_id, step_num, attempt, module, target)

    ref_id
  end

  # Create an execution_ref in target_db from an internal execution_id in source_db.
  defp create_execution_ref_for_id(source_db, target_db, execution_id) do
    {:ok, {run_ext_id, step_num, attempt, module, target}} =
      query_one!(
        source_db,
        """
        SELECT r.external_id, s.number, e.attempt, s.module, s.target
        FROM executions AS e
        INNER JOIN steps AS s ON s.id = e.step_id
        INNER JOIN runs AS r ON r.id = s.run_id
        WHERE e.id = ?1
        """,
        {execution_id}
      )

    {:ok, ref_id} =
      Runs.get_or_create_execution_ref(target_db, run_ext_id, step_num, attempt, module, target)

    ref_id
  end

  # Handle successor copying for result types.
  # Returns {new_value_id, new_successor_id, new_successor_ref_id}.
  defp copy_result_successor(
         _source_db,
         _target_db,
         type,
         _value_id,
         _successor_id,
         _successor_ref_id,
         _execution_ids,
         visited
       )
       when type in [1, 3] do
    # Type 1 (value) and type 3 (cancelled) have no successor
    {nil, nil, nil, visited}
  end

  defp copy_result_successor(
         _source_db,
         _target_db,
         type,
         _value_id,
         successor_id,
         _successor_ref_id,
         execution_ids,
         visited
       )
       when type in [0, 2, 6] do
    # Types 0 (error retry), 2 (abandoned retry), 6 (suspended) — same-run successor
    new_successor_id = if successor_id, do: Map.fetch!(execution_ids, successor_id)
    {nil, new_successor_id, nil, visited}
  end

  defp copy_result_successor(
         source_db,
         target_db,
         _type,
         value_id,
         successor_id,
         successor_ref_id,
         execution_ids,
         visited
       ) do
    # Types 4 (deferred), 5 (cached), 7 (spawned)
    cond do
      # Already resolved: successor_ref_id + value_id both set
      successor_ref_id != nil ->
        new_ref_id = ensure_execution_ref(source_db, target_db, successor_ref_id)
        new_value_id = ensure_value(source_db, target_db, value_id)
        {new_value_id, nil, new_ref_id, visited}

      # In-flight, same run
      successor_id != nil and is_map_key(execution_ids, successor_id) ->
        {nil, Map.fetch!(execution_ids, successor_id), nil, visited}

      # In-flight, cross-run — try to resolve the chain in source_db
      successor_id != nil ->
        resolve_cross_run_successor(source_db, target_db, successor_id, visited)

      # No successor (shouldn't happen for these types)
      true ->
        {nil, nil, nil, visited}
    end
  end

  # For cross-run successors (types 4, 5, 7): try to resolve the result chain.
  # If resolved to a value, copy the value and create an execution_ref.
  # If still pending, copy the target run and remap the successor_id.
  defp resolve_cross_run_successor(source_db, target_db, successor_id, visited) do
    case resolve_result_chain(source_db, successor_id, MapSet.new()) do
      {:ok, chain_value_id} ->
        # Resolved to a value — copy value and create execution_ref for the successor
        new_value_id = ensure_value(source_db, target_db, chain_value_id)
        new_ref_id = create_execution_ref_for_id(source_db, target_db, successor_id)
        {new_value_id, nil, new_ref_id, visited}

      _ ->
        # Pending or cancelled — copy the target run, then remap successor_id
        {:ok, {run_ext_id, step_num, attempt}} =
          query_one!(
            source_db,
            """
            SELECT r.external_id, s.number, e.attempt
            FROM executions AS e
            INNER JOIN steps AS s ON s.id = e.step_id
            INNER JOIN runs AS r ON r.id = s.run_id
            WHERE e.id = ?1
            """,
            {successor_id}
          )

        {:ok, _remap, visited} =
          do_copy_run(source_db, target_db, run_ext_id, visited)

        case query_one(
               target_db,
               """
               SELECT e.id
               FROM executions AS e
               INNER JOIN steps AS s ON s.id = e.step_id
               INNER JOIN runs AS r ON r.id = s.run_id
               WHERE r.external_id = ?1 AND s.number = ?2 AND e.attempt = ?3
               """,
               {run_ext_id, step_num, attempt}
             ) do
          {:ok, {new_id}} -> {nil, new_id, nil, visited}
          {:ok, nil} -> {nil, nil, nil, visited}
        end
    end
  end

  # Follow the result successor chain in a single DB to find a terminal value.
  defp resolve_result_chain(db, execution_id, visited) do
    if MapSet.member?(visited, execution_id) do
      :pending
    else
      visited = MapSet.put(visited, execution_id)

      case query_one(
             db,
             "SELECT type, value_id, successor_id, successor_ref_id FROM results WHERE execution_id = ?1",
             {execution_id}
           ) do
        {:ok, nil} ->
          :pending

        # Plain value
        {:ok, {1, value_id, nil, nil}} ->
          {:ok, value_id}

        # Cancelled
        {:ok, {3, nil, nil, nil}} ->
          :cancelled

        # Types 4, 5, 7 already resolved (successor_ref_id + value_id)
        {:ok, {type, value_id, nil, successor_ref_id}}
        when type in [4, 5, 7] and not is_nil(successor_ref_id) and not is_nil(value_id) ->
          {:ok, value_id}

        # Any type with successor_id — follow the chain
        {:ok, {_type, nil, successor_id, nil}} when not is_nil(successor_id) ->
          resolve_result_chain(db, successor_id, visited)

        _ ->
          :pending
      end
    end
  end
end
