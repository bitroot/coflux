defmodule Coflux.Orchestration.EpochCopy do
  @moduledoc """
  Copies data between epoch databases during rotation and copy-on-reference.
  """

  import Coflux.Store

  @doc """
  Copy config entities from old_db to new_db with latest-state-only semantics.
  Returns a map of ID remappings: %{workspace_ids: %{old→new}, session_ids: %{old→new}, ...}
  """
  def copy_config(old_db, new_db) do
    with_transaction(new_db, fn ->
      # 1. Copy principals and tokens
      principal_ids = copy_table_with_remap(old_db, new_db, "principals")
      token_ids = copy_tokens(old_db, new_db, principal_ids)

      # Update principal token_id references
      Enum.each(principal_ids, fn {old_id, new_id} ->
        case query_one(old_db, "SELECT token_id FROM principals WHERE id = ?1", {old_id}) do
          {:ok, {token_id}} when not is_nil(token_id) ->
            new_token_id = Map.get(token_ids, token_id, token_id)

            execute(
              new_db,
              "UPDATE principals SET token_id = ?1 WHERE id = ?2",
              {new_token_id, new_id}
            )

          _ ->
            :ok
        end
      end)

      # 2. Copy fragment_formats (small table, copy all)
      fragment_format_ids = copy_table_with_remap(old_db, new_db, "fragment_formats")

      # 3. Copy tag_sets and tag_set_items (by hash, dedup)
      tag_set_ids = copy_hash_table(old_db, new_db, "tag_sets")
      copy_dependent_rows(old_db, new_db, "tag_set_items", "tag_set_id", tag_set_ids)

      # 4. Copy parameter_sets and items (by hash, dedup)
      parameter_set_ids = copy_hash_table(old_db, new_db, "parameter_sets")

      copy_dependent_rows(
        old_db,
        new_db,
        "parameter_set_items",
        "parameter_set_id",
        parameter_set_ids
      )

      # 5. Copy instructions (by hash, dedup)
      instruction_ids = copy_hash_table(old_db, new_db, "instructions")

      # 6. Copy cache_configs (by hash, dedup)
      cache_config_ids = copy_hash_table(old_db, new_db, "cache_configs")

      # 7. Copy workspaces
      workspace_ids = copy_table_with_remap(old_db, new_db, "workspaces")

      # Copy latest workspace_names per workspace
      copy_latest_per_workspace(old_db, new_db, "workspace_names", workspace_ids)

      # Copy latest workspace_states per workspace
      copy_latest_per_workspace(old_db, new_db, "workspace_states", workspace_ids)

      # Copy latest workspace_bases per workspace
      copy_latest_workspace_bases(old_db, new_db, workspace_ids)

      # 8. Copy launchers (by hash, dedup)
      launcher_ids = copy_hash_table(old_db, new_db, "launchers")

      # 9. Copy pool_definitions (by hash, dedup)
      pool_definition_ids = copy_hash_table(old_db, new_db, "pool_definitions")

      copy_dependent_rows(
        old_db,
        new_db,
        "pool_definition_modules",
        "pool_definition_id",
        pool_definition_ids
      )

      # 10. Copy pools (remap workspace_id, pool_definition_id)
      pool_ids = copy_pools(old_db, new_db, workspace_ids, pool_definition_ids)

      # 11. Copy active workers (remap pool_id)
      worker_ids = copy_active_workers(old_db, new_db, pool_ids)

      # Copy latest worker_states
      copy_latest_worker_states(old_db, new_db, worker_ids)

      # 12. Copy active sessions (remap workspace_id, worker_id)
      session_ids =
        copy_active_sessions(
          old_db,
          new_db,
          workspace_ids,
          worker_ids,
          tag_set_ids,
          principal_ids
        )

      # Copy session_activations for active sessions
      copy_session_activations(old_db, new_db, session_ids)

      # 13. Copy manifests (by hash, dedup)
      manifest_ids = copy_hash_table(old_db, new_db, "manifests")

      # 14. Copy latest workspace_manifests
      copy_latest_workspace_manifests(old_db, new_db, workspace_ids, manifest_ids, principal_ids)

      # 15. Copy workflows for copied manifests
      copy_workflows(
        old_db,
        new_db,
        manifest_ids,
        parameter_set_ids,
        instruction_ids,
        cache_config_ids,
        tag_set_ids
      )

      %{
        workspace_ids: workspace_ids,
        session_ids: session_ids,
        worker_ids: worker_ids,
        pool_ids: pool_ids,
        principal_ids: principal_ids,
        token_ids: token_ids,
        tag_set_ids: tag_set_ids,
        manifest_ids: manifest_ids,
        launcher_ids: launcher_ids,
        pool_definition_ids: pool_definition_ids,
        parameter_set_ids: parameter_set_ids,
        instruction_ids: instruction_ids,
        cache_config_ids: cache_config_ids,
        fragment_format_ids: fragment_format_ids
      }
    end)
  end

  @doc """
  Copy a run and all its data from source_db to target_db.
  Follows cross-run references recursively.
  Returns ID remapping for the copied data.
  """
  def copy_run(source_db, target_db, run_external_id, visited \\ MapSet.new()) do
    if MapSet.member?(visited, run_external_id) do
      {:ok, %{}, visited}
    else
      visited = MapSet.put(visited, run_external_id)

      case query_one(
             source_db,
             """
             SELECT id, external_id, parent_id, idempotency_key, created_at, created_by
             FROM runs
             WHERE external_id = ?1
             """,
             {run_external_id}
           ) do
        {:ok, nil} ->
          {:ok, %{}, visited}

        {:ok, {old_run_id, ext_id, parent_id, idempotency_key, created_at, created_by}} ->
          with_transaction(target_db, fn ->
            # Check if already exists in target
            case query_one(target_db, "SELECT id FROM runs WHERE external_id = ?1", {ext_id}) do
              {:ok, {_existing_id}} ->
                {:ok, %{}, visited}

              {:ok, nil} ->
                # Copy referenced runs first (parent)
                {visited, parent_id_new} =
                  if parent_id do
                    # parent_id references an execution - find its run
                    case query_one(
                           source_db,
                           """
                           SELECT r.external_id
                           FROM executions AS e
                           INNER JOIN steps AS s ON s.id = e.step_id
                           INNER JOIN runs AS r ON r.id = s.run_id
                           WHERE e.id = ?1
                           """,
                           {parent_id}
                         ) do
                      {:ok, {parent_run_ext_id}} ->
                        {:ok, _remap, visited} =
                          copy_run(source_db, target_db, parent_run_ext_id, visited)

                        # Re-resolve the parent execution ID in the target DB
                        case query_one(
                               source_db,
                               """
                               SELECT r.external_id, s.number, e.attempt
                               FROM executions AS e
                               INNER JOIN steps AS s ON s.id = e.step_id
                               INNER JOIN runs AS r ON r.id = s.run_id
                               WHERE e.id = ?1
                               """,
                               {parent_id}
                             ) do
                          {:ok, {p_run_ext, p_step_num, p_attempt}} ->
                            case query_one(
                                   target_db,
                                   """
                                   SELECT e.id
                                   FROM executions AS e
                                   INNER JOIN steps AS s ON s.id = e.step_id
                                   INNER JOIN runs AS r ON r.id = s.run_id
                                   WHERE r.external_id = ?1 AND s.number = ?2 AND e.attempt = ?3
                                   """,
                                   {p_run_ext, p_step_num, p_attempt}
                                 ) do
                              {:ok, {new_parent_id}} -> {visited, new_parent_id}
                              {:ok, nil} -> {visited, nil}
                            end

                          {:ok, nil} ->
                            {visited, nil}
                        end

                      {:ok, nil} ->
                        {visited, nil}
                    end
                  else
                    {visited, nil}
                  end

                # Insert the run
                {:ok, new_run_id} =
                  insert_one(target_db, :runs, %{
                    external_id: ext_id,
                    parent_id: parent_id_new,
                    idempotency_key: idempotency_key,
                    created_at: created_at,
                    created_by: created_by
                  })

                # Copy all steps and their data
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
                    """,
                    {old_run_id}
                  )

                step_ids =
                  Enum.reduce(steps, %{}, fn {old_step_id, number, _run_id, step_parent_id,
                                              module, target, type, priority, wait_for,
                                              cache_config_id, cache_key, defer_key, memo_key,
                                              retry_limit, retry_delay_min, retry_delay_max,
                                              recurrent, delay, requires_tag_set_id,
                                              step_created_at},
                                             acc ->
                    # step parent_id references an execution - will be resolved after executions are copied.
                    # Use 0 as a sentinel for non-initial steps to avoid violating
                    # the UNIQUE partial index on (run_id) WHERE parent_id IS NULL.
                    {:ok, new_step_id} =
                      insert_one(target_db, :steps, %{
                        number: number,
                        run_id: new_run_id,
                        parent_id: if(step_parent_id, do: 0),
                        module: module,
                        target: target,
                        type: type,
                        priority: priority,
                        wait_for: wait_for,
                        cache_config_id: cache_config_id,
                        cache_key: if(cache_key, do: {:blob, cache_key}),
                        defer_key: if(defer_key, do: {:blob, defer_key}),
                        memo_key: if(memo_key, do: {:blob, memo_key}),
                        retry_limit: retry_limit,
                        retry_delay_min: retry_delay_min,
                        retry_delay_max: retry_delay_max,
                        recurrent: recurrent,
                        delay: delay,
                        requires_tag_set_id: requires_tag_set_id,
                        created_at: step_created_at
                      })

                    # Copy step_arguments
                    {:ok, args} =
                      query(
                        source_db,
                        "SELECT step_id, position, value_id FROM step_arguments WHERE step_id = ?1 ORDER BY position",
                        {old_step_id}
                      )

                    Enum.each(args, fn {_old_sid, position, value_id} ->
                      # Copy value if needed
                      new_value_id = ensure_value(source_db, target_db, value_id)

                      insert_one(target_db, :step_arguments, %{
                        step_id: new_step_id,
                        position: position,
                        value_id: new_value_id
                      })
                    end)

                    Map.put(acc, old_step_id, {new_step_id, step_parent_id})
                  end)

                # Copy executions
                {:ok, execs} =
                  query(
                    source_db,
                    """
                    SELECT e.id, e.step_id, e.attempt, e.workspace_id,
                      e.execute_after, e.created_at, e.created_by
                    FROM executions AS e
                    INNER JOIN steps AS s ON s.id = e.step_id
                    WHERE s.run_id = ?1
                    """,
                    {old_run_id}
                  )

                execution_ids =
                  Enum.reduce(execs, %{}, fn {old_exec_id, old_step_id, attempt, workspace_id,
                                              execute_after, exec_created_at, exec_created_by},
                                             acc ->
                    {new_step_id, _} = Map.fetch!(step_ids, old_step_id)

                    {:ok, new_exec_id} =
                      insert_one(target_db, :executions, %{
                        step_id: new_step_id,
                        attempt: attempt,
                        workspace_id: workspace_id,
                        execute_after: execute_after,
                        created_at: exec_created_at,
                        created_by: exec_created_by
                      })

                    Map.put(acc, old_exec_id, new_exec_id)
                  end)

                # Update step parent_ids (they reference executions)
                Enum.each(step_ids, fn {_old_step_id, {new_step_id, old_parent_exec_id}} ->
                  if old_parent_exec_id do
                    new_parent_exec_id = Map.get(execution_ids, old_parent_exec_id)

                    if new_parent_exec_id do
                      execute(
                        target_db,
                        "UPDATE steps SET parent_id = ?1 WHERE id = ?2",
                        {new_parent_exec_id, new_step_id}
                      )
                    end
                  end
                end)

                # Copy assignments
                Enum.each(execution_ids, fn {old_exec_id, new_exec_id} ->
                  case query_one(
                         source_db,
                         "SELECT session_id, created_at FROM assignments WHERE execution_id = ?1",
                         {old_exec_id}
                       ) do
                    {:ok, {session_id, assign_created_at}} ->
                      insert_one(target_db, :assignments, %{
                        execution_id: new_exec_id,
                        session_id: session_id,
                        created_at: assign_created_at
                      })

                    {:ok, nil} ->
                      :ok
                  end
                end)

                # Copy results
                Enum.each(execution_ids, fn {old_exec_id, new_exec_id} ->
                  case query_one(
                         source_db,
                         """
                         SELECT type, error_id, value_id, successor_id, created_at, created_by
                         FROM results
                         WHERE execution_id = ?1
                         """,
                         {old_exec_id}
                       ) do
                    {:ok,
                     {type, error_id, value_id, successor_id, result_created_at,
                      result_created_by}} ->
                      new_error_id = if error_id, do: ensure_error(source_db, target_db, error_id)
                      new_value_id = if value_id, do: ensure_value(source_db, target_db, value_id)

                      new_successor_id =
                        if successor_id, do: Map.get(execution_ids, successor_id, successor_id)

                      insert_one(target_db, :results, %{
                        execution_id: new_exec_id,
                        type: type,
                        error_id: new_error_id,
                        value_id: new_value_id,
                        successor_id: new_successor_id,
                        created_at: result_created_at,
                        created_by: result_created_by
                      })

                    {:ok, nil} ->
                      :ok
                  end
                end)

                # Copy children
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
                  new_parent = Map.get(execution_ids, old_parent)
                  new_child = Map.get(execution_ids, old_child)

                  if new_parent && new_child do
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
                  end
                end)

                # Copy groups
                Enum.each(execution_ids, fn {old_exec_id, new_exec_id} ->
                  {:ok, groups} =
                    query(
                      source_db,
                      "SELECT group_id, name FROM groups WHERE execution_id = ?1",
                      {old_exec_id}
                    )

                  Enum.each(groups, fn {group_id, name} ->
                    insert_one(
                      target_db,
                      :groups,
                      %{execution_id: new_exec_id, group_id: group_id, name: name},
                      on_conflict: "DO NOTHING"
                    )
                  end)
                end)

                # Copy heartbeats
                Enum.each(execution_ids, fn {old_exec_id, new_exec_id} ->
                  {:ok, heartbeats} =
                    query(
                      source_db,
                      "SELECT status, created_at FROM heartbeats WHERE execution_id = ?1",
                      {old_exec_id}
                    )

                  Enum.each(heartbeats, fn {status, hb_created_at} ->
                    insert_one(target_db, :heartbeats, %{
                      execution_id: new_exec_id,
                      status: status,
                      created_at: hb_created_at
                    })
                  end)
                end)

                # Copy result_dependencies
                Enum.each(execution_ids, fn {old_exec_id, new_exec_id} ->
                  {:ok, deps} =
                    query(
                      source_db,
                      "SELECT dependency_id, created_at FROM result_dependencies WHERE execution_id = ?1",
                      {old_exec_id}
                    )

                  Enum.each(deps, fn {old_dep_id, dep_created_at} ->
                    new_dep_id = Map.get(execution_ids, old_dep_id, old_dep_id)

                    insert_one(
                      target_db,
                      :result_dependencies,
                      %{
                        execution_id: new_exec_id,
                        dependency_id: new_dep_id,
                        created_at: dep_created_at
                      },
                      on_conflict: "DO NOTHING"
                    )
                  end)
                end)

                # Copy execution_assets
                Enum.each(execution_ids, fn {old_exec_id, new_exec_id} ->
                  {:ok, assets} =
                    query(
                      source_db,
                      "SELECT asset_id, created_at FROM execution_assets WHERE execution_id = ?1",
                      {old_exec_id}
                    )

                  Enum.each(assets, fn {asset_id, asset_created_at} ->
                    new_asset_id = ensure_asset(source_db, target_db, asset_id)

                    insert_one(
                      target_db,
                      :execution_assets,
                      %{
                        execution_id: new_exec_id,
                        asset_id: new_asset_id,
                        created_at: asset_created_at
                      },
                      on_conflict: "DO NOTHING"
                    )
                  end)
                end)

                # Copy asset_dependencies
                Enum.each(execution_ids, fn {old_exec_id, new_exec_id} ->
                  {:ok, asset_deps} =
                    query(
                      source_db,
                      "SELECT asset_id, created_at FROM asset_dependencies WHERE execution_id = ?1",
                      {old_exec_id}
                    )

                  Enum.each(asset_deps, fn {asset_id, ad_created_at} ->
                    new_asset_id = ensure_asset(source_db, target_db, asset_id)

                    insert_one(
                      target_db,
                      :asset_dependencies,
                      %{
                        execution_id: new_exec_id,
                        asset_id: new_asset_id,
                        created_at: ad_created_at
                      },
                      on_conflict: "DO NOTHING"
                    )
                  end)
                end)

                {:ok,
                 %{
                   run_ids: %{old_run_id => new_run_id},
                   execution_ids: execution_ids,
                   step_ids: Map.new(step_ids, fn {old, {new, _}} -> {old, new} end)
                 }, visited}
            end
          end)
      end
    end
  end

  # Private helpers

  defp execute(db, sql, args) do
    with_prepare(db, sql, fn statement ->
      :ok = Exqlite.Sqlite3.bind(db, statement, Tuple.to_list(args))
      :done = Exqlite.Sqlite3.step(db, statement)
      :ok
    end)
  end

  defp copy_table_with_remap(old_db, new_db, table) do
    {:ok, rows} = query(old_db, "SELECT * FROM #{table}")

    if Enum.empty?(rows) do
      %{}
    else
      {:ok, columns} =
        with_prepare(old_db, "SELECT * FROM #{table} LIMIT 0", fn statement ->
          Exqlite.Sqlite3.columns(old_db, statement)
        end)

      col_atoms = Enum.map(columns, &String.to_atom/1)

      Enum.reduce(rows, %{}, fn row, acc ->
        values = Enum.zip(col_atoms, Tuple.to_list(row)) |> Map.new()
        old_id = values[:id]
        values = Map.delete(values, :id)

        case insert_one(new_db, String.to_atom(table), values) do
          {:ok, new_id} ->
            Map.put(acc, old_id, new_id)
        end
      end)
    end
  end

  defp copy_hash_table(old_db, new_db, table) do
    {:ok, rows} = query(old_db, "SELECT * FROM #{table}")

    if Enum.empty?(rows) do
      %{}
    else
      {:ok, columns} =
        with_prepare(old_db, "SELECT * FROM #{table} LIMIT 0", fn statement ->
          Exqlite.Sqlite3.columns(old_db, statement)
        end)

      col_atoms = Enum.map(columns, &String.to_atom/1)

      Enum.reduce(rows, %{}, fn row, acc ->
        values = Enum.zip(col_atoms, Tuple.to_list(row)) |> Map.new()
        old_id = values[:id]
        hash = values[:hash]

        # Check if already exists in target by hash
        case query_one(new_db, "SELECT id FROM #{table} WHERE hash = ?1", {{:blob, hash}}) do
          {:ok, {existing_id}} ->
            Map.put(acc, old_id, existing_id)

          {:ok, nil} ->
            values = values |> Map.delete(:id) |> Map.put(:hash, {:blob, hash})
            {:ok, new_id} = insert_one(new_db, String.to_atom(table), values)
            Map.put(acc, old_id, new_id)
        end
      end)
    end
  end

  defp copy_dependent_rows(old_db, new_db, table, fk_column, id_map) do
    Enum.each(id_map, fn {old_id, new_id} ->
      {:ok, rows} = query(old_db, "SELECT * FROM #{table} WHERE #{fk_column} = ?1", {old_id})

      unless Enum.empty?(rows) do
        {:ok, columns} =
          with_prepare(old_db, "SELECT * FROM #{table} LIMIT 0", fn statement ->
            Exqlite.Sqlite3.columns(old_db, statement)
          end)

        col_atoms = Enum.map(columns, &String.to_atom/1)

        Enum.each(rows, fn row ->
          values =
            Enum.zip(col_atoms, Tuple.to_list(row))
            |> Map.new()
            |> Map.delete(:id)
            |> Map.put(String.to_atom(fk_column), new_id)

          insert_one(new_db, String.to_atom(table), values, on_conflict: "DO NOTHING")
        end)
      end
    end)
  end

  defp copy_latest_per_workspace(old_db, new_db, table, workspace_ids) do
    Enum.each(workspace_ids, fn {old_ws_id, new_ws_id} ->
      case query_one(
             old_db,
             "SELECT * FROM #{table} WHERE workspace_id = ?1 ORDER BY created_at DESC LIMIT 1",
             {old_ws_id}
           ) do
        {:ok, nil} ->
          :ok

        {:ok, row} ->
          {:ok, columns} =
            with_prepare(old_db, "SELECT * FROM #{table} LIMIT 0", fn statement ->
              Exqlite.Sqlite3.columns(old_db, statement)
            end)

          col_atoms = Enum.map(columns, &String.to_atom/1)

          values =
            Enum.zip(col_atoms, Tuple.to_list(row))
            |> Map.new()
            |> Map.delete(:id)
            |> Map.put(:workspace_id, new_ws_id)

          insert_one(new_db, String.to_atom(table), values)
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
          new_base_ws_id = if base_ws_id, do: Map.get(workspace_ids, base_ws_id)

          insert_one(new_db, :workspace_bases, %{
            workspace_id: new_ws_id,
            base_workspace_id: new_base_ws_id,
            created_at: created_at,
            created_by: created_by
          })
      end
    end)
  end

  defp copy_tokens(old_db, new_db, principal_ids) do
    {:ok, rows} =
      query(
        old_db,
        """
        SELECT id, external_id, token_hash, name, workspaces,
          created_by, created_at, expires_at, revoked_at
        FROM tokens
        WHERE revoked_at IS NULL
        """
      )

    Enum.reduce(rows, %{}, fn {old_id, ext_id, token_hash, name, workspaces, created_by,
                               created_at, expires_at, revoked_at},
                              acc ->
      new_created_by = if created_by, do: Map.get(principal_ids, created_by)

      {:ok, new_id} =
        insert_one(new_db, :tokens, %{
          external_id: ext_id,
          token_hash: token_hash,
          name: name,
          workspaces: workspaces,
          created_by: new_created_by,
          created_at: created_at,
          expires_at: expires_at,
          revoked_at: revoked_at
        })

      Map.put(acc, old_id, new_id)
    end)
  end

  defp copy_pools(old_db, new_db, workspace_ids, pool_definition_ids) do
    {:ok, rows} =
      query(
        old_db,
        """
        SELECT id, external_id, workspace_id, name,
          pool_definition_id, created_at, created_by
        FROM pools
        """
      )

    Enum.reduce(rows, %{}, fn {old_id, ext_id, old_ws_id, name, old_pd_id, created_at, created_by},
                              acc ->
      new_ws_id = Map.get(workspace_ids, old_ws_id, old_ws_id)
      new_pd_id = if old_pd_id, do: Map.get(pool_definition_ids, old_pd_id, old_pd_id)

      {:ok, new_id} =
        insert_one(new_db, :pools, %{
          external_id: ext_id,
          workspace_id: new_ws_id,
          name: name,
          pool_definition_id: new_pd_id,
          created_at: created_at,
          created_by: created_by
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
      new_pool_id = Map.get(pool_ids, old_pool_id, old_pool_id)

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
          insert_one(new_db, :worker_states, %{
            worker_id: new_id,
            state: state,
            created_at: created_at,
            created_by: created_by
          })

        {:ok, nil} ->
          :ok
      end
    end)
  end

  defp copy_active_sessions(old_db, new_db, workspace_ids, worker_ids, tag_set_ids, principal_ids) do
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
      new_ws_id = Map.get(workspace_ids, old_ws_id, old_ws_id)
      new_worker_id = if old_worker_id, do: Map.get(worker_ids, old_worker_id)
      new_tag_set_id = if old_tag_set_id, do: Map.get(tag_set_ids, old_tag_set_id, old_tag_set_id)
      new_created_by = if created_by, do: Map.get(principal_ids, created_by)

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
          insert_one(new_db, :session_activations, %{session_id: new_id, created_at: created_at})

        {:ok, nil} ->
          :ok
      end
    end)
  end

  defp copy_latest_workspace_manifests(old_db, new_db, workspace_ids, manifest_ids, principal_ids) do
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
        new_manifest_id =
          if old_manifest_id, do: Map.get(manifest_ids, old_manifest_id, old_manifest_id)

        new_created_by = if created_by, do: Map.get(principal_ids, created_by)

        insert_one(new_db, :workspace_manifests, %{
          workspace_id: new_ws_id,
          module: module,
          manifest_id: new_manifest_id,
          created_at: created_at,
          created_by: new_created_by
        })
      end)
    end)
  end

  defp copy_workflows(
         old_db,
         new_db,
         manifest_ids,
         parameter_set_ids,
         instruction_ids,
         cache_config_ids,
         tag_set_ids
       ) do
    Enum.each(manifest_ids, fn {old_manifest_id, new_manifest_id} ->
      {:ok, rows} =
        query(
          old_db,
          """
          SELECT name, parameter_set_id, instruction_id, wait_for,
            cache_config_id, defer_params, delay, retry_limit,
            retry_delay_min, retry_delay_max, recurrent, requires_tag_set_id
          FROM workflows
          WHERE manifest_id = ?1
          """,
          {old_manifest_id}
        )

      Enum.each(rows, fn {name, old_ps_id, old_instr_id, wait_for, old_cc_id, defer_params, delay,
                          retry_limit, retry_delay_min, retry_delay_max, recurrent, old_rts_id} ->
        new_ps_id = Map.get(parameter_set_ids, old_ps_id, old_ps_id)
        new_instr_id = if old_instr_id, do: Map.get(instruction_ids, old_instr_id, old_instr_id)
        new_cc_id = if old_cc_id, do: Map.get(cache_config_ids, old_cc_id, old_cc_id)
        new_rts_id = if old_rts_id, do: Map.get(tag_set_ids, old_rts_id, old_rts_id)

        insert_one(
          new_db,
          :workflows,
          %{
            manifest_id: new_manifest_id,
            name: name,
            parameter_set_id: new_ps_id,
            instruction_id: new_instr_id,
            wait_for: wait_for,
            cache_config_id: new_cc_id,
            defer_params: defer_params,
            delay: delay,
            retry_limit: retry_limit,
            retry_delay_min: retry_delay_min,
            retry_delay_max: retry_delay_max,
            recurrent: recurrent,
            requires_tag_set_id: new_rts_id
          },
          on_conflict: "DO NOTHING"
        )
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
            "SELECT position, fragment_id, execution_id, asset_id FROM value_references WHERE value_id = ?1",
            {value_id}
          )

        Enum.each(refs, fn {position, fragment_id, _execution_id, asset_id} ->
          new_fragment_id = if fragment_id, do: ensure_fragment(source_db, target_db, fragment_id)
          new_asset_id = if asset_id, do: ensure_asset(source_db, target_db, asset_id)
          # Note: execution_id references are cross-run and may not exist in target yet.
          # For now, we skip execution references in value copies.
          # They'll be available after the full run is copied.
          insert_one(
            target_db,
            :value_references,
            %{
              value_id: new_id,
              position: position,
              fragment_id: new_fragment_id,
              execution_id: nil,
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

        {:ok, new_id} =
          insert_one(target_db, :fragments, %{
            hash: {:blob, hash},
            format_id: format_id,
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
end
