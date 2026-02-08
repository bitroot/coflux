defmodule Coflux.Orchestration.Runs do
  alias Coflux.Orchestration.{Models, Values, TagSets, CacheConfigs, Utils}

  import Coflux.Store

  def get_step_by_number(db, run_id, step_number) do
    query_one(
      db,
      """
      SELECT
        id,
        number,
        run_id,
        parent_id,
        module,
        target,
        type,
        priority,
        wait_for,
        cache_config_id,
        cache_key,
        defer_key,
        memo_key,
        retry_limit,
        retry_delay_min,
        retry_delay_max,
        recurrent,
        delay,
        requires_tag_set_id,
        created_at
      FROM steps
      WHERE run_id = ?1 AND number = ?2
      """,
      {run_id, step_number},
      Models.Step
    )
  end

  def get_step_for_execution(db, execution_id) do
    query_one!(
      db,
      """
      SELECT
        s.id,
        s.number,
        s.run_id,
        s.parent_id,
        s.module,
        s.target,
        s.type,
        s.priority,
        s.wait_for,
        s.cache_config_id,
        s.cache_key,
        s.defer_key,
        s.memo_key,
        s.retry_limit,
        s.retry_delay_min,
        s.retry_delay_max,
        s.recurrent,
        s.delay,
        s.requires_tag_set_id,
        s.created_at
      FROM steps AS s
      INNER JOIN executions AS e ON e.step_id = s.id
      WHERE e.id = ?1
      """,
      {execution_id},
      Models.Step
    )
  end

  def get_execution_run_id(db, execution_id) do
    case query_one(
           db,
           """
           SELECT s.run_id
           FROM steps AS s
           INNER JOIN executions AS e ON e.step_id = s.id
           WHERE e.id = ?1
           """,
           {execution_id}
         ) do
      {:ok, {run_id}} -> {:ok, run_id}
    end
  end

  def get_workspace_id_for_execution(db, execution_id) do
    case query_one(
           db,
           "SELECT workspace_id FROM executions WHERE id = ?1",
           {execution_id}
         ) do
      {:ok, {workspace_id}} -> {:ok, workspace_id}
      {:ok, nil} -> {:error, :not_found}
    end
  end

  def get_steps_for_workspace(db, workspace_id) do
    case query(
           db,
           """
           WITH latest_executions AS (
             SELECT s.module, s.target, MAX(e.created_at) AS max_created_at
             FROM executions AS e
             INNER JOIN steps AS s ON s.id = e.step_id
             WHERE e.workspace_id = ?1
             GROUP BY s.module, s.target
           )
           SELECT s.module, s.target, s.type, r.external_id, s.number, e.attempt
           FROM executions AS e
           INNER JOIN steps AS s ON s.id = e.step_id
           INNER JOIN latest_executions AS le ON s.module = le.module AND s.target = le.target AND e.created_at = le.max_created_at
           INNER JOIN runs AS r ON r.id = s.run_id
           WHERE e.workspace_id = ?1
           """,
           {workspace_id}
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.map(rows, fn {module_name, target_name, target_type, run_external_id, step_number,
                            attempt} ->
           {module_name, target_name, Utils.decode_step_type(target_type), run_external_id,
            step_number, attempt}
         end)}
    end
  end

  def schedule_run(
        db,
        module,
        target,
        type,
        arguments,
        workspace_id,
        cache_workspace_ids,
        opts \\ []
      ) do
    idempotency_key = Keyword.get(opts, :idempotency_key)
    parent_id = Keyword.get(opts, :parent_id)
    created_by = Keyword.get(opts, :created_by)
    now = current_timestamp()

    # TODO: check that 'type' is :workflow?

    with_transaction(db, fn ->
      {:ok, run_id, external_run_id} = insert_run(db, parent_id, idempotency_key, now, created_by)

      {:ok, schedule} =
        do_schedule_step(
          db,
          run_id,
          nil,
          module,
          target,
          type,
          arguments,
          true,
          workspace_id,
          cache_workspace_ids,
          now,
          opts
        )

      {:ok, Map.put(schedule, :external_run_id, external_run_id)}
    end)
  end

  def schedule_step(
        db,
        run_id,
        parent_id,
        module,
        target,
        type,
        arguments,
        workspace_id,
        cache_workspace_ids,
        opts \\ []
      ) do
    now = current_timestamp()

    with_transaction(db, fn ->
      do_schedule_step(
        db,
        run_id,
        parent_id,
        module,
        target,
        type,
        arguments,
        false,
        workspace_id,
        cache_workspace_ids,
        now,
        opts
      )
    end)
  end

  defp get_argument_key_parts(parameter) do
    references =
      case parameter do
        {:raw, _data, references} -> references
        {:blob, _blob_key, _size, references} -> references
      end

    references_parts =
      [
        length(references)
        | Enum.flat_map(references, fn
            {:fragment, format, blob_key, _size, metadata} ->
              Enum.concat(
                [1, format, blob_key],
                Enum.flat_map(metadata, fn {key, value} -> [key, Jason.encode!(value)] end)
              )

            {:execution, execution_id} ->
              [2, Integer.to_string(execution_id)]

            {:asset, asset_id} ->
              [3, Integer.to_string(asset_id)]
          end)
      ]

    # TODO: safer data encoding? (consider order of, e.g., dicts)
    case parameter do
      {:raw, data, _references} -> Enum.concat([1, Jason.encode!(data)], references_parts)
      {:blob, blob_key, _size, _references} -> Enum.concat([2, blob_key], references_parts)
    end
  end

  defp build_key(params, arguments, namespace, version \\ nil) do
    params =
      if params == true,
        do: Enum.map(Enum.with_index(arguments), fn {_, i} -> i end),
        else: params

    parameter_parts =
      Enum.map(params, &get_argument_key_parts(Enum.at(arguments, &1)))

    data =
      [namespace, version || ""]
      |> Enum.concat(parameter_parts)
      |> Enum.intersperse(0)

    :crypto.hash(:sha256, data)
  end

  defp do_schedule_step(
         db,
         run_id,
         parent_id,
         module,
         target,
         type,
         arguments,
         is_initial,
         workspace_id,
         cache_workspace_ids,
         now,
         opts
       ) do
    group_id = Keyword.get(opts, :group_id)
    priority = Keyword.get(opts, :priority, 0)
    wait_for = Keyword.get(opts, :wait_for)
    cache = Keyword.get(opts, :cache)
    defer = Keyword.get(opts, :defer)
    memo = Keyword.get(opts, :memo)
    retries = Keyword.get(opts, :retries)
    recurrent = Keyword.get(opts, :recurrent, false)
    delay = Keyword.get(opts, :delay, 0)
    requires = Keyword.get(opts, :requires) || %{}

    # Calculate execute_after from delay
    execute_after = if delay > 0, do: now + delay

    memo_key = if memo, do: build_key(memo, arguments, "#{module}:#{target}")

    memoised_execution =
      if memo_key do
        case find_memoised_execution(db, run_id, cache_workspace_ids, memo_key) do
          {:ok, memoised_execution} -> memoised_execution
        end
      end

    {step_number, execution_id, attempt, now, memo_hit, cache_key} =
      case memoised_execution do
        {step_number, execution_id, attempt, now} ->
          {step_number, execution_id, attempt, now, true, nil}

        nil ->
          cache_key =
            if cache do
              build_key(
                cache.params,
                arguments,
                cache.namespace || "#{module}:#{target}",
                cache.version
              )
            end

          cache_config_id =
            if cache do
              case CacheConfigs.get_or_create_cache_config_id(db, cache) do
                {:ok, cache_config_id} -> cache_config_id
              end
            end

          requires_tag_set_id =
            if Enum.any?(requires) do
              case TagSets.get_or_create_tag_set_id(db, requires) do
                {:ok, tag_set_id} ->
                  tag_set_id
              end
            end

          defer_key =
            if defer,
              do: build_key(defer.params, arguments, "#{module}:#{target}")

          # TODO: validate parent belongs to run?
          {:ok, step_id, step_number} =
            insert_step(
              db,
              run_id,
              if(!is_initial, do: parent_id),
              module,
              target,
              type,
              priority,
              wait_for,
              cache_key,
              cache_config_id,
              defer_key,
              memo_key,
              if(retries, do: retries.limit || -1, else: 0),
              if(retries, do: retries.delay_min, else: 0),
              if(retries, do: retries.delay_max, else: 0),
              recurrent,
              delay,
              requires_tag_set_id,
              now
            )

          {:ok, _} =
            insert_many(
              db,
              :step_arguments,
              {:step_id, :position, :value_id},
              arguments
              |> Enum.with_index()
              |> Enum.map(fn {value, position} ->
                {:ok, value_id} = Values.get_or_create_value(db, value)
                {step_id, position, value_id}
              end)
            )

          attempt = 1

          {:ok, execution_id} =
            insert_execution(db, step_id, attempt, workspace_id, execute_after, now)

          {step_number, execution_id, attempt, now, false, cache_key}
      end

    child_added =
      if parent_id do
        {:ok, id} = insert_child(db, parent_id, execution_id, group_id, now)
        !is_nil(id)
      else
        false
      end

    {:ok,
     %{
       step_number: step_number,
       execution_id: execution_id,
       attempt: attempt,
       created_at: now,
       cache_key: cache_key,
       memo_key: memo_key,
       memo_hit: memo_hit,
       child_added: child_added
     }}
  end

  def create_group(db, execution_id, group_id, name) do
    case insert_one(db, :groups, %{
           execution_id: execution_id,
           group_id: group_id,
           name: name
         }) do
      {:ok, _} -> :ok
    end
  end

  def rerun_step(db, step_id, workspace_id, execute_after, dependency_ids, created_by \\ nil) do
    with_transaction(db, fn ->
      now = current_timestamp()
      # TODO: cancel pending executions for step?
      {:ok, attempt} = get_next_execution_attempt(db, step_id)

      {:ok, execution_id} =
        insert_execution(db, step_id, attempt, workspace_id, execute_after, now, created_by)

      {:ok, _} =
        insert_many(
          db,
          :result_dependencies,
          {:execution_id, :dependency_id, :created_at},
          Enum.map(dependency_ids, &{execution_id, &1, now})
        )

      {:ok, execution_id, attempt, now}
    end)
  end

  def assign_execution(db, execution_id, session_id) do
    with_transaction(db, fn ->
      now = current_timestamp()

      {:ok, _} =
        insert_one(db, :assignments, %{
          execution_id: execution_id,
          session_id: session_id,
          created_at: now
        })

      {:ok, now}
    end)
  end

  def record_hearbeats(db, executions) do
    with_transaction(db, fn ->
      now = current_timestamp()

      {:ok, _} =
        insert_many(
          db,
          :heartbeats,
          {:execution_id, :status, :created_at},
          Enum.map(executions, fn {execution_id, status} ->
            {execution_id, status, now}
          end)
        )

      {:ok, now}
    end)
  end

  def record_result_dependency(db, execution_id, dependency_id) do
    with_transaction(db, fn ->
      insert_one(
        db,
        :result_dependencies,
        %{
          execution_id: execution_id,
          dependency_id: dependency_id,
          created_at: current_timestamp()
        },
        on_conflict: "DO NOTHING"
      )
    end)
  end

  def record_asset_dependency(db, execution_id, asset_id) do
    with_transaction(db, fn ->
      insert_one(
        db,
        :asset_dependencies,
        %{
          execution_id: execution_id,
          asset_id: asset_id,
          created_at: current_timestamp()
        },
        on_conflict: "DO NOTHING"
      )
    end)
  end

  def get_unassigned_executions(db) do
    query(
      db,
      """
      SELECT
        e.id AS execution_id,
        s.id AS step_id,
        s.run_id,
        run.external_id AS run_external_id,
        s.number AS step_number,
        s.module,
        s.target,
        s.type,
        s.wait_for,
        s.cache_key,
        s.cache_config_id,
        s.defer_key,
        s.parent_id,
        s.requires_tag_set_id,
        s.retry_limit,
        s.retry_delay_min,
        s.retry_delay_max,
        e.workspace_id,
        e.execute_after,
        e.attempt,
        e.created_at
      FROM executions AS e
      INNER JOIN steps AS s ON s.id = e.step_id
      INNER JOIN runs AS run ON run.id = s.run_id
      LEFT JOIN assignments AS a ON a.execution_id = e.id
      LEFT JOIN results AS r ON r.execution_id = e.id
      WHERE a.created_at IS NULL AND r.created_at IS NULL
      ORDER BY e.execute_after, e.created_at, s.priority DESC
      """,
      {},
      Models.UnassignedExecution
    )
  end

  def get_module_executions(db, module) do
    case query(
           db,
           """
           SELECT
             s.target,
             r.external_id,
             s.number,
             e.attempt,
             e.execute_after,
             e.created_at,
             a.created_at
           FROM executions AS e
           INNER JOIN steps AS s ON s.id = e.step_id
           INNER JOIN runs AS r ON r.id = s.run_id
           LEFT JOIN assignments AS a ON a.execution_id = e.id
           LEFT JOIN results AS re ON re.execution_id = e.id
           WHERE s.module = ?1 AND re.created_at IS NULL
           """,
           {module}
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.map(rows, fn {target, run_external_id, step_number, attempt, execute_after,
                            created_at, assigned_at} ->
           {target, run_external_id, step_number, attempt, execute_after, created_at, assigned_at}
         end)}
    end
  end

  def get_pending_executions_for_workspace(db, workspace_id) do
    query(
      db,
      """
      SELECT e.id, s.run_id, s.module
      FROM executions AS e
      INNER JOIN steps AS s ON s.id = e.step_id
      LEFT JOIN results AS r ON r.execution_id = e.id
      WHERE e.workspace_id = ?1 AND r.created_at IS NULL
      """,
      {workspace_id}
    )
  end

  def get_pending_assignments(db) do
    query(
      db,
      """
      SELECT a.session_id, a.execution_id
      FROM assignments AS a
      LEFT JOIN results AS r ON r.execution_id = a.execution_id
      WHERE r.created_at IS NULL
      """
    )
  end

  def get_execution_descendants(db, execution_id) do
    query(
      db,
      """
      WITH RECURSIVE descendants AS (
        SELECT ?1 AS execution_id
        UNION
        SELECT e.id AS execution_id
        FROM descendants AS d
        INNER JOIN steps AS s ON s.parent_id = d.execution_id
        INNER JOIN executions AS e ON e.step_id = s.id
      )
      SELECT e.id, s.module, a.created_at, r.created_at
      FROM descendants AS d
      INNER JOIN executions AS e ON e.id = d.execution_id
      INNER JOIN steps AS s ON s.id = e.step_id
      LEFT JOIN assignments AS a ON a.execution_id = e.id
      LEFT JOIN results AS r ON r.execution_id = e.id
      """,
      {execution_id}
    )
  end

  def get_target_runs(db, module, target, type, workspace_id, limit \\ 50) do
    query(
      db,
      """
      SELECT DISTINCT r.external_id, r.created_at,
             p.user_external_id AS created_by_user_external_id,
             t.external_id AS created_by_token_external_id
      FROM runs as r
      INNER JOIN steps AS s ON s.run_id = r.id
      INNER JOIN executions AS e ON e.step_id == s.id
      LEFT JOIN principals AS p ON r.created_by = p.id
      LEFT JOIN tokens AS t ON p.token_id = t.id
      WHERE s.module = ?1 AND s.target = ?2 AND s.type = ?3 AND s.parent_id IS NULL AND e.workspace_id = ?4
      ORDER BY r.created_at DESC
      LIMIT ?5
      """,
      {module, target, Utils.encode_step_type(type), workspace_id, limit}
    )
  end

  def get_run_by_id(db, id) do
    query_one(
      db,
      """
      SELECT r.id, r.external_id, r.parent_id, r.idempotency_key, r.created_at,
             p.user_external_id AS created_by_user_external_id,
             t.external_id AS created_by_token_external_id
      FROM runs AS r
      LEFT JOIN principals AS p ON r.created_by = p.id
      LEFT JOIN tokens AS t ON p.token_id = t.id
      WHERE r.id = ?1
      """,
      {id},
      Models.Run
    )
  end

  def get_run_by_external_id(db, external_id) do
    query_one(
      db,
      """
      SELECT r.id, r.external_id, r.parent_id, r.idempotency_key, r.created_at,
             p.user_external_id AS created_by_user_external_id,
             t.external_id AS created_by_token_external_id
      FROM runs AS r
      LEFT JOIN principals AS p ON r.created_by = p.id
      LEFT JOIN tokens AS t ON p.token_id = t.id
      WHERE r.external_id = ?1
      """,
      {external_id},
      Models.Run
    )
  end

  def get_run_by_execution(db, execution_id) do
    query_one(
      db,
      """
      SELECT r.external_id, s.number, e.attempt, s.module, s.target
      FROM executions AS e
      INNER JOIN steps AS s ON s.id = e.step_id
      INNER JOIN runs AS r ON r.id = s.run_id
      WHERE e.id = ?1
      """,
      {execution_id}
    )
  end

  def get_external_run_id_for_execution(db, execution_id) do
    query_one(
      db,
      """
      SELECT r.external_id
      FROM executions AS e
      INNER JOIN steps AS s ON s.id = e.step_id
      INNER JOIN runs AS r ON r.id = s.run_id
      WHERE e.id = ?1
      """,
      {execution_id}
    )
  end

  def get_run_target(db, run_id) do
    query_one(
      db,
      """
      SELECT module, target
      FROM steps
      WHERE run_id = ?1 AND parent_id IS NULL
      """,
      {run_id}
    )
  end

  def get_run_steps(db, run_id) do
    query(
      db,
      """
      SELECT
        id,
        number,
        run_id,
        parent_id,
        module,
        target,
        type,
        priority,
        wait_for,
        cache_config_id,
        cache_key,
        defer_key,
        memo_key,
        retry_limit,
        retry_delay_min,
        retry_delay_max,
        recurrent,
        requires_tag_set_id,
        created_at
      FROM steps
      WHERE run_id = ?1
      """,
      {run_id},
      Models.Step
    )
  end

  def get_run_executions(db, run_id) do
    query(
      db,
      """
      SELECT e.id, e.step_id, e.attempt, e.workspace_id, e.execute_after, e.created_at, a.created_at,
             p.user_external_id AS created_by_user_external_id,
             t.external_id AS created_by_token_external_id
      FROM steps AS s
      INNER JOIN executions AS e ON e.step_id = s.id
      LEFT JOIN assignments AS a ON a.execution_id = e.id
      LEFT JOIN principals AS p ON e.created_by = p.id
      LEFT JOIN tokens AS t ON p.token_id = t.id
      WHERE s.run_id = ?1
      """,
      {run_id}
    )
  end

  def get_run_dependencies(db, run_id) do
    case query(
           db,
           """
           SELECT d.execution_id, d.dependency_id
           FROM result_dependencies AS d
           INNER JOIN executions AS e ON e.id = d.execution_id
           INNER JOIN steps AS s ON s.id = e.step_id
           WHERE s.run_id = ?1
           """,
           {run_id}
         ) do
      {:ok, rows} ->
        {:ok, Enum.group_by(rows, &elem(&1, 0), &elem(&1, 1))}
    end
  end

  def get_step_assignments(db, step_id) do
    case query(
           db,
           """
           SELECT e.id, a.created_at
           FROM executions AS e
           LEFT JOIN assignments AS a ON a.execution_id = e.id
           WHERE e.step_id = ?1
           """,
           {step_id}
         ) do
      {:ok, rows} ->
        {:ok, Map.new(rows)}
    end
  end

  @doc """
  Gets result types for executions of a step, ordered most recent first.
  """
  def get_step_result_types(db, step_id, limit) do
    case query(
           db,
           """
           SELECT r.type
           FROM executions AS e
           INNER JOIN results AS r ON r.execution_id = e.id
           WHERE e.step_id = ?1
           ORDER BY e.created_at DESC
           LIMIT ?2
           """,
           {step_id, limit}
         ) do
      {:ok, rows} ->
        {:ok, Enum.map(rows, fn {type} -> type end)}
    end
  end

  def get_first_step_execution_id(db, step_id) do
    case query_one(
           db,
           """
           SELECT id
           FROM executions
           WHERE step_id = ?1
           ORDER BY attempt ASC
           LIMIT 1
           """,
           {step_id}
         ) do
      {:ok, {id}} -> {:ok, id}
    end
  end

  def get_step_arguments(db, step_id) do
    case query(
           db,
           """
           SELECT value_id
           FROM step_arguments
           WHERE step_id = ?1
           ORDER BY position
           """,
           {step_id}
         ) do
      {:ok, rows} ->
        values =
          Enum.map(rows, fn {value_id} ->
            case Values.get_value_by_id(db, value_id) do
              {:ok, value} -> value
            end
          end)

        {:ok, values}
    end
  end

  def get_run_children(db, run_id) do
    case query(
           db,
           """
           SELECT c.parent_id, s2.number, e2.attempt, c.group_id
           FROM children AS c
           INNER JOIN executions AS e1 ON e1.id = c.parent_id
           INNER JOIN steps AS s1 ON s1.id = e1.step_id
           INNER JOIN executions AS e2 ON e2.id = c.child_id
           INNER JOIN steps AS s2 ON s2.id = e2.step_id
           WHERE s1.run_id = ?1
           """,
           {run_id}
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.group_by(
           rows,
           fn {parent_id, _, _, _} -> parent_id end,
           fn {_, step_number, attempt, group_id} ->
             {step_number, attempt, group_id}
           end
         )}
    end
  end

  def get_result_dependencies(db, execution_id) do
    query(
      db,
      """
      SELECT dependency_id
      FROM result_dependencies
      WHERE execution_id = ?1
      """,
      {execution_id}
    )
  end

  def get_groups_for_run(db, run_id) do
    query(
      db,
      """
      SELECT g.execution_id, g.group_id, g.name
      FROM groups AS g
      INNER JOIN executions AS e ON e.id = g.execution_id
      INNER JOIN steps AS s ON s.id = e.step_id
      WHERE s.run_id = ?1
      """,
      {run_id}
    )
  end

  defp build_placeholders(count, offset \\ 0) do
    1..count
    |> Enum.map_intersperse(", ", &"?#{&1 + offset}")
    |> Enum.join()
  end

  # TODO: consider changed 'requires'?
  defp find_memoised_execution(db, run_id, workspace_ids, memo_key) do
    case query(
           db,
           """
           SELECT s.number, e.id, e.attempt, e.created_at
           FROM steps AS s
           INNER JOIN executions AS e ON e.step_id = s.id
           LEFT JOIN results AS r ON r.execution_id = e.id
           WHERE
             s.run_id = ?1
             AND e.workspace_id IN (#{build_placeholders(length(workspace_ids), 1)})
             AND s.memo_key = ?#{length(workspace_ids) + 2}
             AND (r.type IS NULL OR r.type = 1)
           ORDER BY e.created_at DESC
           LIMIT 1
           """,
           List.to_tuple([run_id] ++ workspace_ids ++ [{:blob, memo_key}])
         ) do
      {:ok, [row]} ->
        {:ok, row}

      {:ok, []} ->
        {:ok, nil}
    end
  end

  def find_cached_execution(db, workspace_ids, step_id, cache_key, recorded_after) do
    case query(
           db,
           """
           SELECT e.id
           FROM steps AS s
           INNER JOIN executions AS e ON e.step_id = s.id
           LEFT JOIN results AS r ON r.execution_id = e.id
           WHERE
             e.workspace_id IN (#{build_placeholders(length(workspace_ids))})
             AND s.cache_key = ?#{length(workspace_ids) + 1}
             AND (r.type IS NULL OR (r.type = 1 AND r.created_at >= ?#{length(workspace_ids) + 2}))
             AND s.id <> ?#{length(workspace_ids) + 3}
           ORDER BY e.created_at DESC
           LIMIT 1
           """,
           List.to_tuple(workspace_ids ++ [{:blob, cache_key}, recorded_after, step_id])
         ) do
      {:ok, [{execution_id}]} ->
        {:ok, execution_id}

      {:ok, []} ->
        {:ok, nil}
    end
  end

  def find_cached_execution_by_cache_key(db, cache_key, recorded_after) do
    case query(
           db,
           """
           SELECT e.id, r2.external_id
           FROM steps AS s
           INNER JOIN executions AS e ON e.step_id = s.id
           INNER JOIN runs AS r2 ON r2.id = s.run_id
           LEFT JOIN results AS r ON r.execution_id = e.id
           WHERE
             s.cache_key = ?1
             AND (r.type IS NULL OR (r.type = 1 AND r.created_at >= ?2))
           ORDER BY e.created_at DESC
           LIMIT 1
           """,
           {{:blob, cache_key}, recorded_after}
         ) do
      {:ok, [{execution_id, run_external_id}]} ->
        {:ok, {execution_id, run_external_id}}

      {:ok, []} ->
        {:ok, nil}
    end
  end

  def get_result_successors(db, execution_id) do
    query(
      db,
      """
      WITH RECURSIVE successors AS (
        SELECT ?1 AS execution_id
        UNION
        SELECT r.execution_id
        FROM successors AS ss
        INNER JOIN results AS r ON r.successor_id = ss.execution_id
      )
      SELECT run.external_id, ss.execution_id
      FROM successors AS ss
      INNER JOIN executions AS e ON e.id = ss.execution_id
      INNER JOIN steps AS s ON s.id = e.step_id
      INNER JOIN runs AS run ON run.id = s.run_id
      """,
      {execution_id}
    )
  end

  defp insert_run(db, parent_id, idempotency_key, created_at, created_by) do
    case generate_external_id(db, :runs, 2, "R") do
      {:ok, external_id} ->
        case insert_one(db, :runs, %{
               external_id: external_id,
               parent_id: parent_id,
               idempotency_key: idempotency_key,
               created_at: created_at,
               created_by: created_by
             }) do
          {:ok, run_id} ->
            {:ok, run_id, external_id}
        end
    end
  end

  defp insert_step(
         db,
         run_id,
         parent_id,
         module,
         target,
         type,
         priority,
         wait_for,
         cache_key,
         cache_config_id,
         defer_key,
         memo_key,
         retry_limit,
         retry_delay_min,
         retry_delay_max,
         recurrent,
         delay,
         requires_tag_set_id,
         now
       ) do
    {:ok, step_number} = get_next_step_number(db, run_id)

    case insert_one(db, :steps, %{
           run_id: run_id,
           number: step_number,
           parent_id: parent_id,
           module: module,
           target: target,
           type: Utils.encode_step_type(type),
           priority: priority,
           wait_for: Utils.encode_params_set(wait_for || []),
           cache_key: if(cache_key, do: {:blob, cache_key}),
           cache_config_id: cache_config_id,
           defer_key: if(defer_key, do: {:blob, defer_key}),
           memo_key: if(memo_key, do: {:blob, memo_key}),
           retry_limit: retry_limit,
           retry_delay_min: retry_delay_min,
           retry_delay_max: retry_delay_max,
           recurrent: if(recurrent, do: 1, else: 0),
           delay: delay,
           requires_tag_set_id: requires_tag_set_id,
           created_at: now
         }) do
      {:ok, step_id} ->
        {:ok, step_id, step_number}
    end
  end

  defp get_next_step_number(db, run_id) do
    case query(
           db,
           """
           SELECT COALESCE(MAX(number), 0) + 1
           FROM steps
           WHERE run_id = ?1
           """,
           {run_id}
         ) do
      {:ok, [{next_number}]} ->
        {:ok, next_number}
    end
  end

  defp get_next_execution_attempt(db, step_id) do
    case query(
           db,
           """
           SELECT MAX(attempt)
           FROM executions
           WHERE step_id = ?1
           """,
           {step_id}
         ) do
      {:ok, [{nil}]} ->
        {:ok, 1}

      {:ok, [{last_attempt}]} ->
        {:ok, last_attempt + 1}
    end
  end

  defp insert_execution(
         db,
         step_id,
         attempt,
         workspace_id,
         execute_after,
         created_at,
         created_by \\ nil
       ) do
    insert_one(db, :executions, %{
      step_id: step_id,
      attempt: attempt,
      workspace_id: workspace_id,
      execute_after: execute_after,
      created_at: created_at,
      created_by: created_by
    })
  end

  defp insert_child(db, parent_id, child_id, group_id, created_at) do
    insert_one(
      db,
      :children,
      %{
        parent_id: parent_id,
        child_id: child_id,
        group_id: group_id,
        created_at: created_at
      },
      on_conflict: "DO NOTHING"
    )
  end

  def get_execution_key(db, execution_id) do
    case query_one(
           db,
           """
           SELECT r.external_id, s.number, e.attempt
           FROM executions AS e
           INNER JOIN steps AS s ON s.id = e.step_id
           INNER JOIN runs AS r ON r.id = s.run_id
           WHERE e.id = ?1
           """,
           {execution_id}
         ) do
      {:ok, {run_external_id, step_number, attempt}} ->
        {:ok, {run_external_id, step_number, attempt}}

      {:ok, nil} ->
        {:error, :not_found}
    end
  end

  def get_execution_keys(db, execution_ids) do
    Enum.reduce(execution_ids, {:ok, %{}}, fn execution_id, {:ok, acc} ->
      case get_execution_key(db, execution_id) do
        {:ok, key} -> {:ok, Map.put(acc, execution_id, key)}
        {:error, _} -> {:ok, acc}
      end
    end)
  end

  def get_execution_id(db, run_external_id, step_number, attempt) do
    query_one(
      db,
      """
      SELECT e.id
      FROM executions AS e
      INNER JOIN steps AS s ON s.id = e.step_id
      INNER JOIN runs AS r ON r.id = s.run_id
      WHERE r.external_id = ?1 AND s.number = ?2 AND e.attempt = ?3
      """,
      {run_external_id, step_number, attempt}
    )
  end

  defp current_timestamp() do
    System.os_time(:millisecond)
  end
end
