defmodule Coflux.Orchestration.Manifests do
  import Coflux.Store

  alias Coflux.Orchestration.{TagSets, CacheConfigs, Utils}

  def register_manifests(db, workspace_id, manifests) do
    with_transaction(db, fn ->
      manifest_ids =
        Map.new(manifests, fn {module, manifest} ->
          {:ok, manifest_id} =
            if manifest do
              hash = hash_manifest(manifest)

              case query_one(db, "SELECT id FROM manifests WHERE hash = ?1", {{:blob, hash}}) do
                {:ok, nil} ->
                  {:ok, manifest_id} = insert_one(db, :manifests, %{hash: {:blob, hash}})

                  {:ok, _} =
                    insert_many(
                      db,
                      :workflows,
                      {:manifest_id, :name, :instruction_id, :parameter_set_id, :wait_for,
                       :cache_config_id, :defer_params, :delay, :retry_limit, :retry_delay_min,
                       :retry_delay_max, :requires_tag_set_id},
                      Enum.map(manifest.workflows, fn {name, workflow} ->
                        {:ok, instruction_id} =
                          if workflow.instruction do
                            get_or_create_instruction_id(db, workflow.instruction)
                          else
                            {:ok, nil}
                          end

                        {:ok, parameter_set_id} =
                          get_or_create_parameter_set_id(db, workflow.parameters)

                        {:ok, cache_config_id} =
                          if workflow.cache,
                            do: CacheConfigs.get_or_create_cache_config_id(db, workflow.cache),
                            else: {:ok, nil}

                        {:ok, requires_tag_set_id} =
                          if workflow.requires do
                            TagSets.get_or_create_tag_set_id(db, workflow.requires)
                          else
                            {:ok, nil}
                          end

                        {
                          manifest_id,
                          name,
                          instruction_id,
                          parameter_set_id,
                          Utils.encode_params_set(workflow.wait_for),
                          cache_config_id,
                          if(workflow.defer,
                            do: Utils.encode_params_list(workflow.defer.params)
                          ),
                          workflow.delay,
                          if(workflow.retries, do: workflow.retries.limit, else: 0),
                          if(workflow.retries, do: workflow.retries.delay_min, else: 0),
                          if(workflow.retries, do: workflow.retries.delay_max, else: 0),
                          requires_tag_set_id
                        }
                      end)
                    )

                  {:ok, _} =
                    insert_many(
                      db,
                      :sensors,
                      {:manifest_id, :name, :instruction_id, :parameter_set_id,
                       :requires_tag_set_id},
                      Enum.map(manifest.sensors, fn {name, sensor} ->
                        {:ok, instruction_id} =
                          if sensor.instruction do
                            get_or_create_instruction_id(db, sensor.instruction)
                          else
                            {:ok, nil}
                          end

                        {:ok, requires_tag_set_id} =
                          if sensor.requires do
                            TagSets.get_or_create_tag_set_id(db, sensor.requires)
                          else
                            {:ok, nil}
                          end

                        case get_or_create_parameter_set_id(db, sensor.parameters) do
                          {:ok, parameter_set_id} ->
                            {manifest_id, name, instruction_id, parameter_set_id,
                             requires_tag_set_id}
                        end
                      end)
                    )

                  {:ok, manifest_id}

                {:ok, {manifest_id}} ->
                  {:ok, manifest_id}
              end
            else
              {:ok, nil}
            end

          {module, manifest_id}
        end)

      {:ok, current_manifest_ids} = get_latest_manifest_ids(db, workspace_id)

      now = current_timestamp()

      {:ok, _} =
        insert_many(
          db,
          :workspace_manifests,
          {:workspace_id, :module, :manifest_id, :created_at},
          Enum.reduce(manifest_ids, [], fn {module, manifest_id}, result ->
            if manifest_id != Map.get(current_manifest_ids, module) do
              [{workspace_id, module, manifest_id, now} | result]
            else
              result
            end
          end)
        )

      :ok
    end)
  end

  def archive_module(db, workspace_id, module_name) do
    with_transaction(db, fn ->
      now = current_timestamp()

      {:ok, _} =
        insert_one(db, :workspace_manifests, %{
          workspace_id: workspace_id,
          module: module_name,
          manifest_id: nil,
          created_at: now
        })

      :ok
    end)
  end

  defp get_latest_manifest_ids(db, workspace_id) do
    case query(
           db,
           """
           SELECT wm.module, wm.manifest_id
           FROM workspace_manifests wm
           JOIN (
               SELECT module, MAX(created_at) AS latest_created_at
               FROM workspace_manifests
               WHERE workspace_id = ?1
               GROUP BY module
           ) AS latest
           ON wm.module = latest.module AND wm.created_at = latest.latest_created_at
           WHERE wm.workspace_id = ?1
           """,
           {workspace_id}
         ) do
      {:ok, rows} ->
        {:ok, Map.new(rows)}
    end
  end

  def get_latest_manifests(db, workspace_id) do
    case get_latest_manifest_ids(db, workspace_id) do
      {:ok, manifest_ids} ->
        manifests =
          Enum.reduce(manifest_ids, %{}, fn {module, manifest_id}, result ->
            if manifest_id do
              {:ok, workflows} = get_manifest_workflows(db, manifest_id)
              {:ok, sensors} = get_manifest_sensors(db, manifest_id)
              Map.put(result, module, %{workflows: workflows, sensors: sensors})
            else
              result
            end
          end)

        {:ok, manifests}
    end
  end

  def get_latest_workflow(db, workspace_id, module, target_name) do
    case query_one(
           db,
           """
           SELECT w.parameter_set_id, w.instruction_id, w.wait_for, w.cache_config_id, w.defer_params, w.delay, w.retry_limit, w.retry_delay_min, w.retry_delay_max, w.requires_tag_set_id
           FROM workspace_manifests AS wm
           LEFT JOIN workflows AS w ON w.manifest_id = wm.manifest_id
           WHERE wm.workspace_id = ?1 AND wm.module = ?2 AND w.name = ?3
           ORDER BY wm.created_at DESC
           LIMIT 1
           """,
           {workspace_id, module, target_name}
         ) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok,
       {parameter_set_id, instruction_id, wait_for, cache_config_id, defer_params, delay,
        retry_limit, retry_delay_min, retry_delay_max, requires_tag_set_id}} ->
        build_workflow(
          db,
          parameter_set_id,
          instruction_id,
          wait_for,
          cache_config_id,
          defer_params,
          delay,
          retry_limit,
          retry_delay_min,
          retry_delay_max,
          requires_tag_set_id
        )
    end
  end

  def get_latest_sensor(db, workspace_id, module, target_name) do
    case query_one(
           db,
           """
           SELECT s.parameter_set_id, s.instruction_id, s.requires_tag_set_id
           FROM workspace_manifests AS wm
           LEFT JOIN sensors AS s ON s.manifest_id = wm.manifest_id
           WHERE wm.workspace_id = ?1 AND wm.module = ?2 AND s.name = ?3
           ORDER BY wm.created_at DESC
           LIMIT 1
           """,
           {workspace_id, module, target_name}
         ) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, {parameter_set_id, instruction_id, requires_tag_set_id}} ->
        build_sensor(db, parameter_set_id, instruction_id, requires_tag_set_id)
    end
  end

  defp get_manifest_workflows(db, manifest_id) do
    case query(
           db,
           """
           SELECT name, instruction_id, parameter_set_id, wait_for, cache_config_id, defer_params, delay, retry_limit, retry_delay_min, retry_delay_max, requires_tag_set_id
           FROM workflows
           WHERE manifest_id = ?1
           """,
           {manifest_id}
         ) do
      {:ok, rows} ->
        workflows =
          Map.new(rows, fn {name, instruction_id, parameter_set_id, wait_for, cache_config_id,
                            defer_params, delay, retry_limit, retry_delay_min, retry_delay_max,
                            requires_tag_set_id} ->
            {:ok, workflow} =
              build_workflow(
                db,
                parameter_set_id,
                instruction_id,
                wait_for,
                cache_config_id,
                defer_params,
                delay,
                retry_limit,
                retry_delay_min,
                retry_delay_max,
                requires_tag_set_id
              )

            {name, workflow}
          end)

        {:ok, workflows}
    end
  end

  defp get_manifest_sensors(db, manifest_id) do
    case query(
           db,
           """
           SELECT name, parameter_set_id, instruction_id, requires_tag_set_id
           FROM sensors
           WHERE manifest_id = ?1
           """,
           {manifest_id}
         ) do
      {:ok, rows} ->
        sensors =
          Map.new(rows, fn {name, parameter_set_id, instruction_id, requires_tag_set_id} ->
            {:ok, sensor} =
              build_sensor(db, parameter_set_id, instruction_id, requires_tag_set_id)

            {name, sensor}
          end)

        {:ok, sensors}
    end
  end

  defp get_all_workflows_for_workspace(db, workspace_id) do
    case query(
           db,
           """
           SELECT DISTINCT wm.module, w.name
           FROM workspace_manifests AS wm
           INNER JOIN manifests AS m on m.id = wm.manifest_id
           INNER JOIN workflows AS w ON w.manifest_id = m.id
           WHERE wm.workspace_id = ?1
           """,
           {workspace_id}
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.reduce(rows, %{}, fn {module, target_name}, result ->
           result
           |> Map.put_new(module, MapSet.new())
           |> Map.update!(module, &MapSet.put(&1, target_name))
         end)}
    end
  end

  defp get_all_sensors_for_workspace(db, workspace_id) do
    case query(
           db,
           """
           SELECT DISTINCT wm.module, s.name
           FROM workspace_manifests AS wm
           INNER JOIN manifests AS m on m.id = wm.manifest_id
           INNER JOIN sensors AS s ON s.manifest_id = m.id
           WHERE wm.workspace_id = ?1
           """,
           {workspace_id}
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.reduce(rows, %{}, fn {module, target_name}, result ->
           result
           |> Map.put_new(module, MapSet.new())
           |> Map.update!(module, &MapSet.put(&1, target_name))
         end)}
    end
  end

  def get_all_targets_for_workspace(db, workspace_id) do
    with {:ok, workflows} <- get_all_workflows_for_workspace(db, workspace_id),
         {:ok, sensors} <- get_all_sensors_for_workspace(db, workspace_id) do
      {:ok, workflows, sensors}
    end
  end

  defp hash_manifest(manifest) do
    workflows_hash = hash_manifest_workflows(manifest.workflows)
    sensors_hash = hash_manifest_sensors(manifest.sensors)
    :crypto.hash(:sha256, [workflows_hash, 0, sensors_hash])
  end

  defp hash_manifest_workflows(workflows) do
    data =
      Enum.map(workflows, fn {name, workflow} ->
        [
          name,
          hash_parameter_set(workflow.parameters),
          Integer.to_string(Utils.encode_params_set(workflow.wait_for)),
          if(workflow.cache, do: Utils.encode_params_list(workflow.cache.params), else: ""),
          if(workflow.cache[:max_age], do: Integer.to_string(workflow.cache.max_age), else: ""),
          if(workflow.cache[:namespace], do: workflow.cache.namespace, else: ""),
          if(workflow.cache[:version], do: workflow.cache.version, else: ""),
          if(workflow.defer, do: Utils.encode_params_list(workflow.defer.params), else: ""),
          Integer.to_string(workflow.delay),
          if(workflow.retries, do: Integer.to_string(workflow.retries.limit), else: ""),
          if(workflow.retries, do: Integer.to_string(workflow.retries.delay_min), else: ""),
          if(workflow.retries, do: Integer.to_string(workflow.retries.delay_max), else: ""),
          hash_requires(workflow.requires),
          workflow.instruction || ""
        ]
      end)

    :crypto.hash(:sha256, Enum.intersperse(data, 0))
  end

  defp hash_manifest_sensors(sensors) do
    data =
      Enum.map(sensors, fn {name, sensor} ->
        [
          name,
          hash_parameter_set(sensor.parameters),
          hash_requires(sensor.requires),
          sensor.instruction || ""
        ]
      end)

    :crypto.hash(:sha256, Enum.intersperse(data, 0))
  end

  defp build_workflow(
         db,
         parameter_set_id,
         instruction_id,
         wait_for,
         cache_config_id,
         defer_params,
         delay,
         retry_limit,
         retry_delay_min,
         retry_delay_max,
         requires_tag_set_id
       ) do
    {:ok, parameters} = get_parameter_set(db, parameter_set_id)

    {:ok, requires} =
      if requires_tag_set_id do
        TagSets.get_tag_set(db, requires_tag_set_id)
      else
        {:ok, nil}
      end

    {:ok, cache} =
      if cache_config_id do
        CacheConfigs.get_cache_config(db, cache_config_id)
      else
        {:ok, nil}
      end

    defer =
      if defer_params do
        %{
          params: Utils.decode_params_list(defer_params)
        }
      end

    retries =
      if retry_limit do
        %{
          limit: retry_limit,
          delay_min: retry_delay_min,
          delay_max: retry_delay_max
        }
      end

    {:ok,
     %{
       parameters: parameters,
       instruction_id: instruction_id,
       wait_for: Utils.decode_params_set(wait_for),
       cache: cache,
       defer: defer,
       delay: delay,
       retries: retries,
       requires: requires
     }}
  end

  defp build_sensor(db, parameter_set_id, instruction_id, requires_tag_set_id) do
    {:ok, parameters} = get_parameter_set(db, parameter_set_id)

    {:ok, requires} =
      if requires_tag_set_id do
        TagSets.get_tag_set(db, requires_tag_set_id)
      else
        {:ok, nil}
      end

    {:ok,
     %{
       parameters: parameters,
       instruction_id: instruction_id,
       requires: requires
     }}
  end

  defp get_or_create_instruction_id(db, content) do
    hash = :crypto.hash(:sha256, content)

    case query_one(db, "SELECT id FROM instructions WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {id}} ->
        {:ok, id}

      {:ok, nil} ->
        insert_one(db, :instructions, %{hash: {:blob, hash}, content: content})
    end
  end

  def get_instruction(db, instruction_id) do
    case query_one(db, "SELECT content FROM instructions WHERE id = ?1", {instruction_id}) do
      {:ok, {content}} -> {:ok, content}
    end
  end

  defp get_or_create_parameter_set_id(db, parameters) do
    hash = hash_parameter_set(parameters)

    case query_one(db, "SELECT id FROM parameter_sets WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {parameter_set_id}} ->
        {:ok, parameter_set_id}

      {:ok, nil} ->
        case insert_one(db, :parameter_sets, %{hash: {:blob, hash}}) do
          {:ok, parameter_set_id} ->
            {:ok, _} =
              insert_many(
                db,
                :parameter_set_items,
                {:parameter_set_id, :position, :name, :default_, :annotation},
                parameters
                |> Enum.with_index()
                |> Enum.map(fn {{name, default, annotation}, index} ->
                  {parameter_set_id, index, name, default, annotation}
                end)
              )

            {:ok, parameter_set_id}
        end
    end
  end

  defp get_parameter_set(db, parameter_set_id) do
    case query(
           db,
           """
           SELECT name, default_, annotation
           FROM parameter_set_items
           WHERE parameter_set_id = ?1
           ORDER BY position
           """,
           {parameter_set_id}
         ) do
      {:ok, rows} ->
        {:ok, rows}
    end
  end

  defp hash_parameter_set(parameters) do
    data =
      parameters
      |> Enum.map(fn {name, default, annotation} ->
        "#{name}:#{default}:#{annotation}"
      end)
      |> Enum.intersperse(0)

    :crypto.hash(:sha256, data)
  end

  defp hash_requires(requires) do
    requires
    |> Enum.sort()
    |> Enum.map_join(";", fn {key, values} ->
      "#{key}=#{values |> Enum.sort() |> Enum.join(",")}"
    end)
  end

  defp current_timestamp() do
    System.os_time(:millisecond)
  end
end
