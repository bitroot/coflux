defmodule Coflux.Orchestration.Manifests do
  import Coflux.Store

  alias Coflux.Orchestration.{TagSets, CacheConfigs, Utils}

  def register_manifests(db, space_id, manifests) do
    with_transaction(db, fn ->
      manifest_ids =
        Map.new(manifests, fn {module, workflows} ->
          {:ok, manifest_id} =
            if workflows && map_size(workflows) > 0 do
              hash = hash_manifest_workflows(workflows)

              case query_one(db, "SELECT id FROM manifests WHERE hash = ?1", {{:blob, hash}}) do
                {:ok, nil} ->
                  {:ok, manifest_id} = insert_one(db, :manifests, %{hash: {:blob, hash}})

                  {:ok, _} =
                    insert_many(
                      db,
                      :workflows,
                      {:manifest_id, :name, :instruction_id, :parameter_set_id, :wait_for,
                       :cache_config_id, :defer_params, :delay, :retry_limit, :retry_delay_min,
                       :retry_delay_max, :recurrent, :requires_tag_set_id},
                      Enum.map(workflows, fn {name, workflow} ->
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
                          if(workflow.retries, do: workflow.retries.limit || -1, else: 0),
                          if(workflow.retries, do: workflow.retries.delay_min, else: 0),
                          if(workflow.retries, do: workflow.retries.delay_max, else: 0),
                          if(workflow.recurrent, do: 1, else: 0),
                          requires_tag_set_id
                        }
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

      {:ok, current_manifest_ids} = get_latest_manifest_ids(db, space_id)

      now = current_timestamp()

      {:ok, _} =
        insert_many(
          db,
          :space_manifests,
          {:space_id, :module, :manifest_id, :created_at},
          Enum.reduce(manifest_ids, [], fn {module, manifest_id}, result ->
            if manifest_id != Map.get(current_manifest_ids, module) do
              [{space_id, module, manifest_id, now} | result]
            else
              result
            end
          end)
        )

      :ok
    end)
  end

  def archive_module(db, space_id, module_name) do
    with_transaction(db, fn ->
      now = current_timestamp()

      {:ok, _} =
        insert_one(db, :space_manifests, %{
          space_id: space_id,
          module: module_name,
          manifest_id: nil,
          created_at: now
        })

      :ok
    end)
  end

  defp get_latest_manifest_ids(db, space_id) do
    case query(
           db,
           """
           SELECT wm.module, wm.manifest_id
           FROM space_manifests wm
           JOIN (
               SELECT module, MAX(created_at) AS latest_created_at
               FROM space_manifests
               WHERE space_id = ?1
               GROUP BY module
           ) AS latest
           ON wm.module = latest.module AND wm.created_at = latest.latest_created_at
           WHERE wm.space_id = ?1
           """,
           {space_id}
         ) do
      {:ok, rows} ->
        {:ok, Map.new(rows)}
    end
  end

  def get_latest_manifests(db, space_id) do
    case get_latest_manifest_ids(db, space_id) do
      {:ok, manifest_ids} ->
        manifests =
          Enum.reduce(manifest_ids, %{}, fn {module, manifest_id}, result ->
            if manifest_id do
              {:ok, workflows} = get_manifest_workflows(db, manifest_id)
              Map.put(result, module, workflows)
            else
              result
            end
          end)

        {:ok, manifests}
    end
  end

  def get_latest_workflow(db, space_id, module, target_name) do
    case query_one(
           db,
           """
           SELECT w.parameter_set_id, w.instruction_id, w.wait_for, w.cache_config_id, w.defer_params, w.delay, w.retry_limit, w.retry_delay_min, w.retry_delay_max, w.recurrent, w.requires_tag_set_id
           FROM space_manifests AS wm
           LEFT JOIN workflows AS w ON w.manifest_id = wm.manifest_id
           WHERE wm.space_id = ?1 AND wm.module = ?2 AND w.name = ?3
           ORDER BY wm.created_at DESC
           LIMIT 1
           """,
           {space_id, module, target_name}
         ) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok,
       {parameter_set_id, instruction_id, wait_for, cache_config_id, defer_params, delay,
        retry_limit, retry_delay_min, retry_delay_max, recurrent, requires_tag_set_id}} ->
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
          recurrent,
          requires_tag_set_id
        )
    end
  end

  defp get_manifest_workflows(db, manifest_id) do
    case query(
           db,
           """
           SELECT name, instruction_id, parameter_set_id, wait_for, cache_config_id, defer_params, delay, retry_limit, retry_delay_min, retry_delay_max, recurrent, requires_tag_set_id
           FROM workflows
           WHERE manifest_id = ?1
           """,
           {manifest_id}
         ) do
      {:ok, rows} ->
        workflows =
          Map.new(rows, fn {name, instruction_id, parameter_set_id, wait_for, cache_config_id,
                            defer_params, delay, retry_limit, retry_delay_min, retry_delay_max,
                            recurrent, requires_tag_set_id} ->
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
                recurrent,
                requires_tag_set_id
              )

            {name, workflow}
          end)

        {:ok, workflows}
    end
  end

  def get_all_workflows_for_space(db, space_id) do
    case query(
           db,
           """
           SELECT DISTINCT wm.module, w.name
           FROM space_manifests AS wm
           INNER JOIN manifests AS m on m.id = wm.manifest_id
           INNER JOIN workflows AS w ON w.manifest_id = m.id
           WHERE wm.space_id = ?1
           """,
           {space_id}
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

  defp hash_manifest_workflows(workflows) do
    data =
      Enum.map(workflows, fn {name, workflow} ->
        [
          name,
          hash_parameter_set(workflow.parameters),
          Integer.to_string(Utils.encode_params_set(workflow.wait_for)),
          # TODO: fix (workflow.cache.params being true gets encoded to "" as well as not set) - also below/elsewhere
          if(workflow.cache, do: Utils.encode_params_list(workflow.cache.params), else: ""),
          if(workflow.cache[:max_age], do: Integer.to_string(workflow.cache.max_age), else: ""),
          if(workflow.cache[:namespace], do: workflow.cache.namespace, else: ""),
          if(workflow.cache[:version], do: workflow.cache.version, else: ""),
          if(workflow.defer, do: Utils.encode_params_list(workflow.defer.params), else: ""),
          Integer.to_string(workflow.delay),
          if(workflow.retries,
            do: if(workflow.retries.limit, do: Integer.to_string(workflow.retries.limit), else: "unlimited"),
            else: ""
          ),
          if(workflow.retries, do: Integer.to_string(workflow.retries.delay_min), else: ""),
          if(workflow.retries, do: Integer.to_string(workflow.retries.delay_max), else: ""),
          if(workflow.recurrent, do: "1", else: "0"),
          hash_requires(workflow.requires),
          workflow.instruction || ""
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
         recurrent,
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
      cond do
        # 0 = no retries
        retry_limit == 0 ->
          nil

        # -1 = unlimited retries
        retry_limit == -1 ->
          %{
            limit: nil,
            delay_min: retry_delay_min,
            delay_max: retry_delay_max
          }

        # positive = that many retries
        true ->
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
       recurrent: recurrent == 1,
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
