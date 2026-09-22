defmodule Coflux.Orchestration.Manifests do
  import Coflux.Store

  alias Coflux.Orchestration.{TagSets, CacheConfigs, Utils}

  # SQLite binds at most 999 parameters by default, so an id list longer
  # than this is queried in chunks.
  @max_query_ids 900

  def register_manifests(db, workspace_id, manifests, created_by \\ nil) do
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
                       :cache_config_id, :defer_params, :delay_ms, :retry_limit,
                       :retry_backoff_min_ms, :retry_backoff_max_ms, :recurrent, :timeout_ms,
                       :requires_tag_set_id, :memo, :streams_buffer, :streams_timeout_ms,
                       :concurrency_limit, :concurrency_params, :concurrency_namespace},
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

                        # streams_buffer column: NULL = unset (no streams
                        # config), -1 = unbounded (nil buffer in a set
                        # config), N >= 0 = bounded. Mirrors retry_limit's
                        # -1-for-unlimited convention.
                        {streams_buffer, streams_timeout_ms} =
                          case workflow[:streams] do
                            nil -> {nil, nil}
                            streams -> {streams[:buffer] || -1, streams[:timeout_ms]}
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
                          workflow.delay_ms,
                          if(workflow.retries, do: workflow.retries.limit || -1, else: 0),
                          if(workflow.retries, do: workflow.retries.backoff_min_ms, else: 0),
                          if(workflow.retries, do: workflow.retries.backoff_max_ms, else: 0),
                          if(workflow.recurrent, do: 1, else: 0),
                          workflow[:timeout_ms] || 0,
                          requires_tag_set_id,
                          if(workflow[:memo], do: 1),
                          streams_buffer,
                          streams_timeout_ms,
                          if(workflow[:concurrency], do: workflow.concurrency.limit, else: 0),
                          if(workflow[:concurrency],
                            do: Utils.encode_params_list(workflow.concurrency.params)
                          ),
                          if(workflow[:concurrency], do: workflow.concurrency.namespace)
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

      {:ok, current_manifest_ids} = get_latest_manifest_ids(db, workspace_id)

      now = current_timestamp()

      {:ok, _} =
        insert_many(
          db,
          :workspace_manifests,
          {:workspace_id, :module, :manifest_id, :created_at, :created_by},
          Enum.reduce(manifest_ids, [], fn {module, manifest_id}, result ->
            if manifest_id != Map.get(current_manifest_ids, module) do
              [{workspace_id, module, manifest_id, now, created_by} | result]
            else
              result
            end
          end)
        )

      :ok
    end)
  end

  def archive_module(db, workspace_id, module_name, created_by \\ nil) do
    with_transaction(db, fn ->
      now = current_timestamp()

      {:ok, _} =
        insert_one(db, :workspace_manifests, %{
          workspace_id: workspace_id,
          module: module_name,
          manifest_id: nil,
          created_at: now,
          created_by: created_by
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
              Map.put(result, module, workflows)
            else
              result
            end
          end)

        {:ok, manifests}
    end
  end

  @doc "The workflows of the module's latest manifest in the workspace, or nil if none or archived."
  def get_latest_manifest(db, workspace_id, module) do
    case query_one(
           db,
           """
           SELECT manifest_id
           FROM workspace_manifests
           WHERE workspace_id = ?1 AND module = ?2
           ORDER BY created_at DESC
           LIMIT 1
           """,
           {workspace_id, module}
         ) do
      {:ok, nil} -> {:ok, nil}
      {:ok, {nil}} -> {:ok, nil}
      {:ok, {manifest_id}} -> get_manifest_workflows(db, manifest_id)
    end
  end

  def get_latest_workflow(db, workspace_id, module, target_name) do
    case query_one(
           db,
           """
           SELECT w.parameter_set_id, w.instruction_id, w.wait_for, w.cache_config_id, w.defer_params, w.delay_ms, w.retry_limit, w.retry_backoff_min_ms, w.retry_backoff_max_ms, w.recurrent, w.timeout_ms, w.requires_tag_set_id, w.memo, w.streams_buffer, w.streams_timeout_ms, w.concurrency_limit, w.concurrency_params, w.concurrency_namespace
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
       {parameter_set_id, instruction_id, wait_for, cache_config_id, defer_params, delay_ms,
        retry_limit, retry_backoff_min_ms, retry_backoff_max_ms, recurrent, timeout_ms,
        requires_tag_set_id, memo, streams_buffer, streams_timeout_ms, concurrency_limit,
        concurrency_params, concurrency_namespace}} ->
        build_workflow(
          db,
          parameter_set_id,
          instruction_id,
          wait_for,
          cache_config_id,
          defer_params,
          delay_ms,
          retry_limit,
          retry_backoff_min_ms,
          retry_backoff_max_ms,
          recurrent,
          timeout_ms,
          requires_tag_set_id,
          memo,
          streams_buffer,
          streams_timeout_ms,
          concurrency_limit,
          concurrency_params,
          concurrency_namespace
        )
    end
  end

  defp get_manifest_workflows(db, manifest_id) do
    case query(
           db,
           """
           SELECT name, instruction_id, parameter_set_id, wait_for, cache_config_id, defer_params, delay_ms, retry_limit, retry_backoff_min_ms, retry_backoff_max_ms, recurrent, timeout_ms, requires_tag_set_id, memo, streams_buffer, streams_timeout_ms, concurrency_limit, concurrency_params, concurrency_namespace
           FROM workflows
           WHERE manifest_id = ?1
           """,
           {manifest_id}
         ) do
      {:ok, rows} ->
        workflows =
          Map.new(rows, fn {name, instruction_id, parameter_set_id, wait_for, cache_config_id,
                            defer_params, delay_ms, retry_limit, retry_backoff_min_ms,
                            retry_backoff_max_ms, recurrent, timeout_ms, requires_tag_set_id,
                            memo, streams_buffer, streams_timeout_ms, concurrency_limit,
                            concurrency_params, concurrency_namespace} ->
            {:ok, workflow} =
              build_workflow(
                db,
                parameter_set_id,
                instruction_id,
                wait_for,
                cache_config_id,
                defer_params,
                delay_ms,
                retry_limit,
                retry_backoff_min_ms,
                retry_backoff_max_ms,
                recurrent,
                timeout_ms,
                requires_tag_set_id,
                memo,
                streams_buffer,
                streams_timeout_ms,
                concurrency_limit,
                concurrency_params,
                concurrency_namespace
              )

            {name, workflow}
          end)

        {:ok, workflows}
    end
  end

  defp hash_manifest_workflows(workflows) do
    data =
      Enum.map(workflows, fn {name, workflow} ->
        [
          name,
          hash_parameter_set(workflow.parameters),
          Integer.to_string(Utils.encode_params_set(workflow.wait_for)),
          if(workflow.cache, do: Utils.encode_params_list(workflow.cache.params) || "", else: "-"),
          if(workflow.cache[:max_age_ms],
            do: Integer.to_string(workflow.cache.max_age_ms),
            else: ""
          ),
          if(workflow.cache[:namespace], do: workflow.cache.namespace, else: ""),
          if(workflow.cache[:version], do: workflow.cache.version, else: ""),
          if(workflow.defer, do: Utils.encode_params_list(workflow.defer.params) || "", else: "-"),
          Integer.to_string(workflow.delay_ms),
          if(workflow.retries,
            do:
              if(workflow.retries.limit,
                do: Integer.to_string(workflow.retries.limit),
                else: "unlimited"
              ),
            else: ""
          ),
          if(workflow.retries[:backoff_min_ms],
            do: Integer.to_string(workflow.retries.backoff_min_ms),
            else: ""
          ),
          if(workflow.retries[:backoff_max_ms],
            do: Integer.to_string(workflow.retries.backoff_max_ms),
            else: ""
          ),
          if(workflow.recurrent, do: "1", else: "0"),
          Integer.to_string(workflow[:timeout_ms] || 0),
          hash_requires(workflow.requires),
          if(workflow[:memo], do: "1", else: "0"),
          workflow.instruction || "",
          hash_streams(workflow[:streams]),
          hash_concurrency(workflow[:concurrency])
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
         delay_ms,
         retry_limit,
         retry_backoff_min_ms,
         retry_backoff_max_ms,
         recurrent,
         timeout_ms,
         requires_tag_set_id,
         memo,
         streams_buffer,
         streams_timeout_ms,
         concurrency_limit,
         concurrency_params,
         concurrency_namespace
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
            backoff_min_ms: retry_backoff_min_ms,
            backoff_max_ms: retry_backoff_max_ms
          }

        # positive = that many retries
        true ->
          %{
            limit: retry_limit,
            backoff_min_ms: retry_backoff_min_ms,
            backoff_max_ms: retry_backoff_max_ms
          }
      end

    streams =
      if streams_buffer != nil or streams_timeout_ms != nil do
        # -1 in the column means an explicitly-unbounded buffer (nil in
        # the in-memory config); NULL means the config was never set.
        %{
          buffer: if(streams_buffer == -1, do: nil, else: streams_buffer),
          timeout_ms: streams_timeout_ms
        }
      end

    concurrency =
      if concurrency_limit > 0 do
        %{
          limit: concurrency_limit,
          params: Utils.decode_params_list(concurrency_params),
          namespace: concurrency_namespace
        }
      end

    {:ok,
     %{
       parameters: parameters,
       instruction_id: instruction_id,
       wait_for: Utils.decode_params_set(wait_for),
       cache: cache,
       defer: defer,
       delay_ms: delay_ms,
       retries: retries,
       recurrent: recurrent == 1,
       timeout_ms: timeout_ms,
       requires: requires,
       memo: memo == 1,
       streams: streams,
       concurrency: concurrency
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

  @doc """
  The content of each of `instruction_ids`, as `%{id => content}`. Ids
  with no row are absent. One query per batch, so a whole workspace's
  manifests can be resolved without a round trip per workflow.
  """
  def get_instructions(db, instruction_ids) do
    contents =
      instruction_ids
      |> Enum.uniq()
      |> Enum.chunk_every(@max_query_ids)
      |> Enum.reduce(%{}, fn chunk, acc ->
        placeholders = Enum.map_join(1..length(chunk), ", ", &"?#{&1}")

        {:ok, rows} =
          query(
            db,
            "SELECT id, content FROM instructions WHERE id IN (#{placeholders})",
            List.to_tuple(chunk)
          )

        Enum.into(rows, acc, fn {id, content} -> {id, content} end)
      end)

    {:ok, contents}
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

  defp hash_streams(nil), do: "-"

  defp hash_streams(streams) do
    "#{if streams[:buffer] != nil, do: Integer.to_string(streams[:buffer]), else: ""}:" <>
      "#{if streams[:timeout_ms] != nil, do: Integer.to_string(streams[:timeout_ms]), else: ""}"
  end

  defp hash_concurrency(nil), do: "-"

  defp hash_concurrency(concurrency) do
    "#{concurrency.limit}:" <>
      "#{Utils.encode_params_list(concurrency.params) || ""}:" <>
      "#{concurrency.namespace || ""}"
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
