defmodule Coflux.Handlers.Api do
  import Coflux.Handlers.Utils

  alias Coflux.{Auth, Config, Orchestration, MapUtils, Scopes, Version}
  alias Coflux.Orchestration.Ids

  @max_parameters 20

  @ecs_launch_types ["FARGATE", "EC2", "EXTERNAL"]

  # A directory upload arrives as one entry per file, so this bounds an
  # accidental drop of a very large tree. Unlike the sizes, which the
  # client asserts, the count is something the server can see for itself.
  @max_asset_entries 1000
  @max_asset_entry_path_length 1000
  @blob_key_regex ~r/\A[0-9a-f]{64}\z/

  def init(req, opts) do
    req = set_cors_headers(req)

    expected_version =
      case :cowboy_req.header("x-api-version", req) do
        :undefined -> nil
        value -> value
      end

    case Version.check(expected_version) do
      :ok ->
        case :cowboy_req.method(req) do
          "OPTIONS" ->
            req = :cowboy_req.reply(204, req)
            {:ok, req, opts}

          method ->
            token = get_token(req)
            host = get_host(req)

            with {:ok, project_id} <- resolve_project(req),
                 {:ok, access} <- Auth.check(token, project_id, host) do
              req = handle(req, method, :cowboy_req.path_info(req), project_id, access)
              {:ok, req, opts}
            else
              {:error, :not_configured} ->
                req = json_error_response(req, "not_configured", status: 500)
                {:ok, req, opts}

              {:error, :invalid_host} ->
                req = json_error_response(req, "invalid_host", status: 400)
                {:ok, req, opts}

              {:error, :project_required} ->
                req = json_error_response(req, "project_required", status: 400)
                {:ok, req, opts}

              {:error, :project_mismatch} ->
                req = json_error_response(req, "project_mismatch", status: 403)
                {:ok, req, opts}

              {:error, :unauthorized} ->
                req = json_error_response(req, "unauthorized", status: 401)
                {:ok, req, opts}
            end
        end

      {:error, server_version, expected_version} ->
        json_error_response(req, "version_mismatch",
          status: 409,
          details: %{
            "server" => server_version,
            "expected" => expected_version
          }
        )
    end
  end

  # Whether the caller can grant the requested access: each scope asked for
  # has to be contained whole by one the caller holds. Holding a workspace
  # inside a scope is not holding the scope.
  defp workspaces_covered?(:all, _requested), do: true
  defp workspaces_covered?(_caller, nil), do: true

  defp workspaces_covered?(caller_scopes, requested) do
    Enum.all?(requested, &Scopes.contains_any?(caller_scopes, &1))
  end

  defp handle(req, "GET", ["discover"], _project_id, %{workspaces: workspaces}) do
    patterns = if workspaces == :all, do: ["*"], else: workspaces

    json_response(req, %{
      "version" => Version.version(),
      "api_version" => Version.api_version(),
      "access" => %{"workspaces" => patterns}
    })
  end

  defp handle(req, "POST", ["create_workspace"], project_id, access) do
    case read_arguments(req, %{name: "name"}, %{base_id: "baseId"}) do
      {:ok, arguments, req} ->
        case Orchestration.create_workspace(
               project_id,
               arguments.name,
               arguments[:base_id],
               access
             ) do
          {:ok, _workspace_id, workspace_external_id} ->
            json_response(req, %{id: workspace_external_id})

          {:error, :name_restricted} ->
            json_error_response(req, "bad_request", details: %{"name" => "restricted"})

          {:error, field_errors} when is_map(field_errors) ->
            field_errors =
              MapUtils.translate_keys(field_errors, %{name: "name", base_id: "baseId"})

            json_error_response(req, "bad_request", details: field_errors)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["update_workspace"], project_id, access) do
    case read_arguments(
           req,
           %{workspace_id: "workspaceId"},
           %{name: "name", base_id: "baseId"}
         ) do
      {:ok, arguments, req} ->
        case Orchestration.update_workspace(
               project_id,
               arguments.workspace_id,
               Map.take(arguments, [:name, :base_id]),
               access
             ) do
          :ok ->
            :cowboy_req.reply(204, req)

          {:error, :workspace_invalid} ->
            json_error_response(req, "not_found", status: 404)

          {:error, :forbidden} ->
            json_error_response(req, "forbidden", status: 403)

          {:error, :name_restricted} ->
            json_error_response(req, "bad_request", details: %{"name" => "restricted"})

          {:error, field_errors} when is_map(field_errors) ->
            field_errors =
              MapUtils.translate_keys(field_errors, %{name: "name", base_id: "baseId"})

            json_error_response(req, "bad_request", details: field_errors)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["pause_workspace"], project_id, access) do
    case read_arguments(req, %{workspace_id: "workspaceId"}) do
      {:ok, arguments, req} ->
        case Orchestration.pause_workspace(
               project_id,
               arguments.workspace_id,
               access
             ) do
          :ok -> :cowboy_req.reply(204, req)
          {:error, :workspace_invalid} -> json_error_response(req, "not_found", status: 404)
          {:error, :forbidden} -> json_error_response(req, "forbidden", status: 403)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["resume_workspace"], project_id, access) do
    case read_arguments(req, %{workspace_id: "workspaceId"}) do
      {:ok, arguments, req} ->
        case Orchestration.resume_workspace(
               project_id,
               arguments.workspace_id,
               access
             ) do
          :ok -> :cowboy_req.reply(204, req)
          {:error, :workspace_invalid} -> json_error_response(req, "not_found", status: 404)
          {:error, :forbidden} -> json_error_response(req, "forbidden", status: 403)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["archive_workspace"], project_id, access) do
    case read_arguments(req, %{workspace_id: "workspaceId"}) do
      {:ok, arguments, req} ->
        case Orchestration.archive_workspace(
               project_id,
               arguments.workspace_id,
               access
             ) do
          :ok ->
            :cowboy_req.reply(204, req)

          {:error, :descendants} ->
            json_error_response(req, "bad_request",
              details: %{"workspaceId" => "has_dependencies"}
            )

          {:error, :workspace_invalid} ->
            json_error_response(req, "not_found", status: 404)

          {:error, :forbidden} ->
            json_error_response(req, "forbidden", status: 403)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["create_pool"], project_id, access) do
    case read_arguments(req, %{
           workspace_id: "workspaceId",
           pool_name: {"poolName", &parse_pool_name/1},
           pool: {"pool", &parse_pool/1}
         }) do
      {:ok, arguments, req} ->
        case Orchestration.create_pool(
               project_id,
               arguments.workspace_id,
               arguments.pool_name,
               arguments.pool,
               access
             ) do
          :ok -> :cowboy_req.reply(204, req)
          {:error, :already_exists} -> json_error_response(req, "already_exists", status: 409)
          {:error, {:secrets_not_found, names}} -> secrets_not_found_response(req, names)
          {:error, :forbidden} -> json_error_response(req, "forbidden", status: 403)
          {:error, :workspace_invalid} -> json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["update_pool"], project_id, access) do
    case read_arguments(req, %{
           workspace_id: "workspaceId",
           pool_name: {"poolName", &parse_pool_name/1},
           pool: {"pool", &parse_pool_patch/1}
         }) do
      {:ok, arguments, req} ->
        case Orchestration.update_pool(
               project_id,
               arguments.workspace_id,
               arguments.pool_name,
               arguments.pool,
               access
             ) do
          :ok -> :cowboy_req.reply(204, req)
          {:error, :not_found} -> json_error_response(req, "not_found", status: 404)
          {:error, :type_change} -> json_error_response(req, "type_change", status: 409)
          {:error, {:secrets_not_found, names}} -> secrets_not_found_response(req, names)
          {:error, :forbidden} -> json_error_response(req, "forbidden", status: 403)
          {:error, :workspace_invalid} -> json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["disable_pool"], project_id, access) do
    case read_arguments(req, %{
           workspace_id: "workspaceId",
           pool_name: {"poolName", &parse_pool_name/1}
         }) do
      {:ok, arguments, req} ->
        case Orchestration.disable_pool(
               project_id,
               arguments.workspace_id,
               arguments.pool_name,
               access
             ) do
          :ok -> :cowboy_req.reply(204, req)
          {:error, :not_found} -> json_error_response(req, "not_found", status: 404)
          {:error, :workspace_invalid} -> json_error_response(req, "not_found", status: 404)
          {:error, :forbidden} -> json_error_response(req, "forbidden", status: 403)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["enable_pool"], project_id, access) do
    case read_arguments(req, %{
           workspace_id: "workspaceId",
           pool_name: {"poolName", &parse_pool_name/1}
         }) do
      {:ok, arguments, req} ->
        case Orchestration.enable_pool(
               project_id,
               arguments.workspace_id,
               arguments.pool_name,
               access
             ) do
          :ok -> :cowboy_req.reply(204, req)
          {:error, :not_found} -> json_error_response(req, "not_found", status: 404)
          {:error, :workspace_invalid} -> json_error_response(req, "not_found", status: 404)
          {:error, :forbidden} -> json_error_response(req, "forbidden", status: 403)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["get_pools"], project_id, _access) do
    case read_arguments(req, %{workspace_id: "workspaceId"}) do
      {:ok, arguments, req} ->
        case Orchestration.get_pools(project_id, arguments.workspace_id) do
          {:ok, pools, hash} ->
            result =
              Map.new(pools, fn {name, pool} ->
                {name, build_pool_config(pool)}
              end)

            req = :cowboy_req.set_resp_header("etag", "\"#{hash}\"", req)
            json_response(req, result)

          {:error, :workspace_invalid} ->
            json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["update_pools"], project_id, access) do
    case read_arguments(req, %{
           workspace_id: "workspaceId",
           pools: {"pools", &parse_pools/1}
         }) do
      {:ok, arguments, req} ->
        expected_hash =
          case :cowboy_req.header("if-match", req) do
            :undefined -> nil
            raw_etag -> String.trim(raw_etag, "\"")
          end

        case Orchestration.update_pools(
               project_id,
               arguments.workspace_id,
               arguments.pools,
               expected_hash,
               access
             ) do
          :ok ->
            :cowboy_req.reply(204, req)

          {:error, {:secrets_not_found, names}} ->
            secrets_not_found_response(req, names)

          {:error, :conflict} ->
            json_error_response(req, "conflict",
              status: 412,
              details: %{"message" => "Pool configuration has changed"}
            )

          {:error, :forbidden} ->
            json_error_response(req, "forbidden", status: 403)

          {:error, :workspace_invalid} ->
            json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["stop_worker"], project_id, access) do
    case read_arguments(req, %{
           workspace_id: "workspaceId",
           worker_id: "workerId"
         }) do
      {:ok, arguments, req} ->
        case Orchestration.stop_worker(
               project_id,
               arguments.workspace_id,
               arguments.worker_id,
               access
             ) do
          :ok -> :cowboy_req.reply(204, req)
          {:error, :forbidden} -> json_error_response(req, "forbidden", status: 403)
          {:error, :not_found} -> json_error_response(req, "not_found", status: 404)
          {:error, :workspace_invalid} -> json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["resume_worker"], project_id, access) do
    case read_arguments(req, %{
           workspace_id: "workspaceId",
           worker_id: "workerId"
         }) do
      {:ok, arguments, req} ->
        case Orchestration.resume_worker(
               project_id,
               arguments.workspace_id,
               arguments.worker_id,
               access
             ) do
          :ok -> :cowboy_req.reply(204, req)
          {:error, :forbidden} -> json_error_response(req, "forbidden", status: 403)
          {:error, :not_found} -> json_error_response(req, "not_found", status: 404)
          {:error, :workspace_invalid} -> json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["register_manifests"], project_id, access) do
    case read_arguments(req, %{
           workspace_id: "workspaceId",
           manifests: {"manifests", &parse_manifests/1}
         }) do
      {:ok, arguments, req} ->
        case Orchestration.register_manifests(
               project_id,
               arguments.workspace_id,
               arguments.manifests,
               access
             ) do
          :ok -> :cowboy_req.reply(204, req)
          {:error, :forbidden} -> json_error_response(req, "forbidden", status: 403)
          {:error, :workspace_invalid} -> json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["archive_module"], project_id, access) do
    case read_arguments(req, %{
           workspace_id: "workspaceId",
           module_name: "moduleName"
         }) do
      {:ok, arguments, req} ->
        case Orchestration.archive_module(
               project_id,
               arguments.workspace_id,
               arguments.module_name,
               access
             ) do
          :ok -> :cowboy_req.reply(204, req)
          {:error, :forbidden} -> json_error_response(req, "forbidden", status: 403)
          {:error, :workspace_invalid} -> json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["submit_workflow"], project_id, access) do
    case read_arguments(
           req,
           %{
             module: "module",
             target: "target",
             workspace_id: "workspaceId",
             arguments: {"arguments", &parse_arguments/1}
           },
           %{
             wait_for: {"waitFor", &parse_indexes/1},
             cache: {"cache", &parse_cache/1},
             defer: {"defer", &parse_defer/1},
             delay: {"delay", &parse_integer(&1, optional: true)},
             retries: {"retries", &parse_retries/1},
             recurrent: {"recurrent", &parse_boolean(&1, optional: true)},
             timeout: {"timeout", &parse_integer(&1, optional: true)},
             requires: {"requires", &parse_tag_set/1},
             memo: {"memo", &parse_boolean(&1, optional: true)},
             streams: {"streams", &parse_streams_config/1},
             concurrency: {"concurrency", &parse_concurrency/1},
             idempotency_key: {"idempotencyKey", &parse_string(&1, optional: true)},
             catalog: {"catalog", &parse_string(&1, optional: true)}
           }
         ) do
      {:ok, arguments, req} ->
        case Orchestration.start_run(
               project_id,
               arguments.module,
               arguments.target,
               :workflow,
               arguments.arguments,
               access,
               workspace: arguments.workspace_id,
               wait_for: arguments[:wait_for],
               cache: arguments[:cache],
               defer: arguments[:defer],
               delay: arguments[:delay] || 0,
               retries: arguments[:retries],
               recurrent: arguments[:recurrent] == true,
               timeout: arguments[:timeout] || 0,
               requires: arguments[:requires],
               memo: arguments[:memo],
               streams: arguments[:streams],
               concurrency: arguments[:concurrency],
               idempotency_key: arguments[:idempotency_key],
               catalog: arguments[:catalog]
             ) do
          {:ok, run_id, step_number, execution_external_id} ->
            json_response(req, %{
              "runId" => run_id,
              "stepId" => Ids.step(run_id, step_number),
              "executionId" => execution_external_id
            })

          {:error, :forbidden} ->
            json_error_response(req, "forbidden", status: 403)

          {:error, :workspace_invalid} ->
            json_error_response(req, "not_found", status: 404)

          {:error, :asset_not_found} ->
            json_error_response(req, "not_found",
              status: 404,
              details: %{"arguments" => "asset_unknown"}
            )

          {:error, reason}
          when reason in [:catalog_invalid, :catalog_not_found, :catalog_invisible] ->
            json_error_response(req, "bad_request",
              details: %{"catalog" => catalog_error(reason)}
            )
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["cancel_execution"], project_id, access) do
    case read_arguments(req, %{execution_id: "executionId", workspace_id: "workspaceId"}) do
      {:ok, arguments, req} ->
        case Orchestration.cancel_execution(
               project_id,
               arguments.workspace_id,
               arguments.execution_id,
               access
             ) do
          :ok ->
            json_response(req, %{})

          {:error, :forbidden} ->
            json_error_response(req, "forbidden", status: 403)

          {:error, :not_found} ->
            json_error_response(req, "not_found", status: 404)

          {:error, :workspace_invalid} ->
            json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["rerun_step"], project_id, access) do
    case read_arguments(
           req,
           %{
             workspace_id: "workspaceId",
             step_id: "stepId"
           },
           %{catalog: {"catalog", &parse_string(&1, optional: true)}}
         ) do
      {:ok, arguments, req} ->
        case Orchestration.rerun_step(
               project_id,
               arguments.step_id,
               arguments.workspace_id,
               access,
               catalog: arguments[:catalog]
             ) do
          {:ok, execution_external_id, attempt} ->
            json_response(req, %{"executionId" => execution_external_id, "attempt" => attempt})

          {:error, :forbidden} ->
            json_error_response(req, "forbidden", status: 403)

          {:error, :invalid} ->
            json_error_response(req, "bad_request", details: %{"stepId" => "invalid"})

          {:error, reason}
          when reason in [:catalog_invalid, :catalog_not_found, :catalog_invisible] ->
            json_error_response(req, "bad_request",
              details: %{"catalog" => catalog_error(reason)}
            )

          {:error, :not_found} ->
            json_error_response(req, "not_found", status: 404)

          {:error, :workspace_invalid} ->
            json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["respond_input"], project_id, access) do
    case read_arguments(req, %{input_id: "inputId"}, %{value: "value"}) do
      {:ok, arguments, req} ->
        case Orchestration.respond_input(
               project_id,
               arguments.input_id,
               Map.get(arguments, :value),
               access
             ) do
          :ok ->
            json_response(req, %{})

          {:error, :not_found} ->
            json_error_response(req, "not_found", status: 404)

          {:error, :already_responded} ->
            json_error_response(req, "already_responded", status: 409)

          {:error, {:validation_failed, reason}} ->
            json_error_response(req, "validation_failed", details: reason, status: 422)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["dismiss_input"], project_id, access) do
    case read_arguments(req, %{input_id: "inputId"}) do
      {:ok, arguments, req} ->
        case Orchestration.dismiss_input(project_id, arguments.input_id, access) do
          :ok ->
            json_response(req, %{})

          {:error, :not_found} ->
            json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["get_input"], project_id, _access) do
    case read_arguments(req, %{input_id: "inputId"}) do
      {:ok, arguments, req} ->
        case Orchestration.get_input(project_id, arguments.input_id) do
          {:ok, input} ->
            placeholders =
              Map.new(input.placeholders, fn {placeholder, value} ->
                {placeholder, Coflux.TopicUtils.build_value(value)}
              end)

            response =
              case input.response do
                nil ->
                  nil

                %{type: type, value: value, created_at: created_at, created_by: created_by} ->
                  resp = %{
                    "type" => Atom.to_string(type),
                    "createdAt" => created_at
                  }

                  resp = if value, do: Map.put(resp, "value", value), else: resp

                  if created_by do
                    Map.put(resp, "createdBy", %{
                      "type" => created_by.type,
                      "externalId" => created_by.external_id
                    })
                  else
                    resp
                  end
              end

            initial =
              if input.initial do
                Jason.decode!(input.initial)
              end

            json_response(req, %{
              "key" => input.key,
              "template" => input.template,
              "placeholders" => placeholders,
              "schema" => input.schema,
              "initial" => initial,
              "title" => input.title,
              "actions" => input.actions,
              "requires" => input.requires,
              "createdAt" => input.created_at,
              "response" => response
            })

          {:error, :not_found} ->
            json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  # --- Assets ---

  # Assembles an asset from blobs the client has already stored. The server
  # never sees the bytes — only keys, sizes and paths — which is what keeps
  # uploads that go straight to an S3 blob store working.
  defp handle(req, "POST", ["create_asset"], project_id, access) do
    case read_arguments(
           req,
           %{
             workspace_id: "workspaceId",
             entries: {"entries", &parse_asset_entries/1}
           },
           %{name: {"name", &parse_string(&1, optional: true, max_length: 200)}}
         ) do
      {:ok, arguments, req} ->
        case Orchestration.create_asset(
               project_id,
               arguments.workspace_id,
               arguments[:name],
               arguments.entries,
               access
             ) do
          {:ok, external_id, metadata} ->
            json_response(req, %{
              "assetId" => external_id,
              "name" => metadata.name,
              "totalCount" => metadata.total_count,
              "totalSize" => metadata.total_size
            })

          {:error, :forbidden} ->
            json_error_response(req, "forbidden", status: 403)

          {:error, :workspace_invalid} ->
            json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  # --- Catalog ---

  defp handle(req, "POST", ["get_catalog"], project_id, _access) do
    case read_arguments(req, %{workspace_id: "workspaceId"}, %{
           prefix: {"prefix", &parse_string(&1, optional: true)}
         }) do
      {:ok, arguments, req} ->
        case Orchestration.catalog_list(project_id, arguments.workspace_id, arguments[:prefix]) do
          {:ok, versions} ->
            json_response(req, %{
              "entries" => Enum.map(versions, &Coflux.TopicUtils.build_catalog_version/1)
            })

          {:error, :workspace_invalid} ->
            json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["get_catalog_versions"], project_id, _access) do
    case read_arguments(req, %{workspace_id: "workspaceId", path: "path"}, %{
           limit: {"limit", &parse_integer(&1, optional: true)},
           before: {"before", &parse_integer(&1, optional: true)}
         }) do
      {:ok, arguments, req} ->
        limit = min(arguments[:limit] || 50, 500)

        case Orchestration.catalog_versions(
               project_id,
               arguments.workspace_id,
               arguments.path,
               limit,
               arguments[:before]
             ) do
          {:ok, versions} ->
            json_response(req, %{
              "versions" => Enum.map(versions, &Coflux.TopicUtils.build_catalog_version/1)
            })

          {:error, :invalid_path} ->
            json_error_response(req, "bad_request", details: %{"path" => "invalid"})

          {:error, :workspace_invalid} ->
            json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  # Publishes either a JSON document (`value`, encoded the way a submitted
  # argument is) or an existing asset (`assetId`) — exactly one of the two.
  # Takes the same argument shape as `submit_workflow`, so one value editor
  # feeds both.
  defp handle(req, "POST", ["publish_catalog"], project_id, access) do
    case read_arguments(req, %{
           workspace_id: "workspaceId",
           path: "path",
           argument: {"argument", &parse_argument/1}
         }) do
      {:ok, arguments, req} ->
        case Orchestration.publish_catalog(
               project_id,
               arguments.workspace_id,
               arguments.path,
               arguments.argument,
               access
             ) do
          {:ok, version, created?} ->
            json_response(req, %{
              "version" => Coflux.TopicUtils.build_catalog_version(version),
              "created" => created?
            })

          {:error, :invalid_path} ->
            json_error_response(req, "bad_request", details: %{"path" => "invalid"})

          {:error, :asset_not_found} ->
            json_error_response(req, "not_found",
              status: 404,
              details: %{"argument" => "asset_unknown"}
            )

          {:error, :forbidden} ->
            json_error_response(req, "forbidden", status: 403)

          {:error, :workspace_invalid} ->
            json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "GET", ["search"], project_id, _access) do
    qs = :cowboy_req.parse_qs(req)
    workspace_id = get_query_param(qs, "workspaceId")
    query = get_query_param(qs, "query")

    case Topical.execute(
           Coflux.TopicalRegistry,
           ["workspaces", workspace_id, "search"],
           "query",
           {query},
           %{project: project_id}
         ) do
      {:ok, matches} ->
        json_response(req, %{"matches" => matches})

      {:error, _reason} ->
        json_error_response(req, "search_failed", status: 500)
    end
  end

  defp handle(req, "POST", ["create_session"], project_id, access) do
    case read_arguments(
           req,
           %{workspace_id: "workspaceId"},
           %{
             provides: {"provides", &parse_tag_set/1},
             accepts: {"accepts", &parse_tag_set/1}
           }
         ) do
      {:ok, arguments, req} ->
        opts =
          [
            provides: arguments[:provides],
            accepts: arguments[:accepts]
          ]
          |> Enum.reject(fn {_, v} -> is_nil(v) end)

        case Orchestration.create_session(project_id, arguments.workspace_id, access, opts) do
          {:ok, session_id} ->
            json_response(req, %{"sessionId" => session_id})

          {:error, :forbidden} ->
            json_error_response(req, "forbidden", status: 403)

          {:error, :workspace_invalid} ->
            json_error_response(req, "not_found", status: 404)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  # Token management endpoints

  defp handle(req, "POST", ["create_token"], project_id, access) do
    if Config.secret() == nil do
      json_error_response(req, "not_configured",
        status: 501,
        details: %{message: "Service tokens require COFLUX_SECRET to be configured"}
      )
    else
      case read_arguments(req, %{}, %{
             name: "name",
             workspaces: {"workspaces", &parse_workspaces/1}
           }) do
        {:ok, arguments, req} ->
          requested_workspaces = arguments[:workspaces]

          # Check if caller can grant the requested access level
          if not workspaces_covered?(access.workspaces, requested_workspaces) do
            json_error_response(req, "forbidden",
              status: 403,
              details: %{message: "Cannot create token with broader access than your own"}
            )
          else
            # If no workspaces specified, inherit caller's workspaces (unless caller has full access)
            effective_workspaces =
              case {requested_workspaces, access.workspaces} do
                {nil, :all} -> nil
                {nil, patterns} -> patterns
                {requested, _} -> requested
              end

            opts = if effective_workspaces, do: [workspaces: effective_workspaces], else: []

            case Orchestration.create_token(
                   project_id,
                   arguments[:name],
                   access[:principal_id],
                   opts
                 ) do
              {:ok, %{token: token, token_id: token_id, external_id: external_id}} ->
                json_response(req, %{
                  "token" => token,
                  "tokenId" => token_id,
                  "externalId" => external_id
                })
            end
          end

        {:error, errors, req} ->
          json_error_response(req, "bad_request", details: errors)
      end
    end
  end

  defp handle(req, "POST", ["set_secret"], project_id, access) do
    case read_arguments(req, %{
           name: {"name", &parse_secret_name/1},
           value: {"value", &parse_secret_value/1},
           workspaces: {"workspaces", &parse_workspaces/1}
         }) do
      {:ok, arguments, req} ->
        case Orchestration.set_secret(
               project_id,
               arguments.workspaces,
               arguments.name,
               arguments.value,
               access
             ) do
          {:ok, secrets} ->
            json_response(req, %{
              "name" => arguments.name,
              "secrets" =>
                Enum.map(secrets, fn secret ->
                  %{"workspaces" => secret.scope, "version" => secret.version}
                end)
            })

          {:error, :forbidden} ->
            json_error_response(req, "forbidden", status: 403)

          {:error, :no_secret} ->
            json_error_response(req, "bad_request",
              details: %{message: "Secrets require COFLUX_SECRET to be configured"}
            )
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["delete_secret"], project_id, access) do
    case read_arguments(req, %{
           name: {"name", &parse_secret_name/1},
           workspaces: {"workspaces", &parse_workspaces/1}
         }) do
      {:ok, arguments, req} ->
        case Orchestration.delete_secret(
               project_id,
               arguments.workspaces,
               arguments.name,
               access
             ) do
          {:ok, deleted} -> json_response(req, %{"workspaces" => deleted})
          {:error, :not_found} -> json_error_response(req, "not_found", status: 404)
          {:error, :forbidden} -> json_error_response(req, "forbidden", status: 403)
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["revoke_token"], project_id, access) do
    case read_arguments(req, %{external_id: "externalId"}) do
      {:ok, arguments, req} ->
        # Look up token by external_id, then revoke by internal id
        case Orchestration.get_token(project_id, arguments.external_id) do
          {:ok, nil} ->
            json_error_response(req, "not_found", status: 404)

          {:ok, token} ->
            # Allow revocation if caller has full access OR created this token
            can_revoke =
              access.workspaces == :all or
                token.created_by_principal_id == access[:principal_id]

            if can_revoke do
              case Orchestration.revoke_token(project_id, token.id) do
                {:ok, _external_id} ->
                  :cowboy_req.reply(204, req)

                {:error, :not_found} ->
                  json_error_response(req, "not_found", status: 404)
              end
            else
              json_error_response(req, "forbidden", status: 403)
            end
        end

      {:error, errors, req} ->
        json_error_response(req, "bad_request", details: errors)
    end
  end

  defp handle(req, "POST", ["rotate_epoch"], project_id, %{type: :super}) do
    case Orchestration.rotate_epoch(project_id) do
      :ok -> :cowboy_req.reply(204, req)
    end
  end

  defp handle(req, "POST", ["rotate_epoch"], _project_id, _access) do
    json_error_response(req, "forbidden", status: 403)
  end

  defp handle(req, "POST", ["rotate_logs"], project_id, %{type: :super}) do
    :ok = Coflux.Logs.Server.rotate(project_id)
    :cowboy_req.reply(204, req)
  end

  defp handle(req, "POST", ["rotate_logs"], _project_id, _access) do
    json_error_response(req, "forbidden", status: 403)
  end

  defp handle(req, _method, _path, _project, _access) do
    json_error_response(req, "not_found", status: 404)
  end

  # Helper functions for handle/5 clauses

  defp parse_workspaces(value) when is_list(value) do
    if value != [] and Enum.all?(value, &Scopes.valid?/1) do
      {:ok, Enum.uniq(value)}
    else
      {:error, :invalid}
    end
  end

  defp parse_workspaces(_), do: {:error, :invalid}

  defp is_valid_json?(value) do
    if value do
      case Jason.decode(value) do
        {:ok, _} -> true
        {:error, _} -> false
      end
    else
      false
    end
  end

  def is_valid_string?(value, opts) do
    cond do
      not is_binary(value) -> false
      opts[:max_length] && String.length(value) > opts[:max_length] -> false
      opts[:regex] && !Regex.match?(opts[:regex], value) -> false
      true -> true
    end
  end

  defp is_valid_tag_key?(key) do
    is_valid_string?(key, regex: ~r/^[a-z0-9_-]{1,20}$/i)
  end

  defp is_valid_tag_value?(value) do
    is_valid_string?(value, regex: ~r/^[a-z0-9_-]{1,30}$/i)
  end

  defp is_valid_pool_name?(name) do
    is_valid_string?(name, regex: ~r/^[a-z][a-z0-9_-]{0,19}$/i)
  end

  defp parse_pool_name(name) do
    if is_valid_pool_name?(name) do
      {:ok, name}
    else
      {:error, :invalid}
    end
  end

  # A pool's modules are module names, not patterns: the same list is
  # handed to the launcher as the worker's arguments, so a wildcard would
  # be passed to the worker to import - nothing expands it - as well as
  # matching no execution. Accepting one would mean a pool that quietly
  # never runs anything, so they are validated like any other module name.
  defp parse_modules(value) do
    value = List.wrap(value)

    if Enum.all?(value, &is_valid_module_name?/1) do
      {:ok, value}
    else
      {:error, :invalid}
    end
  end

  defp parse_tag_set_item(key, value) do
    value =
      value
      |> List.wrap()
      |> Enum.map(fn
        true -> "true"
        false -> "false"
        other -> other
      end)

    if is_valid_tag_key?(key) &&
         Enum.all?(value, &is_valid_tag_value?/1) &&
         length(value) <= 10 do
      {:ok, key, value}
    else
      {:error, :invalid}
    end
  end

  defp parse_tag_set(value) do
    cond do
      is_nil(value) ->
        {:ok, %{}}

      is_map(value) && map_size(value) <= 10 ->
        Enum.reduce_while(value, {:ok, %{}}, fn {key, value}, {:ok, result} ->
          case parse_tag_set_item(key, value) do
            {:ok, key, value} ->
              {:cont, {:ok, Map.put(result, key, value)}}

            {:error, error} ->
              {:halt, {:error, error}}
          end
        end)

      true ->
        {:error, :invalid}
    end
  end

  defp parse_docker_launcher(value) do
    image = Map.get(value, "image")
    docker_host = Map.get(value, "dockerHost")
    network_mode = Map.get(value, "networkMode")

    cond do
      not is_binary(image) or String.length(image) > 200 ->
        {:error, :invalid}

      not is_nil(docker_host) and (not is_binary(docker_host) or String.length(docker_host) > 200) ->
        {:error, :invalid}

      not is_nil(network_mode) and
          (not is_binary(network_mode) or String.length(network_mode) > 200) ->
        {:error, :invalid}

      true ->
        launcher = %{type: :docker, image: image}

        launcher =
          if docker_host, do: Map.put(launcher, :docker_host, docker_host), else: launcher

        launcher =
          if network_mode, do: Map.put(launcher, :network_mode, network_mode), else: launcher

        {:ok, launcher}
    end
  end

  defp parse_process_launcher(value) do
    directory = Map.get(value, "directory")

    cond do
      not is_binary(directory) or String.length(directory) > 500 ->
        {:error, :invalid}

      true ->
        {:ok, %{type: :process, directory: directory}}
    end
  end

  defp parse_kubernetes_launcher(value) do
    image = Map.get(value, "image")
    namespace = Map.get(value, "namespace")
    service_account = Map.get(value, "serviceAccount")
    api_server = Map.get(value, "apiServer")
    token_secret = Map.get(value, "tokenSecret")
    ca_cert = Map.get(value, "caCert")
    insecure = Map.get(value, "insecure")
    image_pull_policy = Map.get(value, "imagePullPolicy")
    node_selector = Map.get(value, "nodeSelector")
    tolerations = Map.get(value, "tolerations")
    image_pull_secrets = Map.get(value, "imagePullSecrets")
    host_aliases = Map.get(value, "hostAliases")
    resources = Map.get(value, "resources")
    labels = Map.get(value, "labels")
    annotations = Map.get(value, "annotations")
    active_deadline_seconds = Map.get(value, "activeDeadlineSeconds")
    volumes = Map.get(value, "volumes")
    volume_mounts = Map.get(value, "volumeMounts")

    valid_pull_policies = ["Always", "Never", "IfNotPresent"]

    cond do
      not is_binary(image) or String.length(image) > 200 ->
        {:error, :invalid}

      not is_nil(namespace) and (not is_binary(namespace) or String.length(namespace) > 253) ->
        {:error, :invalid}

      not is_nil(service_account) and
          (not is_binary(service_account) or String.length(service_account) > 253) ->
        {:error, :invalid}

      not is_nil(api_server) and (not is_binary(api_server) or String.length(api_server) > 500) ->
        {:error, :invalid}

      not is_nil(token_secret) and not Coflux.Admin.Secrets.valid_name?(token_secret) ->
        {:error, :invalid}

      not is_nil(ca_cert) and not is_binary(ca_cert) ->
        {:error, :invalid}

      not is_nil(insecure) and not is_boolean(insecure) ->
        {:error, :invalid}

      not is_nil(image_pull_policy) and image_pull_policy not in valid_pull_policies ->
        {:error, :invalid}

      not is_nil(node_selector) and not is_map(node_selector) ->
        {:error, :invalid}

      not is_nil(tolerations) and not is_list(tolerations) ->
        {:error, :invalid}

      not is_nil(image_pull_secrets) and
          (not is_list(image_pull_secrets) or
             Enum.any?(image_pull_secrets, &(not is_binary(&1)))) ->
        {:error, :invalid}

      not is_nil(host_aliases) and not is_list(host_aliases) ->
        {:error, :invalid}

      not is_nil(resources) and not is_map(resources) ->
        {:error, :invalid}

      not is_nil(labels) and
          (not is_map(labels) or
             Enum.any?(labels, fn {k, v} -> not is_binary(k) or not is_binary(v) end)) ->
        {:error, :invalid}

      not is_nil(annotations) and
          (not is_map(annotations) or
             Enum.any?(annotations, fn {k, v} -> not is_binary(k) or not is_binary(v) end)) ->
        {:error, :invalid}

      not is_nil(active_deadline_seconds) and
          (not is_integer(active_deadline_seconds) or active_deadline_seconds < 1) ->
        {:error, :invalid}

      not is_nil(volumes) and not is_list(volumes) ->
        {:error, :invalid}

      not is_nil(volume_mounts) and not is_list(volume_mounts) ->
        {:error, :invalid}

      true ->
        launcher = %{type: :kubernetes, image: image}

        launcher =
          if namespace, do: Map.put(launcher, :namespace, namespace), else: launcher

        launcher =
          if service_account,
            do: Map.put(launcher, :service_account, service_account),
            else: launcher

        launcher =
          if api_server, do: Map.put(launcher, :api_server, api_server), else: launcher

        launcher =
          if token_secret, do: Map.put(launcher, :token_secret, token_secret), else: launcher

        launcher = if ca_cert, do: Map.put(launcher, :ca_cert, ca_cert), else: launcher

        launcher =
          if insecure == true, do: Map.put(launcher, :insecure, true), else: launcher

        launcher =
          if image_pull_policy,
            do: Map.put(launcher, :image_pull_policy, image_pull_policy),
            else: launcher

        launcher =
          if node_selector, do: Map.put(launcher, :node_selector, node_selector), else: launcher

        launcher =
          if tolerations, do: Map.put(launcher, :tolerations, tolerations), else: launcher

        launcher =
          if image_pull_secrets,
            do: Map.put(launcher, :image_pull_secrets, image_pull_secrets),
            else: launcher

        launcher =
          if host_aliases,
            do: Map.put(launcher, :host_aliases, host_aliases),
            else: launcher

        launcher =
          if resources, do: Map.put(launcher, :resources, resources), else: launcher

        launcher =
          if labels, do: Map.put(launcher, :labels, labels), else: launcher

        launcher =
          if annotations, do: Map.put(launcher, :annotations, annotations), else: launcher

        launcher =
          if active_deadline_seconds,
            do: Map.put(launcher, :active_deadline_seconds, active_deadline_seconds),
            else: launcher

        launcher =
          if volumes, do: Map.put(launcher, :volumes, volumes), else: launcher

        launcher =
          if volume_mounts, do: Map.put(launcher, :volume_mounts, volume_mounts), else: launcher

        {:ok, launcher}
    end
  end

  defp parse_ecs_launcher(value) do
    cluster = Map.get(value, "cluster")
    task_definition = Map.get(value, "taskDefinition")
    region = Map.get(value, "region")
    container_name = Map.get(value, "containerName")
    launch_type = Map.get(value, "launchType")
    capacity_provider = Map.get(value, "capacityProvider")
    subnets = wrap_list(Map.get(value, "subnets"))
    security_groups = wrap_list(Map.get(value, "securityGroups"))
    assign_public_ip = Map.get(value, "assignPublicIp")
    platform_version = Map.get(value, "platformVersion")
    credentials_secret = Map.get(value, "credentialsSecret")
    endpoint = Map.get(value, "endpoint")

    cond do
      not is_binary(cluster) or cluster == "" or String.length(cluster) > 255 ->
        {:error, :invalid}

      not is_binary(task_definition) or task_definition == "" or
          String.length(task_definition) > 500 ->
        {:error, :invalid}

      not is_binary(region) or not Regex.match?(~r/^[a-z0-9-]{1,30}$/, region) ->
        {:error, :invalid}

      not is_nil(container_name) and
          (not is_binary(container_name) or String.length(container_name) > 255) ->
        {:error, :invalid}

      not is_nil(launch_type) and launch_type not in @ecs_launch_types ->
        {:error, :invalid}

      not is_nil(capacity_provider) and
          (not is_binary(capacity_provider) or String.length(capacity_provider) > 255) ->
        {:error, :invalid}

      # A capacity provider strategy decides the launch type itself.
      not is_nil(launch_type) and not is_nil(capacity_provider) ->
        {:error, :invalid}

      not is_nil(subnets) and not is_string_list?(subnets, 16) ->
        {:error, :invalid}

      not is_nil(security_groups) and not is_string_list?(security_groups, 5) ->
        {:error, :invalid}

      not is_nil(assign_public_ip) and not is_boolean(assign_public_ip) ->
        {:error, :invalid}

      not is_nil(platform_version) and
          (not is_binary(platform_version) or String.length(platform_version) > 50) ->
        {:error, :invalid}

      not is_nil(credentials_secret) and
          not Coflux.Admin.Secrets.valid_name?(credentials_secret) ->
        {:error, :invalid}

      not is_nil(endpoint) and
          (not is_binary(endpoint) or String.length(endpoint) > 500 or
             not String.starts_with?(endpoint, ["http://", "https://"])) ->
        {:error, :invalid}

      true ->
        launcher =
          %{type: :ecs, cluster: cluster, task_definition: task_definition, region: region}
          |> maybe_put_value(:container_name, container_name)
          |> maybe_put_value(:launch_type, launch_type)
          |> maybe_put_value(:capacity_provider, capacity_provider)
          |> maybe_put_value(:subnets, subnets)
          |> maybe_put_value(:security_groups, security_groups)
          |> maybe_put_value(:assign_public_ip, if(assign_public_ip == true, do: true))
          |> maybe_put_value(:platform_version, platform_version)
          |> maybe_put_value(:credentials_secret, credentials_secret)
          |> maybe_put_value(:endpoint, endpoint)

        {:ok, launcher}
    end
  end

  # A single ID is accepted where a list is expected, so `--set
  # subnets=subnet-1` works without JSON.
  defp wrap_list(value) when is_binary(value), do: [value]
  defp wrap_list(value), do: value

  defp is_string_list?(value, max_length) do
    is_list(value) and value != [] and length(value) <= max_length and
      Enum.all?(value, &(is_binary(&1) and &1 != ""))
  end

  defp parse_common_launcher_fields(launcher, value) do
    server_host = Map.get(value, "serverHost")
    server_secure = Map.get(value, "serverSecure")
    adapter = Map.get(value, "adapter")
    concurrency = Map.get(value, "concurrency")
    env = Map.get(value, "env")
    env_secrets = Map.get(value, "envSecrets")

    cond do
      not is_nil(server_host) and (not is_binary(server_host) or String.length(server_host) > 200) ->
        {:error, :invalid}

      not is_nil(server_secure) and not is_boolean(server_secure) ->
        {:error, :invalid}

      not is_nil(adapter) and
          (not is_list(adapter) or adapter == [] or
             Enum.any?(adapter, &(not is_binary(&1)))) ->
        {:error, :invalid}

      not is_nil(concurrency) and (not is_integer(concurrency) or concurrency < 1) ->
        {:error, :invalid}

      not is_nil(env) and not is_map(env) ->
        {:error, :invalid}

      not is_nil(env) and
          Enum.any?(env, fn {k, v} ->
            not is_binary(k) or not is_binary(v) or String.starts_with?(k, "COFLUX_")
          end) ->
        {:error, :invalid}

      not is_nil(env_secrets) and not is_map(env_secrets) ->
        {:error, :invalid}

      not is_nil(env_secrets) and
          Enum.any?(env_secrets, fn {k, v} ->
            not is_binary(k) or String.starts_with?(k, "COFLUX_") or
                not Coflux.Admin.Secrets.valid_name?(v)
          end) ->
        {:error, :invalid}

      true ->
        launcher =
          if server_host, do: Map.put(launcher, :server_host, server_host), else: launcher

        launcher =
          if not is_nil(server_secure),
            do: Map.put(launcher, :server_secure, server_secure),
            else: launcher

        launcher = if adapter, do: Map.put(launcher, :adapter, adapter), else: launcher

        launcher =
          if concurrency, do: Map.put(launcher, :concurrency, concurrency), else: launcher

        launcher = if env, do: Map.put(launcher, :env, env), else: launcher

        launcher =
          if env_secrets, do: Map.put(launcher, :env_secrets, env_secrets), else: launcher

        {:ok, launcher}
    end
  end

  defp parse_launcher(value) do
    allowed = Coflux.Config.launcher_types()

    cond do
      is_map(value) ->
        case Map.fetch(value, "type") do
          {:ok, type} when type in ["docker", "process", "kubernetes", "ecs"] ->
            type_atom = String.to_existing_atom(type)

            if MapSet.member?(allowed, type_atom) do
              with {:ok, launcher} <-
                     (case type do
                        "docker" -> parse_docker_launcher(value)
                        "process" -> parse_process_launcher(value)
                        "kubernetes" -> parse_kubernetes_launcher(value)
                        "ecs" -> parse_ecs_launcher(value)
                      end) do
                parse_common_launcher_fields(launcher, value)
              end
            else
              {:error, :invalid}
            end

          {:ok, _other} ->
            {:error, :invalid}

          :error ->
            {:error, :invalid}
        end

      is_nil(value) ->
        {:ok, nil}

      true ->
        {:error, :invalid}
    end
  end

  defp parse_pools(value) do
    cond do
      is_map(value) ->
        Enum.reduce_while(value, {:ok, %{}}, fn {name, pool_value}, {:ok, result} ->
          case parse_pool_name(name) do
            {:ok, name} ->
              case parse_pool(pool_value) do
                {:ok, pool} when is_map(pool) ->
                  {:cont, {:ok, Map.put(result, name, pool)}}

                # Keep why, against the pool it came from: "invalid" alone
                # leaves the caller no idea which pool.
                {:error, error} ->
                  {:halt, {:error, %{name => error}}}

                _ ->
                  {:halt, {:error, %{name => :invalid}}}
              end

            {:error, _} ->
              {:halt, {:error, %{name => :invalid_name}}}
          end
        end)

      true ->
        {:error, :invalid}
    end
  end

  defp build_pool_config(pool) do
    provides = pool.provides
    accepts = Map.get(pool, :accepts, %{})

    config = %{"modules" => pool.modules}

    config = if Enum.any?(provides), do: Map.put(config, "provides", provides), else: config
    config = if Enum.any?(accepts), do: Map.put(config, "accepts", accepts), else: config
    config = maybe_put_value(config, "idleTimeout", Map.get(pool, :idle_timeout))

    if pool.launcher do
      Map.put(config, "launcher", build_launcher_config(pool.launcher))
    else
      config
    end
  end

  defp build_launcher_config(launcher) do
    type_fields =
      case launcher.type do
        :docker ->
          %{"type" => "docker", "image" => launcher.image}
          |> maybe_put_value("dockerHost", Map.get(launcher, :docker_host))
          |> maybe_put_value("networkMode", Map.get(launcher, :network_mode))

        :process ->
          %{"type" => "process", "directory" => launcher.directory}

        :ecs ->
          %{
            "type" => "ecs",
            "cluster" => launcher.cluster,
            "taskDefinition" => launcher.task_definition,
            "region" => launcher.region
          }
          |> maybe_put_value("containerName", Map.get(launcher, :container_name))
          |> maybe_put_value("launchType", Map.get(launcher, :launch_type))
          |> maybe_put_value("capacityProvider", Map.get(launcher, :capacity_provider))
          |> maybe_put_value("subnets", Map.get(launcher, :subnets))
          |> maybe_put_value("securityGroups", Map.get(launcher, :security_groups))
          |> maybe_put_value("assignPublicIp", Map.get(launcher, :assign_public_ip))
          |> maybe_put_value("platformVersion", Map.get(launcher, :platform_version))
          |> maybe_put_value("credentialsSecret", Map.get(launcher, :credentials_secret))
          |> maybe_put_value("endpoint", Map.get(launcher, :endpoint))

        :kubernetes ->
          %{"type" => "kubernetes", "image" => launcher.image}
          |> maybe_put_value("namespace", Map.get(launcher, :namespace))
          |> maybe_put_value("apiServer", Map.get(launcher, :api_server))
          |> maybe_put_value("serviceAccount", Map.get(launcher, :service_account))
          |> maybe_put_value("tokenSecret", Map.get(launcher, :token_secret))
          |> maybe_put_value("caCert", Map.get(launcher, :ca_cert))
          |> maybe_put_value("insecure", Map.get(launcher, :insecure))
          |> maybe_put_value("imagePullPolicy", Map.get(launcher, :image_pull_policy))
          |> maybe_put_value("nodeSelector", Map.get(launcher, :node_selector))
          |> maybe_put_value("tolerations", Map.get(launcher, :tolerations))
          |> maybe_put_value("imagePullSecrets", Map.get(launcher, :image_pull_secrets))
          |> maybe_put_value("hostAliases", Map.get(launcher, :host_aliases))
          |> maybe_put_value("resources", Map.get(launcher, :resources))
          |> maybe_put_value("labels", Map.get(launcher, :labels))
          |> maybe_put_value("annotations", Map.get(launcher, :annotations))
          |> maybe_put_value("activeDeadlineSeconds", Map.get(launcher, :active_deadline_seconds))
          |> maybe_put_value("volumes", Map.get(launcher, :volumes))
          |> maybe_put_value("volumeMounts", Map.get(launcher, :volume_mounts))
      end

    type_fields
    |> maybe_put_value("serverHost", Map.get(launcher, :server_host))
    |> maybe_put_value("serverSecure", Map.get(launcher, :server_secure))
    |> maybe_put_value("adapter", Map.get(launcher, :adapter))
    |> maybe_put_value("concurrency", Map.get(launcher, :concurrency))
    |> maybe_put_value("env", Map.get(launcher, :env))
    |> maybe_put_value("envSecrets", Map.get(launcher, :env_secrets))
  end

  defp parse_secret_name(value) do
    if Coflux.Admin.Secrets.valid_name?(value), do: {:ok, value}, else: {:error, :invalid}
  end

  defp parse_secret_value(value) do
    if Coflux.Admin.Secrets.valid_value?(value), do: {:ok, value}, else: {:error, :invalid}
  end

  defp secrets_not_found_response(req, names) do
    json_error_response(req, "secrets_not_found", details: %{"secrets" => names})
  end

  defp maybe_put_value(map, _key, nil), do: map
  defp maybe_put_value(map, key, value), do: Map.put(map, key, value)

  defp parse_pool(value) do
    cond do
      is_map(value) ->
        Enum.reduce_while(
          [
            {"modules", &parse_modules/1, :modules, []},
            {"provides", &parse_tag_set/1, :provides, %{}},
            {"accepts", &parse_tag_set/1, :accepts, %{}},
            {"idleTimeout", &parse_idle_timeout/1, :idle_timeout, nil},
            {"launcher", &parse_launcher/1, :launcher, nil}
          ],
          {:ok, %{}},
          fn {source, parser, target, default}, {:ok, result} ->
            case Map.fetch(value, source) do
              {:ok, value} ->
                case parser.(value) do
                  {:ok, parsed} ->
                    {:cont, {:ok, Map.put(result, target, parsed)}}

                  {:error, error} ->
                    {:halt, {:error, error}}
                end

              :error ->
                {:cont, {:ok, Map.put(result, target, default)}}
            end
          end
        )

      is_nil(value) ->
        {:ok, nil}

      true ->
        {:error, :invalid}
    end
  end

  # Seconds an idle worker is kept for. Zero means the next sweep.
  defp parse_idle_timeout(value) when is_integer(value) and value >= 0, do: {:ok, value}
  defp parse_idle_timeout(_value), do: {:error, :invalid}

  # Parses a partial pool update (PATCH semantics).
  # Only keys present in the JSON are included. A JSON null value means "unset".
  defp parse_pool_patch(value) do
    cond do
      is_map(value) ->
        fields = [
          {"modules", &parse_modules/1, :modules},
          {"provides", &parse_tag_set/1, :provides},
          {"accepts", &parse_tag_set/1, :accepts},
          {"idleTimeout", &parse_idle_timeout/1, :idle_timeout},
          {"launcher", &parse_launcher_patch/1, :launcher}
        ]

        Enum.reduce_while(fields, {:ok, %{}}, fn {source, parser, target}, {:ok, result} ->
          case Map.fetch(value, source) do
            {:ok, nil} ->
              # Explicit null — signal to unset the field
              {:cont, {:ok, Map.put(result, target, :unset)}}

            {:ok, field_value} ->
              case parser.(field_value) do
                {:ok, parsed} -> {:cont, {:ok, Map.put(result, target, parsed)}}
                {:error, error} -> {:halt, {:error, error}}
              end

            :error ->
              # Key absent — leave unchanged
              {:cont, {:ok, result}}
          end
        end)

      is_nil(value) ->
        {:ok, nil}

      true ->
        {:error, :invalid}
    end
  end

  # Parses a partial launcher update. Only present keys are included.
  # A JSON null value means "unset this field".
  defp parse_launcher_patch(value) when is_map(value) do
    allowed = Coflux.Config.launcher_types()

    # If "type" is present, validate it; otherwise this is patching an existing launcher
    case Map.fetch(value, "type") do
      {:ok, type} when type in ["docker", "process", "kubernetes", "ecs"] ->
        type_atom = String.to_existing_atom(type)

        if MapSet.member?(allowed, type_atom) do
          parse_launcher_patch_fields(value, type_atom)
        else
          {:error, :invalid}
        end

      {:ok, _other} ->
        {:error, :invalid}

      :error ->
        # No type specified — patching existing launcher fields
        parse_launcher_patch_fields(value, nil)
    end
  end

  defp parse_launcher_patch(nil), do: {:ok, nil}
  defp parse_launcher_patch(_), do: {:error, :invalid}

  defp parse_launcher_patch_fields(value, type) do
    valid_pull_policies = ["Always", "Never", "IfNotPresent"]

    # All possible launcher fields with their validators
    field_specs = [
      {"image", &is_binary/1},
      {"dockerHost", &is_binary/1},
      {"networkMode", &is_binary/1},
      {"directory", &is_binary/1},
      {"namespace", &is_binary/1},
      {"serviceAccount", &is_binary/1},
      {"apiServer", &is_binary/1},
      {"tokenSecret", &Coflux.Admin.Secrets.valid_name?/1},
      {"caCert", &is_binary/1},
      {"insecure", &is_boolean/1},
      {"imagePullPolicy", &(&1 in valid_pull_policies)},
      {"nodeSelector", &is_map/1},
      {"tolerations", &is_list/1},
      {"imagePullSecrets", &is_list/1},
      {"hostAliases", &is_list/1},
      {"resources", &is_map/1},
      {"cluster", &is_binary/1},
      {"taskDefinition", &is_binary/1},
      {"region", &is_binary/1},
      {"containerName", &is_binary/1},
      {"launchType", &(&1 in @ecs_launch_types)},
      {"capacityProvider", &is_binary/1},
      {"subnets", &(is_binary(&1) or is_string_list?(&1, 16))},
      {"securityGroups", &(is_binary(&1) or is_string_list?(&1, 5))},
      {"assignPublicIp", &is_boolean/1},
      {"platformVersion", &is_binary/1},
      {"credentialsSecret", &Coflux.Admin.Secrets.valid_name?/1},
      {"endpoint", &is_binary/1},
      {"serverHost", &is_binary/1},
      {"serverSecure", &is_boolean/1},
      {"adapter", fn v -> is_list(v) and v != [] and Enum.all?(v, &is_binary/1) end},
      {"concurrency", fn v -> is_integer(v) and v >= 1 end},
      {"env",
       fn v ->
         is_map(v) and
           Enum.all?(v, fn {k, val} ->
             is_binary(k) and (is_binary(val) or is_nil(val)) and
               not String.starts_with?(k, "COFLUX_")
           end)
       end},
      {"envSecrets",
       fn v ->
         is_map(v) and
           Enum.all?(v, fn {k, val} ->
             is_binary(k) and (is_nil(val) or Coflux.Admin.Secrets.valid_name?(val)) and
               not String.starts_with?(k, "COFLUX_")
           end)
       end}
    ]

    # JSON key to atom key mapping
    key_map = %{
      "image" => :image,
      "dockerHost" => :docker_host,
      "networkMode" => :network_mode,
      "directory" => :directory,
      "namespace" => :namespace,
      "serviceAccount" => :service_account,
      "apiServer" => :api_server,
      "tokenSecret" => :token_secret,
      "caCert" => :ca_cert,
      "insecure" => :insecure,
      "imagePullPolicy" => :image_pull_policy,
      "nodeSelector" => :node_selector,
      "tolerations" => :tolerations,
      "imagePullSecrets" => :image_pull_secrets,
      "hostAliases" => :host_aliases,
      "resources" => :resources,
      "cluster" => :cluster,
      "taskDefinition" => :task_definition,
      "region" => :region,
      "containerName" => :container_name,
      "launchType" => :launch_type,
      "capacityProvider" => :capacity_provider,
      "subnets" => :subnets,
      "securityGroups" => :security_groups,
      "assignPublicIp" => :assign_public_ip,
      "platformVersion" => :platform_version,
      "credentialsSecret" => :credentials_secret,
      "endpoint" => :endpoint,
      "serverHost" => :server_host,
      "serverSecure" => :server_secure,
      "adapter" => :adapter,
      "concurrency" => :concurrency,
      "env" => :env,
      "envSecrets" => :env_secrets
    }

    result =
      Enum.reduce_while(field_specs, {:ok, %{}}, fn {json_key, validator}, {:ok, acc} ->
        atom_key = Map.fetch!(key_map, json_key)

        case Map.fetch(value, json_key) do
          {:ok, nil} ->
            # Explicit null — unset
            {:cont, {:ok, Map.put(acc, atom_key, :unset)}}

          {:ok, field_value} ->
            if validator.(field_value) do
              processed_value =
                cond do
                  json_key in ["env", "envSecrets"] and is_map(field_value) ->
                    Map.new(field_value, fn
                      {k, nil} -> {k, :unset}
                      {k, v} -> {k, v}
                    end)

                  json_key in ["subnets", "securityGroups"] ->
                    wrap_list(field_value)

                  true ->
                    field_value
                end

              {:cont, {:ok, Map.put(acc, atom_key, processed_value)}}
            else
              {:halt, {:error, :invalid}}
            end

          :error ->
            {:cont, {:ok, acc}}
        end
      end)

    case result do
      {:ok, fields} ->
        launcher = if type, do: Map.put(fields, :type, type), else: fields
        {:ok, launcher}

      error ->
        error
    end
  end

  defp transform_json(value) do
    cond do
      is_number(value) || is_boolean(value) || is_nil(value) || is_binary(value) ->
        value

      is_list(value) ->
        Enum.map(value, &transform_json/1)

      is_map(value) ->
        %{
          "type" => "dict",
          "items" =>
            Enum.flat_map(value, fn {key, value} ->
              [key, transform_json(value)]
            end)
        }
    end
  end

  # The entries of an asset built from outside a run: a path, the key of a
  # blob the client has already stored, its size, and any metadata. The
  # shape mirrors the worker's `put_asset`, which is likewise handed keys
  # rather than bytes.
  defp parse_asset_entries(value) do
    if is_list(value) && value != [] && length(value) <= @max_asset_entries do
      result =
        Enum.reduce_while(value, {:ok, []}, fn entry, {:ok, entries} ->
          case parse_asset_entry(entry) do
            {:ok, entry} -> {:cont, {:ok, [entry | entries]}}
            {:error, error} -> {:halt, {:error, error}}
          end
        end)

      with {:ok, entries} <- result do
        entries = Enum.reverse(entries)
        paths = Enum.map(entries, fn {path, _, _, _} -> path end)

        if length(Enum.uniq(paths)) == length(paths) do
          {:ok, entries}
        else
          {:error, :duplicate_path}
        end
      end
    else
      {:error, :invalid}
    end
  end

  defp parse_asset_entry(value) do
    if is_map(value) do
      with {:ok, path} <- parse_asset_entry_path(Map.get(value, "path")),
           {:ok, blob_key} <- parse_blob_key(Map.get(value, "blobKey")),
           {:ok, size} <- parse_size(Map.get(value, "size")),
           {:ok, metadata} <- parse_asset_entry_metadata(Map.get(value, "metadata")) do
        {:ok, {path, blob_key, size, metadata}}
      end
    else
      {:error, :invalid}
    end
  end

  # An entry path is restored to disk relative to a directory the reader
  # chooses, so it has to stay inside it: relative, no empty, `.` or `..`
  # segment, and no backslash to be mistaken for a separator elsewhere.
  defp parse_asset_entry_path(value) do
    if is_binary(value) && value != "" && byte_size(value) <= @max_asset_entry_path_length &&
         String.valid?(value) && !String.contains?(value, ["\\", <<0>>]) &&
         !Enum.any?(String.split(value, "/"), &(&1 in ["", ".", ".."])) do
      {:ok, value}
    else
      {:error, :invalid}
    end
  end

  defp parse_blob_key(value) do
    if is_binary(value) && Regex.match?(@blob_key_regex, value) do
      {:ok, value}
    else
      {:error, :invalid}
    end
  end

  defp parse_size(value) do
    if is_integer(value) && value >= 0, do: {:ok, value}, else: {:error, :invalid}
  end

  defp parse_asset_entry_metadata(value) do
    cond do
      is_nil(value) -> {:ok, %{}}
      is_map(value) -> {:ok, value}
      true -> {:error, :invalid}
    end
  end

  # One value given from outside a run — an argument to a workflow, or
  # something published to the catalog. A JSON document, encoded as a
  # string so that `null` is a value rather than an omission, or a
  # reference to an existing asset.
  defp parse_argument(["json", json]) do
    if is_valid_json?(json) do
      {:ok, {:raw, json |> Jason.decode!() |> transform_json(), []}}
    else
      {:error, :not_json}
    end
  end

  # A bare asset: a value that is a single reference.
  defp parse_argument(["asset", asset_id]) do
    if is_valid_string?(asset_id, max_length: 100) do
      {:ok, {:raw, %{"type" => "ref", "index" => 0}, [{:asset, asset_id}]}}
    else
      {:error, :invalid}
    end
  end

  defp parse_argument(_other), do: {:error, :invalid}

  defp parse_arguments(arguments) do
    if arguments do
      {values, errors} =
        arguments
        |> Enum.with_index()
        |> Enum.reduce({[], %{}}, fn {argument, index}, {values, errors} ->
          case parse_argument(argument) do
            {:ok, value} -> {[value | values], errors}
            {:error, error} -> {values, Map.put(errors, index, error)}
          end
        end)

      if Enum.any?(errors) do
        {:error, errors}
      else
        {:ok, Enum.reverse(values)}
      end
    else
      {:ok, []}
    end
  end

  def is_valid_module_name?(value) do
    is_valid_string?(value, max_length: 100, regex: ~r/^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)*$/i)
  end

  def is_valid_target_name?(value) do
    is_valid_string?(value, max_length: 100, regex: ~r/^[a-z_][a-z0-9_]*$/i)
  end

  defp parse_parameter(value) do
    # TODO: validate
    name = Map.fetch!(value, "name")
    default = Map.get(value, "default")
    annotation = Map.get(value, "annotation")
    {:ok, {name, default, annotation}}
  end

  defp parse_parameters(value) do
    if is_list(value) && length(value) <= @max_parameters do
      with {:ok, backwards} <-
             Enum.reduce_while(value, {:ok, []}, fn parameter, {:ok, result} ->
               case parse_parameter(parameter) do
                 {:ok, parsed} -> {:cont, {:ok, [parsed | result]}}
               end
             end) do
        {:ok, Enum.reverse(backwards)}
      end
    else
      {:error, :invalid}
    end
  end

  defp parse_indexes(value, opts \\ []) do
    cond do
      opts[:allow_boolean] && !value ->
        {:ok, false}

      opts[:allow_boolean] && value == true ->
        {:ok, true}

      is_list(value) && length(value) <= @max_parameters ->
        with {:ok, backwards} <-
               Enum.reduce_while(value, {:ok, []}, fn item, {:ok, result} ->
                 case parse_integer(item) do
                   {:ok, value} -> {:cont, {:ok, [value | result]}}
                   {:error, error} -> {:halt, {:error, error}}
                 end
               end) do
          {:ok, Enum.reverse(backwards)}
        end

      true ->
        {:error, :invalid}
    end
  end

  defp parse_integer(value, opts \\ []) do
    cond do
      opts[:optional] && is_nil(value) -> {:ok, nil}
      is_integer(value) -> {:ok, value}
      true -> {:error, :invalid}
    end
  end

  defp parse_boolean(value, opts) do
    cond do
      opts[:optional] && is_nil(value) -> {:ok, nil}
      is_boolean(value) -> {:ok, value}
      true -> {:error, :invalid}
    end
  end

  # The `catalog` option names a snapshot as `path@n` (or `latest`).
  defp catalog_error(:catalog_invalid), do: "invalid"
  defp catalog_error(:catalog_not_found), do: "not_found"
  defp catalog_error(:catalog_invisible), do: "invisible"

  defp parse_string(value, opts) do
    cond do
      opts[:optional] && is_nil(value) -> {:ok, nil}
      is_valid_string?(value, opts) -> {:ok, value}
      true -> {:error, :invalid}
    end
  end

  defp parse_cache(value) do
    cond do
      is_nil(value) ->
        {:ok, nil}

      is_map(value) ->
        with {:ok, params} <- parse_indexes(Map.get(value, "params"), allow_boolean: true),
             {:ok, max_age} <- parse_integer(Map.get(value, "maxAge"), optional: true),
             # TODO: regex
             {:ok, namespace} <-
               parse_string(Map.get(value, "namespace"), optional: true, max_length: 200),
             # TODO: regex
             {:ok, version} <-
               parse_string(Map.get(value, "version"), optional: true, max_length: 200) do
          {:ok,
           %{
             params: params,
             max_age: max_age,
             namespace: namespace,
             version: version
           }}
        end

      true ->
        {:error, :invalid}
    end
  end

  defp parse_defer(value) do
    cond do
      is_nil(value) ->
        {:ok, nil}

      is_map(value) ->
        with {:ok, params} <- parse_indexes(Map.get(value, "params"), allow_boolean: true) do
          {:ok, %{params: params}}
        end

      true ->
        {:error, :invalid}
    end
  end

  # A concurrency limit: how many executions sharing the key may run at
  # once. ``params`` selects the argument values the key is built from
  # (false = none, true = all, or a list of indexes); ``namespace``
  # defaults server-side to "module:target".
  defp parse_concurrency(value) do
    cond do
      is_nil(value) ->
        {:ok, nil}

      is_map(value) ->
        with {:ok, limit} <- parse_integer(Map.get(value, "limit")),
             {:ok, params} <- parse_indexes(Map.get(value, "params"), allow_boolean: true),
             # TODO: regex
             {:ok, namespace} <-
               parse_string(Map.get(value, "namespace"), optional: true, max_length: 200) do
          if limit >= 1 do
            {:ok, %{limit: limit, params: params, namespace: namespace}}
          else
            {:error, :invalid}
          end
        end

      true ->
        {:error, :invalid}
    end
  end

  defp parse_retries(value) do
    cond do
      is_nil(value) ->
        {:ok, nil}

      is_map(value) ->
        # limit can be nil (unlimited) or an integer
        # backoff_min and backoff_max default to 0 if not provided (database requires NOT NULL)
        with {:ok, limit} <- parse_integer(Map.get(value, "limit"), optional: true),
             {:ok, backoff_min} <- parse_integer(Map.get(value, "backoffMin"), optional: true),
             {:ok, backoff_max} <- parse_integer(Map.get(value, "backoffMax"), optional: true) do
          {:ok, %{limit: limit, backoff_min: backoff_min || 0, backoff_max: backoff_max || 0}}
        end

      true ->
        {:error, :invalid}
    end
  end

  # Parse a ``streams`` config object from an HTTP request body. Returns
  # nil when the caller omits streams entirely. A present-but-null
  # ``buffer`` key means an explicitly unbounded buffer (distinct from
  # omitting the key), so presence is checked rather than the value.
  defp parse_streams_config(value) do
    cond do
      is_nil(value) ->
        {:ok, nil}

      is_map(value) ->
        with {:ok, buffer} <- parse_integer(Map.get(value, "buffer"), optional: true),
             {:ok, timeout_ms} <-
               parse_integer(Map.get(value, "timeoutMs"), optional: true) do
          if not Map.has_key?(value, "buffer") and timeout_ms == nil do
            {:ok, nil}
          else
            {:ok, %{buffer: buffer, timeout_ms: timeout_ms}}
          end
        end

      true ->
        {:error, :invalid}
    end
  end

  defp parse_workflow(value) do
    if is_map(value) do
      with {:ok, parameters} <- parse_parameters(Map.get(value, "parameters")),
           {:ok, wait_for} <- parse_indexes(Map.get(value, "waitFor")),
           {:ok, cache} <- parse_cache(Map.get(value, "cache")),
           {:ok, defer} <- parse_defer(Map.get(value, "defer")),
           {:ok, delay} <- parse_integer(Map.get(value, "delay")),
           {:ok, retries} <- parse_retries(Map.get(value, "retries")),
           {:ok, recurrent} <- parse_boolean(Map.get(value, "recurrent"), optional: true),
           {:ok, timeout} <- parse_integer(Map.get(value, "timeout"), optional: true),
           {:ok, requires} <- parse_tag_set(Map.get(value, "requires")),
           {:ok, memo} <- parse_boolean(Map.get(value, "memo"), optional: true),
           {:ok, streams} <- parse_manifest_streams(Map.get(value, "streams")),
           {:ok, concurrency} <- parse_concurrency(Map.get(value, "concurrency")),
           {:ok, instruction} <-
             parse_string(
               Map.get(value, "instruction"),
               optional: true,
               max_length: 5000
             ) do
        {:ok,
         %{
           parameters: parameters,
           wait_for: wait_for,
           cache: cache,
           defer: defer,
           delay: delay,
           retries: retries,
           recurrent: recurrent == true,
           timeout: timeout || 0,
           requires: requires,
           memo: memo == true,
           streams: streams,
           concurrency: concurrency,
           instruction: instruction
         }}
      else
        {:error, error} ->
          {:error, error}
      end
    else
      {:error, :invalid}
    end
  end

  # Parse the ``streams`` field on a manifest workflow. The Python
  # adapter serialises this as ``{"buffer": int?, "timeout_ms": int?}``
  # (snake_case, since it's the wire format shared with worker protocol
  # not the HTTP-specific camelCase).
  defp parse_manifest_streams(value) do
    cond do
      is_nil(value) ->
        {:ok, nil}

      is_map(value) ->
        with {:ok, buffer} <- parse_integer(Map.get(value, "buffer"), optional: true),
             {:ok, timeout_ms} <-
               parse_integer(Map.get(value, "timeout_ms"), optional: true) do
          # A present-but-null buffer key means explicitly unbounded —
          # only treat the config as unset when the key is absent too.
          if not Map.has_key?(value, "buffer") and timeout_ms == nil do
            {:ok, nil}
          else
            {:ok, %{buffer: buffer, timeout_ms: timeout_ms}}
          end
        end

      true ->
        {:error, :invalid}
    end
  end

  defp parse_workflows(value) do
    Enum.reduce_while(value, {:ok, %{}}, fn {workflow_name, workflow}, {:ok, result} ->
      if is_valid_target_name?(workflow_name) do
        case parse_workflow(workflow) do
          {:ok, parsed} ->
            {:cont, {:ok, Map.put(result, workflow_name, parsed)}}

          {:error, error} ->
            {:halt, {:error, error}}
        end
      else
        {:halt, {:error, :invalid}}
      end
    end)
  end

  defp parse_manifests(value) do
    if is_map(value) do
      Enum.reduce_while(value, {:ok, %{}}, fn {module, workflows}, {:ok, result} ->
        if is_valid_module_name?(module) do
          case parse_workflows(workflows) do
            {:ok, parsed} ->
              {:cont, {:ok, Map.put(result, module, parsed)}}

            {:error, error} ->
              {:halt, {:error, error}}
          end
        else
          {:halt, {:error, :invalid}}
        end
      end)
    else
      {:error, :invalid}
    end
  end
end
