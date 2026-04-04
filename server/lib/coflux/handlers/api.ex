defmodule Coflux.Handlers.Api do
  import Coflux.Handlers.Utils

  alias Coflux.{Auth, Config, Orchestration, MapUtils, Version}

  @max_parameters 20

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

            with {:ok, project_id} <- resolve_project(host),
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

  defp workspace_matches?(_workspace, "*"), do: true
  defp workspace_matches?(workspace, workspace), do: true

  defp workspace_matches?(workspace, pattern) do
    if String.ends_with?(pattern, "/*") do
      # "staging/*" matches "staging/foo", "staging/foo/bar", etc. (not "staging" itself)
      String.starts_with?(workspace, String.slice(pattern, 0..-2//1))
    else
      false
    end
  end

  # Check if all requested workspace patterns are covered by the caller's access.
  # Returns true if the caller can grant the requested access level.
  defp workspaces_covered?(:all, _requested), do: true
  defp workspaces_covered?(_caller, nil), do: true

  defp workspaces_covered?(caller_patterns, requested) do
    Enum.all?(requested, &pattern_covered_by?(caller_patterns, &1))
  end

  defp pattern_covered_by?(caller_patterns, "*") do
    # Full access - only covered by explicit "*" pattern
    Enum.any?(caller_patterns, &(&1 == "*"))
  end

  defp pattern_covered_by?(caller_patterns, pattern) do
    if String.ends_with?(pattern, "/*") do
      # Wildcard pattern "X/*" - caller needs "*", same pattern, or broader wildcard
      prefix = String.slice(pattern, 0..-2//1)

      Enum.any?(caller_patterns, fn cp ->
        cp == "*" or cp == pattern or
          (String.ends_with?(cp, "/*") and
             String.starts_with?(prefix, String.slice(cp, 0..-2//1)))
      end)
    else
      # Exact pattern - use existing workspace_matches?
      Enum.any?(caller_patterns, &workspace_matches?(pattern, &1))
    end
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

  defp handle(req, "POST", ["update_pool"], project_id, access) do
    case read_arguments(req, %{
           workspace_id: "workspaceId",
           pool_name: {"poolName", &parse_pool_name/1},
           pool: {"pool", &parse_pool/1}
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
          {:error, :forbidden} -> json_error_response(req, "forbidden", status: 403)
          {:error, :workspace_invalid} -> json_error_response(req, "not_found", status: 404)
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
             idempotency_key: {"idempotencyKey", &parse_string(&1, optional: true)}
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
               idempotency_key: arguments[:idempotency_key]
             ) do
          {:ok, run_id, step_number, execution_external_id} ->
            json_response(req, %{
              "runId" => run_id,
              "stepId" => "#{run_id}:#{step_number}",
              "executionId" => execution_external_id
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
    case read_arguments(req, %{
           workspace_id: "workspaceId",
           step_id: "stepId"
         }) do
      {:ok, arguments, req} ->
        case Orchestration.rerun_step(
               project_id,
               arguments.step_id,
               arguments.workspace_id,
               access
             ) do
          {:ok, execution_external_id, attempt} ->
            json_response(req, %{"executionId" => execution_external_id, "attempt" => attempt})

          {:error, :forbidden} ->
            json_error_response(req, "forbidden", status: 403)

          {:error, :invalid} ->
            json_error_response(req, "bad_request", details: %{"stepId" => "invalid"})

          {:error, :not_found} ->
            json_error_response(req, "not_found", status: 404)

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
             provides: {"provides", &parse_tag_set/1}
           }
         ) do
      {:ok, arguments, req} ->
        opts =
          [
            provides: arguments[:provides]
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
    if Enum.all?(value, &is_binary/1) do
      {:ok, value}
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

  defp is_valid_module_pattern?(pattern) do
    cond do
      not is_binary(pattern) ->
        false

      String.length(pattern) > 100 ->
        false

      true ->
        parts = String.split(pattern, ".")
        Enum.all?(parts, &(&1 == "*" || Regex.match?(~r/^[a-z_][a-z0-9_]*$/i, &1)))
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

  defp parse_modules(value) do
    value = List.wrap(value)

    if Enum.all?(value, &is_valid_module_pattern?/1) do
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

    cond do
      not is_binary(image) or String.length(image) > 200 ->
        {:error, :invalid}

      not is_nil(docker_host) and (not is_binary(docker_host) or String.length(docker_host) > 200) ->
        {:error, :invalid}

      true ->
        launcher = %{type: :docker, image: image}

        launcher =
          if docker_host, do: Map.put(launcher, :docker_host, docker_host), else: launcher

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
    token = Map.get(value, "token")
    ca_cert = Map.get(value, "caCert")
    insecure = Map.get(value, "insecure")
    image_pull_policy = Map.get(value, "imagePullPolicy")
    node_selector = Map.get(value, "nodeSelector")
    tolerations = Map.get(value, "tolerations")
    image_pull_secrets = Map.get(value, "imagePullSecrets")
    host_aliases = Map.get(value, "hostAliases")
    resources = Map.get(value, "resources")

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

      not is_nil(token) and not is_binary(token) ->
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

        launcher = if token, do: Map.put(launcher, :token, token), else: launcher
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

        {:ok, launcher}
    end
  end

  defp parse_common_launcher_fields(launcher, value) do
    server_host = Map.get(value, "serverHost")
    server_secure = Map.get(value, "serverSecure")
    adapter = Map.get(value, "adapter")
    concurrency = Map.get(value, "concurrency")
    env = Map.get(value, "env")

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
        {:ok, launcher}
    end
  end

  defp parse_launcher(value) do
    allowed = Coflux.Config.launcher_types()

    cond do
      is_map(value) ->
        case Map.fetch(value, "type") do
          {:ok, type} when type in ["docker", "process", "kubernetes"] ->
            type_atom = String.to_existing_atom(type)

            if MapSet.member?(allowed, type_atom) do
              with {:ok, launcher} <-
                     (case type do
                        "docker" -> parse_docker_launcher(value)
                        "process" -> parse_process_launcher(value)
                        "kubernetes" -> parse_kubernetes_launcher(value)
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

  defp parse_pool(value) do
    cond do
      is_map(value) ->
        Enum.reduce_while(
          [
            {"modules", &parse_modules/1, :modules, []},
            {"provides", &parse_tag_set/1, :provides, %{}},
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

  defp parse_arguments(arguments) do
    if arguments do
      errors =
        arguments
        |> Enum.with_index()
        |> Enum.reduce(%{}, fn {argument, index}, errors ->
          case argument do
            ["json", value] ->
              if is_valid_json?(value) do
                errors
              else
                Map.put(errors, index, :not_json)
              end
          end
        end)

      if Enum.any?(errors) do
        {:error, errors}
      else
        result =
          Enum.map(arguments, fn argument ->
            case argument do
              ["json", json] ->
                value =
                  json
                  |> Jason.decode!()
                  |> transform_json()

                {:raw, value, []}
            end
          end)

        {:ok, result}
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
