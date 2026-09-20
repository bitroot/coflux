defmodule Coflux.EcsLauncher do
  @moduledoc """
  Runs workers as Amazon ECS tasks.

  A pool names a task definition, and each worker is one task run from it,
  with the container's command overridden to the worker's arguments (the
  modules to host) and its environment to what the worker needs to
  connect. Everything else about the task - image, CPU and memory, IAM
  roles, logging - belongs to the task definition, which is where ECS
  users expect to configure it, so the launcher doesn't try to own it.

  ECS has no log API of its own: container output goes wherever the task
  definition's log configuration sends it. What a stopped task does carry
  is a `stoppedReason`, which is the diagnostic when a task never started
  (an image that couldn't be pulled, say), so that stands in for the log
  tail.

  Credentials are resolved on every call rather than kept with the task:
  from the secret the pool names, or failing that from the server's own
  surroundings, and then through the role the pool names, if it does.
  See `Coflux.Launchers.AwsCredentials`.
  """

  import Coflux.Launchers.Utils, only: [truncate_bytes: 2]

  alias Coflux.Launchers.AwsCredentials

  @api_target_prefix "AmazonEC2ContainerServiceV20141113"
  @logs_target_prefix "Logs_20140328"

  # Matches the other launchers, so a failed worker reads the same however
  # it was launched.
  @log_tail_lines 20
  @log_max_bytes 1024
  @reason_max_bytes 1024

  # ECS limits `startedBy` to 36 characters.
  @started_by_max_length 36

  @unauthorized_errors [
    "UnrecognizedClientException",
    "InvalidSignatureException",
    "SignatureDoesNotMatch",
    "ExpiredTokenException",
    "ExpiredToken",
    "InvalidClientTokenId",
    "InvalidAccessKeyId",
    "IncompleteSignature",
    "MissingAuthenticationToken"
  ]

  @invalid_errors [
    "InvalidParameterException",
    "PlatformTaskDefinitionIncompatibilityException",
    "PlatformUnknownException",
    "UnsupportedFeatureException",
    "BlockedException"
  ]

  def launch(env, args, config, opts \\ %{}) do
    with {:ok, conn} <- build_conn(config),
         {:ok, container_name} <- resolve_container_name(conn, config),
         {:ok, task_arn} <- run_task(conn, config, container_name, env, args, opts) do
      {:ok,
       %{
         task_arn: task_arn,
         cluster: Map.fetch!(config, :cluster),
         container_name: container_name
       }}
    else
      {:error, reason} ->
        case normalize_launch_error(reason) do
          {error, nil} -> {:error, error}
          {error, detail} -> {:error, error, detail}
        end
    end
  end

  def stop(%{task_arn: task_arn, cluster: cluster}, config) do
    with {:ok, conn} <- build_conn(config),
         {:ok, _body} <-
           ecs_request(conn, "StopTask", %{
             "cluster" => cluster,
             "task" => task_arn,
             "reason" => "Stopped by Coflux"
           }) do
      :ok
    else
      # Already gone, which is what was being asked for.
      {:error, {:api, _status, "InvalidParameterException", message}}
      when is_binary(message) ->
        if message =~ ~r/not found/i, do: :ok, else: {:error, "stop_invalid"}

      {:error, reason} ->
        {:error, describe_error(reason)}
    end
  end

  def poll(%{task_arn: task_arn, cluster: cluster} = data, config) do
    with {:ok, conn} <- build_conn(config),
         {:ok, body} <-
           ecs_request(conn, "DescribeTasks", %{"cluster" => cluster, "tasks" => [task_arn]}) do
      case body do
        %{"tasks" => [task | _]} ->
          interpret_task(task, data, conn)

        # Stopped tasks are only described for an hour or so afterwards;
        # one that has aged out has nothing left to say.
        %{"failures" => [%{"reason" => "MISSING"} | _]} ->
          {:ok, false, nil, nil}

        %{"failures" => [%{"reason" => reason} | _]} ->
          {:error, "describe_failed:#{reason}"}

        _ ->
          {:error, "unexpected_response"}
      end
    else
      {:error, reason} -> {:error, describe_error(reason)}
    end
  end

  # --- Task state ---

  defp interpret_task(%{"lastStatus" => "STOPPED"} = task, data, conn) do
    error = stop_error(task, data[:container_name])
    logs = if error, do: failure_logs(task, data[:container_name], conn)
    {:ok, false, error, logs}
  end

  # Anything else - provisioning, pending, running, or on its way to
  # stopped - is a task that hasn't finished yet.
  defp interpret_task(_task, _data, _conn), do: {:ok, true}

  # --- Failure diagnostics ---

  # What to show for a worker that died. The other launchers put the
  # container's own output here; ECS doesn't serve that through its API,
  # so this reads CloudWatch when the task definition logs there and the
  # role is allowed to read it, and falls back to what ECS itself said.
  #
  # Every step is best-effort. A worker that died has already been
  # recorded; a log fetch that fails must never turn into a failed poll,
  # so this narrows to a string or nil and swallows anything else.
  defp failure_logs(task, container_name, conn) do
    fetch_log_tail(task, container_name, conn) || stop_detail(task, container_name)
  rescue
    _ -> stop_detail(task, container_name)
  catch
    _, _ -> stop_detail(task, container_name)
  end

  # ECS's own account of the stop: the task-level reason (for a container
  # exit this is only ever "Essential container in task exited"), plus
  # whatever the container itself reported, which is where anything
  # specific tends to be.
  defp stop_detail(task, container_name) do
    container = find_container(task["containers"], container_name)

    [
      stopped_reason(task),
      container && container["reason"],
      container && exit_code_detail(container)
    ]
    |> Enum.reject(&(&1 in [nil, ""]))
    |> Enum.uniq()
    |> case do
      [] -> nil
      parts -> truncate_bytes(Enum.join(parts, " - "), @reason_max_bytes)
    end
  end

  defp exit_code_detail(%{"exitCode" => code}) when is_integer(code), do: "exit code #{code}"
  defp exit_code_detail(_container), do: nil

  # The tail of the container's log stream, when there is one to read.
  # Returns nil for every reason it might not be readable - no log
  # configuration, a driver other than awslogs, no stream prefix to build
  # the name from, the role lacking `logs:GetLogEvents`, or the events
  # simply not having been delivered yet.
  defp fetch_log_tail(task, container_name, conn) do
    with {:ok, options} <- awslogs_options(task, container_name, conn),
         {:ok, group} <- fetch_option(options, "awslogs-group"),
         {:ok, prefix} <- fetch_option(options, "awslogs-stream-prefix"),
         {:ok, name} <- container_name_for_logs(task, container_name),
         {:ok, task_id} <- task_id(task) do
      region = options["awslogs-region"] || conn.region
      stream = "#{prefix}/#{name}/#{task_id}"

      case logs_request(conn, region, "GetLogEvents", %{
             "logGroupName" => group,
             "logStreamName" => stream,
             "limit" => @log_tail_lines,
             "startFromHead" => false
           }) do
        {:ok, %{"events" => events}} when is_list(events) -> format_events(events)
        _ -> nil
      end
    else
      _ -> nil
    end
  end

  defp format_events(events) do
    events
    |> Enum.map(&Map.get(&1, "message"))
    |> Enum.filter(&is_binary/1)
    |> Enum.join("\n")
    |> String.trim()
    |> case do
      "" -> nil
      text -> truncate_bytes(text, @log_max_bytes)
    end
  end

  # The log configuration is on the container definition, so this costs a
  # DescribeTaskDefinition - only ever on the failure path, and only worth
  # anything for the awslogs driver.
  defp awslogs_options(%{"taskDefinitionArn" => arn}, container_name, conn) when is_binary(arn) do
    case ecs_request(conn, "DescribeTaskDefinition", %{"taskDefinition" => arn}) do
      {:ok, %{"taskDefinition" => %{"containerDefinitions" => containers}}}
      when is_list(containers) ->
        container =
          Enum.find(containers, &(&1["name"] == container_name)) || List.first(containers)

        case container do
          %{"logConfiguration" => %{"logDriver" => "awslogs", "options" => options}}
          when is_map(options) ->
            {:ok, options}

          _ ->
            :error
        end

      _ ->
        :error
    end
  end

  defp awslogs_options(_task, _container_name, _conn), do: :error

  defp fetch_option(options, key) do
    case Map.get(options, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> :error
    end
  end

  # The stream is named after the container the log configuration belongs
  # to, which is the one the pool overrides, or the task's only one.
  defp container_name_for_logs(_task, name) when is_binary(name) and name != "", do: {:ok, name}

  defp container_name_for_logs(%{"containers" => [%{"name" => name} | _]}, _name)
       when is_binary(name),
       do: {:ok, name}

  defp container_name_for_logs(_task, _name), do: :error

  defp task_id(%{"taskArn" => arn}) when is_binary(arn) do
    case arn |> String.split("/") |> List.last() do
      id when is_binary(id) and id != "" -> {:ok, id}
      _ -> :error
    end
  end

  defp task_id(_task), do: :error

  # Returns nil for a task that stopped because it was asked to, or an
  # error code for one that didn't.
  defp stop_error(task, container_name) do
    case task["stopCode"] do
      "UserInitiated" -> nil
      "ServiceSchedulerInitiated" -> nil
      "SpotInterruption" -> "spot_interrupted"
      "TerminationNotice" -> "spot_interrupted"
      "TaskFailedToStart" -> failed_to_start_error(task["stoppedReason"])
      "EssentialContainerExited" -> container_exit_error(task["containers"], container_name)
      _ -> container_exit_error(task["containers"], container_name) || generic_stop_error(task)
    end
  end

  defp failed_to_start_error(reason) when is_binary(reason) do
    cond do
      reason =~ "CannotPull" -> "image_pull_error"
      reason =~ "ResourceInitializationError" -> "resource_initialization_error"
      reason =~ "CannotCreateContainerError" -> "container_start_error"
      reason =~ "CannotStartContainerError" -> "container_start_error"
      true -> "task_failed_to_start"
    end
  end

  defp failed_to_start_error(_reason), do: "task_failed_to_start"

  defp container_exit_error(containers, container_name) when is_list(containers) do
    case find_container(containers, container_name) do
      %{"reason" => reason} = container when is_binary(reason) ->
        if reason =~ "OutOfMemory", do: "oom_killed", else: exit_code_error(container)

      %{} = container ->
        exit_code_error(container)

      nil ->
        "container_exited"
    end
  end

  defp container_exit_error(_containers, _container_name), do: "container_exited"

  defp find_container(containers, container_name) when is_list(containers) do
    Enum.find(containers, &(&1["name"] == container_name)) ||
      Enum.find(containers, &is_integer(&1["exitCode"]))
  end

  defp find_container(_containers, _container_name), do: nil

  defp exit_code_error(%{"exitCode" => 0}), do: nil
  defp exit_code_error(%{"exitCode" => code}) when is_integer(code), do: "exit_code:#{code}"
  defp exit_code_error(_container), do: "container_exited"

  defp generic_stop_error(%{"stoppedReason" => reason}) when is_binary(reason), do: "task_stopped"
  defp generic_stop_error(_task), do: nil

  defp stopped_reason(%{"stoppedReason" => reason}) when is_binary(reason) and reason != "",
    do: truncate_bytes(reason, @reason_max_bytes)

  defp stopped_reason(_task), do: nil

  # --- Launching ---

  defp resolve_container_name(_conn, %{container_name: name}) when is_binary(name),
    do: {:ok, name}

  # Without a name to override, the task definition says what its
  # containers are called; a worker task has one.
  defp resolve_container_name(conn, config) do
    case ecs_request(conn, "DescribeTaskDefinition", %{
           "taskDefinition" => Map.fetch!(config, :task_definition)
         }) do
      {:ok, %{"taskDefinition" => %{"containerDefinitions" => [%{"name" => name} | _]}}}
      when is_binary(name) ->
        {:ok, name}

      {:ok, _body} ->
        {:error, :no_container}

      # A task definition that doesn't exist isn't reported as not found
      # here, only as not describable.
      {:error, {:api, _status, "ClientException", message}} ->
        {:error, {:task_definition_not_found, message}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp run_task(conn, config, container_name, env, args, opts) do
    override = %{
      "name" => container_name,
      "command" => args,
      "environment" => Enum.map(env, fn {name, value} -> %{"name" => name, "value" => value} end)
    }

    body =
      %{
        "cluster" => Map.fetch!(config, :cluster),
        "taskDefinition" => Map.fetch!(config, :task_definition),
        "count" => 1,
        "overrides" => %{"containerOverrides" => [override]},
        "startedBy" => started_by(opts)
      }
      |> put_launch_type(config)
      |> put_network_configuration(config)
      |> maybe_put("platformVersion", config[:platform_version])

    case ecs_request(conn, "RunTask", body) do
      {:ok, %{"tasks" => [%{"taskArn" => task_arn} | _]}} when is_binary(task_arn) ->
        {:ok, task_arn}

      # No task and no HTTP error: the request was fine, but nothing could
      # place it (no capacity, typically).
      {:ok, %{"failures" => [%{"reason" => reason} = failure | _]}} ->
        {:error, {:run_task_failure, reason, failure["detail"]}}

      {:ok, _body} ->
        {:error, :unexpected_response}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp put_launch_type(body, config) do
    case config[:capacity_provider] do
      nil ->
        Map.put(body, "launchType", config[:launch_type] || "FARGATE")

      provider ->
        Map.put(body, "capacityProviderStrategy", [
          %{"capacityProvider" => provider, "weight" => 1}
        ])
    end
  end

  defp put_network_configuration(body, config) do
    case config[:subnets] do
      subnets when is_list(subnets) and subnets != [] ->
        awsvpc =
          %{
            "subnets" => subnets,
            "assignPublicIp" => if(config[:assign_public_ip], do: "ENABLED", else: "DISABLED")
          }
          |> maybe_put("securityGroups", config[:security_groups])

        Map.put(body, "networkConfiguration", %{"awsvpcConfiguration" => awsvpc})

      _ ->
        body
    end
  end

  defp started_by(opts) do
    "coflux:#{Map.get(opts, :pool_name, "worker")}"
    |> String.slice(0, @started_by_max_length)
  end

  # --- Errors ---

  # Returns {code, detail}: the code is what the worker is deactivated
  # with, the detail (when there is one) is the API's own message, which
  # for an invalid request is the only thing that says what was wrong.
  defp normalize_launch_error(:credentials_missing), do: {"launch_credentials_missing", nil}

  defp normalize_launch_error({:assume_role, code, message}),
    do: {"launch_assume_role_failed", if(message, do: "#{code}: #{message}", else: code)}

  defp normalize_launch_error(:request_failed), do: {"launch_request_failed", nil}
  defp normalize_launch_error(:no_container), do: {"launch_no_container", nil}
  defp normalize_launch_error(:unexpected_response), do: {"launch_api_error", nil}

  defp normalize_launch_error({:run_task_failure, reason, detail}),
    do: {"launch_failed:#{reason}", detail}

  defp normalize_launch_error({:task_definition_not_found, message}),
    do: {"launch_task_definition_not_found", message}

  defp normalize_launch_error({:api, _status, type, message}) do
    code =
      cond do
        type == "ClusterNotFoundException" ->
          "launch_cluster_not_found"

        type == "ClientException" and task_definition_not_found?(message) ->
          "launch_task_definition_not_found"

        type == "ClientException" ->
          "launch_invalid"

        type in @invalid_errors ->
          "launch_invalid"

        type == "AccessDeniedException" ->
          "launch_forbidden"

        type in @unauthorized_errors ->
          "launch_unauthorized"

        type in ["ThrottlingException", "TooManyRequestsException"] ->
          "launch_throttled"

        type == "ServerException" ->
          "launch_server_error"

        true ->
          "launch_api_error"
      end

    {code, message}
  end

  defp normalize_launch_error(_reason), do: {"launch_api_error", nil}

  defp task_definition_not_found?(message) when is_binary(message),
    do: message =~ ~r/task ?definition/i and message =~ ~r/not found/i

  defp task_definition_not_found?(_message), do: false

  # For poll and stop, where the error is retried rather than shown.
  defp describe_error(:credentials_missing), do: "credentials_missing"
  defp describe_error({:assume_role, code, _message}), do: "assume_role_failed:#{code}"
  defp describe_error(:request_failed), do: "request_failed"

  defp describe_error({:api, _status, type, _message}) when is_binary(type),
    do: "api_error:#{type}"

  defp describe_error({:api, status, _type, _message}), do: "api_status:#{status}"
  defp describe_error(reason) when is_binary(reason), do: reason
  defp describe_error(reason), do: inspect(reason)

  # --- Connection ---

  defp build_conn(config) do
    region = Map.fetch!(config, :region)

    with {:ok, credentials} <-
           AwsCredentials.resolve(static_credentials(config),
             region: region,
             role_arn: config[:role_arn],
             external_id: config[:role_external_id],
             req_options: Map.get(config, :req_options, [])
           ) do
      {:ok,
       %{
         region: region,
         endpoint: config[:endpoint] || default_endpoint(region),
         credentials: credentials,
         # Extra options for `Req.request/1`, so a test can stub the API
         # without reaching AWS. Never set in normal operation.
         req_options: Map.get(config, :req_options, [])
       }}
    end
  end

  defp default_endpoint(region), do: "https://ecs.#{region}.amazonaws.com"

  defp static_credentials(
         %{access_key_id: access_key_id, secret_access_key: secret_access_key} = config
       )
       when is_binary(access_key_id) and is_binary(secret_access_key) do
    %{access_key_id: access_key_id, secret_access_key: secret_access_key}
    |> maybe_put(:session_token, config[:session_token])
  end

  defp static_credentials(_config), do: nil

  # --- ECS API ---

  # The ECS API is JSON 1.1 over HTTPS: every call is a POST to the
  # regional endpoint, and the header says which operation.
  defp ecs_request(conn, action, body) do
    aws_json_request(
      conn,
      "ecs",
      conn.region,
      conn.endpoint,
      "#{@api_target_prefix}.#{action}",
      body
    )
  end

  # CloudWatch Logs, for the tail of a failed worker. A separate service
  # and endpoint, but the same JSON 1.1 shape and the same credentials.
  # An `endpoint` override on the pool names an ECS endpoint, so it is
  # deliberately not reused here.
  defp logs_request(conn, region, action, body) do
    aws_json_request(
      conn,
      "logs",
      region,
      "https://logs.#{region}.amazonaws.com",
      "#{@logs_target_prefix}.#{action}",
      body
    )
  end

  defp aws_json_request(conn, service, region, endpoint, target, body) do
    credentials = conn.credentials

    sigv4 =
      [
        service: service,
        region: region,
        access_key_id: credentials.access_key_id,
        secret_access_key: credentials.secret_access_key
      ]
      |> maybe_put_keyword(:token, credentials[:session_token])

    request =
      [
        method: :post,
        url: endpoint,
        headers: [
          {"content-type", "application/x-amz-json-1.1"},
          {"x-amz-target", target}
        ],
        body: Jason.encode!(body),
        aws_sigv4: sigv4,
        retry: false,
        decode_body: false
      ]
      |> Keyword.merge(Map.get(conn, :req_options, []))

    case Req.request(request) do
      {:ok, %{status: status, body: raw}} ->
        decoded =
          case Jason.decode(raw) do
            {:ok, map} when is_map(map) -> map
            _ -> %{}
          end

        if status in 200..299 do
          {:ok, decoded}
        else
          {:error, {:api, status, error_type(decoded), decoded["message"] || decoded["Message"]}}
        end

      {:error, _exception} ->
        {:error, :request_failed}
    end
  end

  # The type comes back as e.g. "ClusterNotFoundException", or namespaced
  # as "com.amazonaws.ecs#ClusterNotFoundException", or with a suffix as
  # "ClusterNotFoundException:http://...".
  defp error_type(%{"__type" => type}) when is_binary(type) do
    type
    |> String.split("#")
    |> List.last()
    |> String.split(":")
    |> hd()
  end

  defp error_type(_body), do: nil

  # --- Helpers ---

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, _key, []), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp maybe_put_keyword(keyword, _key, nil), do: keyword
  defp maybe_put_keyword(keyword, key, value), do: Keyword.put(keyword, key, value)
end
