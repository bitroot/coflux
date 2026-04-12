defmodule Coflux.KubernetesLauncher do
  import Coflux.Launchers.Utils, only: [truncate_bytes: 2]

  @log_tail_lines 20
  @log_max_bytes 1024

  def launch(env, modules, config, opts \\ %{}) do
    namespace = Map.get(config, :namespace, "default")
    conn = build_conn(config)
    job_name = generate_job_name(opts)

    container_env =
      Enum.map(env, fn {k, v} ->
        %{"name" => k, "value" => v}
      end)

    container = %{
      "name" => "worker",
      "image" => Map.fetch!(config, :image),
      "args" => modules,
      "env" => container_env
    }

    container =
      case config[:image_pull_policy] do
        nil -> container
        policy -> Map.put(container, "imagePullPolicy", policy)
      end

    container = apply_resources(container, config)
    container = apply_volume_mounts(container, config)

    pod_spec = %{
      "restartPolicy" => "Never",
      "containers" => [container]
    }

    pod_spec = apply_service_account(pod_spec, config)
    pod_spec = apply_node_selector(pod_spec, config)
    pod_spec = apply_tolerations(pod_spec, config)
    pod_spec = apply_image_pull_secrets(pod_spec, config)
    pod_spec = apply_host_aliases(pod_spec, config)
    pod_spec = apply_volumes(pod_spec, config)

    default_labels = %{
      "app.kubernetes.io/managed-by" => "coflux",
      "coflux.com/component" => "worker"
    }

    extra_labels = Map.get(config, :labels, %{})
    merged_labels = Map.merge(default_labels, extra_labels)
    extra_annotations = Map.get(config, :annotations, %{})

    pod_metadata = %{"labels" => Map.merge(merged_labels, %{"job-name" => job_name})}

    pod_metadata =
      if map_size(extra_annotations) > 0,
        do: Map.put(pod_metadata, "annotations", extra_annotations),
        else: pod_metadata

    job_spec = %{
      "backoffLimit" => 0,
      "ttlSecondsAfterFinished" => 300,
      "template" => %{
        "metadata" => pod_metadata,
        "spec" => pod_spec
      }
    }

    job_spec =
      case config[:active_deadline_seconds] do
        nil -> job_spec
        seconds -> Map.put(job_spec, "activeDeadlineSeconds", seconds)
      end

    job = %{
      "apiVersion" => "batch/v1",
      "kind" => "Job",
      "metadata" => %{
        "name" => job_name,
        "namespace" => namespace,
        "labels" => merged_labels
      },
      "spec" => job_spec
    }

    path = "/apis/batch/v1/namespaces/#{namespace}/jobs"

    case k8s_request(conn, :post, path, json: job) do
      {:ok, %{"metadata" => %{"name" => name}}} ->
        {:ok, %{job_name: name, namespace: namespace, k8s_conn: conn}}

      {:error, reason} ->
        {:error, normalize_launch_error(reason)}
    end
  end

  def stop(%{job_name: job_name, namespace: namespace, k8s_conn: conn}) do
    path =
      "/apis/batch/v1/namespaces/#{namespace}/jobs/#{job_name}?propagationPolicy=Background"

    case k8s_request(conn, :delete, path) do
      {:ok, _} -> :ok
      {:error, :not_found} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  def poll(%{job_name: job_name, namespace: namespace, k8s_conn: conn}) do
    path = "/apis/batch/v1/namespaces/#{namespace}/jobs/#{job_name}"

    case k8s_request(conn, :get, path) do
      {:ok, job} ->
        conditions = get_in(job, ["status", "conditions"]) || []

        cond do
          has_condition?(conditions, "Complete", "True") ->
            {:ok, false, nil, nil}

          has_condition?(conditions, "Failed", "True") ->
            {pod_error, pod_logs} = check_pod_terminated(conn, namespace, job_name)
            error = pod_error || get_failure_reason(conditions)
            logs = pod_logs || fetch_pod_logs(conn, namespace, job_name)
            {:ok, false, error, logs}

          true ->
            # Job has no terminal condition yet — check pod status for
            # container-level errors (e.g. ErrImageNeverPull, CrashLoopBackOff)
            # that K8s won't surface as job conditions.
            case check_pod_error(conn, namespace, job_name) do
              {:error, reason, logs} -> {:ok, false, reason, logs}
              :ok -> {:ok, true}
            end
        end

      {:error, :not_found} ->
        {:ok, false, nil, nil}

      {:error, _reason} ->
        {:ok, false, "k8s_error", nil}
    end
  end

  # --- Error normalization ---

  defp normalize_launch_error(:not_found), do: "launch_not_found"
  defp normalize_launch_error(:conflict), do: "launch_conflict"
  defp normalize_launch_error(:unprocessable_entity), do: "launch_invalid"
  defp normalize_launch_error(:unauthorized), do: "launch_unauthorized"
  defp normalize_launch_error(:forbidden), do: "launch_forbidden"
  defp normalize_launch_error(:request_failed), do: "launch_request_failed"
  defp normalize_launch_error(_), do: "launch_api_error"

  defp normalize_pod_error("ErrImageNeverPull"), do: "image_pull_error"
  defp normalize_pod_error("ImagePullBackOff"), do: "image_pull_error"
  defp normalize_pod_error("InvalidImageName"), do: "image_pull_error"
  defp normalize_pod_error("CreateContainerConfigError"), do: "container_config_error"
  defp normalize_pod_error("CreateContainerError"), do: "container_config_error"
  defp normalize_pod_error("PreCreateHookError"), do: "container_hook_error"
  defp normalize_pod_error("PostStartHookError"), do: "container_hook_error"
  defp normalize_pod_error(_), do: "container_config_error"

  # --- Connection ---

  defp build_conn(config) do
    cond do
      config[:api_server] ->
        %{
          url: String.trim_trailing(config.api_server, "/"),
          token: config[:token],
          ca_cert: config[:ca_cert],
          insecure: config[:insecure] == true
        }

      File.exists?("/var/run/secrets/kubernetes.io/serviceaccount/token") ->
        token = File.read!("/var/run/secrets/kubernetes.io/serviceaccount/token")
        ca_cert = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

        %{
          url: "https://kubernetes.default.svc",
          token: String.trim(token),
          ca_cert: ca_cert,
          insecure: false
        }

      true ->
        # Fall back to localhost (e.g. kubectl proxy)
        %{url: "http://localhost:8001", token: nil, ca_cert: nil, insecure: false}
    end
  end

  # --- K8s API ---

  defp k8s_request(conn, method, path, opts \\ []) do
    url = conn.url <> path

    req_opts =
      [method: method, url: url, retry: false] ++
        auth_opts(conn) ++
        tls_opts(conn) ++
        opts

    case Req.request(req_opts) do
      {:ok, response} ->
        case response.status do
          status when status in 200..299 ->
            {:ok, response.body}

          404 ->
            {:error, :not_found}

          409 ->
            {:error, :conflict}

          422 ->
            {:error, :unprocessable_entity}

          401 ->
            {:error, :unauthorized}

          403 ->
            {:error, :forbidden}

          _status ->
            {:error, :api_error}
        end

      {:error, _exception} ->
        {:error, :request_failed}
    end
  end

  defp auth_opts(%{token: nil}), do: []
  defp auth_opts(%{token: token}), do: [headers: [{"authorization", "Bearer #{token}"}]]

  defp tls_opts(%{insecure: true}),
    do: [connect_options: [transport_opts: [verify: :verify_none]]]

  defp tls_opts(%{ca_cert: ca_cert}) when is_binary(ca_cert) and ca_cert != "" do
    [connect_options: [transport_opts: [cacertfile: String.to_charlist(ca_cert)]]]
  end

  defp tls_opts(_), do: []

  # --- Pod spec helpers ---

  defp apply_resources(container, config) do
    case config[:resources] do
      nil ->
        container

      resources when is_map(resources) ->
        requests = %{}
        limits = %{}

        {requests, limits} =
          Enum.reduce(resources, {requests, limits}, fn
            {"cpu", v}, {req, lim} ->
              {Map.put(req, "cpu", v), lim}

            {"memory", v}, {req, lim} ->
              {Map.put(req, "memory", v), lim}

            {"gpu", v}, {req, lim} ->
              gpu_req = Map.put(req, "nvidia.com/gpu", v)
              {gpu_req, Map.put(lim, "nvidia.com/gpu", v)}

            {"limits", v}, {req, lim} when is_map(v) ->
              lim = if v["cpu"], do: Map.put(lim, "cpu", v["cpu"]), else: lim
              lim = if v["memory"], do: Map.put(lim, "memory", v["memory"]), else: lim
              {req, lim}

            _, acc ->
              acc
          end)

        resource_spec = %{}

        resource_spec =
          if requests != %{},
            do: Map.put(resource_spec, "requests", requests),
            else: resource_spec

        resource_spec =
          if limits != %{}, do: Map.put(resource_spec, "limits", limits), else: resource_spec

        if resource_spec != %{} do
          Map.put(container, "resources", resource_spec)
        else
          container
        end
    end
  end

  defp apply_service_account(pod_spec, config) do
    case config[:service_account] do
      nil -> pod_spec
      sa -> Map.put(pod_spec, "serviceAccountName", sa)
    end
  end

  defp apply_node_selector(pod_spec, config) do
    case config[:node_selector] do
      nil -> pod_spec
      ns when map_size(ns) == 0 -> pod_spec
      ns -> Map.put(pod_spec, "nodeSelector", ns)
    end
  end

  defp apply_tolerations(pod_spec, config) do
    case config[:tolerations] do
      nil -> pod_spec
      [] -> pod_spec
      tolerations -> Map.put(pod_spec, "tolerations", tolerations)
    end
  end

  defp apply_image_pull_secrets(pod_spec, config) do
    case config[:image_pull_secrets] do
      nil ->
        pod_spec

      [] ->
        pod_spec

      secrets ->
        Map.put(
          pod_spec,
          "imagePullSecrets",
          Enum.map(secrets, fn name -> %{"name" => name} end)
        )
    end
  end

  defp apply_host_aliases(pod_spec, config) do
    case config[:host_aliases] do
      nil ->
        pod_spec

      [] ->
        pod_spec

      aliases when is_list(aliases) ->
        Map.put(pod_spec, "hostAliases", aliases)
    end
  end

  defp apply_volumes(pod_spec, config) do
    case config[:volumes] do
      nil -> pod_spec
      [] -> pod_spec
      volumes when is_list(volumes) -> Map.put(pod_spec, "volumes", volumes)
    end
  end

  defp apply_volume_mounts(container, config) do
    case config[:volume_mounts] do
      nil -> container
      [] -> container
      mounts when is_list(mounts) -> Map.put(container, "volumeMounts", mounts)
    end
  end

  # --- Helpers ---

  defp generate_job_name(opts) do
    suffix = :crypto.strong_rand_bytes(5) |> Base.hex_encode32(case: :lower, padding: false)
    pool = Map.get(opts, :pool_name, "worker")

    name =
      "coflux-#{pool}-#{suffix}"
      |> String.downcase()
      |> String.replace(~r/[^a-z0-9\-]/, "-")
      |> String.slice(0, 63)

    name
  end

  defp has_condition?(conditions, type, status) do
    Enum.any?(conditions, fn c ->
      c["type"] == type && c["status"] == status
    end)
  end

  defp get_failure_reason(conditions) do
    case Enum.find(conditions, &(&1["type"] == "Failed" && &1["status"] == "True")) do
      nil -> "job_failed"
      %{"reason" => "DeadlineExceeded"} -> "job_deadline_exceeded"
      %{"reason" => "BackoffLimitExceeded"} -> "job_backoff_exceeded"
      _ -> "job_failed"
    end
  end

  @terminal_waiting_reasons MapSet.new([
                              "ErrImageNeverPull",
                              "ImagePullBackOff",
                              "InvalidImageName",
                              "CreateContainerConfigError",
                              "PreCreateHookError",
                              "CreateContainerError",
                              "PostStartHookError"
                            ])

  # Check terminated container status for specific exit reasons (OOM, etc.)
  # Returns {error_or_nil, logs_or_nil}
  defp check_pod_terminated(conn, namespace, job_name) do
    path =
      "/api/v1/namespaces/#{namespace}/pods?labelSelector=job-name%3D#{job_name}"

    case k8s_request(conn, :get, path) do
      {:ok, %{"items" => [pod | _]}} ->
        container_statuses = get_in(pod, ["status", "containerStatuses"]) || []

        case find_terminated_reason(container_statuses) do
          nil ->
            {nil, nil}

          {reason, exit_code} ->
            pod_name = get_in(pod, ["metadata", "name"])
            logs = fetch_pod_log(conn, namespace, pod_name)
            error = normalize_terminated_reason(reason, exit_code)
            {error, logs}
        end

      _ ->
        {nil, nil}
    end
  end

  defp find_terminated_reason(container_statuses) do
    Enum.find_value(container_statuses, fn status ->
      case get_in(status, ["state", "terminated"]) do
        %{"reason" => reason} = terminated when is_binary(reason) ->
          {reason, terminated["exitCode"]}

        _ ->
          nil
      end
    end)
  end

  defp normalize_terminated_reason("OOMKilled", _exit_code), do: "oom_killed"
  defp normalize_terminated_reason("DeadlineExceeded", _exit_code), do: "job_deadline_exceeded"
  defp normalize_terminated_reason("Evicted", _exit_code), do: "pod_evicted"

  defp normalize_terminated_reason("Error", exit_code) when is_integer(exit_code),
    do: "exit_code:#{exit_code}"

  defp normalize_terminated_reason(_reason, _exit_code), do: nil

  defp check_pod_error(conn, namespace, job_name) do
    path =
      "/api/v1/namespaces/#{namespace}/pods?labelSelector=job-name%3D#{job_name}"

    case k8s_request(conn, :get, path) do
      {:ok, %{"items" => [pod | _]}} ->
        container_statuses = get_in(pod, ["status", "containerStatuses"]) || []

        case find_terminal_waiting(container_statuses) do
          nil -> :ok
          {reason, nil} -> {:error, normalize_pod_error(reason), nil}
          {reason, ""} -> {:error, normalize_pod_error(reason), nil}
          {reason, message} -> {:error, normalize_pod_error(reason), message}
        end

      _ ->
        :ok
    end
  end

  defp find_terminal_waiting(container_statuses) do
    Enum.find_value(container_statuses, fn status ->
      case get_in(status, ["state", "waiting"]) do
        %{"reason" => reason} = waiting when is_binary(reason) ->
          if MapSet.member?(@terminal_waiting_reasons, reason) do
            {reason, waiting["message"]}
          end

        _ ->
          nil
      end
    end)
  end

  defp fetch_pod_logs(conn, namespace, job_name) do
    # Find pods by job-name label
    path =
      "/api/v1/namespaces/#{namespace}/pods?labelSelector=job-name%3D#{job_name}"

    case k8s_request(conn, :get, path) do
      {:ok, %{"items" => [pod | _]}} ->
        pod_name = get_in(pod, ["metadata", "name"])
        fetch_pod_log(conn, namespace, pod_name)

      _ ->
        nil
    end
  end

  defp fetch_pod_log(conn, namespace, pod_name) do
    path =
      "/api/v1/namespaces/#{namespace}/pods/#{pod_name}/log?tailLines=#{@log_tail_lines}"

    url = conn.url <> path

    req_opts =
      [method: :get, url: url, decode_body: false] ++
        auth_opts(conn) ++
        tls_opts(conn)

    case Req.request(req_opts) do
      {:ok, %{status: 200, body: body}} when body != "" ->
        truncate_bytes(body, @log_max_bytes)

      _ ->
        nil
    end
  end
end
