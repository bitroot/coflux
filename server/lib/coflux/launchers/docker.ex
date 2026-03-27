defmodule Coflux.DockerLauncher do
  @docker_api_version "v1.47"
  @log_tail_lines 20
  @log_max_bytes 1024

  def launch(env, modules, config) do
    docker_conn = parse_docker_host(config[:docker_host])

    container_env = Enum.map(env, fn {k, v} -> "#{k}=#{v}" end)

    with {:ok, %{"Id" => container_id}} <-
           create_container(
             docker_conn,
             %{
               "Image" => Map.fetch!(config, :image),
               "HostConfig" => %{"NetworkMode" => "host"},
               "Cmd" => modules,
               "Env" => container_env
             }
           ),
         :ok <- start_container(docker_conn, container_id) do
      {:ok, %{container: container_id, docker_conn: docker_conn}}
    else
      {:error, reason} -> {:error, Atom.to_string(reason)}
    end
  end

  def stop(%{container: container_id, docker_conn: docker_conn}) do
    case stop_container(docker_conn, container_id) do
      :ok ->
        case remove_container(docker_conn, container_id) do
          :ok -> :ok
        end

      {:error, :no_such_container} ->
        :ok
    end
  end

  def poll(%{container: container_id, docker_conn: docker_conn}) do
    case inspect_container(docker_conn, container_id) do
      {:ok, result} ->
        state = result["State"]

        if state["Running"] do
          {:ok, true}
        else
          error = build_error(state)
          logs = if error, do: fetch_logs(docker_conn, container_id)
          {:ok, false, error, logs}
        end

      {:error, :no_such_container} ->
        {:ok, false, nil, nil}
    end
  end

  # Returns nil for successful exit, or error code string for failures
  defp build_error(state) do
    cond do
      state["OOMKilled"] == true -> "oom_killed"
      state["ExitCode"] != 0 -> "exit_code:#{state["ExitCode"]}"
      true -> nil
    end
  end

  # Parses Docker host configuration following Docker's DOCKER_HOST convention:
  # - unix:///path/to/socket or /path/to/socket -> Unix socket
  # - tcp://hostname:port -> TCP connection
  defp parse_docker_host(nil), do: {:unix, "/var/run/docker.sock"}
  defp parse_docker_host("/" <> _ = path), do: {:unix, path}

  defp parse_docker_host(docker_host) do
    case URI.parse(docker_host) do
      %{scheme: "unix", path: path} when is_binary(path) ->
        {:unix, path}

      %{scheme: "tcp", host: host, port: port} when is_binary(host) ->
        {:tcp, host, port || 2375}

      _ ->
        raise ArgumentError, "Invalid docker_host: #{inspect(docker_host)}"
    end
  end

  defp docker_request(docker_conn, method, path, opts \\ []) do
    {url, conn_opts} =
      case docker_conn do
        {:unix, socket_path} ->
          {"http:///#{@docker_api_version}#{path}", [unix_socket: socket_path]}

        {:tcp, host, port} ->
          {"http://#{host}:#{port}/#{@docker_api_version}#{path}", []}
      end

    Req.request!(Keyword.merge(conn_opts, [{:method, method}, {:url, url} | opts]))
  end

  defp create_container(docker_conn, config) do
    response = docker_request(docker_conn, :post, "/containers/create", json: config)

    case response.status do
      201 -> {:ok, response.body}
      400 -> {:error, :bad_parameter}
      404 -> {:error, :no_such_image}
      409 -> {:error, :conflict}
      500 -> {:error, :server_error}
    end
  end

  defp start_container(docker_conn, container_id) do
    response = docker_request(docker_conn, :post, "/containers/#{container_id}/start")

    case response.status do
      204 -> :ok
      304 -> {:error, :container_already_started}
      404 -> {:error, :no_such_container}
      500 -> {:error, :server_error}
    end
  end

  defp inspect_container(docker_conn, container_id) do
    response = docker_request(docker_conn, :get, "/containers/#{container_id}/json")

    case response.status do
      200 -> {:ok, response.body}
      404 -> {:error, :no_such_container}
      500 -> {:error, :server_error}
    end
  end

  defp fetch_logs(docker_conn, container_id) do
    case container_logs(docker_conn, container_id, @log_tail_lines) do
      {:ok, logs} when logs != "" -> truncate_bytes(logs, @log_max_bytes)
      _ -> nil
    end
  end

  defp container_logs(docker_conn, container_id, tail) do
    response =
      docker_request(docker_conn, :get, "/containers/#{container_id}/logs",
        params: [stdout: true, stderr: true, tail: tail]
      )

    case response.status do
      200 -> {:ok, demux_docker_logs(response.body)}
      404 -> {:error, :no_such_container}
      500 -> {:error, :server_error}
    end
  end

  # Docker multiplexed stream format: each frame has an 8-byte header
  # [stream_type(1), padding(3), size(4, big-endian)] followed by the payload.
  defp demux_docker_logs(data) when is_binary(data) do
    demux_docker_logs(data, [])
  end

  defp demux_docker_logs(
         <<_type::8, _pad::24, size::32, payload::binary-size(size), rest::binary>>,
         acc
       ) do
    demux_docker_logs(rest, [acc, payload])
  end

  defp demux_docker_logs(_, acc), do: IO.iodata_to_binary(acc)

  defp truncate_bytes(string, max_bytes) when byte_size(string) <= max_bytes, do: string

  defp truncate_bytes(string, max_bytes) do
    string
    |> binary_part(byte_size(string) - max_bytes, max_bytes)
    |> String.replace(~r/^[^\n]*\n/, "")
  end

  defp stop_container(docker_conn, container_id) do
    response = docker_request(docker_conn, :post, "/containers/#{container_id}/stop")

    case response.status do
      204 -> :ok
      304 -> :ok
      404 -> {:error, :no_such_container}
      500 -> {:error, :server_error}
    end
  end

  defp remove_container(docker_conn, container_id) do
    response = docker_request(docker_conn, :delete, "/containers/#{container_id}")

    case response.status do
      204 -> :ok
      400 -> {:error, :bad_parameter}
      404 -> {:error, :no_such_container}
      409 -> {:error, :conflict}
      500 -> {:error, :server_error}
    end
  end
end
