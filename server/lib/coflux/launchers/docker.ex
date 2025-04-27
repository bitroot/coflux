defmodule Coflux.DockerLauncher do
  def launch(project_id, workspace_name, agent_id, repositories, provides, config \\ %{}) do
    # TODO: option to configure docker host/socket?
    # TODO: option to configure coflux host?
    with {:ok, %{"Id" => container_id}} <-
           create_container(%{
             "Image" => Map.fetch!(config, :image),
             "HostConfig" => %{"NetworkMode" => "host"},
             "Cmd" => repositories,
             "Env" => [
               "COFLUX_HOST=localhost:7777",
               "COFLUX_WORKSPACE=#{workspace_name}",
               "COFLUX_PROJECT=#{project_id}",
               "COFLUX_LAUNCH=#{agent_id}",
               "COFLUX_PROVIDES=#{encode_provides(provides)}"
             ]
           }),
         :ok <- start_container(container_id) do
      # TODO: inspect container, and check running?
      {:ok, %{container: container_id}}
    end
  end

  def stop(%{container: container_id}) do
    case stop_container(container_id) do
      :ok ->
        case remove_container(container_id) do
          :ok -> :ok
        end

      {:error, :no_such_container} ->
        # TODO: return error?
        :ok
    end
  end

  def poll(%{container: container_id}) do
    case inspect_container(container_id) do
      {:ok, result} ->
        {:ok, result["State"]["Running"] == true}

      {:error, :no_such_container} ->
        {:ok, false}
    end
  end

  defp create_container(config) do
    response =
      Req.post!(
        "http:///v1.47/containers/create",
        unix_socket: "/var/run/docker.sock",
        json: config
      )

    case response.status do
      201 -> {:ok, response.body}
      400 -> {:error, :bad_parameter}
      404 -> {:error, :no_such_image}
      409 -> {:error, :conflict}
      500 -> {:error, :server_error}
    end
  end

  defp start_container(container_id) do
    response =
      Req.post!(
        "http:///v1.47/containers/#{container_id}/start",
        unix_socket: "/var/run/docker.sock"
      )

    case response.status do
      204 -> :ok
      304 -> {:error, :container_already_started}
      404 -> {:error, :no_such_container}
      500 -> {:error, :server_error}
    end
  end

  defp inspect_container(container_id) do
    response =
      Req.get!(
        "http:///v1.47/containers/#{container_id}/json",
        unix_socket: "/var/run/docker.sock"
      )

    case response.status do
      200 -> {:ok, response.body}
      404 -> {:error, :no_such_container}
      500 -> {:error, :server_error}
    end
  end

  defp stop_container(container_id) do
    response =
      Req.post!(
        "http:///v1.47/containers/#{container_id}/stop",
        unix_socket: "/var/run/docker.sock"
      )

    case response.status do
      204 -> :ok
      304 -> :ok
      404 -> {:error, :no_such_container}
      500 -> {:error, :server_error}
    end
  end

  defp remove_container(container_id) do
    # TODO: remove volumes?
    response =
      Req.delete!(
        "http:///v1.47/containers/#{container_id}",
        unix_socket: "/var/run/docker.sock"
      )

    case response.status do
      204 -> :ok
      400 -> {:error, :bad_parameter}
      404 -> {:error, :no_such_container}
      409 -> {:error, :conflict}
      500 -> {:error, :server_error}
    end
  end

  defp encode_provides(provides) do
    provides
    |> Enum.flat_map(fn {key, values} ->
      Enum.map(values, &"#{key}:#{&1}")
    end)
    |> Enum.join(" ")
  end
end
