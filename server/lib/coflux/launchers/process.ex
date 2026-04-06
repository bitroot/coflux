defmodule Coflux.ProcessLauncher do
  @log_tail_lines 20
  @log_max_bytes 1024

  def launch(env, modules, config, _opts \\ %{}) do
    cli_path = Coflux.Config.cli_path()
    directory = Map.fetch!(config, :directory)

    # Use `exec` so the shell is replaced by the command, ensuring
    # the port's OS process IS the worker (not a wrapper shell).
    argv = Enum.map_join([cli_path, "worker" | modules], " ", &shell_escape/1)
    shell_cmd = "exec #{argv}"

    port_env =
      Enum.map(env, fn {k, v} -> {String.to_charlist(k), String.to_charlist(v)} end)

    port_opts =
      [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        {:env, port_env},
        {:args, ["-c", shell_cmd]},
        {:cd, String.to_charlist(directory)}
      ]

    case DynamicSupervisor.start_child(
           Coflux.ProcessLauncher.Supervisor,
           {Coflux.ProcessLauncher.Worker, port_opts}
         ) do
      {:ok, pid} ->
        {:ok, %{pid: pid}}

      {:error, _reason} ->
        {:error, "launch_process_failed"}
    end
  end

  def stop(%{pid: pid}) do
    if Process.alive?(pid) do
      GenServer.call(pid, :stop)
    end

    :ok
  end

  def poll(%{pid: pid}) do
    if Process.alive?(pid) do
      case GenServer.call(pid, :status) do
        :running ->
          {:ok, true}

        {:exited, exit_status, output} ->
          GenServer.stop(pid, :normal)
          error = if exit_status != 0, do: "exit_code:#{exit_status}"
          logs = if error, do: format_logs(output)
          {:ok, false, error, logs}
      end
    else
      {:ok, false, "process_lost", nil}
    end
  end

  defp format_logs(""), do: nil

  defp format_logs(content) do
    content
    |> tail_lines(@log_tail_lines)
    |> truncate_bytes(@log_max_bytes)
  end

  defp tail_lines(string, n) do
    string
    |> String.split("\n")
    |> Enum.take(-n)
    |> Enum.join("\n")
  end

  defdelegate truncate_bytes(string, max_bytes), to: Coflux.Launchers.Utils

  defp shell_escape(arg) do
    "'" <> String.replace(arg, "'", "'\\''") <> "'"
  end
end

defmodule Coflux.ProcessLauncher.Worker do
  use GenServer, restart: :temporary

  def start_link(port_opts) do
    GenServer.start_link(__MODULE__, port_opts)
  end

  @impl true
  def init(port_opts) do
    port = Port.open({:spawn_executable, "/bin/sh"}, port_opts)
    {:ok, %{port: port, output: [], exit_status: nil, stop_requested: false}}
  end

  @impl true
  def handle_info({port, {:data, data}}, %{port: port} = state) do
    {:noreply, %{state | output: [state.output, data]}}
  end

  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    {:noreply, %{state | exit_status: status}}
  end

  @impl true
  def handle_call(:status, _from, state) do
    if is_nil(state.exit_status) do
      {:reply, :running, state}
    else
      logs = IO.iodata_to_binary(state.output)
      # Treat as clean exit if stop was requested (even if exit code is non-zero
      # due to SIGTERM producing exit code 143).
      exit_status = if state.stop_requested, do: 0, else: state.exit_status
      {:reply, {:exited, exit_status, logs}, state}
    end
  end

  def handle_call(:stop, _from, %{exit_status: nil} = state) do
    {:os_pid, os_pid} = Port.info(state.port, :os_pid)
    System.cmd("kill", [Integer.to_string(os_pid)], stderr_to_stdout: true)
    {:reply, :ok, %{state | stop_requested: true}}
  end

  def handle_call(:stop, _from, state) do
    {:reply, :ok, %{state | stop_requested: true}}
  end

  @impl true
  def terminate(_reason, %{exit_status: nil, port: port}) do
    Port.close(port)
  end

  def terminate(_reason, _state), do: :ok
end
