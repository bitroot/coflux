defmodule Coflux.ProcessLauncher do
  @moduledoc """
  Runs workers as processes on the server's own machine.

  What identifies a launched worker is its OS process, not the Erlang
  process supervising the port. The server can be restarted while the
  worker carries on running, and across that restart an Erlang pid means
  nothing: it decodes into a pid of the *new* VM, which may well have been
  reused by something unrelated. So the launch data is an OS pid and a
  fingerprint of when that process started - the pair, because pid numbers
  are reused too, and a recycled pid must not be mistaken for the worker
  or signalled as if it were.

  The supervising process is found by looking the OS pid up in a registry
  that only holds this VM's workers. When it is there it is preferred,
  because it has the output to report; when it isn't - after a restart -
  the OS pid alone still answers "is it alive?" and "stop".
  """

  @registry Coflux.ProcessLauncher.Registry

  @log_tail_lines 20
  @log_max_bytes 1024

  # How long a worker is given to exit after SIGTERM before SIGKILL.
  @stop_grace_ms 10_000

  # Exit codes that mean "it did what we asked": 128 + SIGTERM and
  # 128 + SIGKILL.
  @signalled_exit_codes [143, 137]

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
        case GenServer.call(pid, :os_pid) do
          {:ok, os_pid} ->
            {:ok, %{os_pid: os_pid, started: process_started(os_pid)}}

          :error ->
            {:error, "launch_process_failed"}
        end

      {:error, _reason} ->
        {:error, "launch_process_failed"}
    end
  end

  def stop(data) do
    case resolve(data) do
      {:supervised, pid} ->
        GenServer.call(pid, :stop)
        :ok

      {:unsupervised, os_pid} ->
        signal(os_pid, "TERM")
        :ok

      # Already gone, which is what was being asked for.
      :gone ->
        :ok
    end
  end

  def poll(data) do
    case resolve(data) do
      {:supervised, pid} ->
        case GenServer.call(pid, :status) do
          :running ->
            {:ok, true}

          {:exited, exit_status, stop_requested, output} ->
            GenServer.stop(pid, :normal)
            error = exit_error(exit_status, stop_requested)
            logs = if error, do: format_logs(output)
            {:ok, false, error, logs}
        end

      {:unsupervised, _os_pid} ->
        # Running, but launched before this server started, so there is no
        # output to report if and when it exits.
        {:ok, true}

      :gone ->
        {:ok, false, "process_lost", nil}
    end
  end

  # A stop asks for SIGTERM and escalates to SIGKILL, so exiting from
  # either is the worker doing as it was told, not a failure. Any other
  # non-zero exit during a drain is a real crash and is reported as one.
  defp exit_error(0, _stop_requested), do: nil
  defp exit_error(status, true) when status in @signalled_exit_codes, do: nil
  defp exit_error(status, _stop_requested), do: "exit_code:#{status}"

  defp resolve(%{os_pid: os_pid} = data) do
    cond do
      # Only this VM's workers are in the registry, so finding one is
      # proof that the process it names is the one that was launched.
      pid = lookup(os_pid) ->
        {:supervised, pid}

      process_alive?(os_pid) && process_started(os_pid) == Map.get(data, :started) ->
        {:unsupervised, os_pid}

      true ->
        :gone
    end
  end

  # Launch data from before the OS pid was recorded. Nothing that knows how
  # to reach the process survives, so it is treated as gone rather than
  # having an unrelated Erlang pid signalled on its behalf.
  defp resolve(_data), do: :gone

  defp lookup(os_pid) do
    case Registry.lookup(@registry, os_pid) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  defp process_alive?(os_pid) do
    match?({_, 0}, System.cmd("kill", ["-0", Integer.to_string(os_pid)], stderr_to_stdout: true))
  end

  # When the process started, which is what distinguishes it from a later
  # one that reused its pid. `ps` reports this on both Linux and macOS;
  # nil where it can't be read, in which case the pid has to stand alone.
  defp process_started(os_pid) do
    case System.cmd("ps", ["-o", "lstart=", "-p", Integer.to_string(os_pid)],
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        case String.trim(output) do
          "" -> nil
          value -> value
        end

      {_output, _} ->
        nil
    end
  catch
    :error, _ -> nil
  end

  defp signal(os_pid, name) do
    System.cmd("kill", ["-#{name}", Integer.to_string(os_pid)], stderr_to_stdout: true)
    :ok
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

  def registry, do: @registry
  def stop_grace_ms, do: @stop_grace_ms
end

defmodule Coflux.ProcessLauncher.Worker do
  use GenServer, restart: :temporary

  def start_link(port_opts) do
    GenServer.start_link(__MODULE__, port_opts)
  end

  @impl true
  def init(port_opts) do
    port = Port.open({:spawn_executable, "/bin/sh"}, port_opts)

    with {:os_pid, os_pid} <- Port.info(port, :os_pid),
         # An exited worker's process lingers until a poll reports the
         # exit, so the OS could in principle hand its pid to a new one
         # first. Refusing to launch is the right answer: the pool retries.
         {:ok, _} <- Registry.register(Coflux.ProcessLauncher.registry(), os_pid, nil) do
      {:ok,
       %{
         port: port,
         os_pid: os_pid,
         output: [],
         exit_status: nil,
         stop_requested: false,
         kill_timer: nil
       }}
    else
      nil -> {:stop, :no_os_pid}
      {:error, {:already_registered, _}} -> {:stop, :os_pid_conflict}
    end
  end

  @impl true
  def handle_info({port, {:data, data}}, %{port: port} = state) do
    {:noreply, %{state | output: [state.output, data]}}
  end

  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    if state.kill_timer, do: Process.cancel_timer(state.kill_timer)
    {:noreply, %{state | exit_status: status, kill_timer: nil}}
  end

  # A worker that ignored SIGTERM is killed outright rather than left
  # running with nothing further going to happen to it.
  def handle_info(:kill, %{exit_status: nil} = state) do
    System.cmd("kill", ["-KILL", Integer.to_string(state.os_pid)], stderr_to_stdout: true)
    {:noreply, %{state | kill_timer: nil}}
  end

  def handle_info(:kill, state), do: {:noreply, %{state | kill_timer: nil}}

  @impl true
  def handle_call(:os_pid, _from, state) do
    {:reply, {:ok, state.os_pid}, state}
  end

  def handle_call(:status, _from, state) do
    if is_nil(state.exit_status) do
      {:reply, :running, state}
    else
      logs = IO.iodata_to_binary(state.output)
      # The real exit code, with whether a stop was asked for alongside it
      # rather than folded into it: a worker that crashed while draining
      # should not look like one that shut down cleanly.
      {:reply, {:exited, state.exit_status, state.stop_requested, logs}, state}
    end
  end

  def handle_call(:stop, _from, %{exit_status: nil} = state) do
    System.cmd("kill", ["-TERM", Integer.to_string(state.os_pid)], stderr_to_stdout: true)
    timer = Process.send_after(self(), :kill, Coflux.ProcessLauncher.stop_grace_ms())
    {:reply, :ok, %{state | stop_requested: true, kill_timer: timer}}
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
