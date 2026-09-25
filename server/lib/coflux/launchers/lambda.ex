defmodule Coflux.LambdaLauncher do
  @moduledoc """
  Runs workers as AWS Lambda invocations.

  A pool names a function, and each worker is one invocation of it. The
  function is expected to run `coflux worker` with the arguments (the
  modules to host) and environment (what the worker needs to connect)
  that the invocation's payload carries - a function's own environment
  variables are set on the function, not per call - and to return once
  the worker exits. Everything else - image, memory, timeout, role,
  networking - belongs to the function.

  The invocation is synchronous, and held open by a process here until
  the function returns. That's the only way to know when an invocation
  has finished - Lambda has nothing to describe one by - and the response
  carries whether the function failed, and the tail of its log.

  Nothing can stop an invocation from outside. The worker is asked to
  leave over its connection like any other, and the function's timeout is
  the hard bound, which is reported as the worker's expiry so that
  nothing it's given runs past it.

  Credentials are resolved on every call, as for ECS. See
  `Coflux.Launchers.Aws`.
  """

  import Coflux.Launchers.Utils, only: [truncate_bytes: 2]

  alias Coflux.Launchers.Aws
  alias Coflux.LambdaLauncher.{Cache, Invocation}

  @registry Coflux.LambdaLauncher.Registry
  @supervisor Coflux.LambdaLauncher.Supervisor

  # How long a launch waits for Lambda to turn the invocation down. A
  # synchronous invocation says nothing until the function returns, but a
  # rejection - throttled, not found, not allowed - comes straight back.
  @accept_window_ms 500

  # How long past the function's timeout the invocation is held for,
  # for the response to arrive.
  @response_margin_ms 30_000

  # After a server restart there's no held invocation to ask, so one is
  # assumed to be running until its timeout, and this, has passed.
  @restart_grace_ms 60_000

  # How long a function's configuration is reused for, so a burst of
  # launches makes one call for it rather than one each.
  @configuration_ttl_ms 60_000

  # Matches the other launchers, so a failed worker reads the same however
  # it was launched.
  @log_tail_lines 20
  @log_max_bytes 1024

  # The shortest function timeout a worker can do anything with. The
  # server gives a worker nothing new in its last 30 seconds, so what's
  # left has to cover the function starting as well as some work.
  @min_timeout_ms 60_000

  # Why a request can fail before anything reaches Lambda: there was no
  # connection to be had, or not a secure one.
  @unsent_reasons [:nxdomain, :econnrefused, :ehostunreach, :enetunreach, :eaddrnotavail]

  @api_version "2015-03-31"

  # `opts` may carry an `on_exit` function, which is called as soon as the
  # function returns, so the worker needn't wait for its next poll to be
  # known to have gone.
  def launch(env, args, config, opts \\ %{}) do
    with {:ok, conn} <- build_conn(config),
         {:ok, timeout_ms} <- function_timeout_ms(conn, config),
         :ok <- check_timeout(timeout_ms),
         {:ok, data} <- invoke(conn, config, env, args, timeout_ms, opts[:on_exit]) do
      {:ok, data}
    else
      {:error, reason} ->
        case normalize_launch_error(reason) do
          {error, nil} -> {:error, error}
          {error, detail} -> {:error, error, detail}
        end
    end
  end

  # There's nothing to ask Lambda to do: an invocation can't be stopped.
  # The worker has already been asked to leave over its connection, and
  # the function's timeout bounds it if it doesn't. Letting go of the held
  # request would lose the only word of when it has gone, and wouldn't
  # stop the function either.
  def stop(_data, _config), do: :ok

  def poll(%{invocation_id: invocation_id} = data, _config) do
    case lookup(invocation_id) do
      nil ->
        assume_by_deadline(data)

      pid ->
        case Invocation.status(pid) do
          :running -> {:ok, true}
          {:done, result} -> interpret_result(result, data)
        end
    end
  catch
    # The invocation's process went between the lookup and the call.
    :exit, _ -> assume_by_deadline(data)
  end

  # --- Invoking ---

  defp invoke(conn, config, env, args, timeout_ms, on_exit) do
    invocation_id = generate_id()
    launched_at = System.os_time(:millisecond)
    payload = Jason.encode!(%{"env" => env, "args" => args})
    deadline = launched_at + timeout_ms

    request = fn ->
      case Aws.rest_request(conn, "lambda", :post, function_path(config, "invocations"),
             query: qualifier_query(config),
             headers: [
               {"content-type", "application/json"},
               {"x-amz-invocation-type", "RequestResponse"},
               {"x-amz-log-type", "Tail"}
             ],
             body: payload,
             receive_timeout: timeout_ms + @response_margin_ms,
             # A synchronous invocation holds the connection quiet for as
             # long as the function runs; without keepalive something in
             # between may decide it's dead.
             connect_options: [transport_opts: [keepalive: true]]
           ) do
        {:error, {:request_failed, exception}} = error ->
          if unsent?(exception, deadline), do: {:error, {:not_sent, exception}}, else: error

        result ->
          result
      end
    end

    case DynamicSupervisor.start_child(
           @supervisor,
           {Invocation, {invocation_id, request, on_exit}}
         ) do
      {:ok, pid} ->
        case Invocation.await(pid, @accept_window_ms) do
          :running ->
            {:ok, invocation_data(invocation_id, config, launched_at, timeout_ms)}

          # Turned down - the only kind of answer that comes this quickly.
          {:done, {:error, reason}} ->
            {:error, reason}

          # Or the function returned already, which the poll will make
          # sense of like any other return.
          {:done, {:ok, _response}} ->
            {:ok, invocation_data(invocation_id, config, launched_at, timeout_ms)}
        end

      {:error, _reason} ->
        {:error, :invocation_failed}
    end
  end

  defp invocation_data(invocation_id, config, launched_at, timeout_ms) do
    %{
      invocation_id: invocation_id,
      function: Map.fetch!(config, :function),
      qualifier: config[:qualifier],
      launched_at: launched_at,
      timeout_ms: timeout_ms,
      # Measured from before the request was sent, so it's never later
      # than when Lambda will actually end the function.
      expires_at: launched_at + timeout_ms
    }
  end

  # A request that failed before reaching Lambda has started nothing, and
  # one that failed afterwards may have. The response is waited for past
  # the function's timeout, so a timeout before then was the connection's.
  defp unsent?(%Req.TransportError{reason: reason}, deadline) do
    case reason do
      reason when reason in @unsent_reasons -> true
      {:tls_alert, _} -> true
      :timeout -> System.os_time(:millisecond) < deadline
      _ -> false
    end
  end

  defp unsent?(_exception, _deadline), do: false

  defp check_timeout(timeout_ms) when timeout_ms < @min_timeout_ms,
    do: {:error, {:timeout_too_short, timeout_ms}}

  defp check_timeout(_timeout_ms), do: :ok

  # Keyed by the credentials' key as well as the function, since a name
  # (unlike an ARN) is only unique within an account.
  defp function_timeout_ms(conn, config) do
    key =
      {conn.endpoint, conn.credentials.access_key_id, Map.fetch!(config, :function),
       config[:qualifier]}

    now = System.os_time(:millisecond)

    case Cache.get(key, now) do
      {:ok, timeout_ms} ->
        {:ok, timeout_ms}

      :none ->
        case Aws.rest_request(conn, "lambda", :get, function_path(config, "configuration"),
               query: qualifier_query(config)
             ) do
          {:ok, response} ->
            case Jason.decode(response.body) do
              {:ok, %{"Timeout" => seconds}} when is_integer(seconds) and seconds > 0 ->
                timeout_ms = seconds * 1000
                Cache.put(key, timeout_ms, now + @configuration_ttl_ms)
                {:ok, timeout_ms}

              _ ->
                {:error, :unexpected_response}
            end

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  # A function named by ARN has colons in it, which are left as they are:
  # AWS signs the path encoded twice where the signer encodes it once, so
  # an encoded colon would be signed as the wrong thing, while a plain one
  # comes out encoded the same way on both sides.
  defp function_path(config, suffix) do
    name = URI.encode(Map.fetch!(config, :function), &(URI.char_unreserved?(&1) or &1 == ?:))
    "/#{@api_version}/functions/#{name}/#{suffix}"
  end

  defp qualifier_query(config) do
    case config[:qualifier] do
      nil -> []
      qualifier -> [{"Qualifier", qualifier}]
    end
  end

  # --- Results ---

  defp interpret_result({:ok, response}, _data) do
    case Aws.header(response, "x-amz-function-error") do
      nil ->
        {:ok, false, nil, nil}

      _kind ->
        payload =
          case Jason.decode(response.body) do
            {:ok, map} when is_map(map) -> map
            _ -> %{}
          end

        error = if timed_out?(payload), do: "timeout", else: "function_error"
        logs = log_tail(response) || error_detail(payload)
        {:ok, false, error, logs}
    end
  end

  # Turned down after the launch had already been accepted, which Lambda
  # doesn't do, but if it did this is what it said.
  defp interpret_result({:error, {:api, _status, type, message}}, _data) do
    {:ok, false, "invoke_failed:#{type || "unknown"}",
     message && truncate_bytes(message, @log_max_bytes)}
  end

  # The request never reached Lambda, after the launch had stopped waiting
  # to hear, so nothing was started.
  defp interpret_result({:error, {:not_sent, reason}}, _data),
    do: {:ok, false, "launch_request_failed", describe_unsent(reason)}

  # The held request was lost - the connection dropped, say - which says
  # nothing about the function, so fall back to what the deadline does.
  defp interpret_result({:error, _reason}, data), do: assume_by_deadline(data)

  defp assume_by_deadline(data) do
    deadline = Map.get(data, :expires_at, 0) + @restart_grace_ms

    if System.os_time(:millisecond) < deadline do
      {:ok, true}
    else
      {:ok, false, nil, nil}
    end
  end

  # Lambda reports a timeout as an ordinary function error; newer
  # runtimes give it a type, older ones only the message.
  defp timed_out?(%{"errorType" => "Sandbox.Timedout"}), do: true

  defp timed_out?(%{"errorMessage" => message}) when is_binary(message),
    do: message =~ "Task timed out"

  defp timed_out?(_payload), do: false

  defp error_detail(payload) do
    case {payload["errorType"], payload["errorMessage"]} do
      {nil, nil} -> nil
      {type, nil} -> type
      {nil, message} -> truncate_bytes(message, @log_max_bytes)
      {type, message} -> truncate_bytes("#{type}: #{message}", @log_max_bytes)
    end
  end

  # The last 4KB of the function's log, which Lambda returns with a
  # synchronous invocation that asks for it.
  defp log_tail(response) do
    with encoded when is_binary(encoded) <- Aws.header(response, "x-amz-log-result"),
         {:ok, text} <- Base.decode64(encoded),
         text when text != "" <- String.trim(text) do
      text
      |> String.split("\n")
      |> Enum.take(-@log_tail_lines)
      |> Enum.join("\n")
      |> truncate_bytes(@log_max_bytes)
    else
      _ -> nil
    end
  end

  # --- Errors ---

  defp normalize_launch_error(:credentials_missing), do: {"launch_credentials_missing", nil}

  defp normalize_launch_error({:assume_role, code, message}),
    do: {"launch_assume_role_failed", if(message, do: "#{code}: #{message}", else: code)}

  defp normalize_launch_error({:request_failed, _exception}), do: {"launch_request_failed", nil}

  defp normalize_launch_error({:not_sent, reason}),
    do: {"launch_request_failed", describe_unsent(reason)}

  defp normalize_launch_error({:timeout_too_short, timeout_ms}) do
    {"launch_function_timeout_too_short",
     "The function's timeout is #{div(timeout_ms, 1000)}s; " <>
       "a worker needs at least #{div(@min_timeout_ms, 1000)}s"}
  end

  defp normalize_launch_error(:unexpected_response), do: {"launch_api_error", nil}
  defp normalize_launch_error(:invocation_failed), do: {"launch_invocation_failed", nil}

  defp normalize_launch_error({:api, _status, type, message}) do
    code =
      cond do
        type == "ResourceNotFoundException" ->
          "launch_function_not_found"

        type in ["TooManyRequestsException", "ThrottlingException"] ->
          "launch_throttled"

        type in ["ResourceNotReadyException", "ResourceConflictException"] ->
          "launch_function_not_ready"

        type in [
          "InvalidParameterValueException",
          "InvalidRequestContentException",
          "RequestTooLargeException",
          "UnsupportedMediaTypeException"
        ] ->
          "launch_invalid"

        is_binary(type) and
            (String.starts_with?(type, "EC2") or String.starts_with?(type, "ENI") or
               String.starts_with?(type, "Subnet") or
               String.starts_with?(type, "InvalidSubnet") or
               String.starts_with?(type, "InvalidSecurityGroup")) ->
          "launch_network_error"

        type == "AccessDeniedException" ->
          "launch_forbidden"

        Aws.unauthorized_error?(type) ->
          "launch_unauthorized"

        type == "ServiceException" ->
          "launch_server_error"

        true ->
          "launch_api_error"
      end

    {code, message}
  end

  defp normalize_launch_error(_reason), do: {"launch_api_error", nil}

  defp describe_unsent(%{__exception__: true} = exception),
    do: truncate_bytes(Exception.message(exception), @log_max_bytes)

  defp describe_unsent(_reason), do: nil

  # --- Helpers ---

  defp build_conn(config), do: Aws.build_conn(config, &"https://lambda.#{&1}.amazonaws.com")

  defp lookup(invocation_id) do
    case Registry.lookup(@registry, invocation_id) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  defp generate_id, do: Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)

  def registry, do: @registry
  def supervisor, do: @supervisor
end

defmodule Coflux.LambdaLauncher.Invocation do
  @moduledoc """
  One synchronous invocation, held open until the function returns, and
  its result kept for a while afterwards for the launcher's polls to
  find.
  """

  use GenServer, restart: :temporary

  # How long a finished invocation's result is kept. Polls come every few
  # seconds, so this only has to outlast a slow one.
  @retention_ms 600_000

  def start_link({invocation_id, request, on_exit}) do
    GenServer.start_link(__MODULE__, {request, on_exit},
      name: {:via, Registry, {Coflux.LambdaLauncher.Registry, invocation_id}}
    )
  end

  @doc """
  Waits up to `timeout_ms` for the invocation to finish: `{:done,
  result}` if it did, `:running` if it's still going.
  """
  def await(pid, timeout_ms), do: GenServer.call(pid, {:await, timeout_ms}, timeout_ms + 5_000)

  @doc "`:running`, or `{:done, result}` with the request's result."
  def status(pid), do: GenServer.call(pid, :status)

  @impl true
  def init({request, on_exit}) do
    task = Task.Supervisor.async_nolink(Coflux.LauncherSupervisor, request)
    {:ok, %{task_ref: task.ref, result: nil, waiters: %{}, on_exit: on_exit}}
  end

  @impl true
  def handle_call({:await, timeout_ms}, from, %{result: nil} = state) do
    ref = make_ref()
    Process.send_after(self(), {:await_timeout, ref}, timeout_ms)
    {:noreply, put_in(state.waiters[ref], from)}
  end

  def handle_call({:await, _timeout_ms}, _from, state),
    do: {:reply, {:done, state.result}, state}

  def handle_call(:status, _from, %{result: nil} = state), do: {:reply, :running, state}
  def handle_call(:status, _from, state), do: {:reply, {:done, state.result}, state}

  @impl true
  def handle_info({ref, result}, %{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    finish(state, result)
  end

  # The request crashed rather than failing, which is how the HTTP client
  # says it had no connection free to send it on.
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task_ref: ref} = state) do
    exception =
      case reason do
        {%{__exception__: true} = exception, _stacktrace} -> exception
        _ -> nil
      end

    finish(state, {:error, {:not_sent, exception}})
  end

  def handle_info({:await_timeout, ref}, state) do
    case Map.pop(state.waiters, ref) do
      {nil, _waiters} ->
        {:noreply, state}

      {from, waiters} ->
        GenServer.reply(from, :running)
        {:noreply, %{state | waiters: waiters}}
    end
  end

  def handle_info(:expire, state), do: {:stop, :normal, state}

  defp finish(state, result) do
    Enum.each(state.waiters, fn {_ref, from} -> GenServer.reply(from, {:done, result}) end)

    # Only worth saying once the launch has been answered: one turned down
    # has nothing to poll.
    if state.on_exit && map_size(state.waiters) == 0, do: state.on_exit.()

    Process.send_after(self(), :expire, @retention_ms)
    {:noreply, %{state | result: result, waiters: %{}}}
  end
end

defmodule Coflux.LambdaLauncher.Cache do
  @moduledoc """
  Holds functions' configuration for a short while.

  The process only owns the table: launcher tasks read and write it
  directly. Without the process (as under test) nothing is cached.
  """

  use GenServer

  @table :coflux_lambda_functions

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get(key, now) do
    if table?() do
      case :ets.lookup(@table, key) do
        [{^key, expires_at, value}] when expires_at > now -> {:ok, value}
        _ -> :none
      end
    else
      :none
    end
  end

  def put(key, value, expires_at) do
    if table?() do
      :ets.insert(@table, {key, expires_at, value})
    end

    :ok
  end

  defp table?, do: :ets.whereis(@table) != :undefined

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    {:ok, nil}
  end
end
