defmodule Coflux.LambdaLauncherTest do
  @moduledoc """
  The Lambda launcher against a stubbed API: what it sends, what it makes
  of what comes back, and what it assumes when there's nothing to ask.

  An invocation is held open until the function returns, so the stub
  holds it too - until the test releases it with the response it wants.
  """
  use ExUnit.Case, async: true

  alias Coflux.LambdaLauncher

  @base_config %{
    region: "eu-west-1",
    access_key_id: "AKIATEST",
    secret_access_key: "test-secret"
  }

  @env %{"COFLUX_HOST" => "coflux.example.com", "COFLUX_SESSION" => "token"}
  @args ["myapp"]

  # The application isn't started under test, so what the launcher holds
  # invocations with is started here.
  setup_all do
    start_supervised!({Task.Supervisor, name: Coflux.LauncherSupervisor})

    start_supervised!(
      {DynamicSupervisor, name: LambdaLauncher.supervisor(), strategy: :one_for_one}
    )

    start_supervised!({Registry, keys: :unique, name: LambdaLauncher.registry()})
    start_supervised!(LambdaLauncher.Cache)
    :ok
  end

  describe "launching" do
    test "invokes the function synchronously, with the worker in the payload" do
      config = config(qualifier: "live")
      task = Task.async(fn -> LambdaLauncher.launch(@env, @args, config) end)

      assert_receive {:configuration, configuration}, 2_000
      assert configuration.method == :get
      assert configuration.query == "Qualifier=live"

      assert_receive {:invoked, invoke, _stub}, 2_000
      assert invoke.method == :post
      assert invoke.path == "/2015-03-31/functions/#{config.function}/invocations"
      assert invoke.query == "Qualifier=live"
      assert invoke.headers["x-amz-invocation-type"] == ["RequestResponse"]
      assert invoke.headers["x-amz-log-type"] == ["Tail"]
      assert [authorization] = invoke.headers["authorization"]
      assert authorization =~ "/eu-west-1/lambda/aws4_request"
      assert Jason.decode!(invoke.body) == %{"env" => @env, "args" => @args}

      assert {:ok, data} = Task.await(task)
      assert data.function == config.function
      assert data.qualifier == "live"
      assert data.timeout_ms == 60_000
      assert data.expires_at == data.launched_at + 60_000
    end

    test "names a function given by ARN with its colons as they are" do
      arn = "arn:aws:lambda:eu-west-1:123456789012:function:worker-#{unique()}"
      config = config(function: arn)
      task = Task.async(fn -> LambdaLauncher.launch(@env, @args, config) end)

      assert_receive {:invoked, invoke, _stub}, 2_000
      assert invoke.path == "/2015-03-31/functions/#{arn}/invocations"
      assert {:ok, _data} = Task.await(task)
    end

    test "a function that doesn't exist fails the launch, with Lambda's message" do
      config = config(configuration: {404, "ResourceNotFoundException", "Function not found"})

      assert LambdaLauncher.launch(@env, @args, config) ==
               {:error, "launch_function_not_found", "Function not found"}
    end

    test "a throttled invocation fails the launch as throttled" do
      config = config(invoke: {429, "TooManyRequestsException", "Rate exceeded"})

      assert LambdaLauncher.launch(@env, @args, config) ==
               {:error, "launch_throttled", "Rate exceeded"}
    end

    test "a function with too short a timeout for a worker to do anything fails the launch" do
      config = config(timeout: 30)

      assert LambdaLauncher.launch(@env, @args, config) ==
               {:error, "launch_function_timeout_too_short",
                "The function's timeout is 30s; a worker needs at least 60s"}

      refute_received {:invoked, _, _}
    end

    test "a function's configuration is reused only with the same credentials" do
      function = "worker-#{unique()}"

      for access_key_id <- ["AKIAONE", "AKIAONE", "AKIATWO"] do
        config = config(function: function, access_key_id: access_key_id)
        task = Task.async(fn -> LambdaLauncher.launch(@env, @args, config) end)
        assert_receive {:invoked, _invoke, _stub}, 2_000
        {:ok, _data} = Task.await(task)
      end

      assert_received {:configuration, _}
      assert_received {:configuration, _}
      refute_received {:configuration, _}
    end

    test "credentials that aren't accepted fail the launch as unauthorized" do
      config =
        config(
          configuration: {403, "UnrecognizedClientException", "The security token is invalid"}
        )

      assert {:error, "launch_unauthorized", _message} =
               LambdaLauncher.launch(@env, @args, config)
    end
  end

  describe "polling" do
    test "an invocation still held is running" do
      {data, config, _stub} = launched()
      assert LambdaLauncher.poll(data, config) == {:ok, true}
    end

    test "a function that returned is a worker that left cleanly" do
      {data, config, stub} = launched()
      release(stub, {200, [], %{"exitCode" => 0}})

      assert eventually(fn -> LambdaLauncher.poll(data, config) end) == {:ok, false, nil, nil}
    end

    test "a function error is the worker's error, with the tail of its log" do
      {data, config, stub} = launched()
      log = "START RequestId: abc\nworker exited\nEND RequestId: abc"

      release(
        stub,
        {200,
         [
           {"x-amz-function-error", "Unhandled"},
           {"x-amz-log-result", Base.encode64(log)}
         ], %{"errorType" => "RuntimeError", "errorMessage" => "worker exited with code 1"}}
      )

      assert eventually(fn -> LambdaLauncher.poll(data, config) end) ==
               {:ok, false, "function_error", log}
    end

    test "without a log, the error's own message stands in for one" do
      {data, config, stub} = launched()

      release(
        stub,
        {200, [{"x-amz-function-error", "Unhandled"}],
         %{"errorType" => "RuntimeError", "errorMessage" => "worker exited with code 1"}}
      )

      assert eventually(fn -> LambdaLauncher.poll(data, config) end) ==
               {:ok, false, "function_error", "RuntimeError: worker exited with code 1"}
    end

    test "a function that ran out of time is reported as timed out" do
      {data, config, stub} = launched()

      release(
        stub,
        {200, [{"x-amz-function-error", "Unhandled"}],
         %{
           "errorType" => "Sandbox.Timedout",
           "errorMessage" => "Task timed out after 60.00 seconds"
         }}
      )

      assert {:ok, false, "timeout", _logs} =
               eventually(fn -> LambdaLauncher.poll(data, config) end)
    end

    test "says so as soon as the function returns, rather than waiting to be asked" do
      test = self()
      config = config()
      opts = %{on_exit: fn -> send(test, :exited) end}
      task = Task.async(fn -> LambdaLauncher.launch(@env, @args, config, opts) end)
      assert_receive {:invoked, _invoke, stub}, 2_000
      {:ok, _data} = Task.await(task)

      refute_received :exited
      release(stub, {200, [], %{"exitCode" => 0}})
      assert_receive :exited, 2_000
    end

    test "a request that never reached Lambda is a failed launch, however late it's known" do
      {data, config, stub} = launched()
      send(stub, {:fail, :econnrefused})

      assert {:ok, false, "launch_request_failed", _detail} =
               eventually(fn -> LambdaLauncher.poll(data, config) end)
    end

    test "a request lost once it may have reached Lambda is assumed running" do
      {data, config, stub} = launched()
      send(stub, {:fail, :closed})

      # Give the failure time to land, then it's still assumed running.
      Process.sleep(100)
      assert LambdaLauncher.poll(data, config) == {:ok, true}
    end

    test "an invocation that isn't held here is assumed running until its deadline" do
      now = System.os_time(:millisecond)
      config = config()

      running = %{invocation_id: "unknown", expires_at: now + 10_000}
      assert LambdaLauncher.poll(running, config) == {:ok, true}

      # Past its timeout, and the grace after it.
      gone = %{invocation_id: "unknown", expires_at: now - 120_000}
      assert LambdaLauncher.poll(gone, config) == {:ok, false, nil, nil}
    end
  end

  test "stopping asks nothing of Lambda, which has no way to" do
    {data, config, _stub} = launched()
    assert LambdaLauncher.stop(data, config) == :ok
    refute_receive {:invoked, _, _}
    assert LambdaLauncher.poll(data, config) == {:ok, true}
  end

  # --- Helpers ---

  defp launched do
    config = config()
    task = Task.async(fn -> LambdaLauncher.launch(@env, @args, config) end)
    assert_receive {:invoked, _invoke, stub}, 2_000
    {:ok, data} = Task.await(task)
    {data, config, stub}
  end

  defp release(stub, response), do: send(stub, {:respond, response})

  # A released invocation's result lands asynchronously.
  defp eventually(fun, attempts \\ 50) do
    case fun.() do
      {:ok, true} when attempts > 0 ->
        Process.sleep(20)
        eventually(fun, attempts - 1)

      result ->
        result
    end
  end

  defp unique, do: System.unique_integer([:positive])

  # A config pointed at a stub of the Lambda API. `configuration` and
  # `invoke` are {status, error_type, message} to fail those calls with;
  # otherwise the configuration says a `timeout` (default 60s), and an
  # invocation is held until the test sends the stub its response, or a
  # transport error's reason to fail with.
  defp config(opts \\ []) do
    test = self()
    configuration_error = Keyword.get(opts, :configuration)
    invoke_error = Keyword.get(opts, :invoke)

    adapter = fn request ->
      summary = %{
        method: request.method,
        path: request.url.path,
        query: request.url.query,
        headers: request.headers,
        body: IO.iodata_to_binary(request.body || "")
      }

      cond do
        String.ends_with?(summary.path, "/configuration") ->
          send(test, {:configuration, summary})

          case configuration_error do
            nil -> {request, response(200, [], %{"Timeout" => Keyword.get(opts, :timeout, 60)})}
            {status, type, message} -> {request, error(status, type, message)}
          end

        String.ends_with?(summary.path, "/invocations") ->
          send(test, {:invoked, summary, self()})

          case invoke_error do
            nil ->
              receive do
                {:respond, {status, headers, body}} ->
                  {request, response(status, headers, body)}

                {:fail, reason} ->
                  {request, %Req.TransportError{reason: reason}}
              after
                2_000 -> {request, response(200, [], %{})}
              end

            {status, type, message} ->
              {request, error(status, type, message)}
          end
      end
    end

    @base_config
    |> Map.put(:access_key_id, Keyword.get(opts, :access_key_id, @base_config.access_key_id))
    |> Map.put(:function, Keyword.get(opts, :function, "worker-#{unique()}"))
    |> Map.put(:req_options, adapter: adapter)
    |> then(&if q = opts[:qualifier], do: Map.put(&1, :qualifier, q), else: &1)
  end

  defp response(status, headers, body) do
    Req.Response.new(status: status, headers: headers, body: Jason.encode!(body))
  end

  defp error(status, type, message) do
    response(status, [{"x-amzn-errortype", type}], %{"Type" => "User", "message" => message})
  end
end
