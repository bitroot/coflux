defmodule Coflux.EcsLauncherTest do
  @moduledoc """
  What a failed ECS worker reports.

  ECS doesn't serve container output through its API, so the launcher
  reads CloudWatch when it can and falls back to what ECS itself said
  when it can't. Everything here is about that fallback holding: a worker
  that died is recorded either way, and no failure to read logs is
  allowed to become a failed poll.
  """
  use ExUnit.Case, async: true

  @config %{
    region: "eu-west-1",
    cluster: "coflux",
    task_definition: "coflux-worker",
    access_key_id: "AKIATEST",
    secret_access_key: "test-secret"
  }

  @data %{
    task_arn: "arn:aws:ecs:eu-west-1:123456789012:task/coflux/abc123",
    cluster: "coflux",
    container_name: "worker"
  }

  describe "a worker whose container exited" do
    test "reports the tail of its log stream when the task definition logs to CloudWatch" do
      {:ok, false, error, logs} = poll(stub())

      assert error == "exit_code:1"
      assert logs == "Usage:\n  coflux worker\nunknown flag: --all-modules"
    end

    test "asks for the stream the awslogs driver would have written to" do
      poll(stub())

      assert_receive {:logs_request, body}

      assert body["logGroupName"] == "/coflux/worker"
      assert body["logStreamName"] == "worker/worker/abc123"
      assert body["startFromHead"] == false
    end

    test "signs the log request for the region the logs are in, not the cluster's" do
      poll(stub(log_options: %{"awslogs-region" => "us-east-1"}))

      assert_receive {:logs_url, url}
      assert url == "https://logs.us-east-1.amazonaws.com"
    end
  end

  describe "falling back when the logs can't be read" do
    test "the role is not allowed to read them" do
      {:ok, false, error, logs} =
        poll(stub(logs_response: {400, %{"__type" => "AccessDeniedException"}}))

      assert error == "exit_code:1"
      assert logs =~ "Essential container in task exited"
      assert logs =~ "exit code 1"
    end

    test "CloudWatch is unreachable" do
      {:ok, false, _error, logs} = poll(stub(logs_response: :unreachable))
      assert logs =~ "Essential container in task exited"
    end

    test "the task definition has no log configuration" do
      {:ok, false, _error, logs} = poll(stub(log_driver: nil))
      assert logs =~ "Essential container in task exited"
    end

    test "it logs somewhere other than CloudWatch" do
      {:ok, false, _error, logs} = poll(stub(log_driver: "awsfirelens"))
      assert logs =~ "Essential container in task exited"
    end

    test "there is no stream prefix to build a stream name from" do
      {:ok, false, _error, logs} = poll(stub(log_options: %{"awslogs-stream-prefix" => nil}))
      assert logs =~ "Essential container in task exited"
    end

    test "the task definition can't be described" do
      {:ok, false, _error, logs} =
        poll(stub(definition_response: {400, %{"__type" => "AccessDeniedException"}}))

      assert logs =~ "Essential container in task exited"
    end

    test "the events haven't been delivered yet" do
      {:ok, false, _error, logs} = poll(stub(events: []))
      assert logs =~ "Essential container in task exited"
    end

    test "the container's own reason is included, since that's where anything specific is" do
      {:ok, false, error, logs} =
        poll(stub(log_driver: nil, container_reason: "OutOfMemoryError: container killed"))

      assert error == "oom_killed"
      assert logs =~ "OutOfMemoryError: container killed"
    end
  end

  describe "a worker that stopped for a reason that isn't a failure" do
    test "reports no error and no logs, and never reads any" do
      {:ok, false, error, logs} = poll(stub(stop_code: "UserInitiated"))

      assert error == nil
      assert logs == nil
      refute_receive {:logs_request, _}
    end
  end

  # --- Helpers ---

  defp poll(adapter) do
    Coflux.EcsLauncher.poll(@data, Map.put(@config, :req_options, adapter: adapter))
  end

  # A stubbed ECS/CloudWatch pair. Routes on the x-amz-target header, so
  # one adapter answers all three calls a failing poll makes.
  defp stub(opts \\ []) do
    test = self()

    stop_code = Keyword.get(opts, :stop_code, "EssentialContainerExited")
    log_driver = Keyword.get(opts, :log_driver, "awslogs")
    container_reason = Keyword.get(opts, :container_reason)
    definition_response = Keyword.get(opts, :definition_response)
    logs_response = Keyword.get(opts, :logs_response)
    events = Keyword.get(opts, :events, default_events())

    log_options =
      %{"awslogs-group" => "/coflux/worker", "awslogs-stream-prefix" => "worker"}
      |> Map.merge(Keyword.get(opts, :log_options, %{}))
      |> Enum.reject(fn {_k, v} -> is_nil(v) end)
      |> Map.new()

    fn request ->
      target = request.headers |> Map.new() |> Map.get("x-amz-target") |> List.first()
      body = Jason.decode!(request.body)

      case target do
        "AmazonEC2ContainerServiceV20141113.DescribeTasks" ->
          {request, json(200, describe_tasks(stop_code, container_reason))}

        "AmazonEC2ContainerServiceV20141113.DescribeTaskDefinition" ->
          case definition_response do
            {status, payload} -> {request, json(status, payload)}
            nil -> {request, json(200, describe_task_definition(log_driver, log_options))}
          end

        "Logs_20140328.GetLogEvents" ->
          send(test, {:logs_request, body})
          send(test, {:logs_url, to_string(request.url)})

          case logs_response do
            {status, payload} -> {request, json(status, payload)}
            :unreachable -> {request, %Req.TransportError{reason: :econnrefused}}
            nil -> {request, json(200, %{"events" => events})}
          end
      end
    end
  end

  defp default_events do
    [
      %{"message" => "Usage:"},
      %{"message" => "  coflux worker"},
      %{"message" => "unknown flag: --all-modules"}
    ]
  end

  defp describe_tasks(stop_code, container_reason) do
    container =
      %{"name" => "worker", "exitCode" => 1}
      |> then(&if container_reason, do: Map.put(&1, "reason", container_reason), else: &1)

    %{
      "tasks" => [
        %{
          "taskArn" => "arn:aws:ecs:eu-west-1:123456789012:task/coflux/abc123",
          "taskDefinitionArn" => "arn:aws:ecs:eu-west-1:123456789012:task-definition/w:1",
          "lastStatus" => "STOPPED",
          "stopCode" => stop_code,
          "stoppedReason" => "Essential container in task exited",
          "containers" => [container]
        }
      ]
    }
  end

  defp describe_task_definition(nil, _options) do
    %{"taskDefinition" => %{"containerDefinitions" => [%{"name" => "worker"}]}}
  end

  defp describe_task_definition(driver, options) do
    %{
      "taskDefinition" => %{
        "containerDefinitions" => [
          %{
            "name" => "worker",
            "logConfiguration" => %{"logDriver" => driver, "options" => options}
          }
        ]
      }
    }
  end

  defp json(status, payload) do
    Req.Response.new(status: status, body: Jason.encode!(payload))
  end
end
