defmodule Coflux.RunTopicsTest do
  @moduledoc """
  The run-family topic modules, driven by a real project server: each is
  initialised as Topical would initialise it, then fed the events the
  server delivers, so the whole path from emit to wire shape runs.
  """

  # Not async: the data directory is global.
  use ExUnit.Case, async: false

  alias Coflux.Orchestration
  alias Coflux.Topics.{Run, RunExecution, RunGroup, RunSteps}

  setup do
    dir =
      Path.join(
        System.tmp_dir!(),
        "coflux-run-topics-test-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    previous = :persistent_term.get(:coflux_data_dir, nil)
    :persistent_term.put(:coflux_data_dir, dir)
    project = "p#{System.unique_integer([:positive])}"
    start_supervised!(Coflux.Orchestration.Supervisor)

    on_exit(fn ->
      if previous do
        :persistent_term.put(:coflux_data_dir, previous)
      else
        :persistent_term.erase(:coflux_data_dir)
      end

      File.rm_rf!(dir)
    end)

    {:ok, project: project}
  end

  test "the run topics load and stay in sync through a grouped fan-out", %{project: project} do
    {:ok, _workspace_id, ws} = Orchestration.create_workspace(project, "default", nil)
    {:ok, token} = Orchestration.create_session(project, ws)
    {:ok, session, []} = Orchestration.resume_session(project, token, ws, self())

    :ok =
      Orchestration.declare_targets(
        project,
        session,
        %{"test" => %{workflow: ["main"], task: ["item", "leaf"]}},
        8
      )

    {:ok, run, 1, root} =
      Orchestration.start_run(project, "test", "main", :workflow, [], nil, workspace: ws)

    :ok = Orchestration.register_group(project, root, 0, "batch", 0)

    items =
      for x <- 1..3 do
        {:ok, _, _, item, _} =
          Orchestration.schedule_step(project, root, "test", "item", :task, [{:raw, x, []}],
            group_id: 0
          )

        item
      end

    {:ok, _, _, leaf, _} =
      Orchestration.schedule_step(project, root, "test", "leaf", :task, [], [])

    params = %{project: project, run_id: run, workspace_id: ws}
    {:ok, run_topic} = Run.init(params)
    {:ok, steps_topic} = RunSteps.init(params)
    {:ok, execution_topic} = RunExecution.init(Map.put(params, :execution_id, Enum.at(items, 1)))

    {:ok, group_topic} =
      RunGroup.init(Map.merge(params, %{execution_id: root, group_id: "0"}))

    assert Map.keys(run_topic.value.steps) |> length() == 3
    assert group_topic.value.order |> length() == 3
    assert Map.keys(steps_topic.value.steps) |> length() == 5

    [first, second, third] = items
    :ok = Orchestration.record_result(project, second, {:value, {:raw, "b", []}})
    :ok = Orchestration.notify_terminated(project, [second])

    :ok =
      Orchestration.record_result(
        project,
        first,
        {:error, "ValueError", "boom", [], false}
      )

    :ok = Orchestration.notify_terminated(project, [first])
    :ok = Orchestration.record_result(project, third, {:value, {:raw, "c", []}})
    :ok = Orchestration.notify_terminated(project, [third])
    :ok = Orchestration.record_result(project, leaf, {:value, {:raw, "l", []}})
    :ok = Orchestration.notify_terminated(project, [leaf])
    :ok = Orchestration.record_result(project, root, {:value, {:raw, "done", []}})
    :ok = Orchestration.notify_terminated(project, [root])

    # Sync everything the server delivered through each topic.
    {run_topic, steps_topic, execution_topic, group_topic} =
      drain({run_topic, steps_topic, execution_topic, group_topic})

    root_execution = run_topic.value.steps["#{run}:1"].executions["1"]
    assert root_execution.groups["0"].members.total == 3
    assert root_execution.groups["0"].members.byStatus.completed == 2
    assert root_execution.groups["0"].members.byStatus.errored == 1
    assert root_execution.completion.kind == "succeeded"
    assert group_topic.value.members["#{run}:3"].status == "completed"
    assert execution_topic.value.root == second
    assert steps_topic.value.steps["#{run}:5"].attempts["1"].completion.kind == "succeeded"
  end

  # Every `{:topic, ref, events}` message in the mailbox, applied to
  # whichever topic it is for. The four topics hold four subscriptions.
  defp drain(topics) do
    receive do
      {:topic, _ref, _events} = message ->
        {run_topic, steps_topic, execution_topic, group_topic} = topics
        {:ok, run_topic} = Run.handle_info(message, run_topic)
        {:ok, steps_topic} = RunSteps.handle_info(message, steps_topic)
        {:ok, execution_topic} = RunExecution.handle_info(message, execution_topic)
        {:ok, group_topic} = RunGroup.handle_info(message, group_topic)
        drain({run_topic, steps_topic, execution_topic, group_topic})
    after
      0 -> topics
    end
  end
end
