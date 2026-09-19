defmodule Coflux.Topics.WorkflowModelTest do
  use ExUnit.Case, async: true

  alias Coflux.Events.{
    CompletionRecorded,
    ExecutionAssigned,
    ExecutionScheduled,
    ManifestRegistered,
    ModuleArchived,
    RunCreated,
    RunOutcome
  }

  alias Coflux.Topics.Workflow.Model

  @workflow %{
    parameters: [{"x", nil, nil}],
    instruction: "Do it",
    wait_for: [],
    cache: nil,
    defer: nil,
    delay: 5,
    retries: %{limit: 2, backoff_min: 0, backoff_max: 0},
    recurrent: false,
    timeout: 0,
    requires: %{},
    memo: false,
    streams: nil,
    concurrency: nil
  }

  defp identity(run, execution) do
    %{
      execution: execution,
      run: run,
      step: 1,
      attempt: 1,
      workspace: "W1",
      module: "m",
      target: "main",
      type: :workflow,
      root_module: "m",
      root_target: "main"
    }
  end

  defp registered,
    do: %ManifestRegistered{workspace: "W1", module: "m", workflows: %{"main" => @workflow}}

  defp created(run, created_at, type \\ :workflow),
    do: %RunCreated{
      run: run,
      workspace: "W1",
      root_module: "m",
      root_target: "main",
      type: type,
      created_at: created_at,
      created_by: %{type: "user", external_id: "U1"}
    }

  defp outcome(run, outcome),
    do: %RunOutcome{
      run: run,
      workspace: "W1",
      root_module: "m",
      root_target: "main",
      outcome: outcome
    }

  defp scheduled(run, execution),
    do:
      struct(
        ExecutionScheduled,
        Map.merge(identity(run, execution), %{
          execute_after: nil,
          created_at: 1,
          created_by: nil,
          requires: %{}
        })
      )

  defp assigned(run, execution),
    do: struct(ExecutionAssigned, Map.put(identity(run, execution), :assigned_at, 2))

  defp completed(run, execution),
    do:
      struct(
        CompletionRecorded,
        Map.merge(identity(run, execution), %{kind: :succeeded, successor: nil, completed_at: 3})
      )

  defp fold(events, max_runs \\ 50),
    do: elem(Model.fold(Model.new("m", "main", max_runs), events), 0)

  test "an unregistered workflow with no runs projects to nothing" do
    assert Model.project(fold([])) == %{
             parameters: nil,
             instruction: nil,
             configuration: nil,
             runs: %{}
           }
  end

  test "the manifest supplies the definition" do
    value = Model.project(fold([registered()]))
    assert value.parameters == [%{name: "x", default: nil, annotation: nil}]
    assert value.instruction == "Do it"
    assert value.configuration.delay == 5
    assert value.configuration.retries == %{limit: 2, backoffMin: 0, backoffMax: 0}
    assert value.configuration.requires == %{}
  end

  test "archiving the module drops the definition but not the runs" do
    model = fold([registered(), created("R1", 10), %ModuleArchived{workspace: "W1", module: "m"}])
    value = Model.project(model)
    assert value.parameters == nil
    assert value.configuration == nil
    assert Map.keys(value.runs) == ["R1"]
  end

  test "a run appears on creation, goes queued then running, and settles with its outcome" do
    events = [created("R1", 10)]

    assert Model.project(fold(events)).runs == %{
             "R1" => %{
               id: "R1",
               createdAt: 10,
               createdBy: %{type: "user", externalId: "U1"},
               outcome: nil,
               active: nil
             }
           }

    events = events ++ [scheduled("R1", "R1:1:1")]
    assert Model.project(fold(events)).runs["R1"].active == "queued"

    events = events ++ [assigned("R1", "R1:1:1")]
    assert Model.project(fold(events)).runs["R1"].active == "running"

    events = events ++ [completed("R1", "R1:1:1"), outcome("R1", :completed)]
    run = Model.project(fold(events)).runs["R1"]
    assert run.active == nil
    assert run.outcome == "completed"
  end

  test "only the most recent runs are kept" do
    events = Enum.map(1..4, &created("R#{&1}", &1 * 10))
    assert Map.keys(Model.project(fold(events, 3)).runs) |> Enum.sort() == ["R2", "R3", "R4"]
  end

  test "an outcome for an evicted run, and a run of another type, are ignored" do
    model =
      fold(
        [created("R1", 10), created("R2", 20), outcome("R1", :errored), created("T1", 30, :task)],
        1
      )

    assert Map.keys(Model.project(model).runs) == ["R2"]
  end

  test "facts are idempotent" do
    events = [
      registered(),
      created("R1", 10),
      scheduled("R1", "R1:1:1"),
      assigned("R1", "R1:1:1")
    ]

    assert fold(events) ==
             fold(events ++ [created("R1", 10), scheduled("R1", "R1:1:1"), registered()])
  end
end
