defmodule Coflux.Topics.QueueModelTest do
  use ExUnit.Case, async: true

  alias Coflux.Events.{
    CompletionRecorded,
    ExecutionAssigned,
    ExecutionScheduled,
    ExecutionWaiting
  }

  alias Coflux.Topics.Queue.Model

  @identity %{
    execution: "R1:2:1",
    run: "R1",
    step: 2,
    attempt: 1,
    workspace: "W1",
    module: "m",
    target: "t",
    type: :task,
    root_module: "m",
    root_target: "main"
  }

  defp scheduled(fields \\ %{}) do
    struct(
      ExecutionScheduled,
      Map.merge(
        Map.merge(@identity, %{
          execute_after: nil,
          created_at: 10,
          created_by: nil,
          requires: %{}
        }),
        fields
      )
    )
  end

  defp assigned(at), do: struct(ExecutionAssigned, Map.put(@identity, :assigned_at, at))

  defp waiting(gates),
    do: %ExecutionWaiting{execution: "R1:2:1", run: "R1", workspace: "W1", gates: gates}

  defp completed,
    do:
      struct(
        CompletionRecorded,
        Map.merge(@identity, %{kind: :succeeded, successor: nil, completed_at: 30})
      )

  defp fold(events), do: elem(Model.fold(Model.new(), events), 0)

  test "a scheduled execution projects to a queue entry" do
    model = fold([scheduled()])

    assert Model.project(model) == %{
             "R1:2:1" => %{
               module: "m",
               target: "t",
               runId: "R1",
               stepId: "R1:2",
               stepNumber: 2,
               attempt: 1,
               executeAfter: nil,
               createdAt: 10,
               assignedAt: nil,
               dependencies: [],
               requires: %{}
             }
           }
  end

  test "assignment and gates update the entry; completion removes it" do
    gate = %{type: "execution", executionId: "R1:1:1"}
    model = fold([scheduled(), waiting([gate])])
    assert Model.project_entry(model, "R1:2:1").dependencies == [gate]

    model = fold([scheduled(), waiting([gate]), waiting([]), assigned(20)])
    entry = Model.project_entry(model, "R1:2:1")
    assert entry.dependencies == []
    assert entry.assignedAt == 20

    model = fold([scheduled(), assigned(20), completed()])
    assert Model.project(model) == %{}
    assert Model.project_entry(model, "R1:2:1") == nil
  end

  test "applying a fact twice changes nothing" do
    once = fold([scheduled(), assigned(20), waiting([])])
    twice = fold([scheduled(), assigned(20), waiting([]), scheduled(), assigned(20)])
    assert once == twice
  end

  test "an update for an execution the queue doesn't hold is ignored" do
    assert fold([assigned(20)]) == %{}
    assert fold([waiting([%{type: "catalog", path: "p", number: 1}])]) == %{}
    assert fold([scheduled(), completed(), assigned(20)]) == %{}
  end

  test "fold reports the ids that changed" do
    {_model, dirty} = Model.fold(Model.new(), [scheduled(), completed()])
    assert dirty == MapSet.new(["R1:2:1"])
  end
end
