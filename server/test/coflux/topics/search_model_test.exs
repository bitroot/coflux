defmodule Coflux.Topics.SearchModelTest do
  use ExUnit.Case, async: true

  alias Coflux.Events.{ExecutionScheduled, ManifestRegistered, ModuleArchived}
  alias Coflux.Topics.Search.Model

  defp registered(module, names),
    do: %ManifestRegistered{
      workspace: "W1",
      module: module,
      workflows: Map.new(names, &{&1, %{}})
    }

  defp scheduled(module, target, type, run, step, attempt) do
    %ExecutionScheduled{
      execution: "#{run}:#{step}:#{attempt}",
      run: run,
      step: step,
      attempt: attempt,
      workspace: "W1",
      module: module,
      target: target,
      type: type,
      root_module: module,
      root_target: "main",
      execute_after: nil,
      created_at: 1,
      created_by: nil,
      requires: %{}
    }
  end

  test "registered workflows are known before they run" do
    assert Model.fold(Model.new(), [registered("m", ["a", "b"])]) == %{
             "m" => %{"a" => {:workflow, nil}, "b" => {:workflow, nil}}
           }
  end

  test "a scheduled execution records the latest run of its target" do
    targets =
      Model.fold(Model.new(), [
        scheduled("m", "t", :task, "R1", 2, 1),
        scheduled("m", "t", :task, "R2", 3, 1)
      ])

    assert targets == %{"m" => %{"t" => {:task, {"R2", 3, 1}}}}
  end

  test "a registration doesn't forget a workflow's latest run" do
    events = [scheduled("m", "a", :workflow, "R1", 1, 1), registered("m", ["a", "b"])]

    assert Model.fold(Model.new(), events) == %{
             "m" => %{"a" => {:workflow, {"R1", 1, 1}}, "b" => {:workflow, nil}}
           }
  end

  test "archiving forgets workflows that never ran" do
    events = [
      registered("m", ["a", "b"]),
      scheduled("m", "a", :workflow, "R1", 1, 1),
      %ModuleArchived{workspace: "W1", module: "m"}
    ]

    assert Model.fold(Model.new(), events) == %{"m" => %{"a" => {:workflow, {"R1", 1, 1}}}}

    assert Model.fold(Model.new(), [
             registered("m", ["a"]),
             %ModuleArchived{workspace: "W1", module: "m"}
           ]) ==
             %{}
  end
end
