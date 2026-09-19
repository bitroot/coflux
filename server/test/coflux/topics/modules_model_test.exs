defmodule Coflux.Topics.ModulesModelTest do
  use ExUnit.Case, async: true

  alias Coflux.Events.{
    CompletionRecorded,
    ExecutionAssigned,
    ExecutionScheduled,
    ManifestRegistered,
    ModuleArchived
  }

  alias Coflux.Topics.Modules.Model

  @workflow %{parameters: [], instruction: nil}

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

  defp registered(module \\ "m", workflows \\ %{"main" => @workflow}),
    do: %ManifestRegistered{workspace: "W1", module: module, workflows: workflows}

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

  defp fold(events), do: elem(Model.fold(Model.new(), events), 0)

  test "registered modules project their workflows with no active runs" do
    assert Model.project(fold([registered()])) == %{
             "m" => %{workflows: %{"main" => %{activeRuns: %{}}}}
           }
  end

  test "a run is queued until any execution is assigned, and gone once all complete" do
    events = [registered(), scheduled("R1", "R1:1:1")]

    assert Model.project_module(fold(events), "m").workflows["main"].activeRuns == %{
             "R1" => "queued"
           }

    events = events ++ [assigned("R1", "R1:1:1"), scheduled("R1", "R1:2:1")]

    assert Model.project_module(fold(events), "m").workflows["main"].activeRuns == %{
             "R1" => "running"
           }

    # Only the unassigned execution is left, so the run is queued again.
    events = events ++ [completed("R1", "R1:1:1")]

    assert Model.project_module(fold(events), "m").workflows["main"].activeRuns == %{
             "R1" => "queued"
           }

    events = events ++ [completed("R1", "R1:2:1")]
    assert Model.project_module(fold(events), "m").workflows["main"].activeRuns == %{}
  end

  test "activity is tracked before the manifest arrives, and shown once it does" do
    model = fold([scheduled("R1", "R1:1:1")])
    assert Model.project(model) == %{}

    model = fold([scheduled("R1", "R1:1:1"), registered()])
    assert Model.project_module(model, "m").workflows["main"].activeRuns == %{"R1" => "queued"}
  end

  test "archiving removes the module" do
    model =
      fold([
        registered(),
        scheduled("R1", "R1:1:1"),
        %ModuleArchived{workspace: "W1", module: "m"}
      ])

    assert Model.project(model) == %{}
    assert Model.project_module(model, "m") == nil
  end

  test "a late scheduled fact can't undo an assigned one, and facts are idempotent" do
    once = fold([registered(), scheduled("R1", "R1:1:1"), assigned("R1", "R1:1:1")])

    twice =
      fold([
        registered(),
        scheduled("R1", "R1:1:1"),
        assigned("R1", "R1:1:1"),
        scheduled("R1", "R1:1:1"),
        registered()
      ])

    assert once == twice
    assert Model.project_module(once, "m").workflows["main"].activeRuns == %{"R1" => "running"}
  end

  test "fold reports the modules that changed" do
    {_, dirty} = Model.fold(Model.new(), [registered("a", %{}), scheduled("R1", "R1:1:1")])
    assert dirty == MapSet.new(["a", "m"])
  end
end
