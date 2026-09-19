defmodule Coflux.EventsTest do
  @moduledoc """
  Snapshot equivalence: for each subscription key, folding the snapshot taken
  at subscription time plus every event delivered since must give the same
  model as folding a fresh snapshot. This is the invariant that lets a
  topic have one code path for its initial load and its live updates, and
  it catches exactly the bug the old two-path shape produced (a field set
  one way in `init` and another way in a notification clause).

  Runs against a real project server in a temporary data directory, driven
  through `Coflux.Orchestration` the way the API and worker handlers drive
  it. The test process stands in for a worker session's connection.
  """

  # Not async: the data directory is global.
  use ExUnit.Case, async: false

  alias Coflux.Events.{CompletionRecorded, ResultRecorded}
  alias Coflux.Orchestration
  alias Coflux.Orchestration.Server.Dependencies
  alias Coflux.RunView
  alias Coflux.RunView.Sync
  alias Coflux.Topics.{Manifests, Modules, Queue, Search, Sessions, Tokens, Workflow, Workspaces}

  @workflow %{
    parameters: [{"x", nil, nil}],
    wait_for: [],
    cache: nil,
    defer: nil,
    delay: 0,
    retries: nil,
    recurrent: false,
    timeout: 0,
    requires: %{},
    memo: false,
    streams: nil,
    concurrency: nil,
    instruction: "Run main"
  }

  setup do
    dir =
      Path.join(
        System.tmp_dir!(),
        "coflux-events-test-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    previous = :persistent_term.get(:coflux_data_dir, nil)
    :persistent_term.put(:coflux_data_dir, dir)
    # Service tokens are signed with the configured secret.
    previous_secret = :persistent_term.get(:coflux_secret, nil)
    :persistent_term.put(:coflux_secret, :crypto.strong_rand_bytes(32))
    project = "p#{System.unique_integer([:positive])}"

    # The application isn't started under test; the project server needs
    # its supervisor and registry, and stops with them when the test ends.
    start_supervised!(Coflux.Orchestration.Supervisor)

    on_exit(fn ->
      if previous do
        :persistent_term.put(:coflux_data_dir, previous)
      else
        :persistent_term.erase(:coflux_data_dir)
      end

      if previous_secret do
        :persistent_term.put(:coflux_secret, previous_secret)
      else
        :persistent_term.erase(:coflux_secret)
      end

      File.rm_rf!(dir)
    end)

    {:ok, project: project}
  end

  test "snapshots are fold-equivalent to the event history", %{project: project} do
    {:ok, _workspace_id, ws} = Orchestration.create_workspace(project, "default", nil)
    :ok = Orchestration.register_manifests(project, ws, %{"test" => %{"main" => @workflow}})

    keys = [
      {{:queue, ws}, [], model(Queue.Model, &Queue.Model.new/0)},
      {{:modules, ws}, [], model(Modules.Model, &Modules.Model.new/0)},
      {{:workflow, "test", "main", ws}, [max_runs: 2],
       model(Workflow.Model, fn -> Workflow.Model.new("test", "main", 2) end)},
      {{:targets, ws}, [],
       fn s0, history -> Search.Model.fold(Search.Model.new(), s0 ++ history) end},
      {{:manifests, ws}, [], model(Manifests.Model, &Manifests.Model.new/0)},
      {{:sessions, ws}, [], model(Sessions.Model, &Sessions.Model.new/0)},
      {:workspaces, [], model(Workspaces.Model, &Workspaces.Model.new/0)},
      {:tokens, [], model(Tokens.Model, &Tokens.Model.new/0)}
    ]

    subs = Enum.map(keys, &subscribe(project, &1))

    # A worker session with one slot, offering the workflow and the task.
    {:ok, token} = Orchestration.create_session(project, ws)
    {:ok, session, []} = Orchestration.resume_session(project, token, ws, self())

    :ok =
      Orchestration.declare_targets(
        project,
        session,
        %{"test" => %{workflow: ["main"], task: ["child"]}},
        1
      )

    # The run's initial execution takes the slot.
    {:ok, run, 1, root} =
      Orchestration.start_run(project, "test", "main", :workflow, [], nil, workspace: ws)

    # The run topic: structure is folded from the snapshot, detail for what
    # is shown is fetched, and live events carry both. Compared by
    # projection, since a nested result reaches the view two ways.
    fetch = fn request ->
      {:ok, events} = Orchestration.get_run_details(project, run, request)
      events
    end

    run_view = fn s0, history ->
      view = RunView.new(s0, [ws])
      {view, _effects} = RunView.apply_all(view, history)
      view = Sync.load(view, fetch, RunView.visible_steps(view))
      {view.run, RunView.project_structure(view), RunView.project(view)}
    end

    subs = [subscribe(project, {{:run, run}, [], run_view}) | subs]

    {subs, models} = check(project, subs)
    {_run, _structure, steps} = models[{:run, run}]
    assert steps["#{run}:1"].executions["1"].assignedAt
    assert models[{:sessions, ws}][session].executing == 1
    assert models[{:sessions, ws}][session].targets == %{"test" => ["child", "main"]}
    assert models[{:queue, ws}][root].assigned_at
    assert models[{:modules, ws}].active[{"test", "main"}] == %{run => %{root => true}}
    assert models[{:workflow, "test", "main", ws}].runs[run].outcome == nil
    assert models[{:targets, ws}]["test"]["main"] == {:workflow, {run, 1, 1}}

    # A child waits for the slot; a second child also waits for the first.
    {:ok, _, 2, child, _} =
      Orchestration.schedule_step(project, root, "test", "child", :task, [], [])

    {:ok, _, 3, waiter, _} =
      Orchestration.schedule_step(
        project,
        root,
        "test",
        "child",
        :task,
        [{:raw, nil, [{:execution, child}]}],
        wait_for: [0]
      )

    {subs, models} = check(project, subs)
    assert models[{:queue, ws}][child].assigned_at == nil
    assert models[{:queue, ws}][waiter].gates == [%{type: "execution", executionId: child}]
    assert models[{:targets, ws}]["test"]["child"] == {:task, {run, 3, 1}}
    {_run, structure, steps} = models[{:run, run}]
    assert Map.keys(structure) |> Enum.sort() == ["#{run}:1", "#{run}:2", "#{run}:3"]
    assert steps["#{run}:3"].executions["1"].dependencies[child].pending

    # The root finishing frees the slot for the child. As the worker does,
    # the result is reported and then the execution's termination, which is
    # what writes the completion.
    :ok = Orchestration.record_result(project, root, {:value, {:raw, "done", []}})
    :ok = Orchestration.notify_terminated(project, [root])
    {subs, models} = check(project, subs)
    refute Map.has_key?(models[{:queue, ws}], root)
    assert models[{:queue, ws}][child].assigned_at
    assert models[{:workflow, "test", "main", ws}].runs[run].outcome == :completed

    # The child finishing resolves the waiter's dependency, and it runs.
    :ok = Orchestration.record_result(project, child, {:value, {:raw, 42, []}})
    :ok = Orchestration.notify_terminated(project, [child])
    {subs, models} = check(project, subs)
    assert models[{:queue, ws}][waiter].gates == []
    assert models[{:queue, ws}][waiter].assigned_at

    :ok = Orchestration.record_result(project, waiter, {:value, {:raw, "x", []}})
    :ok = Orchestration.notify_terminated(project, [waiter])
    {subs, models} = check(project, subs)
    assert models[{:queue, ws}] == %{}
    assert models[{:modules, ws}].active == %{}
    {_run, _structure, steps} = models[{:run, run}]
    assert steps["#{run}:3"].executions["1"].result.type == "value"
    assert steps["#{run}:3"].executions["1"].completion.kind == "succeeded"

    # More runs than the workflow keeps; a re-registration; an archive.
    for _ <- 1..3 do
      {:ok, _, 1, execution} =
        Orchestration.start_run(project, "test", "main", :workflow, [], nil, workspace: ws)

      :ok = Orchestration.record_result(project, execution, {:value, {:raw, 1, []}})
      :ok = Orchestration.notify_terminated(project, [execution])
    end

    :ok =
      Orchestration.register_manifests(project, ws, %{
        "test" => %{"main" => %{@workflow | delay: 5}, "other" => @workflow}
      })

    {subs, models} = check(project, subs)
    assert map_size(models[{:workflow, "test", "main", ws}].runs) == 2
    assert models[{:workflow, "test", "main", ws}].workflow.delay == 5
    assert models[{:targets, ws}]["test"]["other"] == {:workflow, nil}

    :ok = Orchestration.archive_module(project, ws, "test")
    {subs, models} = check(project, subs)
    assert models[{:modules, ws}].manifests == %{}
    assert models[{:manifests, ws}] == %{}

    # Workspaces and tokens.
    {:ok, _child_id, child_ws} = Orchestration.create_workspace(project, "child", ws)
    :ok = Orchestration.pause_workspace(project, ws)
    {:ok, token} = Orchestration.create_token(project, "t1", nil)
    {subs, models} = check(project, subs)
    assert models[:workspaces][ws].state == :paused
    assert models[:workspaces][child_ws] == %{name: "child", base: ws, state: :active}
    assert models[:tokens][token.external_id].name == "t1"

    :ok = Orchestration.resume_workspace(project, ws)
    {:ok, _} = Orchestration.revoke_token(project, token.id)
    {_subs, models} = check(project, subs)
    assert models[:workspaces][ws].state == :active
    assert models[:tokens] == %{}
  end

  # The claim every in-memory index in `Coflux.Orchestration.Server.State`
  # makes: it accelerates a question the database can already answer, so
  # it is never authoritative and can always be thrown away and rebuilt.
  # That is what lets a restart and an epoch rotation re-derive it rather
  # than carry it, and it is only true as long as this passes.
  test "the dependency ledgers are re-derivable from the database", %{project: project} do
    {:ok, _workspace_id, ws} = Orchestration.create_workspace(project, "default", nil)
    {:ok, token} = Orchestration.create_session(project, ws)
    {:ok, session, []} = Orchestration.resume_session(project, token, ws, self())

    :ok =
      Orchestration.declare_targets(
        project,
        session,
        %{"test" => %{workflow: ["main"], task: ["child"]}},
        1
      )

    {:ok, _run, 1, root} =
      Orchestration.start_run(project, "test", "main", :workflow, [], nil, workspace: ws)

    # A child, and a second child waiting on the first: the waiter is
    # blocked on a dependency, and with one slot something is gated.
    {:ok, _, 2, child, _} =
      Orchestration.schedule_step(project, root, "test", "child", :task, [],
        concurrency: %{limit: 1, params: [], namespace: nil}
      )

    {:ok, _, 3, _waiter, _} =
      Orchestration.schedule_step(
        project,
        root,
        "test",
        "child",
        :task,
        [{:raw, nil, [{:execution, child}]}],
        wait_for: [0]
      )

    # The root finishing frees the worker's only slot, so the child is
    # assigned and takes a concurrency permit.
    :ok = Orchestration.record_result(project, root, {:value, {:raw, "done", []}})
    :ok = Orchestration.notify_terminated(project, [root])
    {:ok, _} = Orchestration.get_workspaces(project)

    {:ok, pid} = Coflux.Orchestration.Supervisor.get_server(project)
    state = :sys.get_state(pid)

    refute Enum.empty?(state.pending_dependencies), "nothing was blocked; the test proves nothing"

    refute Enum.empty?(state.concurrency_permits),
           "nothing held a permit; the test proves nothing"

    rebuilt = Dependencies.rebuild(state)

    for field <- [
          :pending_dependencies,
          :dependency_waiters,
          :dependency_groups,
          :stream_dependency_keys,
          :concurrency_permits
        ] do
      assert Map.fetch!(rebuilt, field) == Map.fetch!(state, field),
             """
             #{field} does not survive a rebuild from the database.
             held:     #{inspect(Map.fetch!(state, field), pretty: true)}
             rebuilt:  #{inspect(Map.fetch!(rebuilt, field), pretty: true)}
             """
    end

    # `concurrency_gated` is deliberately not in that list: it is the last
    # scheduler pass's decision, not a fact about the database, and the
    # next pass recomputes it.
    assert rebuilt.concurrency_gated == %{}
  end

  # The fan-out a result gets when other runs handed off to the execution
  # that produced it. Exercised across a server restart, because the runs
  # to notify have to be resolved from the database: the server's
  # external-id cache holds only what this lifetime scheduled or assigned,
  # and a queued execution another run deferred onto is in neither.
  test "a result reaches the runs that deferred onto it, across a restart", %{project: project} do
    {:ok, _workspace_id, ws} = Orchestration.create_workspace(project, "default", nil)
    :ok = Orchestration.register_manifests(project, ws, %{"test" => %{"main" => @workflow}})

    # No worker, so neither execution is ever assigned, and the two runs
    # share a defer key - on the next tick one defers onto the other.
    runs =
      for _ <- 1..2 do
        {:ok, run, 1, _execution} =
          Orchestration.start_run(project, "test", "main", :workflow, [], nil,
            workspace: ws,
            defer: %{params: true}
          )

        run
      end

    # A call after the tick that starting the runs queued: by the time it
    # replies, the deferral has been written.
    {:ok, _} = Orchestration.get_workspaces(project)
    assert {deferring_run, leader} = deferral(project, runs)

    restart(project)

    {:ok, _events, ref} = Orchestration.subscribe(project, {:run, deferring_run}, self())
    :ok = Orchestration.cancel_execution(project, ws, leader)

    assert Enum.any?(drain(ref), &match?(%ResultRecorded{execution: ^leader}, &1)),
           "run #{deferring_run} heard nothing about the result of #{leader}, " <>
             "which it deferred onto"
  end

  # The run that deferred, and the execution it deferred onto.
  defp deferral(project, runs) do
    Enum.find_value(runs, fn run ->
      {:ok, events, ref} = Orchestration.subscribe(project, {:run, run}, self())
      Orchestration.unsubscribe(project, ref)
      drain(ref)

      Enum.find_value(events, fn
        %CompletionRecorded{kind: :deferred, successor: %{id: leader}} -> {run, leader}
        _ -> nil
      end)
    end)
  end

  # Stops the project's server so the next call starts a fresh one, with
  # everything it holds in memory rebuilt from the database.
  defp restart(project) do
    {:ok, pid} = Coflux.Orchestration.Supervisor.get_server(project)
    ref = Process.monitor(pid)
    GenServer.stop(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}
    await_new_server(project, pid)
  end

  # The registry drops the dead entry asynchronously, so the first lookup
  # after the exit can still hand back the old pid.
  defp await_new_server(project, old, attempts \\ 100) do
    case Coflux.Orchestration.Supervisor.get_server(project) do
      {:ok, ^old} when attempts > 0 ->
        Process.sleep(10)
        await_new_server(project, old, attempts - 1)

      {:ok, pid} ->
        pid
    end
  end

  defp subscribe(project, {key, opts, compare}) do
    {:ok, s0, ref} = Orchestration.subscribe(project, key, self(), opts)
    %{key: key, opts: opts, compare: compare, s0: s0, ref: ref, history: []}
  end

  # A topic model compared as the fold of the events.
  defp model(module, new) do
    fn s0, history -> elem(module.fold(new.(), s0 ++ history), 0) end
  end

  # For each subscription, compares the original snapshot plus everything
  # delivered since against a fresh snapshot. Returns the subscriptions with
  # their histories extended, and the comparables by key.
  defp check(project, subs) do
    subs =
      Enum.map(subs, fn sub ->
        {:ok, fresh, fresh_ref} = Orchestration.subscribe(project, sub.key, self(), sub.opts)
        Orchestration.unsubscribe(project, fresh_ref)

        history = sub.history ++ drain(sub.ref)
        replayed = sub.compare.(sub.s0, history)
        expected = sub.compare.(fresh, [])

        if replayed != expected do
          flunk("""
          #{inspect(sub.key)}: replaying the history diverges from a fresh snapshot
          replayed: #{inspect(replayed, pretty: true)}
          fresh:    #{inspect(expected, pretty: true)}
          """)
        end

        Map.merge(sub, %{history: history, model: replayed})
      end)

    {subs, Map.new(subs, &{&1.key, &1.model})}
  end

  # Everything the server has delivered for `ref`. The server sends events
  # before it replies to the call that followed them, so by the time a
  # snapshot call returns they are all in the mailbox.
  defp drain(ref, acc \\ []) do
    receive do
      {:topic, ^ref, events} -> drain(ref, acc ++ events)
    after
      0 -> acc
    end
  end
end
