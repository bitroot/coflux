defmodule Coflux.RunViewTest do
  use ExUnit.Case, async: true

  alias Coflux.RunView
  alias Coflux.RunView.{Diff, Sync}
  alias Topical.Topic

  @run %{external_id: "R1", created_at: 0, created_by: nil, requires: %{}}
  @ws "W1"

  # ---------------------------------------------------------------------------
  # Snapshot builders (the shape `build_run_data` produces)

  defp id(number, attempt), do: "R1:#{number}:#{attempt}"

  # A snapshot step. With `detail: false` it carries structure only, as
  # `subscribe_run` returns it: no arguments or streams, and executions
  # without results, dependencies and the rest.
  defp step(number, opts \\ []) do
    detail? = Keyword.get(opts, :detail, true)

    base = %{
      module: "m",
      target: "t#{number}",
      type: "task",
      parent_id: Keyword.get(opts, :parent),
      cache_config: nil,
      cache_key: nil,
      memo_key: nil,
      concurrency_key: nil,
      concurrency_limit: 0,
      group_key: nil,
      group_limit: 0,
      retry_limit: 0,
      retry_backoff_min: 0,
      retry_backoff_max: 0,
      recurrent: 0,
      timeout: 0,
      created_at: number * 10,
      requires: %{},
      executions:
        Map.new(Keyword.get(opts, :executions, []), fn {attempt, execution_opts} ->
          {attempt, execution(number, attempt, [{:detail, detail?} | execution_opts])}
        end)
    }

    if detail?, do: Map.merge(base, %{arguments: [], streams: %{}}), else: base
  end

  defp execution(number, attempt, opts) do
    base = %{
      execution_id: id(number, attempt),
      workspace_id: Keyword.get(opts, :ws, @ws),
      created_at: Keyword.get(opts, :created_at, number * 10 + attempt),
      created_by: nil,
      execute_after: nil,
      assigned_at: Keyword.get(opts, :assigned_at),
      completed_at: Keyword.get(opts, :completed_at),
      completion:
        case Keyword.get(opts, :completion) do
          nil -> nil
          kind -> %{kind: kind, successor: nil}
        end,
      groups: Keyword.get(opts, :groups, %{}),
      children: Keyword.get(opts, :children, [])
    }

    if Keyword.get(opts, :detail, true),
      do: Map.merge(base, detail(Keyword.get(opts, :dependencies, %{}))),
      else: base
  end

  # Detail as `get_run_details` returns it.
  defp detail(dependencies \\ %{}, result \\ nil) do
    %{
      assets: %{},
      dependencies: dependencies,
      pending_dependencies: MapSet.new(),
      inputs: %{},
      result: result,
      result_at: nil,
      result_created_by: nil,
      metric_definitions: %{},
      checkpoints: %{before: %{}, after: %{}}
    }
  end

  # 1 (running)
  # ├─ 2 (completed)
  # └─ group 1 "batch": 3 (running) ─ 6 (assigning)
  #                     4 (errored)
  #                     5 (assigning)
  defp fixture(detail? \\ true) do
    steps = %{
      1 =>
        step(1,
          detail: detail?,
          executions: [
            {1,
             [
               assigned_at: 1,
               groups: %{1 => %{name: "batch", concurrency: 0}},
               children: [{2, 1, nil}, {3, 1, 1}, {4, 1, 1}, {5, 1, 1}]
             ]}
          ]
        ),
      2 =>
        step(2,
          detail: detail?,
          parent: id(1, 1),
          executions: [{1, [assigned_at: 1, completed_at: 2, completion: "succeeded"]}]
        ),
      3 =>
        step(3,
          detail: detail?,
          parent: id(1, 1),
          executions: [{1, [assigned_at: 1, children: [{6, 1, nil}]]}]
        ),
      4 =>
        step(4,
          detail: detail?,
          parent: id(1, 1),
          executions: [{1, [assigned_at: 1, completed_at: 2, completion: "errored"]}]
        ),
      5 => step(5, detail: detail?, parent: id(1, 1), executions: [{1, []}]),
      6 => step(6, detail: detail?, parent: id(3, 1), executions: [{1, []}])
    }

    RunView.new(@run, steps, [@ws])
  end

  # ---------------------------------------------------------------------------
  # Notification builders

  defp n_step(number, parent) do
    {:step, number,
     %{
       module: "m",
       target: "t#{number}",
       type: "task",
       parent_id: parent,
       cache_config: nil,
       cache_key: nil,
       memo_key: nil,
       concurrency_key: nil,
       concurrency_limit: 0,
       group_key: nil,
       group_limit: 0,
       retries: nil,
       recurrent: false,
       timeout: 0,
       created_at: number * 10,
       arguments: [],
       requires: %{}
     }, @ws}
  end

  defp n_execution(number, attempt, opts \\ []) do
    {:execution, number, attempt, id(number, attempt), Keyword.get(opts, :ws, @ws),
     Keyword.get(opts, :created_at, number * 10 + attempt), nil, %{}, nil, %{}, MapSet.new()}
  end

  defp n_child(parent_id, number, attempt, group_id) do
    {:child, parent_id, {number, attempt, group_id}}
  end

  defp n_assigned(execution_id), do: {:assigned, %{execution_id => 100}}
  defp n_completion(execution_id, kind), do: {:completion, execution_id, kind, nil, 200}

  # ---------------------------------------------------------------------------
  # A topic kept in sync, and oracles

  defp topic(view, fetch \\ fn request -> flunk("unexpected fetch: #{inspect(request)}") end) do
    Topic.new(%{steps: RunView.project(view)}, %{
      view: view,
      visible: RunView.visible_steps(view),
      fetch: fetch
    })
  end

  # A fetch that answers with arguments naming the step and a value result,
  # and records what it was asked for.
  defp recording_fetch(log) do
    fn request ->
      Agent.update(log, &[request | &1])

      %{
        executions:
          Map.new(request.executions, fn {number, attempt} ->
            {id(number, attempt), detail(%{}, {:value, {:raw, "r#{number}", []}})}
          end),
        steps:
          Map.new(request.steps, fn number ->
            {number, %{arguments: [{:raw, "arg#{number}", []}], streams: %{}}}
          end)
      }
    end
  end

  defp sync(topic, notifications) do
    {view, effects} = RunView.apply_all(topic.state.view, notifications)
    topic = %{topic | state: %{topic.state | view: view}}
    Sync.steps(topic, effects)
  end

  defp assert_in_sync(topic) do
    assert topic.value.steps == RunView.project(topic.state.view)
    assert topic.state.visible == RunView.visible_steps(topic.state.view)
    assert_branches(topic.state.view)
  end

  # A structure topic kept in sync alongside
  defp structure_topic(view) do
    Topic.new(%{steps: RunView.project_structure(view)}, %{view: view})
  end

  defp sync_structure(topic, notifications) do
    {view, effects} = RunView.apply_all(topic.state.view, notifications)
    topic = %{topic | state: %{topic.state | view: view}}
    Sync.structure(topic, effects)
  end

  # Studio's `getBranchStatus`: the latest attempt's status combined with
  # every child's branch, recursively.
  defp assert_branches(view) do
    for step <- Map.keys(view.steps), RunView.latest(view, step) do
      statuses = oracle_statuses(view, step, MapSet.new())

      expected =
        Enum.find(
          [:running, :assigning, :errored, :aborted, :suspended],
          :completed,
          &(&1 in statuses)
        )

      assert RunView.branch_status(view, step) == expected, "branch status of step #{step}"
    end
  end

  defp oracle_statuses(view, step, seen) do
    case RunView.latest(view, step) do
      nil ->
        []

      attempt ->
        execution = view.executions[view.attempts[step][attempt]]
        seen = MapSet.put(seen, step)

        children =
          view.child_index
          |> Map.get(execution.id, %{})
          |> Map.keys()
          |> Enum.reject(&MapSet.member?(seen, &1))

        [own_status(execution) | Enum.flat_map(children, &oracle_statuses(view, &1, seen))]
    end
  end

  defp own_status(%{completion: nil, assigned_at: nil}), do: :assigning
  defp own_status(%{completion: nil}), do: :running

  defp own_status(%{completion: %{kind: kind}}) do
    case kind do
      "succeeded" -> :completed
      "errored" -> :errored
      "cancelled" -> :aborted
      "suspended" -> :suspended
    end
  end

  # ---------------------------------------------------------------------------

  describe "the default view" do
    test "shows the latest tree with each group collapsed to its first member" do
      view = fixture()
      assert RunView.visible_steps(view) == MapSet.new([1, 2, 3, 6])

      steps = RunView.project(view)
      assert steps |> Map.keys() |> Enum.sort() == ["R1:1", "R1:2", "R1:3", "R1:6"]

      root = steps["R1:1"]
      assert Map.keys(root.attempts) == ["1"]
      assert Map.keys(root.executions) == ["1"]
      execution = root.executions["1"]
      assert Enum.map(execution.children, & &1.stepId) == ["R1:2", "R1:3"]

      assert execution.groups["1"] == %{
               name: "batch",
               concurrency: 0,
               members: %{
                 total: 3,
                 byStatus: %{
                   assigning: 1,
                   running: 1,
                   errored: 1,
                   completed: 0,
                   aborted: 0,
                   suspended: 0
                 }
               }
             }

      assert steps["R1:3"].attempts["1"] == %{
               executionId: "R1:3:1",
               workspaceId: @ws,
               createdAt: 31,
               executeAfter: nil,
               assignedAt: 1,
               completedAt: nil,
               completion: nil
             }
    end

    test "branch status rolls up through latest attempts and every group member" do
      view = fixture()
      assert_branches(view)
      assert RunView.branch_status(view, 1) == :running
      assert RunView.branch_status(view, 3) == :running

      {view, _} =
        RunView.apply_all(view, [
          n_completion(id(1, 1), :succeeded),
          n_completion(id(3, 1), :succeeded)
        ])

      assert RunView.branch_status(view, 3) == :assigning
      assert RunView.branch_status(view, 1) == :assigning

      {view, effects} =
        RunView.apply_all(view, [
          n_assigned(id(6, 1)),
          n_completion(id(6, 1), :succeeded),
          n_assigned(id(5, 1)),
          n_completion(id(5, 1), :succeeded)
        ])

      assert RunView.branch_status(view, 1) == :errored
      # The root's group summary moved, so its entry is dirty
      assert MapSet.member?(effects.dirty, 1)
      assert MapSet.equal?(effects.branches, MapSet.new([1, 3, 5, 6]))
      assert_branches(view)

      members = RunView.project(view)["R1:1"].executions["1"].groups["1"].members

      assert members.byStatus == %{
               completed: 2,
               errored: 1,
               assigning: 0,
               running: 0,
               aborted: 0,
               suspended: 0
             }
    end

    test "lists a group's members in order with their branch status" do
      members = RunView.group_members(fixture(), id(1, 1), 1)

      assert Enum.map(members, &{&1.stepId, &1.status}) == [
               {"R1:3", "running"},
               {"R1:4", "errored"},
               {"R1:5", "assigning"}
             ]
    end
  end

  describe "keeping a topic in sync" do
    test "a new attempt is expanded and what only the old one reached is dropped" do
      topic = sync(topic(fixture()), [n_execution(3, 2)])
      view = topic.state.view

      assert RunView.visible_steps(view) == MapSet.new([1, 2, 3])
      assert topic.value.steps["R1:3"].attempts |> Map.keys() |> Enum.sort() == ["1", "2"]
      assert Map.keys(topic.value.steps["R1:3"].executions) == ["2"]
      refute Map.has_key?(topic.value.steps, "R1:6")
      assert RunView.branch_status(view, 3) == :assigning
      assert topic.value.steps["R1:1"].executions["1"].groups["1"].members.byStatus.assigning == 2
      assert_in_sync(topic)
    end

    test "a new child appears under an expanded execution and is counted under a collapsed group" do
      topic =
        sync(topic(fixture()), [
          n_step(7, id(2, 1)),
          n_execution(7, 1),
          n_child(id(2, 1), 7, 1, nil)
        ])

      assert Map.has_key?(topic.value.steps, "R1:7")
      assert Enum.map(topic.value.steps["R1:2"].executions["1"].children, & &1.stepId) == ["R1:7"]
      assert_in_sync(topic)

      topic = sync(topic, [n_step(8, id(1, 1)), n_execution(8, 1), n_child(id(1, 1), 8, 1, 1)])
      refute Map.has_key?(topic.value.steps, "R1:8")
      assert topic.value.steps["R1:1"].executions["1"].groups["1"].members.total == 4
      assert_in_sync(topic)

      # A memo hit linking that member under a shown execution brings it in
      topic = sync(topic, [n_child(id(2, 1), 8, 1, nil)])
      assert Map.has_key?(topic.value.steps, "R1:8")
      assert_in_sync(topic)
    end

    test "an attempt in another workspace is ignored until one lands in a shown workspace" do
      topic =
        sync(topic(fixture()), [
          n_step(7, id(2, 1)),
          n_execution(7, 1, ws: "W2"),
          n_child(id(2, 1), 7, 1, nil)
        ])

      refute Map.has_key?(topic.value.steps, "R1:7")
      assert Map.has_key?(topic.state.view.steps, 7)
      assert topic.value.steps["R1:1"].executions["1"].groups["1"].members.total == 3
      assert_in_sync(topic)

      topic = sync(topic, [n_execution(7, 2)])
      assert Map.keys(topic.value.steps["R1:7"].attempts) == ["2"]
      assert_in_sync(topic)
    end

    test "matches a fresh projection through a sequence of batches" do
      batches = [
        [n_assigned(id(5, 1))],
        [n_step(7, id(3, 1)), n_execution(7, 1), n_child(id(3, 1), 7, 1, nil)],
        [n_assigned(id(7, 1)), n_completion(id(7, 1), :errored)],
        [n_execution(7, 2)],
        [n_assigned(id(7, 2)), n_completion(id(7, 2), :succeeded)],
        [n_execution(3, 2)],
        [n_completion(id(3, 2), :succeeded)],
        [n_step(8, id(3, 2)), n_execution(8, 1), n_child(id(3, 2), 8, 1, nil)],
        [n_child(id(3, 2), 6, 1, nil)],
        [n_completion(id(1, 1), :succeeded)],
        [n_execution(1, 2)],
        [
          n_step(9, id(1, 2)),
          n_execution(9, 1),
          n_child(id(1, 2), 9, 1, nil),
          n_child(id(1, 2), 3, 2, nil)
        ]
      ]

      Enum.reduce(batches, topic(fixture()), fn batch, topic ->
        topic = sync(topic, batch)
        assert_in_sync(topic)
        topic
      end)
    end

    test "input responses reach submissions and dependencies" do
      topic =
        sync(topic(fixture()), [
          {:input_submitted, id(3, 1), "I1", "Pick"},
          {:input_dependency, id(2, 1), "I1", "Pick", nil, true}
        ])

      assert topic.value.steps["R1:3"].executions["1"].inputs == %{
               "I1" => %{title: "Pick", status: nil}
             }

      assert topic.value.steps["R1:2"].executions["1"].dependencies["I1"].pending == true

      topic = sync(topic, [{:input_response, "I1", "value"}])
      assert topic.value.steps["R1:3"].executions["1"].inputs["I1"].status == "value"
      assert topic.value.steps["R1:2"].executions["1"].dependencies["I1"].status == "value"
      assert_in_sync(topic)

      # A later submission of the same input starts with its status
      topic = sync(topic, [{:input_submitted, id(6, 1), "I1", "Pick"}])
      assert topic.value.steps["R1:6"].executions["1"].inputs["I1"].status == "value"
      assert_in_sync(topic)
    end
  end

  describe "the structure" do
    test "carries every step with all attempts and complete children, and nothing else" do
      view = fixture(false)
      steps = RunView.project_structure(view)

      assert steps |> Map.keys() |> Enum.sort() == [
               "R1:1",
               "R1:2",
               "R1:3",
               "R1:4",
               "R1:5",
               "R1:6"
             ]

      root = steps["R1:1"].attempts["1"]

      assert Enum.map(root.children, &{&1.stepId, &1.groupId}) == [
               {"R1:2", nil},
               {"R1:3", 1},
               {"R1:4", 1},
               {"R1:5", 1}
             ]

      assert root.completion == nil
      refute Map.has_key?(steps["R1:1"], :executions)
      refute Map.has_key?(steps["R1:1"], :arguments)
    end

    test "stays in sync as steps, attempts and links arrive" do
      batches = [
        [n_execution(3, 2)],
        [n_completion(id(3, 2), :succeeded)],
        [n_step(7, id(3, 2)), n_execution(7, 1), n_child(id(3, 2), 7, 1, nil)],
        [n_step(8, id(1, 1)), n_execution(8, 1, ws: "W2"), n_child(id(1, 1), 8, 1, 1)],
        [n_execution(8, 2)]
      ]

      topic =
        Enum.reduce(batches, structure_topic(fixture(false)), fn batch, topic ->
          topic = sync_structure(topic, batch)
          assert topic.value.steps == RunView.project_structure(topic.state.view)
          topic
        end)

      assert topic.value.steps["R1:3"].attempts |> Map.keys() |> Enum.sort() == ["1", "2"]
      assert Enum.map(topic.value.steps["R1:3"].attempts["2"].children, & &1.stepId) == ["R1:7"]
      # Step 8's first attempt was in another workspace; it joined with its second
      assert Map.keys(topic.value.steps["R1:8"].attempts) == ["2"]

      assert Enum.map(topic.value.steps["R1:1"].attempts["1"].children, & &1.stepId) == [
               "R1:2",
               "R1:3",
               "R1:4",
               "R1:5",
               "R1:8"
             ]
    end
  end

  describe "loading detail on demand" do
    test "a structure-only snapshot reports what the visible steps need" do
      view = fixture(false)
      assert RunView.visible_steps(view) == MapSet.new([1, 2, 3, 6])

      request = RunView.missing_details(view, RunView.visible_steps(view))
      assert Enum.sort(request.steps) == [1, 2, 3, 6]
      assert Enum.sort(request.executions) == [{1, 1}, {2, 1}, {3, 1}, {6, 1}]

      # Only arguments, when asked for members
      assert RunView.missing_details(view, [4, 5], false) == %{executions: [], steps: [5, 4]}

      # Status is structure, so it's known without any detail
      assert_branches(view)
      assert RunView.project(view)["R1:1"].executions["1"].groups["1"].members.total == 3
    end

    test "a topic loads detail for what it shows, and again for what comes into view" do
      {:ok, log} = Agent.start_link(fn -> [] end)
      fetch = recording_fetch(log)
      view = fixture(false)
      view = Sync.load(view, fetch, RunView.visible_steps(view))
      topic = topic(view, fetch)

      assert [%{steps: steps, executions: executions}] = Agent.get(log, & &1)
      assert Enum.sort(steps) == [1, 2, 3, 6]
      assert length(executions) == 4
      assert topic.value.steps["R1:2"].arguments == [%{type: "raw", data: "arg2", references: []}]
      assert topic.value.steps["R1:2"].executions["1"].result.value.data == "r2"
      assert_in_sync(topic)

      # A status change on a loaded step needs nothing more
      topic = sync(topic, [n_completion(id(3, 1), :succeeded)])
      assert length(Agent.get(log, & &1)) == 1
      assert_in_sync(topic)

      # A memo hit brings the unloaded member 4 into view under the leaf
      topic = sync(topic, [n_child(id(2, 1), 4, 1, nil)])
      assert [%{steps: [4], executions: [{4, 1}]} | _] = Agent.get(log, & &1)
      assert topic.value.steps["R1:4"].arguments == [%{type: "raw", data: "arg4", references: []}]
      assert_in_sync(topic)

      # A new attempt arrives with its detail, so nothing is fetched for it
      topic = sync(topic, [n_execution(2, 2)])
      assert length(Agent.get(log, & &1)) == 2
      assert Map.keys(topic.value.steps["R1:2"].executions) == ["2"]
      assert_in_sync(topic)
    end

    test "a pinned view re-rooting loads the attempt it uncovers" do
      {:ok, log} = Agent.start_link(fn -> [] end)
      fetch = recording_fetch(log)
      view = fixture(false)
      selection = RunView.selection(view, id(4, 1))
      view = RunView.with_selection(view, selection)
      view = Sync.load(view, fetch, RunView.visible_steps(view))
      topic = topic(view, fetch)
      assert Map.keys(topic.value.steps) == ["R1:4"]

      # The root is re-run: the view moves up to the root's old attempt,
      # whose detail wasn't loaded
      {view, _effects} = RunView.apply_all(topic.state.view, [n_execution(1, 2)])
      selection = RunView.selection(view, id(4, 1))
      assert selection.root == {:step, 1}
      view = RunView.with_selection(view, selection)
      topic = %{topic | state: %{topic.state | view: view}}
      topic = Sync.reset(topic)

      assert Map.keys(topic.value.steps) |> Enum.sort() == ["R1:1", "R1:2", "R1:4"]
      assert [%{steps: steps, executions: executions} | _] = Agent.get(log, & &1)
      assert Enum.sort(steps) == [1, 2]
      assert Enum.sort(executions) == [{1, 1}, {2, 1}]
      assert topic.value.steps["R1:1"].executions["1"].result.value.data == "r1"
      assert_in_sync(topic)
    end
  end

  describe "selection" do
    test "an execution in the default view needs nothing" do
      view = fixture()
      assert %{root: :none, parent: nil} = RunView.selection(view, id(2, 1))
      assert %{root: :none, parent: nil} = RunView.selection(view, id(6, 1))
      assert RunView.selection(view, "R1:9:1") == nil
    end

    test "a group member other than the first roots the view at it" do
      view = fixture()
      selection = RunView.selection(view, id(4, 1))
      assert selection.root == {:step, 4}
      assert selection.parent == %{execution_id: id(1, 1), group_id: 1}
      assert selection.pins.groups == %{{id(1, 1), 1} => 4}
      assert selection.path == MapSet.new([1, 4])

      view = RunView.with_selection(view, selection)
      assert RunView.visible_steps(view) == MapSet.new([4])
    end

    test "a superseded attempt roots the view at its step and shows what it reached" do
      {view, _} = RunView.apply_all(fixture(), [n_execution(3, 2)])
      selection = RunView.selection(view, id(3, 1))
      assert selection.root == {:step, 3}
      assert selection.parent == %{execution_id: id(1, 1), group_id: 1}
      assert selection.pins.steps == %{3 => 1, 1 => 1}

      view = RunView.with_selection(view, selection)
      assert RunView.visible_steps(view) == MapSet.new([3, 6])
      assert Map.keys(RunView.project(view)["R1:3"].executions) == ["1"]
    end

    test "a re-run above a pinned execution moves the root up" do
      view = fixture()
      selection = RunView.selection(view, id(4, 1))

      {view, effects} = RunView.apply_all(view, [n_execution(1, 2)])
      assert RunView.touches?(view, effects.events, selection.path)

      selection = RunView.selection(view, id(4, 1))
      assert selection.root == {:step, 1}
      assert selection.parent == nil
      assert selection.pins.steps == %{1 => 1, 4 => 1}

      view = RunView.with_selection(view, selection)
      assert RunView.visible_steps(view) == MapSet.new([1, 2, 4])
      assert Map.keys(RunView.project(view)["R1:1"].executions) == ["1"]
    end

    test "the path follows the most recently created parent, and a memo hit counts once per parent" do
      {view, _} =
        RunView.apply_all(fixture(), [
          n_execution(2, 2, created_at: 500),
          n_child(id(2, 2), 6, 1, nil)
        ])

      assert view.parents[6] == [id(3, 1), id(2, 2)]
      assert view.child_counts[id(2, 2)] == %{assigning: 1}

      selection = RunView.selection(view, id(6, 1))
      assert selection.pins.steps == %{6 => 1, 2 => 2, 1 => 1}
      assert selection.root == :none
    end

    test "a pinned view stays in sync as the run moves on" do
      view = fixture()
      selection = RunView.selection(view, id(4, 1))
      view = RunView.with_selection(view, selection)
      topic = topic(view)

      topic =
        sync(topic, [
          n_step(7, id(4, 1)),
          n_execution(7, 1),
          n_child(id(4, 1), 7, 1, nil),
          n_assigned(id(4, 1))
        ])

      assert Map.keys(topic.value.steps) |> Enum.sort() == ["R1:4", "R1:7"]
      assert_in_sync(topic)

      # A retry of the pinned step doesn't move the pin
      topic = sync(topic, [n_execution(4, 2)])
      assert Map.keys(topic.value.steps["R1:4"].executions) == ["1"]
      assert Map.has_key?(topic.value.steps, "R1:7")
      assert_in_sync(topic)
    end
  end

  describe "diff" do
    test "sends only what changed" do
      old = %{x: 1, y: %{z: 1, w: 2}, l: [1], gone: true}
      new = %{x: 1, y: %{z: 2}, l: [1, 2], n: 3}
      topic = Diff.apply(Topic.new(%{steps: %{"a" => old}}), [:steps, "a"], old, new)

      assert topic.value.steps["a"] == new

      assert Enum.sort(topic.updates) ==
               Enum.sort([
                 {:unset, [:steps, "a"], :gone},
                 {:unset, [:steps, "a", :y], :w},
                 {:set, [:steps, "a", :y, :z], 2},
                 {:set, [:steps, "a", :l], [1, 2]},
                 {:set, [:steps, "a", :n], 3}
               ])
    end
  end
end
