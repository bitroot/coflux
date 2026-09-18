defmodule Coflux.Topics.RunGroup do
  @moduledoc """
  The members of one group, for the group popover: every child submitted
  into the group by that execution, in submission order, with its branch
  status. The run topic only carries the group's first member and a
  summary of the rest.
  """

  use Topical.Topic,
    route: [
      "workspaces",
      :workspace_id,
      "runs",
      :run_id,
      "executions",
      :execution_id,
      "groups",
      :group_id
    ]

  alias Coflux.RunView
  alias Coflux.RunView.Loader

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    run_id = Map.fetch!(params, :run_id)
    workspace_id = Map.fetch!(params, :workspace_id)
    execution_id = Map.fetch!(params, :execution_id)

    with {group_id, ""} <- Integer.parse(Map.fetch!(params, :group_id)),
         {:ok, view, _run, _parent} <- Loader.load(project_id, run_id, workspace_id, self()),
         %{groups: %{^group_id => group}} <- Map.get(view.executions, execution_id) do
      members = RunView.group_members(view, execution_id, group_id)

      value = %{
        name: group.name,
        concurrency: group.concurrency,
        children: members
      }

      state = %{
        view: view,
        execution_id: execution_id,
        group_id: group_id,
        # Position of each member step in `children`, for updating in place.
        index: members |> Enum.with_index() |> Map.new(fn {member, i} -> {member.stepId, i} end)
      }

      {:ok, Topic.new(value, state)}
    else
      _ -> {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    {view, effects} = RunView.apply_all(topic.state.view, notifications)
    topic = %{topic | state: %{topic.state | view: view}}
    %{execution_id: execution_id, group_id: group_id} = topic.state

    topic =
      Enum.reduce(effects.events, topic, fn
        {:link, ^execution_id, step, ^group_id}, topic ->
          link = Enum.find(view.children[execution_id], &(&1.step == step))
          member = RunView.group_member(view, link)
          position = map_size(topic.state.index)

          topic
          |> Topic.insert([:children], member)
          |> put_in([Access.key(:state), :index, member.stepId], position)

        _event, topic ->
          topic
      end)

    topic =
      Enum.reduce(effects.branches, topic, fn step, topic ->
        update_member(topic, view, step, :status, RunView.member_status(view, step))
      end)

    topic =
      Enum.reduce(effects.events, topic, fn
        {:attempt, step}, topic ->
          update_member(topic, view, step, :attempt, RunView.latest(view, step))

        _event, topic ->
          topic
      end)

    group = view.executions[execution_id].groups[group_id]

    topic =
      topic
      |> maybe_set([:name], group.name)
      |> maybe_set([:concurrency], group.concurrency)

    {:ok, topic}
  end

  defp update_member(topic, view, step, field, value) do
    case Map.fetch(topic.state.index, RunView.step_key(view, step)) do
      {:ok, position} ->
        if Map.fetch!(Enum.at(topic.value.children, position), field) == value,
          do: topic,
          else: Topic.set(topic, [:children, position, field], value)

      :error ->
        topic
    end
  end

  defp maybe_set(topic, [key] = path, value) do
    if Map.get(topic.value, key) == value, do: topic, else: Topic.set(topic, path, value)
  end
end
