defmodule Coflux.Topics.RunExecution do
  @moduledoc """
  The part of a run's view that changes when an execution is pinned: an old
  attempt, or a group member other than the first. The value is the tree
  from the topmost step at which the pinned path leaves the default view,
  with the path pinned below it, plus the hop from the default view into
  that root. Studio merges it over the run topic by step. An execution
  that's in the default view already gives an empty value.
  """

  use Topical.Topic,
    route: ["workspaces", :workspace_id, "runs", :run_id, "executions", :execution_id]

  alias Coflux.RunView
  alias Coflux.RunView.{Loader, Sync}

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    run_id = Map.fetch!(params, :run_id)
    workspace_id = Map.fetch!(params, :workspace_id)
    execution_id = Map.fetch!(params, :execution_id)

    with {:ok, view, fetch} <- Loader.load(project_id, run_id, workspace_id, self()),
         %{} = selection <- RunView.selection(view, execution_id) do
      view = RunView.with_selection(view, selection)
      visible = RunView.visible_steps(view)
      view = Sync.load(view, fetch, visible)

      value = %{
        root: root_id(view),
        parent: hop(selection.parent),
        steps: RunView.project(view)
      }

      state = %{
        view: view,
        visible: visible,
        selection: selection,
        execution_id: execution_id,
        fetch: fetch
      }

      {:ok, Topic.new(value, state)}
    else
      nil -> {:error, :not_found}
      {:error, :not_found} -> {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, events}, topic) do
    {view, effects} = RunView.apply_all(topic.state.view, events)
    topic = %{topic | state: %{topic.state | view: view}}
    previous = topic.state.selection

    # A change on the pinned path (a new attempt above the root, a new
    # parent link) can move where the view diverges from the default. Then
    # the pins and root are rederived and the value replaced.
    if RunView.touches?(view, effects.events, previous.path) do
      selection = RunView.selection(view, topic.state.execution_id)

      if selection.root == previous.root and selection.pins == previous.pins and
           selection.parent == previous.parent do
        {:ok, Sync.steps(topic, effects)}
      else
        view = RunView.with_selection(view, selection)
        topic = %{topic | state: %{topic.state | view: view, selection: selection}}

        topic =
          topic
          |> Sync.reset()
          |> Topic.set([:root], root_id(view))
          |> Topic.set([:parent], hop(selection.parent))

        {:ok, topic}
      end
    else
      {:ok, Sync.steps(topic, effects)}
    end
  end

  defp hop(nil), do: nil

  defp hop(%{execution_id: execution_id, group_id: group_id}) do
    %{executionId: execution_id, groupId: group_id}
  end

  defp root_id(view) do
    case RunView.root_step(view) do
      nil ->
        nil

      step ->
        case RunView.expanded_execution(view, step) do
          nil -> nil
          execution -> execution.id
        end
    end
  end
end
