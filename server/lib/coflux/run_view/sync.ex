defmodule Coflux.RunView.Sync do
  @moduledoc """
  Keeps a topic's `steps` in step with its view after a batch of events
  has been applied.

  The topic's state carries the `view`, the set of `visible` steps, and a
  `fetch` function that loads detail (`Orchestration.get_run_details`).
  Steps that enter or leave the view are set or unset whole; steps that
  stay are re-projected and diffed, so only what changed is sent. Detail
  for anything about to be projected is fetched first if it isn't loaded.

  Which steps are visible is recomputed in full only when a visible step
  gained an attempt (a re-run or retry can change which subtree is shown).
  Anything else can only add: a new child link the view shows, or a first
  attempt on a step that's already linked from a shown execution, brings
  in the subtree it reaches.
  """

  alias Coflux.RunView
  alias Coflux.Topics.Diff
  alias Topical.Topic

  def steps(topic, effects) do
    {added, removed, visible} = revisit(topic.state.view, topic.state.visible, effects.events)

    dirty =
      effects.dirty
      |> MapSet.intersection(visible)
      |> MapSet.difference(added)

    topic = ensure(topic, MapSet.union(added, dirty))
    view = topic.state.view

    topic =
      Enum.reduce(removed, topic, fn step, topic ->
        Topic.unset(topic, [:steps], RunView.step_key(view, step))
      end)

    topic =
      Enum.reduce(added, topic, fn step, topic ->
        Topic.set(topic, [:steps, RunView.step_key(view, step)], RunView.project_step(view, step))
      end)

    topic =
      Enum.reduce(dirty, topic, fn step, topic ->
        key = RunView.step_key(view, step)
        Diff.apply(topic, [:steps, key], topic.value.steps[key], RunView.project_step(view, step))
      end)

    %{topic | state: %{topic.state | visible: visible}}
  end

  @doc "Replaces the whole `steps` map, for when the view's root or pins moved."
  def reset(topic) do
    visible = RunView.visible_steps(topic.state.view)
    topic = ensure(topic, visible)
    view = topic.state.view
    topic = Diff.apply(topic, [:steps], topic.value.steps, RunView.project(view))
    %{topic | state: %{topic.state | visible: visible}}
  end

  @doc """
  Keeps a structure topic's `steps` in step with its view: every step that
  changed is re-projected and diffed. Steps only ever join (with their first
  shown attempt), never leave, and no detail is involved.
  """
  def structure(topic, effects) do
    view = topic.state.view

    Enum.reduce(effects.dirty, topic, fn step, topic ->
      if RunView.latest(view, step) do
        key = RunView.step_key(view, step)

        Diff.apply(
          topic,
          [:steps, key],
          topic.value.steps[key],
          RunView.project_structure_step(view, step)
        )
      else
        topic
      end
    end)
  end

  @doc "Loads any detail the given steps need before they can be projected."
  def ensure(topic, steps, executions? \\ true) do
    view = topic.state.view
    request = RunView.missing_details(view, steps, executions?)

    if RunView.empty_request?(request) do
      topic
    else
      view = RunView.put_details(view, request, topic.state.fetch.(request))
      %{topic | state: %{topic.state | view: view}}
    end
  end

  @doc "The view with detail loaded for the given steps, through `fetch`."
  def load(view, fetch, steps, executions? \\ true) do
    request = RunView.missing_details(view, steps, executions?)

    if RunView.empty_request?(request),
      do: view,
      else: RunView.put_details(view, request, fetch.(request))
  end

  defp revisit(view, visible, events) do
    full? =
      Enum.any?(events, fn
        {:attempt, step} -> MapSet.member?(visible, step)
        _ -> false
      end)

    if full? do
      now = RunView.visible_steps(view)
      {MapSet.difference(now, visible), MapSet.difference(visible, now), now}
    else
      added =
        Enum.reduce(events, MapSet.new(), fn
          {:link, parent_id, step, group_id}, added ->
            add_through(view, visible, added, parent_id, step, group_id)

          {:attempt, step}, added ->
            view.parents
            |> Map.get(step, [])
            |> Enum.reduce(added, fn parent_id, added ->
              group_id = view.child_index[parent_id][step]
              add_through(view, visible, added, parent_id, step, group_id)
            end)

          _event, added ->
            added
        end)

      {added, MapSet.new(), MapSet.union(visible, added)}
    end
  end

  # Adds what `step` reaches if the link to it from `parent_id` is one the
  # view shows and its parent is shown.
  defp add_through(view, visible, added, parent_id, step, group_id) do
    seen = MapSet.union(visible, added)
    parent_step = view.executions[parent_id].step

    if MapSet.member?(seen, parent_step) and not MapSet.member?(seen, step) and
         RunView.link_visible?(view, parent_id, step, group_id) do
      MapSet.union(added, RunView.reachable(view, step, seen))
    else
      added
    end
  end
end
