defmodule Coflux.RunView.Sync do
  @moduledoc """
  Keeps a topic's `steps` in step with its view after a batch of
  notifications has been applied.

  The topic's state carries the `view` and the set of `visible` steps. Steps
  that enter or leave the view are set or unset whole; steps that stay are
  re-projected and diffed, so only what changed is sent.

  Which steps are visible is recomputed in full only when a visible step
  gained an attempt (a re-run or retry can change which subtree is shown).
  Anything else can only add: a new child link the view shows, or a first
  attempt on a step that's already linked from a shown execution, brings
  in the subtree it reaches.
  """

  alias Coflux.RunView
  alias Coflux.RunView.Diff
  alias Topical.Topic

  def steps(topic, effects) do
    view = topic.state.view
    visible = topic.state.visible
    {added, removed, visible} = revisit(view, visible, effects.events)

    topic =
      Enum.reduce(removed, topic, fn step, topic ->
        Topic.unset(topic, [:steps], RunView.step_key(view, step))
      end)

    topic =
      Enum.reduce(added, topic, fn step, topic ->
        Topic.set(topic, [:steps, RunView.step_key(view, step)], RunView.project_step(view, step))
      end)

    dirty =
      effects.dirty
      |> MapSet.intersection(visible)
      |> MapSet.difference(added)

    topic =
      Enum.reduce(dirty, topic, fn step, topic ->
        key = RunView.step_key(view, step)
        Diff.apply(topic, [:steps, key], topic.value.steps[key], RunView.project_step(view, step))
      end)

    %{topic | state: %{topic.state | visible: visible}}
  end

  @doc "Replaces the whole `steps` map, for when the view's root or pins moved."
  def reset(topic) do
    view = topic.state.view
    steps = RunView.project(view)
    topic = Diff.apply(topic, [:steps], topic.value.steps, steps)
    %{topic | state: %{topic.state | visible: RunView.visible_steps(view)}}
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
