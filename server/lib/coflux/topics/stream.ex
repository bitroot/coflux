defmodule Coflux.Topics.Stream do
  @moduledoc """
  Inspection topic for a single stream, keyed by the stream's id
  (``<run>:<step>_<index>``). Used by the Studio UI when a user opens a
  stream dialog — the topic keeps a bounded tail of items (with resolved
  values) plus closure state, and receives live updates as items are
  appended, as executions register on the stream (opening it, or resuming
  it after a suspend), or as the stream is closed.
  """
  use Topical.Topic, route: ["streams", :id]

  alias Coflux.Orchestration
  alias Coflux.TopicUtils

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    id = Map.fetch!(params, :id)

    case Orchestration.subscribe_stream_topic(project_id, id, self()) do
      {:ok, initial, ref} ->
        {:ok,
         Topic.new(
           %{
             id: initial.id,
             step: initial.step,
             workspaceId: initial.workspaceId,
             index: initial.index,
             position: initial.position,
             buffer: initial.buffer,
             timeoutMs: initial.timeoutMs,
             openedAt: initial.openedAt,
             attempts: initial.attempts,
             closure: initial.closure,
             items: Enum.map(initial.items, &build_item/1),
             totalCount: initial.totalCount,
             tailSize: initial.tailSize
           },
           %{ref: ref, tail_size: initial.tailSize}
         )}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification/2)
    {:ok, topic}
  end

  defp process_notification({:item_appended, sequence, value, attempt, created_at}, topic) do
    tail_size = topic.state.tail_size || 200

    item = build_item({sequence, value, attempt, created_at})
    existing = topic.value.items

    # Keep items bounded: drop the head once we're at capacity.
    new_items =
      if length(existing) >= tail_size do
        [_dropped | rest] = existing
        rest ++ [item]
      else
        existing ++ [item]
      end

    topic
    |> Topic.set([:items], new_items)
    |> Topic.set([:totalCount], topic.value.totalCount + 1)
  end

  # An execution registered on the stream — the latest registration's
  # config is the one in force.
  defp process_notification({:registered, attempt, buffer, timeout_ms, _created_at}, topic) do
    attempts = topic.value.attempts

    topic
    |> Topic.set([:attempts], if(attempt in attempts, do: attempts, else: attempts ++ [attempt]))
    |> Topic.set([:buffer], buffer)
    |> Topic.set([:timeoutMs], timeout_ms)
  end

  defp process_notification({:closed, reason, error, attempt, closed_at}, topic) do
    Topic.set(topic, [:closure], %{
      reason: reason,
      error: error,
      attempt: attempt,
      closedAt: closed_at
    })
  end

  defp build_item({sequence, value, attempt, created_at}) do
    %{
      sequence: sequence,
      value: TopicUtils.build_value(value),
      attempt: attempt,
      createdAt: created_at
    }
  end
end
