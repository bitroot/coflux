defmodule Coflux.Topics.Stream do
  @moduledoc """
  Inspection topic for a single stream, keyed by the producer's external
  execution id and sequence. Used by the Studio UI when a user opens a
  stream dialog — the topic keeps a bounded tail of items (with resolved
  values) plus closure state, and receives live updates as items are
  appended or the stream is closed.
  """
  use Topical.Topic, route: ["streams", :execution_id, :sequence]

  alias Coflux.Orchestration
  alias Coflux.TopicUtils

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    execution_id = Map.fetch!(params, :execution_id)
    sequence = parse_sequence(Map.fetch!(params, :sequence))

    case Orchestration.subscribe_stream_topic(
           project_id,
           execution_id,
           sequence,
           self()
         ) do
      {:ok, initial, ref} ->
        {:ok,
         Topic.new(
           %{
             producer: initial.producer,
             openedAt: initial.openedAt,
             closedAt: initial.closedAt,
             closure: build_closure(initial.closure),
             items: Enum.map(initial.items, &build_item/1),
             firstPosition: initial.firstPosition,
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

  defp process_notification({:item_appended, position, value, created_at}, topic) do
    tail_size = topic.state.tail_size || 200

    item = build_item({position, value, created_at})

    existing = topic.value.items
    total = topic.value.totalCount + 1

    # Keep items bounded: if we're already at capacity, drop the head.
    # Otherwise append and leave firstPosition alone (or set it if empty).
    {new_items, new_first_position} =
      cond do
        existing == [] ->
          {[item], position}

        length(existing) >= tail_size ->
          [_dropped | rest] = existing
          new_items = rest ++ [item]
          [first_item | _] = new_items
          {new_items, first_item.position}

        true ->
          {existing ++ [item], topic.value.firstPosition}
      end

    topic
    |> Topic.set([:items], new_items)
    |> Topic.set([:firstPosition], new_first_position)
    |> Topic.set([:totalCount], total)
  end

  defp process_notification({:closed, error, closed_at}, topic) do
    closure = build_closure_from_notification(error, closed_at)

    topic
    |> Topic.set([:closedAt], closed_at)
    |> Topic.set([:closure], closure)
  end

  defp build_item({position, value, created_at}) do
    %{
      position: position,
      value: TopicUtils.build_value(value),
      createdAt: created_at
    }
  end

  defp build_closure(nil), do: nil

  defp build_closure(%{reason: reason, error: error, closedAt: closed_at}) do
    %{
      reason: reason,
      error: error,
      closedAt: closed_at
    }
  end

  # The live-close notification carries the already-encoded error summary
  # (type/message) and closedAt — it doesn't include the reason because
  # the notification is the same shape used by the run topic. Default to
  # "errored" when there's an error, "complete" otherwise; the distinction
  # between errored/lifecycle is resolved server-side before we get here.
  defp build_closure_from_notification(error, closed_at) do
    reason = if error, do: "errored", else: "complete"

    %{
      reason: reason,
      error: error,
      closedAt: closed_at
    }
  end

  defp parse_sequence(s) when is_integer(s), do: s

  defp parse_sequence(s) when is_binary(s) do
    case Integer.parse(s) do
      {n, ""} -> n
      _ -> raise ArgumentError, "invalid stream sequence: #{inspect(s)}"
    end
  end
end
