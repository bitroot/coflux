defmodule Coflux.Topics.Stream do
  @moduledoc """
  Inspection topic for a single stream, keyed by the stream's opaque id
  (``<producer_execution_id>_<index>``). Used by the Studio UI when a
  user opens a stream dialog — the topic keeps a bounded tail of items
  (with resolved values) plus closure state, and receives live updates
  as items are appended or the stream is closed.
  """
  use Topical.Topic, route: ["streams", :id]

  alias Coflux.Orchestration
  alias Coflux.TopicUtils

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)

    case parse_id(Map.fetch!(params, :id)) do
      {:ok, execution_id, index} ->
        do_init(project_id, execution_id, index)

      :error ->
        {:error, :not_found}
    end
  end

  defp do_init(project_id, execution_id, index) do
    case Orchestration.subscribe_stream_topic(
           project_id,
           execution_id,
           index,
           self()
         ) do
      {:ok, initial, ref} ->
        {:ok,
         Topic.new(
           %{
             producer: initial.producer,
             buffer: initial.buffer,
             timeoutMs: initial.timeoutMs,
             openedAt: initial.openedAt,
             closure: build_closure(initial.closure),
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

  defp process_notification({:item_appended, sequence, value, created_at}, topic) do
    tail_size = topic.state.tail_size || 200

    item = build_item({sequence, value, created_at})
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

  defp process_notification({:closed, reason, error, closed_at}, topic) do
    closure = %{reason: reason, error: error, closedAt: closed_at}
    Topic.set(topic, [:closure], closure)
  end

  defp build_item({sequence, value, created_at}) do
    %{
      sequence: sequence,
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

  # Split an opaque stream id back into (execution_id, index). The
  # separator is `_` — execution ids use alphanumerics + `:`, so the last
  # `_` unambiguously marks the index suffix.
  defp parse_id(id) when is_binary(id) do
    case String.split(id, "_") do
      parts when length(parts) >= 2 ->
        {index_str, execution_parts} = List.pop_at(parts, -1)

        with {index, ""} when index >= 0 <- Integer.parse(index_str),
             execution_id when execution_id != "" <-
               Enum.join(execution_parts, "_") do
          {:ok, execution_id, index}
        else
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp parse_id(_), do: :error
end
