defmodule Coflux.Topics.Stream do
  @moduledoc """
  Inspection topic for a single stream, keyed by the stream's id
  (``<run>:<step>_<index>``). Used by the Studio UI when a user opens a
  stream dialog: it keeps a bounded tail of items (with resolved values)
  plus closure state, and receives live updates as items are appended, as
  executions register on the stream (opening it, or resuming it after a
  suspend), or as the stream is closed.
  """

  use Topical.Topic, route: ["streams", :id]

  alias Coflux.Orchestration
  alias Coflux.Topics.Diff
  alias Coflux.Topics.Stream.Model

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    id = Map.fetch!(params, :id)

    case Orchestration.subscribe(project_id, {:stream, id}, self()) do
      {:ok, events, ref} ->
        model = Model.fold(Model.new(), events)
        {:ok, Topic.new(Model.project(model), %{model: model, ref: ref})}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, events}, topic) do
    model = Model.fold(topic.state.model, events)
    topic = Diff.apply(topic, [], topic.value, Model.project(model))
    {:ok, %{topic | state: %{topic.state | model: model}}}
  end
end

defmodule Coflux.Topics.Stream.Model do
  @moduledoc """
  A stream as a fold over events. Items are kept as a bounded tail; the
  total count follows the highest sequence seen, since sequences are
  contiguous from zero.
  """

  import Kernel, except: [apply: 2]

  alias Coflux.Events.{StreamClosed, StreamItemAppended, StreamRegistered}
  alias Coflux.Orchestration.{Ids, Streams}
  alias Coflux.TopicUtils

  def new, do: nil

  def fold(model, events), do: Enum.reduce(events, model, &apply(&2, &1))

  # The first registration opens the stream; later ones add an attempt,
  # and the latest registration's config is the one in force.
  def apply(nil, %StreamRegistered{} = e) do
    %{
      id: e.stream,
      step: %{stepId: Ids.step(e.run, e.step), module: e.module, target: e.target},
      workspace: e.workspace,
      index: e.index,
      position: e.position,
      buffer: e.buffer,
      timeout_ms: e.timeout_ms,
      opened_at: e.opened_at,
      attempts: [e.attempt],
      closure: nil,
      items: [],
      total: 0
    }
  end

  def apply(model, %StreamRegistered{} = e) do
    attempts =
      if e.attempt in model.attempts, do: model.attempts, else: model.attempts ++ [e.attempt]

    %{model | attempts: attempts, buffer: e.buffer, timeout_ms: e.timeout_ms}
  end

  def apply(nil, _event), do: nil

  def apply(model, %StreamItemAppended{} = e) do
    item = %{
      sequence: e.sequence,
      value: TopicUtils.build_value(e.value),
      attempt: e.attempt,
      createdAt: e.created_at
    }

    if Enum.any?(model.items, &(&1.sequence == e.sequence)) do
      model
    else
      items = Enum.take(model.items ++ [item], -Streams.tail_size())
      %{model | items: items, total: max(model.total, e.sequence + 1)}
    end
  end

  def apply(model, %StreamClosed{} = e) do
    %{
      model
      | closure: %{reason: e.reason, error: e.error, attempt: e.attempt, closedAt: e.closed_at}
    }
  end

  def project(nil), do: %{}

  def project(model) do
    %{
      id: model.id,
      step: model.step,
      workspaceId: model.workspace,
      index: model.index,
      position: model.position,
      buffer: model.buffer,
      timeoutMs: model.timeout_ms,
      openedAt: model.opened_at,
      attempts: model.attempts,
      closure: model.closure,
      items: model.items,
      totalCount: model.total,
      tailSize: Streams.tail_size()
    }
  end
end
