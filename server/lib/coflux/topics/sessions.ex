defmodule Coflux.Topics.Sessions do
  @moduledoc "The worker sessions of a workspace, keyed by session id."

  use Topical.Topic, route: ["workspaces", :workspace_id, "sessions"]

  alias Coflux.Orchestration
  alias Coflux.Topics.Diff
  alias Coflux.Topics.Sessions.Model

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.subscribe(project_id, {:sessions, workspace_id}, self()) do
      {:ok, events, ref} ->
        {model, _dirty} = Model.fold(Model.new(), events)
        {:ok, Topic.new(Model.project(model), %{model: model, ref: ref})}

      {:error, :workspace_invalid} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, events}, topic) do
    {model, dirty} = Model.fold(topic.state.model, events)

    topic =
      Enum.reduce(dirty, topic, fn id, topic ->
        Diff.apply(topic, [id], Map.get(topic.value, id), Model.project_entry(model, id))
      end)

    {:ok, %{topic | state: %{topic.state | model: model}}}
  end
end

defmodule Coflux.Topics.Sessions.Model do
  @moduledoc false

  import Kernel, except: [apply: 2]

  alias Coflux.Events.{
    SessionConnected,
    SessionEnded,
    SessionExecuting,
    SessionExecutions,
    SessionUpdated
  }

  def new, do: %{}

  def fold(model, events) do
    Enum.reduce(events, {model, MapSet.new()}, fn event, {model, dirty} ->
      {model, keys} = apply(model, event)
      {model, Enum.into(keys, dirty)}
    end)
  end

  def apply(model, %SessionUpdated{} = e) do
    session = %{
      connected: e.connected,
      executing: e.executing,
      concurrency: e.concurrency,
      pool: e.pool,
      targets: e.targets,
      provides: e.provides,
      accepts: e.accepts,
      worker_state: e.worker_state,
      executions: e.executions
    }

    {Map.put(model, e.session, session), [e.session]}
  end

  def apply(model, %SessionEnded{} = e), do: {Map.delete(model, e.session), [e.session]}

  def apply(model, %SessionConnected{} = e),
    do: update(model, e.session, &%{&1 | connected: e.connected})

  def apply(model, %SessionExecuting{} = e),
    do: update(model, e.session, &%{&1 | executing: e.executing})

  def apply(model, %SessionExecutions{} = e),
    do: update(model, e.session, &%{&1 | executions: e.executions})

  defp update(model, id, fun) do
    case Map.fetch(model, id) do
      {:ok, session} -> {Map.put(model, id, fun.(session)), [id]}
      :error -> {model, []}
    end
  end

  def project(model), do: Map.new(model, fn {id, _} -> {id, project_entry(model, id)} end)

  def project_entry(model, id) do
    case Map.fetch(model, id) do
      {:ok, session} ->
        %{
          connected: session.connected,
          executing: session.executing,
          concurrency: session.concurrency,
          poolName: session.pool,
          targets: session.targets,
          provides: session.provides,
          accepts: session.accepts,
          workerState: session.worker_state,
          executions: session.executions
        }

      :error ->
        nil
    end
  end
end
