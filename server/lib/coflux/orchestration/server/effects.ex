defmodule Coflux.Orchestration.Server.Effects do
  @moduledoc """
  What an operation records to happen once it finishes, and the delivery
  of it.

  An operation on the orchestration server decides things and changes
  state; telling anyone about it is deferred. Events for topic subscribers
  and commands for worker sessions are appended to buffers on the state as
  they are decided, and `flush/1` - called once, at the callback boundary
  - delivers both. Nothing between those two points touches a process.

  That buys three things. An operation cannot half-tell the world and then
  fail. What a session is told is decided against the state the operation
  left behind, not the state it passed through. And the code that decides
  is a plain function of state, so it can be moved out of the GenServer
  and tested without one.

  Ordering within a flush is the order things were recorded: commands
  first, then events. That is the order they went out in before they were
  buffered - a worker was told to act while the operation ran, and
  subscribers heard when it finished.
  """

  alias Coflux.Orchestration.Server.{Routing}

  @doc """
  Records a fact. `Coflux.Orchestration.Routing` says which topic keys
  hear about it; keys nobody listens on cost nothing beyond the struct.
  """
  def emit(state, event) do
    event
    |> Routing.route(state)
    |> Enum.reduce(state, fn key, state ->
      if Map.has_key?(state.topics, key) do
        update_in(state.notifications[key], &[event | &1 || []])
      else
        state
      end
    end)
  end

  @doc """
  Records a command for a worker session, built with
  `Coflux.Orchestration.Commands`.
  """
  def command(state, session_id, command) do
    update_in(state.commands, &[{session_id, command} | &1])
  end

  @doc "Delivers everything recorded, and clears the buffers."
  def flush(state) do
    state
    |> deliver_commands()
    |> deliver_notifications()
  end

  defp deliver_notifications(state) do
    Enum.each(state.notifications, fn {topic, notifications} ->
      notifications = Enum.reverse(notifications)

      state.topics
      |> Map.get(topic, %{})
      |> Enum.each(fn {ref, pid} ->
        send(pid, {:topic, ref, notifications})
      end)
    end)

    Map.put(state, :notifications, %{})
  end

  # Whether a session is connected is decided here rather than when the
  # command was recorded, so the answer is the one that holds when the
  # operation is done: a session that reconnected during it is sent to
  # rather than queued, and one that ended has its commands dropped -
  # there is nothing left to deliver them to.
  defp deliver_commands(state) do
    state.commands
    |> Enum.reverse()
    |> Enum.reduce(Map.put(state, :commands, []), fn {session_id, command}, state ->
      case Map.fetch(state.sessions, session_id) do
        {:ok, %{connection: nil}} ->
          update_in(state.sessions[session_id].queue, &[command | &1])

        {:ok, session} ->
          {pid, ^session_id} = state.connections[session.connection]
          send(pid, command)
          state

        :error ->
          state
      end
    end)
  end
end
