defmodule Coflux.Orchestration.Server.Listeners do
  @moduledoc """
  Who is subscribed to what, and when the server may stop.

  A subscription is a reference held by a topic process: `topics` maps a
  key to the processes watching it, and `listeners` maps back, so a
  process that dies unsubscribes from everything at once.

  The idle shutdown lives here because it asks about both halves of what
  keeps a project's server alive - no worker sessions and no listeners.
  A server with neither is doing nothing that anyone can observe, so it
  stops rather than holding a database open.
  """

  alias Coflux.MapUtils

  # How long a server with no sessions and no listeners waits before stopping.
  @idle_timeout_ms 30_000

  def add_listener(state, topic, pid) do
    ref = Process.monitor(pid)

    state =
      state
      |> put_in([Access.key(:listeners), ref], topic)
      |> put_in([Access.key(:topics), Access.key(topic, %{}), ref], pid)
      |> maybe_schedule_idle_shutdown()

    {:ok, ref, state}
  end

  def remove_listener(state, ref) do
    case Map.fetch(state.listeners, ref) do
      {:ok, topic} ->
        state
        |> Map.update!(:listeners, &Map.delete(&1, ref))
        |> Map.update!(:topics, fn topics ->
          MapUtils.delete_in(topics, [topic, ref])
        end)
    end
  end

  def maybe_schedule_idle_shutdown(state) do
    idle? = Enum.empty?(state.sessions) and Enum.empty?(state.listeners)

    cond do
      idle? and is_nil(state.idle_timer) ->
        ref = make_ref()
        timer = Process.send_after(self(), {:idle_shutdown, ref}, @idle_timeout_ms)
        %{state | idle_timer: {timer, ref}}

      not idle? and not is_nil(state.idle_timer) ->
        {timer, _ref} = state.idle_timer
        Process.cancel_timer(timer)
        %{state | idle_timer: nil}

      true ->
        state
    end
  end
end
