defmodule Coflux.Orchestration.Server.Commands do
  @moduledoc """
  What the server says to a worker session.

  The counterpart of `Coflux.Events`: that module is the vocabulary the
  server speaks to topic subscribers, this one is the vocabulary it speaks
  to workers. Every message a session can receive is built here, so the
  protocol can be read in one place rather than reconstructed from a dozen
  emit sites.

  Each is a tuple the worker handler (`Coflux.Handlers.Worker`) matches in
  `websocket_info/2` and turns into a wire frame, so the shapes here are
  half of a contract - changing one means changing that clause too.

  Messages are recorded with `Coflux.Orchestration.Server`'s effect buffer
  rather than sent, and delivered when the operation flushes. Nothing here
  touches a process.
  """

  @doc "Run this execution. `streams` is the step's default stream config, or nil."
  def execute(execution, module, target, arguments, run, workspace, timeout, streams, checkpoints) do
    {:execute, execution, module, target, arguments, run, workspace, timeout, streams,
     checkpoints}
  end

  @doc "Stop running this execution. The worker kills it and reports termination."
  def abort(execution), do: {:abort, execution}

  @doc """
  The answer to a request the worker is blocked on (a select). `payload` is
  `{handle_index, result}` for a handle that resolved, or `:timeout`.
  """
  def result(request_id, payload), do: {:result, request_id, payload}

  @doc "Credit for a producer to append `delta` more items to its `index`-th stream."
  def stream_demand(execution, index, delta), do: {:stream_demand, execution, index, delta}

  @doc "Whether the producer should pause the idle timer on its `index`-th stream."
  def stream_timer_pause(execution, index, paused?),
    do: {:stream_timer_pause, execution, index, paused?}

  @doc "Items for a consumer's subscription, as `[[sequence, value], ...]`."
  def stream_items(execution, subscription_id, items),
    do: {:stream_items, execution, subscription_id, items}

  @doc """
  The end of a consumer's subscription. `reason` is a string; `error` is
  non-nil only for `"errored"`.
  """
  def stream_closed(execution, subscription_id, reason, error),
    do: {:stream_closed, execution, subscription_id, reason, error}
end
