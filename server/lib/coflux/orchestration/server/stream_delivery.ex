defmodule Coflux.Orchestration.Server.StreamDelivery do
  @moduledoc """
  Getting a stream's items from its producer to its consumers, and the
  backpressure in both directions.

  Owns three pieces of server state, none of which has a row behind it:

    * `stream_producers` - per-stream producer state, present only when
      the producer opted into backpressure by registering with a buffer.
      Holds the credit granted so far, in sequence space, so a producer
      resuming a paused stream picks up where the last one left off.

    * `stream_subscriptions` - one entry per open consumer subscription,
      keyed by `{consumer_execution_id, subscription_id}` so concurrent
      consumer adapters, each counting subscriptions from zero, cannot
      collide.

    * `stream_subscribers` - the reverse index, stream to subscriptions,
      so an append can find who wants it without scanning.

  Delivery to a consumer is credit-gated: it declares a `prefetch` window
  on subscribe and reports progress with acks, and items are pushed only
  while `delivered - acked < prefetch`. Nothing is lost by not pushing -
  items are durable, and the next ack pumps them from the database.

  Credit for the producer is measured against the *slowest* subscriber's
  acknowledged position, recomputed on demand rather than cached, since it
  changes often and is cheap to derive.

  Subscriptions are dropped when the consumer unsubscribes, when its
  execution terminates, when its session disconnects, or when the stream
  closes - whichever comes first.
  """

  alias Coflux.Events.{StreamClosed, StreamItemAppended, StreamRegistered}

  alias Coflux.Orchestration.{Ids, Runs, Streams, Values, Workspaces}

  alias Coflux.Orchestration.Server.{Archives, Commands, Effects, Resolve, State}

  # How many backlog items one pump reads from the database at a time.
  @backlog_page_size 1024

  # Producer-side backpressure state for a stream, (re)initialised on
  # every registration. A resuming execution replaces the entry left by
  # the suspended one: the session changes, and its credit starts at the
  # head it was told to sequence from.
  def init_stream_producer(state, stream, _execution_external_id, nil, _head, _session_id) do
    # buffer=nil means the producer has opted out of backpressure — no
    # tracking required on the server side. It'll emit freely and the
    # adapter's driver never waits. Drop anything a previous producer of
    # the stream left behind.
    drop_stream_producer(state, stream.id)
  end

  def init_stream_producer(state, stream, execution_external_id, buffer, head, session_id)
      when is_integer(buffer) and buffer >= 0 do
    put_in(state.stream_producers[stream.id], %{
      buffer: buffer,
      demand_granted: head + 1,
      base: head,
      session_id: session_id,
      execution_external_id: execution_external_id,
      index: stream.index
    })
  end

  # Recompute the target demand for one stream and, if it's grown,
  # send a delta grant to the producer's session.
  #
  # Formula:
  #   target = watermark + buffer + (1 if has_subscribers else 0)
  #
  # `watermark` is the *slowest* subscriber's acknowledged position, so
  # `buffer` means what it claims: how far ahead of actual consumption
  # the producer may run. Measuring against the fastest subscriber (or
  # against delivery rather than acknowledgement) would let the producer
  # run arbitrarily far ahead of a slow consumer, which is the thing the
  # budget exists to prevent.
  #
  # The +1 on subscriber presence is what makes buffer=0 lockstep rather
  # than deadlock: the consumer needs one item in hand before it can ack
  # anything.
  #
  # With no subscribers the watermark is 0, so a producer may pre-warm up
  # to `buffer` items before anyone attaches.
  #
  # demand_granted is monotonic; if the target drops (e.g. a slower
  # consumer joined, pulling the minimum down) we don't claw back —
  # future grants just wait until consumption passes the old high-water
  # mark. The target *rising* does have to be noticed, though, which is
  # why drop_subscription refreshes: losing the slowest subscriber
  # raises the minimum, and nothing else would recompute it.

  # Recompute the target demand for one stream and, if it's grown,
  # send a delta grant to the producer's session.
  #
  # Formula:
  #   target = watermark + buffer + (1 if has_subscribers else 0)
  #
  # `watermark` is the *slowest* subscriber's acknowledged position, so
  # `buffer` means what it claims: how far ahead of actual consumption
  # the producer may run. Measuring against the fastest subscriber (or
  # against delivery rather than acknowledgement) would let the producer
  # run arbitrarily far ahead of a slow consumer, which is the thing the
  # budget exists to prevent.
  #
  # The +1 on subscriber presence is what makes buffer=0 lockstep rather
  # than deadlock: the consumer needs one item in hand before it can ack
  # anything.
  #
  # With no subscribers the watermark is 0, so a producer may pre-warm up
  # to `buffer` items before anyone attaches.
  #
  # demand_granted is monotonic; if the target drops (e.g. a slower
  # consumer joined, pulling the minimum down) we don't claw back —
  # future grants just wait until consumption passes the old high-water
  # mark. The target *rising* does have to be noticed, though, which is
  # why drop_subscription refreshes: losing the slowest subscriber
  # raises the minimum, and nothing else would recompute it.
  def refresh_stream_demand(state, stream_id) do
    case Map.fetch(state.stream_producers, stream_id) do
      :error ->
        state

      {:ok, %{session_id: nil}} ->
        # Producer's session is gone — typically because the producer
        # execution has long since terminated and we rebuilt its in-memory
        # state for a late subscriber. There's nothing to grant demand to;
        # the stream is durable in the DB and backlog reads don't consume
        # credits.
        state

      {:ok, producer} ->
        # The producer's stream_producers entry can outlive its session
        # (e.g. a subscription is dropped during that session's teardown).
        # send_session would raise on the missing session, so treat that
        # like session_id: nil above.
        if not Map.has_key?(state.sessions, producer.session_id) do
          state
        else
          refresh_stream_demand_for(state, stream_id, producer)
        end
    end
  end

  def refresh_stream_demand_for(state, stream_id, producer) do
    target =
      if has_stream_subscribers?(state, stream_id) do
        slowest_ack_watermark(state, stream_id) + producer.buffer + 1
      else
        # Nobody attached: pre-warm `buffer` items past where the stream
        # stood when this producer registered. A fresh stream warms from
        # the start; one resumed after a suspend warms from its head,
        # rather than being held to a budget its predecessor already used.
        Map.get(producer, :base, -1) + 1 + producer.buffer
      end

    # A consumer suspended waiting on a sequence has no subscription to
    # ack through, and only comes back once that item exists. Its wait is
    # demand for it — otherwise a lockstep producer is never granted the
    # credit that would wake its own consumer, and the two wait on each
    # other forever.
    target =
      case highest_waited_sequence(state, stream_id) do
        nil -> target
        sequence -> max(target, sequence + 1)
      end

    delta = target - producer.demand_granted

    if delta > 0 do
      state
      |> put_in([Access.key(:stream_producers), stream_id, :demand_granted], target)
      |> Effects.command(
        producer.session_id,
        Commands.stream_demand(producer.execution_external_id, producer.index, delta)
      )
    else
      state
    end
  end

  def highest_waited_sequence(state, stream_id) do
    state.stream_dependency_keys
    |> Map.get(stream_id, MapSet.new())
    |> Enum.map(fn {:stream, _stream_id, sequence} -> sequence end)
    |> Enum.max(fn -> nil end)
  end

  def has_stream_subscribers?(state, stream_id) do
    case Map.get(state.stream_subscribers, stream_id) do
      nil -> false
      set -> MapSet.size(set) > 0
    end
  end

  def slowest_ack_watermark(state, stream_id) do
    watermarks =
      state.stream_subscribers
      |> Map.get(stream_id, MapSet.new())
      |> Enum.flat_map(fn sub_key ->
        case Map.get(state.stream_subscriptions, sub_key) do
          nil -> []
          sub -> [ack_watermark(sub)]
        end
      end)

    case watermarks do
      [] -> 0
      watermarks -> Enum.min(watermarks)
    end
  end

  # How far this subscriber has got, in sequence space.
  #
  # With nothing outstanding, the consumer has processed (or been
  # stride-skipped past) everything below `cursor` — that's the honest
  # position, and it's what keeps a stride subscriber from stalling the
  # producer over sequences it will never be sent.
  #
  # Otherwise the oldest unacked item sits at or above `acked_seq + 1`.
  # Using that is conservative: it can understate progress when delivery
  # is sparse, which errs towards producing less rather than more.

  # How far this subscriber has got, in sequence space.
  #
  # With nothing outstanding, the consumer has processed (or been
  # stride-skipped past) everything below `cursor` — that's the honest
  # position, and it's what keeps a stride subscriber from stalling the
  # producer over sequences it will never be sent.
  #
  # Otherwise the oldest unacked item sits at or above `acked_seq + 1`.
  # Using that is conservative: it can understate progress when delivery
  # is sparse, which errs towards producing less rather than more.
  def ack_watermark(%{delivered: delivered, acked_count: acked_count, cursor: cursor})
      when delivered == acked_count,
      do: cursor

  def ack_watermark(%{acked_seq: acked_seq}), do: acked_seq + 1

  def drop_stream_producer(state, stream_id) do
    Map.update!(state, :stream_producers, &Map.delete(&1, stream_id))
  end

  # Lazily rebuild stream_producer state from the DB if it's missing.
  # Used after server restart — in-memory producer state is gone but
  # the registration still has the config. We rebuild on first append or
  # subscribe for a given stream, recovering flow control.
  #
  # ``session_id`` is the internal id of the producer's current session;
  # supply ``nil`` if not known, in which case demand grants will be
  # deferred until the session is resolvable.

  # Lazily rebuild stream_producer state from the DB if it's missing.
  # Used after server restart — in-memory producer state is gone but
  # the registration still has the config. We rebuild on first append or
  # subscribe for a given stream, recovering flow control.
  #
  # ``session_id`` is the internal id of the producer's current session;
  # supply ``nil`` if not known, in which case demand grants will be
  # deferred until the session is resolvable.
  def ensure_stream_producer(state, stream_id, session_id) do
    if Map.has_key?(state.stream_producers, stream_id) do
      state
    else
      case Streams.get_config(state.db, stream_id) do
        {:ok, {nil, _timeout_ms}} ->
          # Stream opted out of backpressure; nothing to track.
          state

        {:ok, {buffer, _timeout_ms}} when is_integer(buffer) ->
          # Reconstruct state. demand_granted starts at items already
          # produced — we assume earlier-us granted enough for those,
          # and rely on the producer having kept its local credit
          # counter consistent.
          {:ok, head} = Streams.get_stream_head(state.db, stream_id)
          {:ok, stream} = Streams.get_stream(state.db, stream_id)

          put_in(state.stream_producers[stream_id], %{
            buffer: buffer,
            demand_granted: head + 1,
            session_id: session_id,
            execution_external_id: Resolve.stream_producer(state.db, stream_id),
            index: stream.index
          })

        {:error, :not_found} ->
          state
      end
    end
  end

  # The session the stream's current producer is running on, if it's live.

  # The session the stream's current producer is running on, if it's live.
  def producer_session_id(state, stream_id) do
    case Resolve.stream_producer(state.db, stream_id) do
      nil ->
        nil

      ext_id ->
        case State.session_for_execution(state, ext_id) do
          {:ok, sid} -> sid
          :error -> nil
        end
    end
  end

  # The stream a producer means by `index`: the one with that step index
  # on the producer's own step.

  # The stream a producer means by `index`: the one with that step index
  # on the producer's own step.
  def resolve_step_stream(db, execution_id, index) do
    {:ok, {step_id, _workspace_id, _attempt}} = Runs.get_execution_location(db, execution_id)

    case Streams.get_stream_by_step_index(db, step_id, index) do
      {:ok, stream_id} -> {:ok, stream_id}
      {:error, :not_found} -> {:error, :not_registered}
    end
  end

  # An execution registered on a stream: either opening it or resuming it
  # after a suspend. The run topic keeps streams under their step, with
  # the attempts that have produced into each.
  def notify_stream_registered(state, stream, execution_id, _registration, buffer, timeout_ms) do
    {:ok, {_r, _s, attempt}} = Runs.get_execution_key(state.db, execution_id)

    {:ok, workspace_external_id} =
      Workspaces.get_workspace_external_id(state.db, stream.workspace_id)

    ext_id = Ids.stream(stream.run_external_id, stream.step_number, stream.index)

    Effects.emit(state, %StreamRegistered{
      run: stream.run_external_id,
      step: stream.step_number,
      index: stream.index,
      stream: ext_id,
      workspace: workspace_external_id,
      position: stream.position,
      attempt: attempt,
      buffer: buffer,
      timeout_ms: timeout_ms,
      opened_at: stream.created_at,
      module: stream.module,
      target: stream.target
    })
  end

  def notify_stream_item_appended(state, stream_id, execution_id, sequence, value, created_at) do
    {:ok, ext_id} = stream_external_id_for(state.db, stream_id)
    topic = {:stream, ext_id}

    # Skip the build_value (which hits the DB to resolve refs) when the
    # inspection topic has no active subscribers.
    if Map.has_key?(state.topics, topic) do
      {:ok, {_r, _s, attempt}} = Runs.get_execution_key(state.db, execution_id)
      resolved = Resolve.value(state.db, Values.normalize(value))

      Effects.emit(state, %StreamItemAppended{
        stream: ext_id,
        sequence: sequence,
        value: resolved,
        attempt: attempt,
        created_at: created_at
      })
    else
      state
    end
  end

  # Fire the stream-closed notification on run + stream topics. `reason`
  # is a semantic atom from the full set (:complete / :errored /
  # :cancelled / :abandoned / :crashed / :timeout / :recurred) — Studio
  # renders each directly in UI-appropriate language, rather than
  # displaying a fabricated exception type. `error` is non-nil only for
  # :errored. `execution_id` is the execution that closed the stream.

  # Fire the stream-closed notification on run + stream topics. `reason`
  # is a semantic atom from the full set (:complete / :errored /
  # :cancelled / :abandoned / :crashed / :timeout / :recurred) — Studio
  # renders each directly in UI-appropriate language, rather than
  # displaying a fabricated exception type. `error` is non-nil only for
  # :errored. `execution_id` is the execution that closed the stream.
  def notify_stream_closed(state, stream_id, execution_id, reason, error, closed_at) do
    {:ok, stream} = Streams.get_stream(state.db, stream_id)
    ext_id = Ids.stream(stream.run_external_id, stream.step_number, stream.index)
    {:ok, {_r, _s, attempt}} = Runs.get_execution_key(state.db, execution_id)

    encoded_error = encode_stream_error_summary(error)
    reason_str = if reason, do: Atom.to_string(reason)

    Effects.emit(state, %StreamClosed{
      run: stream.run_external_id,
      step: stream.step_number,
      index: stream.index,
      stream: ext_id,
      reason: reason_str,
      error: encoded_error,
      attempt: attempt,
      closed_at: closed_at
    })
  end

  def build_stream_topic_closure(state, stream_id) do
    case Streams.get_stream_closure(state.db, stream_id) do
      {:ok, nil} ->
        nil

      {:ok, {reason, stored_error, closed_by, closed_at}} ->
        # DB stores `:lifecycle` for closures driven by an execution
        # ending; on read we resolve that to the specific cause
        # (:cancelled / :abandoned / :crashed / :timeout / :errored /
        # :recurred) so clients don't need to know about the internal
        # bucket. `error` only accompanies a genuine :errored close.
        {effective_reason, effective_error} =
          Resolve.closure_reason(state.db, reason, stored_error, closed_by)

        {:ok, {_r, _s, attempt}} = Runs.get_execution_key(state.db, closed_by)

        %{
          reason: if(effective_reason, do: Atom.to_string(effective_reason)),
          error: encode_stream_error_summary(effective_error),
          attempt: attempt,
          closedAt: closed_at
        }
    end
  end

  def encode_stream_error_summary(nil), do: nil

  def encode_stream_error_summary({type, message, frames}) do
    %{
      type: type,
      message: message,
      frames:
        Enum.map(frames, fn {file, line, name, code} ->
          %{file: file, line: line, name: name, code: code}
        end)
    }
  end

  def stream_external_id_for(db, stream_id) do
    case Streams.get_stream(db, stream_id) do
      {:ok, stream} ->
        {:ok, Ids.stream(stream.run_external_id, stream.step_number, stream.index)}

      err ->
        err
    end
  end

  # --- Stream subscription helpers ---

  # Does a `sequence` pass a subscription's stride? A stride is
  # ``%{"start" => int, "stop" => int | nil, "step" => int}`` — the
  # client composes any chain of slice/partition/stride calls into one
  # before sending. `nil` means the trivial identity stride (everything).
  def stride_matches?(nil, _sequence), do: true

  def stride_matches?(%{"start" => start, "stop" => stop, "step" => step}, sequence) do
    sequence >= start and
      (stop == nil or sequence < stop) and
      rem(sequence - start, step) == 0
  end

  # Is `cursor` past the stride's stop? Lets us close the subscription
  # early once nothing more can match (i.e. the upper bound is finite
  # and we've reached it).

  # Is `cursor` past the stride's stop? Lets us close the subscription
  # early once nothing more can match (i.e. the upper bound is finite
  # and we've reached it).
  def stride_exhausted?(nil, _cursor), do: false

  def stride_exhausted?(%{"stop" => stop}, cursor) when is_integer(stop),
    do: cursor >= stop

  def stride_exhausted?(_stride, _cursor), do: false

  # Per-fetch page size when draining backlog for a newly subscribed
  # consumer. Keeps any single DB read bounded and lets us push each page
  # to the session before loading the next.
  @backlog_page_size 1024

  # How many more items may be sent to this subscriber before it has to
  # acknowledge some. This is what bounds the consumer's in-memory queue;
  # anything we hold back stays durable in the DB and goes out on the
  # next pump.

  # How many more items may be sent to this subscriber before it has to
  # acknowledge some. This is what bounds the consumer's in-memory queue;
  # anything we hold back stays durable in the DB and goes out on the
  # next pump.
  def available_credit(%{prefetch: prefetch, delivered: delivered, acked_count: acked_count}) do
    prefetch - (delivered - acked_count)
  end

  # Deliver as much as credit allows, then settle any pending closure.
  # Driven on subscribe, on ack (credit freed up), and after an append
  # that couldn't be sent inline.

  # Deliver as much as credit allows, then settle any pending closure.
  # Driven on subscribe, on ack (credit freed up), and after an append
  # that couldn't be sent inline.
  def pump_subscription(state, key) do
    state
    |> push_backlog_page(key)
    |> maybe_finish_subscription(key)
  end

  # Send items from the DB for a subscriber that's behind — either newly
  # subscribed, or previously held back for want of credit. Pages the DB
  # reads + session pushes so a very long stream doesn't materialise the
  # entire tail in memory at once.

  # Send items from the DB for a subscriber that's behind — either newly
  # subscribed, or previously held back for want of credit. Pages the DB
  # reads + session pushes so a very long stream doesn't materialise the
  # entire tail in memory at once.
  def push_backlog_page(state, key) do
    case Map.fetch(state.stream_subscriptions, key) do
      :error ->
        state

      {:ok, sub} ->
        if available_credit(sub) <= 0 do
          state
        else
          {:ok, items} =
            Streams.get_stream_items(state.db, sub.stream_id, sub.cursor, @backlog_page_size)

          if items == [] do
            state
          else
            state = push_backlog_items(state, key, sub, items)

            # Stop if the subscription was dropped (stride exhausted), if
            # nothing in this page moved us forward, if we've run out of
            # credit, or if the page was short (tail reached). Otherwise
            # keep paging.
            case Map.fetch(state.stream_subscriptions, key) do
              :error ->
                state

              {:ok, next_sub} ->
                cond do
                  next_sub.cursor == sub.cursor -> state
                  available_credit(next_sub) <= 0 -> state
                  length(items) < @backlog_page_size -> state
                  true -> push_backlog_page(state, key)
                end
            end
          end
        end
    end
  end

  def push_backlog_items(state, key, sub, items) do
    {_consumer_execution_id, subscription_id} = key
    credit = available_credit(sub)

    # Walk the page in order, taking matching items until credit runs out.
    # `advance_to` tracks how far the cursor may honestly move: past
    # everything we've decided about, and no further. An item we had no
    # credit for must be left for the next pump to re-read, so the cursor
    # has to stop short of it — advancing past the whole page (as an
    # unbounded push could) would silently drop it.
    {selected, count, advance_to, exhausted} =
      Enum.reduce_while(items, {[], 0, sub.cursor, false}, fn {sequence, value, _at},
                                                              {acc, count, _advance, _exhausted} ->
        cond do
          stride_exhausted?(sub.stride, sequence) ->
            {:halt, {acc, count, sequence, true}}

          not stride_matches?(sub.stride, sequence) ->
            # Skipped sequences cost no credit — advance past them freely,
            # otherwise we'd re-fetch them forever.
            {:cont, {acc, count, sequence + 1, false}}

          count < credit ->
            {:cont, {[{sequence, value} | acc], count + 1, sequence + 1, false}}

          true ->
            {:halt, {acc, count, sequence, false}}
        end
      end)

    state =
      if selected == [] do
        state
      else
        resolved_items =
          selected
          |> Enum.reverse()
          |> Enum.map(fn {sequence, value} -> [sequence, Resolve.value(state.db, value)] end)

        send_to_consumer(
          state,
          sub,
          Commands.stream_items(
            sub.consumer_execution_external_id,
            subscription_id,
            resolved_items
          )
        )
      end

    state =
      update_in(
        state.stream_subscriptions[key],
        fn s -> %{s | cursor: advance_to, delivered: s.delivered + count} end
      )

    state =
      if exhausted or stride_exhausted?(sub.stride, advance_to) do
        # The stride has reached its stop — nothing more can match, so
        # close now rather than leaving the consumer waiting for a close
        # that would only arrive when the producer finishes. This is a
        # "complete" outcome from their perspective: they've received
        # everything that was addressed to them.
        finish_subscription(state, key, "complete", nil)
      else
        state
      end

    # The push moved this consumer's cursor forward, which may have moved
    # the slowest-subscriber watermark and unblocked the producer.
    refresh_stream_demand(state, sub.stream_id)
  end

  # Resolve the consumer's current session and send, skipping if the
  # execution is no longer live on any session (reconnect window, etc.).

  # Resolve the consumer's current session and send, skipping if the
  # execution is no longer live on any session (reconnect window, etc.).
  def send_to_consumer(state, sub, payload) do
    case State.session_for_execution(state, sub.consumer_execution_external_id) do
      {:ok, session_id} -> Effects.command(state, session_id, payload)
      :error -> state
    end
  end

  # Push a freshly-appended item to every subscriber of this stream.
  #
  # This is a fast path: it exists so the just-appended value can be sent
  # straight from memory instead of being re-read from SQLite by the
  # pump. That means it necessarily restates the delivery rules
  # (stride skip, credit check, cursor advance, stride-exhaustion close)
  # that push_backlog_items implements over a page of items.
  #
  # push_backlog_items is the authority on those rules. Any change to
  # stride or credit semantics must be made there *and* mirrored here.
  # Anything this path declines to send is left untouched and durable,
  # so the pump re-reads it in order — declining is always safe.

  # Push a freshly-appended item to every subscriber of this stream.
  #
  # This is a fast path: it exists so the just-appended value can be sent
  # straight from memory instead of being re-read from SQLite by the
  # pump. That means it necessarily restates the delivery rules
  # (stride skip, credit check, cursor advance, stride-exhaustion close)
  # that push_backlog_items implements over a page of items.
  #
  # push_backlog_items is the authority on those rules. Any change to
  # stride or credit semantics must be made there *and* mirrored here.
  # Anything this path declines to send is left untouched and durable,
  # so the pump re-reads it in order — declining is always safe.
  def push_stream_item(state, stream_id, sequence, value) do
    subscribers = Map.get(state.stream_subscribers, stream_id, MapSet.new())

    state =
      Enum.reduce(subscribers, state, fn key, state ->
        {_consumer_execution_id, subscription_id} = key
        sub = Map.fetch!(state.stream_subscriptions, key)

        cond do
          sequence != sub.cursor ->
            # Not the sequence this subscriber is waiting for — either it
            # already has this one via the backlog, or it's behind and this
            # item is ahead of its cursor. Either way the item is durable,
            # so leave it for the pump to read in order.
            state

          not stride_matches?(sub.stride, sequence) ->
            # Advance the cursor past non-matching sequences too (matching
            # push_backlog_items' behaviour): demand grants are derived
            # from subscriber positions, so leaving the cursor behind would
            # permanently stall a bounded-buffer producer against a
            # partition/slice consumer whose stride skips this sequence.
            # Skipping costs no credit — nothing is delivered.
            state =
              update_in(
                state.stream_subscriptions[key],
                &Map.put(&1, :cursor, sequence + 1)
              )

            if stride_exhausted?(sub.stride, sequence + 1) do
              finish_subscription(state, key, "complete", nil)
            else
              state
            end

          available_credit(sub) <= 0 ->
            # Consumer's window is full. Hold the item back *without*
            # advancing the cursor — it's already durable, and the next ack
            # will pump it from the DB in order.
            state

          true ->
            # Value came off the wire in parse form (ext-id refs, no metadata).
            # Normalise + resolve to match the form the pump sends; the WS
            # handler composes to wire JSON.
            resolved = Resolve.value(state.db, Values.normalize(value))
            item = [sequence, resolved]

            state =
              send_to_consumer(
                state,
                sub,
                Commands.stream_items(
                  sub.consumer_execution_external_id,
                  subscription_id,
                  [item]
                )
              )

            state =
              update_in(
                state.stream_subscriptions[key],
                fn s -> %{s | cursor: sequence + 1, delivered: s.delivered + 1} end
              )

            # If the stride has reached its stop, close the subscription
            # early — no more items will match. Treated as a "complete"
            # close for the consumer (they got everything that was
            # addressed to them).
            if stride_exhausted?(sub.stride, sequence + 1) do
              finish_subscription(state, key, "complete", nil)
            else
              state
            end
        end
      end)

    # Subscriber cursors may have advanced — recompute demand once per
    # stream (cheaper than once per subscriber, same result).
    refresh_stream_demand(state, stream_id)
  end

  # On close, tell every subscriber. `reason` is a semantic atom
  # (`:complete | :errored | :cancelled | :abandoned | :crashed |
  # :timeout`) — the client chooses how to represent each in its own
  # idiom. `error` is non-nil only when `reason == :errored`, carrying
  # the producer's actual `{type, message, frames}`.
  #
  # The closure is recorded as *pending* rather than sent immediately:
  # with credit-gated delivery a subscriber may still be behind the
  # stream head, and emitting the close now would land it ahead of the
  # items the consumer hasn't been given room for yet. Each subscription
  # emits its close once it has drained.

  # On close, tell every subscriber. `reason` is a semantic atom
  # (`:complete | :errored | :cancelled | :abandoned | :crashed |
  # :timeout`) — the client chooses how to represent each in its own
  # idiom. `error` is non-nil only when `reason == :errored`, carrying
  # the producer's actual `{type, message, frames}`.
  #
  # The closure is recorded as *pending* rather than sent immediately:
  # with credit-gated delivery a subscriber may still be behind the
  # stream head, and emitting the close now would land it ahead of the
  # items the consumer hasn't been given room for yet. Each subscription
  # emits its close once it has drained.
  def push_stream_closed(state, stream_id, reason, error) do
    subscribers = Map.get(state.stream_subscribers, stream_id, MapSet.new())

    reason_str = if reason, do: Atom.to_string(reason)
    encoded_error = encode_stream_error(error)

    # The stream is closed in the DB by the time we get here, so the head
    # is final. Read it once and record it on each pending close rather
    # than re-querying per subscriber per ack for the rest of the drain.
    {:ok, head} = Streams.get_stream_head(state.db, stream_id)

    Enum.reduce(subscribers, state, fn key, state ->
      state
      |> mark_pending_close(key, reason_str, encoded_error, head)
      |> maybe_finish_subscription(key)
    end)
  end

  # `head` is the stream's final sequence — safe to cache on the pending
  # close because a stream only gets one closure and no item may be
  # appended after it.

  # `head` is the stream's final sequence — safe to cache on the pending
  # close because a stream only gets one closure and no item may be
  # appended after it.
  def mark_pending_close(state, key, reason, error, head) do
    case Map.fetch(state.stream_subscriptions, key) do
      :error -> state
      {:ok, _} -> put_in(state.stream_subscriptions[key].pending_close, {reason, error, head})
    end
  end

  # Emit a recorded closure, but only once the consumer has been sent
  # everything it is going to get. Until then the close waits — the
  # subscription stays alive so later acks can pump the remainder.

  # Emit a recorded closure, but only once the consumer has been sent
  # everything it is going to get. Until then the close waits — the
  # subscription stays alive so later acks can pump the remainder.
  def maybe_finish_subscription(state, key) do
    case Map.fetch(state.stream_subscriptions, key) do
      :error ->
        state

      {:ok, %{pending_close: nil}} ->
        state

      {:ok, %{pending_close: {reason, error, head}} = sub} ->
        if sub.cursor > head do
          finish_subscription(state, key, reason, error)
        else
          state
        end
    end
  end

  # Send the terminal close for a subscription and drop it.

  # Send the terminal close for a subscription and drop it.
  def finish_subscription(state, key, reason, error) do
    case Map.fetch(state.stream_subscriptions, key) do
      :error ->
        state

      {:ok, sub} ->
        {_consumer_execution_id, subscription_id} = key

        state
        |> send_to_consumer(
          sub,
          Commands.stream_closed(
            sub.consumer_execution_external_id,
            subscription_id,
            reason,
            error
          )
        )
        |> drop_subscription(key)
    end
  end

  # Wire encoding for the producer's actual error on an `:errored` close.
  # Frames are included so consumers can reconstruct tracebacks for
  # debuggability. Lifecycle reasons (:cancelled/:abandoned/...) don't
  # go through this — they're conveyed as the reason atom alone.

  # Wire encoding for the producer's actual error on an `:errored` close.
  # Frames are included so consumers can reconstruct tracebacks for
  # debuggability. Lifecycle reasons (:cancelled/:abandoned/...) don't
  # go through this — they're conveyed as the reason atom alone.
  def encode_stream_error(nil), do: nil

  def encode_stream_error({type, message, frames}) do
    %{
      "type" => type,
      "message" => message,
      "frames" =>
        Enum.map(frames, fn {file, line, name, code} ->
          [file, line, name, code]
        end)
    }
  end

  # If a subscription attaches to an already-closed stream, record the
  # closure as pending. The pump emits it once the backlog has been
  # delivered — which, for a consumer whose prefetch is smaller than the
  # backlog, takes several rounds of acks.

  # If a subscription attaches to an already-closed stream, record the
  # closure as pending. The pump emits it once the backlog has been
  # delivered — which, for a consumer whose prefetch is smaller than the
  # backlog, takes several rounds of acks.
  def mark_closed_if_closed(state, key) do
    case Map.fetch(state.stream_subscriptions, key) do
      :error ->
        state

      {:ok, sub} ->
        do_mark_closed(state, sub, key)
    end
  end

  def do_mark_closed(state, sub, key) do
    case Streams.get_stream_closure(state.db, sub.stream_id) do
      {:ok, nil} ->
        state

      {:ok, {reason, stored_error, closed_by, _closed_at}} ->
        # Resolve :lifecycle to the specific cause for the wire — same
        # treatment as live closures so late subscribers don't get a
        # less-informative signal than those attached at close time.
        {effective_reason, effective_error} =
          Resolve.closure_reason(state.db, reason, stored_error, closed_by)

        reason_str = if effective_reason, do: Atom.to_string(effective_reason)

        # Already closed, so the head is final — see mark_pending_close.
        {:ok, head} = Streams.get_stream_head(state.db, sub.stream_id)

        mark_pending_close(
          state,
          key,
          reason_str,
          encode_stream_error(effective_error),
          head
        )
    end
  end

  def drop_subscription(state, key) do
    case Map.fetch(state.stream_subscriptions, key) do
      :error ->
        state

      {:ok, sub} ->
        stream_key = sub.stream_id

        state
        |> Map.update!(:stream_subscriptions, &Map.delete(&1, key))
        |> Map.update!(:stream_subscribers, fn m ->
          case Map.get(m, stream_key) do
            nil ->
              m

            subs ->
              remaining = MapSet.delete(subs, key)

              if MapSet.size(remaining) == 0 do
                Map.delete(m, stream_key)
              else
                Map.put(m, stream_key, remaining)
              end
          end
        end)
        # Demand is measured against the *slowest* subscriber, so losing
        # one can only raise the target — and if the departing subscriber
        # was the slowest, this is the last chance to notice. The
        # remaining subscribers may already have acked everything they'll
        # ever ack, and a blocked producer appends nothing, so neither of
        # the other refresh sites (ack_stream, push_stream_item) would
        # fire again and the producer would wait forever.
        |> refresh_stream_demand(stream_key)
    end
  end

  # Drop every subscription held by the disconnected session's executions.
  # Called just before the session is removed, so we can read its live
  # execution set directly.

  # Drop every subscription held by the disconnected session's executions.
  # Called just before the session is removed, so we can read its live
  # execution set directly.
  def drop_session_subscriptions(state, session_id) do
    session = Map.fetch!(state.sessions, session_id)

    session.starting
    |> MapSet.union(session.executing)
    |> Enum.reduce(state, fn ext_id, state ->
      case Map.fetch(state.execution_ids, ext_id) do
        {:ok, execution_id} -> drop_execution_subscriptions(state, execution_id)
        :error -> state
      end
    end)
  end

  # Drop every subscription owned by a terminated consumer execution so the
  # server stops pushing items and the subscription map doesn't leak. Called
  # from notify_terminated — by that point the consumer's generator iterator
  # (and thus the subscription) is definitely gone.

  # Drop every subscription owned by a terminated consumer execution so the
  # server stops pushing items and the subscription map doesn't leak. Called
  # from notify_terminated — by that point the consumer's generator iterator
  # (and thus the subscription) is definitely gone.
  def drop_execution_subscriptions(state, consumer_execution_id) do
    keys =
      state.stream_subscriptions
      |> Map.keys()
      |> Enum.filter(fn {cons_id, _sub_id} -> cons_id == consumer_execution_id end)

    Enum.reduce(keys, state, &drop_subscription(&2, &1))
  end

  # Clean up an execution's state and send an abort message to the worker.
  # If the execution has already terminated (completion recorded), there's
  # nothing to abort — skip silently. Only warn when an actively-running
  # execution unexpectedly has no session.

  @doc """
  Everything here that names a stream or an execution by internal id,
  re-expressed by external id. Internal ids do not survive an epoch
  rotation, and nothing can rebuild a live subscription from the
  database, so it is captured before the switch and restored after it.
  """
  # Everything in `stream_subscriptions` / `stream_subscribers` /
  # `stream_producers` that names a stream or an execution by internal id,
  # re-expressed by external id so it can be re-resolved after the copy.
  def capture(state) do
    subscriptions =
      Enum.flat_map(state.stream_subscriptions, fn {{_consumer_id, subscription_id}, sub} ->
        case stream_external_id_for(state.db, sub.stream_id) do
          {:ok, stream_ext_id} ->
            [{sub.consumer_execution_external_id, subscription_id, stream_ext_id, sub}]

          _ ->
            []
        end
      end)

    producers =
      Enum.flat_map(state.stream_producers, fn {stream_id, producer} ->
        case stream_external_id_for(state.db, stream_id) do
          {:ok, stream_ext_id} -> [{stream_ext_id, producer}]
          _ -> []
        end
      end)

    {subscriptions, producers}
  end

  # The inverse of capture_stream_state, against the new active database.
  # Consumer executions are resolved through the rebuilt `execution_ids`;
  # streams through resolve_stream_id, which copies a producer's run
  # forward if it wasn't in flight (a finished producer whose consumer is
  # still reading). Anything that can't be resolved is dropped, as it
  # would have been before.
  def restore(state, {subscriptions, producers}) do
    state = %{
      state
      | stream_subscriptions: %{},
        stream_subscribers: %{},
        stream_producers: %{}
    }

    state =
      Enum.reduce(subscriptions, state, fn {consumer_ext_id, subscription_id, stream_ext_id, sub},
                                           state ->
        with {:ok, consumer_id} <- Map.fetch(state.execution_ids, consumer_ext_id),
             {:ok, stream_id} <- Archives.resolve_stream_id(state, stream_ext_id) do
          key = {consumer_id, subscription_id}

          state
          |> put_in([Access.key(:stream_subscriptions), key], %{sub | stream_id: stream_id})
          |> update_in(
            [Access.key(:stream_subscribers), Access.key(stream_id, MapSet.new())],
            &MapSet.put(&1, key)
          )
        else
          _ -> state
        end
      end)

    Enum.reduce(producers, state, fn {stream_ext_id, producer}, state ->
      case Archives.resolve_stream_id(state, stream_ext_id) do
        {:ok, stream_id} -> put_in(state.stream_producers[stream_id], producer)
        _ -> state
      end
    end)
  end
end
