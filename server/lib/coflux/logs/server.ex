defmodule Coflux.Logs.Server do
  @moduledoc """
  Per-project GenServer for log storage with write buffering and partitioned storage.

  Features:
  - Buffers writes in memory for batch inserts
  - Flushes on interval or when buffer is full
  - Partitioned SQLite epoch files with size-based rotation
  - Bloom filter index for efficient cross-partition lookups
  - Pub/sub for real-time log streaming
  - Template deduplication caching (per-partition)
  """

  use GenServer

  require Logger

  alias Coflux.Logs.Store
  alias Coflux.Store.{Bloom, Epochs, Index}
  alias Exqlite.Sqlite3

  @flush_interval_ms 500
  @max_buffer_size 1000
  @rotation_size_threshold_bytes 100 * 1024 * 1024

  defmodule State do
    @moduledoc false
    defstruct [
      :project_id,
      :epochs,
      :log_index,
      :flush_timer,
      :index_task,
      index_queue: [],
      buffer: [],
      # run_id -> %{ref -> {pid, execution_id, workspace_ids}}
      subscribers: %{},
      # template_hash -> template_id (per active partition)
      template_cache: %{}
    ]
  end

  ## Client API

  def start_link(opts) do
    project_id = Keyword.fetch!(opts, :project_id)

    GenServer.start_link(__MODULE__, project_id,
      name: {:via, Registry, {Coflux.Logs.Registry, project_id}}
    )
  end

  @doc """
  Write a batch of log entries.

  Each entry should be a map with:
  - run_id: string
  - execution_id: integer
  - workspace_id: integer
  - timestamp: integer (unix ms)
  - level: integer (0-5)
  - template: string or nil
  - values: map
  """
  def write_logs(project_id, entries) when is_list(entries) do
    {:ok, server} = Coflux.Logs.Supervisor.get_server(project_id)
    GenServer.cast(server, {:write_logs, entries})
  end

  @doc """
  Query logs for a run.

  Options:
  - :run_id (required)
  - :execution_id - filter by execution
  - :workspace_ids - list of workspace IDs to include
  - :after - cursor for pagination
  - :from - unix ms timestamp to skip partitions created before this time
  - :limit - max results
  """
  def query_logs(project_id, opts) do
    {:ok, server} = Coflux.Logs.Supervisor.get_server(project_id)
    GenServer.call(server, {:query_logs, opts})
  end

  @doc """
  Subscribe to real-time log updates for a run.

  Options:
  - :execution_id - filter to only logs from this execution
  - :workspace_ids - list of workspace IDs to include
  - :from - unix ms timestamp to skip old partitions in initial snapshot

  Returns {:ok, ref, initial_logs} where ref is used to unsubscribe.
  The subscriber will receive {:logs, ref, entries} messages.
  """
  def subscribe(project_id, run_id, pid, opts \\ []) do
    {:ok, server} = Coflux.Logs.Supervisor.get_server(project_id)
    GenServer.call(server, {:subscribe, run_id, pid, opts})
  end

  @doc """
  Unsubscribe from log updates.
  """
  def unsubscribe(project_id, ref) do
    case Coflux.Logs.Supervisor.get_server(project_id) do
      {:ok, server} -> GenServer.cast(server, {:unsubscribe, ref})
      _ -> :ok
    end
  end

  @doc """
  Force a partition rotation regardless of size.
  """
  def rotate(project_id) do
    {:ok, server} = Coflux.Logs.Supervisor.get_server(project_id)
    GenServer.call(server, :rotate)
  end

  ## Server Callbacks

  @impl true
  def init(project_id) do
    index_path = ["projects", project_id, "logs", "index.json"]
    {:ok, log_index} = Index.load(index_path, ["runs"])
    unindexed_epoch_ids = Index.unindexed_epoch_ids(log_index)
    archived_epoch_ids = Index.all_epoch_ids(log_index)

    case Epochs.open(project_id, "logs",
           unindexed_epoch_ids: unindexed_epoch_ids,
           archived_epoch_ids: archived_epoch_ids
         ) do
      {:ok, epochs} ->
        state = %State{
          project_id: project_id,
          epochs: epochs,
          log_index: log_index,
          index_queue: unindexed_epoch_ids
        }

        state = maybe_start_index_build(state)
        {:ok, state}
    end
  end

  @impl true
  def handle_cast({:write_logs, entries}, state) do
    state = %{state | buffer: state.buffer ++ entries}
    state = maybe_flush(state)
    state = schedule_flush(state)
    {:noreply, state}
  end

  @impl true
  def handle_cast({:unsubscribe, ref}, state) do
    state = do_unsubscribe(state, ref)
    {:noreply, state}
  end

  @impl true
  def handle_call({:query_logs, opts}, _from, state) do
    # Flush buffer first to include recent writes
    state = flush_buffer(state)
    result = query_cross_partition(state, opts)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:subscribe, run_id, pid, opts}, _from, state) do
    ref = make_ref()
    execution_id = Keyword.get(opts, :execution_id)
    workspace_ids = Keyword.get(opts, :workspace_ids)
    from = Keyword.get(opts, :from)

    # Monitor the subscriber
    Process.monitor(pid)

    # Flush buffer first
    state = flush_buffer(state)

    {:ok, initial_logs, _cursor} =
      query_cross_partition(state,
        run_id: run_id,
        execution_id: execution_id,
        workspace_ids: workspace_ids,
        from: from
      )

    # Add to subscribers with optional filters
    state =
      state
      |> update_in(
        [Access.key(:subscribers), Access.key(run_id, %{})],
        &Map.put(&1, ref, {pid, execution_id, workspace_ids})
      )

    {:reply, {:ok, ref, initial_logs}, state}
  end

  @impl true
  def handle_call(:rotate, _from, state) do
    state = flush_buffer(state)
    state = do_rotate(state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:flush, state) do
    state = flush_buffer(state)
    {:noreply, %{state | flush_timer: nil}}
  end

  @impl true
  def handle_info({task_ref, run_bloom}, state)
      when task_ref == state.index_task do
    Process.demonitor(task_ref, [:flush])
    [epoch_id | rest] = state.index_queue

    log_index = Index.update_filters(state.log_index, epoch_id, %{"runs" => run_bloom})
    :ok = Index.save(log_index)
    epochs = Epochs.promote_to_indexed(state.epochs, epoch_id)

    state =
      %{state | log_index: log_index, epochs: epochs, index_task: nil, index_queue: rest}

    {:noreply, maybe_start_index_build(state)}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    cond do
      ref == state.index_task ->
        # Bloom build failed - drop from queue, retry on next startup
        [failed | rest] = state.index_queue

        Logger.warning(
          "log index build failed for epoch #{failed} in project #{state.project_id}"
        )

        state = %{state | index_task: nil, index_queue: rest}
        state = maybe_start_index_build(state)
        {:noreply, state}

      true ->
        # Subscriber process died
        state = remove_subscriber_by_pid(state, pid)
        {:noreply, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    if state.epochs do
      Epochs.close(state.epochs)
    end

    :ok
  end

  ## Private Functions — Flush & Rotation

  defp flush_buffer(%{buffer: []} = state), do: state

  defp flush_buffer(state) do
    db = Epochs.active_db(state.epochs)

    case Store.insert_logs(db, state.buffer, state.template_cache) do
      {:ok, template_cache} ->
        notify_subscribers(state, state.buffer)

        %{state | buffer: [], template_cache: template_cache}
        |> maybe_rotate()

      {:error, reason} ->
        Logger.warning("Failed to flush logs: #{inspect(reason)}")
        %{state | buffer: []}
    end
  end

  defp maybe_flush(state) when length(state.buffer) >= @max_buffer_size do
    flush_buffer(state)
  end

  defp maybe_flush(state), do: state

  defp schedule_flush(%{flush_timer: nil} = state) do
    timer = Process.send_after(self(), :flush, @flush_interval_ms)
    %{state | flush_timer: timer}
  end

  defp schedule_flush(state), do: state

  defp maybe_rotate(state) do
    if Epochs.active_db_size(state.epochs) >= @rotation_size_threshold_bytes do
      do_rotate(state)
    else
      state
    end
  end

  defp do_rotate(state) do
    epoch_id = Epochs.next_epoch_id(state.epochs)

    # Write placeholder entry to index first (null value)
    log_index = Index.add_epoch(state.log_index, epoch_id)
    :ok = Index.save(log_index)

    # Now rotate
    {:ok, new_epochs, _old_db} = Epochs.rotate(state.epochs, epoch_id)

    %{
      state
      | epochs: new_epochs,
        log_index: log_index,
        template_cache: %{},
        index_queue: state.index_queue ++ [epoch_id]
    }
    |> maybe_start_index_build()
  end

  ## Private Functions — Bloom Filter Building

  defp maybe_start_index_build(%{index_task: nil, index_queue: [epoch_id | _]} = state) do
    path = Epochs.archive_path(state.epochs, epoch_id)

    task =
      Task.Supervisor.async_nolink(Coflux.LauncherSupervisor, fn ->
        {:ok, db} = Sqlite3.open(path)

        try do
          build_bloom_for_partition(db)
        after
          Sqlite3.close(db)
        end
      end)

    %{state | index_task: task.ref}
  end

  defp maybe_start_index_build(state), do: state

  defp build_bloom_for_partition(db) do
    import Coflux.Store

    {:ok, [{count}]} = query(db, "SELECT COUNT(DISTINCT run_id) FROM messages")
    bloom = Bloom.new(max(100, count))

    {:ok, run_ids} = query(db, "SELECT DISTINCT run_id FROM messages")

    Enum.reduce(run_ids, bloom, fn {id}, b -> Bloom.add(b, id) end)
  end

  # Returns true if the epoch was definitely archived before the given timestamp.
  # Parses the date from the epoch_id (YYYYMMDD_XXXX format) and compares at
  # day granularity. Conservative: a partition archived on the same day as `from`
  # is included (not skipped).
  defp archived_before?(_epoch_id, nil), do: false

  defp archived_before?(epoch_id, from_ms) do
    case epoch_id_date(epoch_id) do
      nil ->
        false

      date ->
        from_date = from_ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_date()
        Date.compare(date, from_date) == :lt
    end
  end

  defp epoch_id_date(epoch_id) do
    case String.split(epoch_id, "_") do
      [date_str, _counter] ->
        case Date.from_iso8601(date_str, :basic) do
          {:ok, date} -> date
          _ -> nil
        end

      _ ->
        nil
    end
  end

  ## Private Functions — Cross-Partition Query

  defp query_cross_partition(state, opts) do
    run_id = Keyword.fetch!(opts, :run_id)
    from = Keyword.get(opts, :from)
    after_cursor = Keyword.get(opts, :after)
    limit = Keyword.get(opts, :limit, 1000)

    # Parse cursor to determine starting partition
    {cursor_epoch_id, cursor_timestamp, cursor_id} = parse_cross_cursor(after_cursor)

    # If cursor is in the active partition, skip archives entirely
    skip_archives? = cursor_epoch_id == :active

    # Get candidate archived partition epoch_ids from index
    # (includes unindexed partitions with nil bloom — can't filter them out)
    archived_candidates =
      if skip_archives? do
        []
      else
        candidates =
          state.log_index
          |> Index.find_epochs("runs", run_id)
          |> Enum.reject(&archived_before?(&1, from))
          |> Enum.sort()

        # Drop partitions before the cursor's partition
        if cursor_epoch_id do
          Enum.drop_while(candidates, &(&1 != cursor_epoch_id))
        else
          candidates
        end
      end

    # Build map of unindexed (open) DB handles
    unindexed_map = Map.new(state.epochs.unindexed)

    # Query archived partitions in chronological order
    {entries, remaining} =
      Enum.reduce_while(archived_candidates, {[], limit}, fn epoch_id, {acc, remaining} ->
        if remaining <= 0 do
          {:halt, {acc, 0}}
        else
          partition_opts =
            opts
            |> Keyword.put(:limit, remaining)
            |> Keyword.delete(:from)
            |> maybe_apply_cursor(epoch_id == cursor_epoch_id, cursor_timestamp, cursor_id)

          {db, close_after?} = get_archived_db(state, epoch_id, unindexed_map)

          result =
            try do
              Store.query_logs(db, partition_opts)
            after
              if close_after?, do: Sqlite3.close(db)
            end

          {:ok, partition_entries, _cursor} = result

          tagged =
            Enum.map(partition_entries, &Map.put(&1, :_epoch_id, epoch_id))

          {:cont, {acc ++ tagged, remaining - length(partition_entries)}}
        end
      end)

    # Query active partition if capacity remains
    {entries, _remaining} =
      if remaining > 0 do
        active_opts =
          opts
          |> Keyword.put(:limit, remaining)
          |> Keyword.delete(:from)
          |> maybe_apply_cursor(skip_archives?, cursor_timestamp, cursor_id)

        db = Epochs.active_db(state.epochs)
        {:ok, active_entries, _cursor} = Store.query_logs(db, active_opts)
        tagged = Enum.map(active_entries, &Map.put(&1, :_epoch_id, :active))
        {entries ++ tagged, remaining - length(active_entries)}
      else
        {entries, remaining}
      end

    # Build cursor from last entry
    cursor = build_cross_cursor(List.last(entries))

    # Strip internal tag before returning
    entries = Enum.map(entries, &Map.delete(&1, :_epoch_id))

    {:ok, entries, cursor}
  end

  defp parse_cross_cursor(nil), do: {nil, nil, nil}

  defp parse_cross_cursor(cursor) when is_binary(cursor) do
    case String.split(cursor, ":") do
      # Format: "epoch_id:timestamp:id" or ":timestamp:id" (active)
      [epoch_id, timestamp_str, id_str] ->
        case {Integer.parse(timestamp_str), Integer.parse(id_str)} do
          {{timestamp, ""}, {id, ""}} ->
            partition = if epoch_id == "", do: :active, else: epoch_id
            {partition, timestamp, id}

          _ ->
            {nil, nil, nil}
        end

      # Old format: "timestamp:id" — treat as active partition cursor
      [timestamp_str, id_str] ->
        case {Integer.parse(timestamp_str), Integer.parse(id_str)} do
          {{timestamp, ""}, {id, ""}} -> {:active, timestamp, id}
          _ -> {nil, nil, nil}
        end

      _ ->
        {nil, nil, nil}
    end
  end

  defp maybe_apply_cursor(opts, true, timestamp, id) when not is_nil(timestamp) do
    Keyword.put(opts, :after, "#{timestamp}:#{id}")
  end

  defp maybe_apply_cursor(opts, _, _, _) do
    Keyword.delete(opts, :after)
  end

  defp get_archived_db(state, epoch_id, unindexed_map) do
    case Map.fetch(unindexed_map, epoch_id) do
      {:ok, db} ->
        {db, false}

      :error ->
        path = Epochs.archive_path(state.epochs, epoch_id)
        {:ok, db} = Sqlite3.open(path)
        {db, true}
    end
  end

  defp build_cross_cursor(nil), do: nil

  defp build_cross_cursor(entry) do
    epoch_prefix = if entry._epoch_id == :active, do: "", else: entry._epoch_id
    "#{epoch_prefix}:#{entry.timestamp}:#{entry.id}"
  end

  ## Private Functions — Subscribers

  defp notify_subscribers(state, entries) do
    # Group entries by run_id
    entries_by_run = Enum.group_by(entries, & &1.run_id)

    Enum.each(entries_by_run, fn {run_id, run_entries} ->
      case Map.get(state.subscribers, run_id) do
        nil ->
          :ok

        subs ->
          Enum.each(subs, fn {ref, {pid, filter_execution_id, filter_workspace_ids}} ->
            filtered_entries =
              run_entries
              |> maybe_filter_by_execution_id(filter_execution_id)
              |> maybe_filter_by_workspace_ids(filter_workspace_ids)

            if Enum.any?(filtered_entries) do
              formatted_entries = Enum.map(filtered_entries, &format_entry_for_notification/1)
              send(pid, {:logs, ref, formatted_entries})
            end
          end)
      end
    end)
  end

  defp maybe_filter_by_execution_id(entries, nil), do: entries

  defp maybe_filter_by_execution_id(entries, execution_id) do
    Enum.filter(entries, &(&1.execution_id == execution_id))
  end

  defp maybe_filter_by_workspace_ids(entries, nil), do: entries
  defp maybe_filter_by_workspace_ids(entries, []), do: entries

  defp maybe_filter_by_workspace_ids(entries, workspace_ids) do
    Enum.filter(entries, &(&1.workspace_id in workspace_ids))
  end

  defp format_entry_for_notification(entry) do
    Map.take(entry, [:execution_id, :workspace_id, :timestamp, :level, :template, :values])
  end

  defp do_unsubscribe(state, ref) do
    {state, _} =
      Enum.reduce(state.subscribers, {state, false}, fn
        {run_id, subs}, {state, false} ->
          if Map.has_key?(subs, ref) do
            new_subs = Map.delete(subs, ref)

            state =
              if map_size(new_subs) == 0 do
                update_in(state, [:subscribers], &Map.delete(&1, run_id))
              else
                put_in(state, [:subscribers, run_id], new_subs)
              end

            {state, true}
          else
            {state, false}
          end

        _, acc ->
          acc
      end)

    state
  end

  defp remove_subscriber_by_pid(state, pid) do
    new_subscribers =
      state.subscribers
      |> Enum.map(fn {run_id, subs} ->
        new_subs =
          subs
          |> Enum.reject(fn {_ref, {sub_pid, _execution_id, _workspace_ids}} ->
            sub_pid == pid
          end)
          |> Map.new()

        {run_id, new_subs}
      end)
      |> Enum.reject(fn {_run_id, subs} -> map_size(subs) == 0 end)
      |> Map.new()

    %{state | subscribers: new_subscribers}
  end
end
