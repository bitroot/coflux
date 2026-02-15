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

  alias Coflux.Logs.{Index, Store}
  alias Coflux.Store.Epoch
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
    {:ok, log_index} = Index.load(project_id)
    unindexed_epoch_ids = Index.unindexed_epoch_ids(log_index)
    archived_epoch_ids = Index.all_epoch_ids(log_index)

    case Epoch.open(project_id, "logs",
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

    log_index = Index.update_bloom(state.log_index, epoch_id, run_bloom)
    :ok = Index.save(state.project_id, log_index)
    epochs = Epoch.promote_to_indexed(state.epochs, epoch_id)

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
      Epoch.close(state.epochs)
    end

    :ok
  end

  ## Private Functions — Flush & Rotation

  defp flush_buffer(%{buffer: []} = state), do: state

  defp flush_buffer(state) do
    db = Epoch.active_db(state.epochs)

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
    if Epoch.active_db_size(state.epochs) >= @rotation_size_threshold_bytes do
      do_rotate(state)
    else
      state
    end
  end

  defp do_rotate(state) do
    {:ok, new_epochs, old_epoch_id} = Epoch.rotate(state.epochs)
    now = System.os_time(:millisecond)
    log_index = Index.add_partition(state.log_index, old_epoch_id, now)
    :ok = Index.save(state.project_id, log_index)

    %{
      state
      | epochs: new_epochs,
        log_index: log_index,
        template_cache: %{},
        index_queue: state.index_queue ++ [old_epoch_id]
    }
    |> maybe_start_index_build()
  end

  ## Private Functions — Bloom Filter Building

  defp maybe_start_index_build(%{index_task: nil, index_queue: [epoch_id | _]} = state) do
    project_id = state.project_id
    path = Path.join(Epoch.epochs_dir(project_id, "logs"), "#{epoch_id}.sqlite")

    task =
      Task.Supervisor.async_nolink(Coflux.LauncherSupervisor, fn ->
        {:ok, db} = Sqlite3.open(path)

        try do
          Index.build_bloom_for_partition(db)
        after
          Sqlite3.close(db)
        end
      end)

    %{state | index_task: task.ref}
  end

  defp maybe_start_index_build(state), do: state

  ## Private Functions — Cross-Partition Query

  defp query_cross_partition(state, opts) do
    run_id = Keyword.fetch!(opts, :run_id)
    from = Keyword.get(opts, :from)
    after_cursor = Keyword.get(opts, :after)
    limit = Keyword.get(opts, :limit, 1000)

    # Parse cursor to determine starting partition
    {cursor_epoch_id, cursor_timestamp, cursor_id} = parse_cross_cursor(after_cursor)

    # Get candidate archived partition epoch_ids from index
    # (includes unindexed partitions with nil bloom — can't filter them out)
    archived_candidates =
      Index.find_partitions_for_run(state.log_index, run_id, from: from)
      |> Enum.sort()

    active_epoch_id = state.epochs.active_epoch_id

    # Build ordered list: archived (chronological) + active
    all_candidate_ids = archived_candidates ++ [active_epoch_id]

    # Skip partitions before cursor's partition
    candidate_ids = filter_by_cursor(all_candidate_ids, cursor_epoch_id, active_epoch_id)

    # Build map of unindexed (open) DB handles
    unindexed_map = Map.new(state.epochs.unindexed)

    # Query each partition in order, collecting results up to limit
    {entries, _remaining} =
      Enum.reduce_while(candidate_ids, {[], limit}, fn epoch_id, {acc, remaining} ->
        if remaining <= 0 do
          {:halt, {acc, 0}}
        else
          # Build opts for this partition
          partition_opts =
            opts
            |> Keyword.put(:limit, remaining)
            |> Keyword.delete(:from)
            |> apply_partition_cursor(
              epoch_id,
              cursor_epoch_id,
              cursor_timestamp,
              cursor_id,
              active_epoch_id
            )

          # Get or open DB handle
          {db, close_after?} =
            get_partition_db(state, epoch_id, unindexed_map, active_epoch_id)

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

    # Build cursor from last entry
    cursor = build_cross_cursor(List.last(entries), active_epoch_id)

    # Strip internal tag before returning
    entries = Enum.map(entries, &Map.delete(&1, :_epoch_id))

    {:ok, entries, cursor}
  end

  defp parse_cross_cursor(nil), do: {nil, nil, nil}

  defp parse_cross_cursor(cursor) when is_binary(cursor) do
    case String.split(cursor, ":") do
      # New format: "epoch_id:timestamp:id" or ":timestamp:id"
      [epoch_id, timestamp_str, id_str] ->
        case {Integer.parse(timestamp_str), Integer.parse(id_str)} do
          {{timestamp, ""}, {id, ""}} -> {epoch_id, timestamp, id}
          _ -> {nil, nil, nil}
        end

      # Old format: "timestamp:id" — treat as active partition cursor
      [timestamp_str, id_str] ->
        case {Integer.parse(timestamp_str), Integer.parse(id_str)} do
          {{timestamp, ""}, {id, ""}} -> {"", timestamp, id}
          _ -> {nil, nil, nil}
        end

      _ ->
        {nil, nil, nil}
    end
  end

  defp filter_by_cursor(all_ids, nil, _active_epoch_id), do: all_ids

  defp filter_by_cursor(all_ids, "", _active_epoch_id) do
    # Active-partition cursor — only query active (last in list)
    case List.last(all_ids) do
      nil -> []
      last -> [last]
    end
  end

  defp filter_by_cursor(all_ids, cursor_epoch_id, _active_epoch_id) do
    # Drop all partitions before the cursor's partition
    Enum.drop_while(all_ids, &(&1 != cursor_epoch_id))
  end

  defp apply_partition_cursor(
         opts,
         epoch_id,
         cursor_epoch_id,
         cursor_ts,
         cursor_id,
         active_epoch_id
       ) do
    is_cursor_partition =
      cond do
        is_nil(cursor_epoch_id) -> false
        cursor_epoch_id == "" -> epoch_id == active_epoch_id
        true -> epoch_id == cursor_epoch_id
      end

    if is_cursor_partition and cursor_ts != nil do
      Keyword.put(opts, :after, "#{cursor_ts}:#{cursor_id}")
    else
      Keyword.delete(opts, :after)
    end
  end

  defp get_partition_db(state, epoch_id, unindexed_map, active_epoch_id) do
    cond do
      epoch_id == active_epoch_id ->
        {Epoch.active_db(state.epochs), false}

      Map.has_key?(unindexed_map, epoch_id) ->
        {Map.fetch!(unindexed_map, epoch_id), false}

      true ->
        # Indexed archived partition — open for read
        dir = Epoch.epochs_dir(state.project_id, "logs")
        path = Path.join(dir, "#{epoch_id}.sqlite")
        {:ok, db} = Sqlite3.open(path)
        {db, true}
    end
  end

  defp build_cross_cursor(nil, _active_epoch_id), do: nil

  defp build_cross_cursor(entry, active_epoch_id) do
    epoch_prefix = if entry._epoch_id == active_epoch_id, do: "", else: entry._epoch_id
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
