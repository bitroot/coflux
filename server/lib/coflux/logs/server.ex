defmodule Coflux.Logs.Server do
  @moduledoc """
  Per-project GenServer for log storage with write buffering.

  Features:
  - Buffers writes in memory for batch inserts
  - Flushes on interval or when buffer is full
  - Pub/sub for real-time log streaming
  - Template deduplication caching
  """

  use GenServer

  require Logger

  alias Coflux.Logs.Store

  @flush_interval_ms 500
  @max_buffer_size 1000

  defmodule State do
    @moduledoc false
    defstruct [
      :project_id,
      :db,
      :flush_timer,
      buffer: [],
      # run_id -> %{ref -> {pid, execution_id, workspace_ids}}
      subscribers: %{},
      # template_hash -> template_id
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

  ## Server Callbacks

  @impl true
  def init(project_id) do
    case Store.open(project_id) do
      {:ok, db} ->
        {:ok, %State{project_id: project_id, db: db}}

      {:error, reason} ->
        {:stop, reason}
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
    result = Store.query_logs(state.db, opts)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:subscribe, run_id, pid, opts}, _from, state) do
    ref = make_ref()
    execution_id = Keyword.get(opts, :execution_id)
    workspace_ids = Keyword.get(opts, :workspace_ids)

    # Monitor the subscriber
    Process.monitor(pid)

    # Flush buffer first
    state = flush_buffer(state)

    {:ok, initial_logs, _cursor} =
      Store.query_logs(state.db,
        run_id: run_id,
        execution_id: execution_id,
        workspace_ids: workspace_ids
      )

    # Add to subscribers with optional filters
    # Store as {pid, execution_id, workspace_ids}
    state =
      state
      |> update_in(
        [Access.key(:subscribers), Access.key(run_id, %{})],
        &Map.put(&1, ref, {pid, execution_id, workspace_ids})
      )

    {:reply, {:ok, ref, initial_logs}, state}
  end

  @impl true
  def handle_info(:flush, state) do
    state = flush_buffer(state)
    {:noreply, %{state | flush_timer: nil}}
  end

  @impl true
  def handle_info({:DOWN, _monitor_ref, :process, pid, _reason}, state) do
    # Find and remove all subscriptions for this pid
    state = remove_subscriber_by_pid(state, pid)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    if state.db do
      Store.close(state.db)
    end

    :ok
  end

  ## Private Functions

  defp flush_buffer(%{buffer: []} = state), do: state

  defp flush_buffer(state) do
    case Store.insert_logs(state.db, state.buffer, state.template_cache) do
      {:ok, template_cache} ->
        notify_subscribers(state, state.buffer)
        %{state | buffer: [], template_cache: template_cache}

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
    # Find which run_id this ref belongs to and remove it
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
    # Find and remove all subscriptions for this pid
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
