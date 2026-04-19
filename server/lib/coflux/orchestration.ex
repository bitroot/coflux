defmodule Coflux.Orchestration do
  alias Coflux.Orchestration

  # Principal management

  def ensure_principal(project_id, external_id) do
    call_server(project_id, {:ensure_principal, external_id})
  end

  # Token management

  def check_token(project_id, token_hash) do
    call_server(project_id, {:check_token, token_hash})
  end

  def verify_session(project_id, token) do
    call_server(project_id, {:verify_session, token})
  end

  def create_token(project_id, name, principal_id, opts \\ []) do
    call_server(project_id, {:create_token, name, principal_id, opts})
  end

  def list_tokens(project_id) do
    call_server(project_id, :list_tokens)
  end

  def subscribe_tokens(project_id, pid) do
    call_server(project_id, {:subscribe_tokens, pid})
  end

  def revoke_token(project_id, token_id) do
    call_server(project_id, {:revoke_token, token_id})
  end

  def get_token(project_id, external_id) do
    call_server(project_id, {:get_token, external_id})
  end

  # Workspace management

  def get_workspaces(project_id) do
    call_server(project_id, :get_workspaces)
  end

  def create_workspace(project_id, name, base_id, access \\ nil) do
    call_server(project_id, {:create_workspace, name, base_id, access})
  end

  def update_workspace(project_id, workspace_id, updates, access \\ nil) do
    call_server(project_id, {:update_workspace, workspace_id, updates, access})
  end

  def pause_workspace(project_id, workspace_id, access \\ nil) do
    call_server(project_id, {:pause_workspace, workspace_id, access})
  end

  def resume_workspace(project_id, workspace_id, access \\ nil) do
    call_server(project_id, {:resume_workspace, workspace_id, access})
  end

  def archive_workspace(project_id, workspace_id, access \\ nil) do
    call_server(project_id, {:archive_workspace, workspace_id, access})
  end

  def get_pools(project_id, workspace_id) do
    call_server(project_id, {:get_pools, workspace_id})
  end

  def update_pools(project_id, workspace_id, pools, expected_hash, access \\ nil) do
    call_server(project_id, {:update_pools, workspace_id, pools, expected_hash, access})
  end

  def create_pool(project_id, workspace_id, pool_name, pool, access \\ nil) do
    call_server(project_id, {:create_pool, workspace_id, pool_name, pool, access})
  end

  def update_pool(project_id, workspace_id, pool_name, pool, access \\ nil) do
    call_server(project_id, {:update_pool, workspace_id, pool_name, pool, access})
  end

  def disable_pool(project_id, workspace_id, pool_name, access \\ nil) do
    call_server(project_id, {:disable_pool, workspace_id, pool_name, access})
  end

  def enable_pool(project_id, workspace_id, pool_name, access \\ nil) do
    call_server(project_id, {:enable_pool, workspace_id, pool_name, access})
  end

  def stop_worker(project_id, workspace_id, worker_id, access \\ nil) do
    call_server(project_id, {:stop_worker, workspace_id, worker_id, access})
  end

  def resume_worker(project_id, workspace_id, worker_id, access \\ nil) do
    call_server(project_id, {:resume_worker, workspace_id, worker_id, access})
  end

  def register_manifests(project_id, workspace_id, manifests, access \\ nil) do
    call_server(project_id, {:register_manifests, workspace_id, manifests, access})
  end

  def archive_module(project_id, workspace_id, module_name, access \\ nil) do
    call_server(project_id, {:archive_module, workspace_id, module_name, access})
  end

  def get_manifests(project_id, workspace_id) do
    call_server(project_id, {:get_manifests, workspace_id})
  end

  def subscribe_manifests(project_id, workspace_id, pid) do
    call_server(project_id, {:subscribe_manifests, workspace_id, pid})
  end

  def get_workflow(project_id, workspace_id, module, target_name) do
    call_server(project_id, {:get_workflow, workspace_id, module, target_name})
  end

  def resume_session(project_id, session_id, workspace_id, pid) do
    call_server(project_id, {:resume_session, session_id, workspace_id, pid})
  end

  def create_session(project_id, workspace_id, access \\ nil, opts \\ []) do
    call_server(project_id, {:create_session, workspace_id, access, opts})
  end

  def declare_targets(project_id, session_id, targets, concurrency) do
    call_server(project_id, {:declare_targets, session_id, targets, concurrency})
  end

  def session_draining(project_id, session_id) do
    call_server(project_id, {:session_draining, session_id})
  end

  def start_run(project_id, module, target, type, arguments, access \\ nil, opts \\ []) do
    call_server(project_id, {:start_run, module, target, type, arguments, access, opts})
  end

  def schedule_step(project_id, parent_id, module, target, type, arguments, opts \\ []) do
    call_server(
      project_id,
      {:schedule_step, parent_id, module, target, type, arguments, opts}
    )
  end

  def register_group(project_id, parent_id, group_id, name) do
    call_server(project_id, {:register_group, parent_id, group_id, name})
  end

  def cancel_execution(project_id, workspace_id, execution_id, access \\ nil) do
    call_server(project_id, {:cancel_execution, workspace_id, execution_id, access})
  end

  def cancel(project_id, handles, workspace_id, from_execution_id) do
    call_server(project_id, {:cancel, handles, workspace_id, from_execution_id})
  end

  def rerun_step(project_id, step_id, workspace_id, access \\ nil) do
    call_server(
      project_id,
      {:rerun_step, step_id, workspace_id, access}
    )
  end

  def execution_started(project_id, execution_id, metadata) do
    call_server(project_id, {:execution_started, execution_id, metadata})
  end

  def define_metric(project_id, execution_id, key, definition) do
    call_server(project_id, {:define_metric, execution_id, key, definition})
  end

  def record_heartbeats(project_id, executions, session_id) do
    call_server(project_id, {:record_heartbeats, executions, session_id})
  end

  def notify_terminated(project_id, execution_ids) do
    call_server(project_id, {:notify_terminated, execution_ids})
  end

  def record_result(project_id, execution_id, result) do
    call_server(project_id, {:record_result, execution_id, result})
  end

  # Stream producer messages — worker registers a stream, appends items,
  # and closes the stream. `index` identifies the stream within its
  # producer execution; `sequence` identifies an item within the stream.
  # Both are worker-assigned and monotonic from 0.

  def register_stream(project_id, execution_id, index, buffer, session_id) do
    call_server(
      project_id,
      {:register_stream, execution_id, index, buffer, session_id}
    )
  end

  def append_stream_item(project_id, execution_id, index, sequence, value) do
    call_server(project_id, {:append_stream_item, execution_id, index, sequence, value})
  end

  def close_stream(project_id, execution_id, index, error) do
    call_server(project_id, {:close_stream, execution_id, index, error})
  end

  # Stream consumer messages — consumer opens a subscription to receive
  # items from a producer's stream; server pushes stream_items /
  # stream_closed commands to the consumer's session.

  def subscribe_stream(
        project_id,
        session_id,
        subscription_id,
        consumer_execution_id,
        producer_execution_id,
        index,
        from_sequence,
        filter
      ) do
    call_server(
      project_id,
      {:subscribe_stream, session_id, subscription_id, consumer_execution_id,
       producer_execution_id, index, from_sequence, filter}
    )
  end

  def unsubscribe_stream(project_id, session_id, consumer_execution_id, subscription_id) do
    call_server(
      project_id,
      {:unsubscribe_stream, session_id, consumer_execution_id, subscription_id}
    )
  end

  def select(
        project_id,
        handles,
        from_execution_id,
        timeout_ms,
        suspend,
        cancel_remaining,
        request_id
      ) do
    call_server(
      project_id,
      {:select, handles, from_execution_id, timeout_ms, suspend, cancel_remaining, request_id}
    )
  end

  def put_asset(project_id, execution_id, name, entries) do
    call_server(project_id, {:put_asset, execution_id, name, entries})
  end

  def get_asset(project_id, asset_id, from_execution_id \\ nil) do
    call_server(project_id, {:get_asset, asset_id, from_execution_id})
  end

  def subscribe_workspaces(project_id, pid) do
    call_server(project_id, {:subscribe_workspaces, pid})
  end

  def subscribe_modules(project_id, workspace_id, pid) do
    call_server(project_id, {:subscribe_modules, workspace_id, pid})
  end

  def subscribe_queue(project_id, workspace_id, pid) do
    call_server(project_id, {:subscribe_queue, workspace_id, pid})
  end

  def subscribe_pools(project_id, workspace_id, pid) do
    call_server(project_id, {:subscribe_pools, workspace_id, pid})
  end

  def subscribe_pool(project_id, workspace_id, pool_name, pid) do
    call_server(project_id, {:subscribe_pool, workspace_id, pool_name, pid})
  end

  def subscribe_sessions(project_id, workspace_id, pid) do
    call_server(project_id, {:subscribe_sessions, workspace_id, pid})
  end

  def subscribe_workflow(project_id, module, target, workspace_id, max_runs, pid) do
    call_server(project_id, {:subscribe_workflow, module, target, workspace_id, max_runs, pid})
  end

  def subscribe_run(project_id, run_id, pid) do
    call_server(project_id, {:subscribe_run, run_id, pid})
  end

  def subscribe_stream_topic(project_id, execution_external_id, index, pid) do
    call_server(
      project_id,
      {:subscribe_stream_topic, execution_external_id, index, pid}
    )
  end

  def subscribe_targets(project_id, workspace_id, pid) do
    call_server(project_id, {:subscribe_targets, workspace_id, pid})
  end

  # Input management

  def submit_input(
        project_id,
        execution_id,
        template,
        placeholders,
        schema_json,
        key,
        title,
        actions,
        initial,
        requires
      ) do
    call_server(
      project_id,
      {:submit_input, execution_id, template, placeholders, schema_json, key, title, actions,
       initial, requires}
    )
  end

  def respond_input(project_id, input_external_id, value, access \\ nil) do
    call_server(project_id, {:respond_input, input_external_id, value, access})
  end

  def dismiss_input(project_id, input_external_id, access \\ nil) do
    call_server(project_id, {:dismiss_input, input_external_id, access})
  end

  def get_input(project_id, input_external_id) do
    call_server(project_id, {:get_input, input_external_id})
  end

  def subscribe_input(project_id, input_external_id, pid) do
    call_server(project_id, {:subscribe_input, input_external_id, pid})
  end

  def subscribe_inputs(project_id, workspace_id, pid) do
    call_server(project_id, {:subscribe_inputs, workspace_id, pid})
  end

  def rotate_epoch(project_id) do
    call_server(project_id, :rotate_epoch)
  end

  def unsubscribe(project_id, ref) do
    cast_server(project_id, {:unsubscribe, ref})
  end

  defp call_server(project_id, request) do
    case Orchestration.Supervisor.get_server(project_id) do
      {:ok, server} ->
        GenServer.call(server, request)
    end
  end

  defp cast_server(project_id, request) do
    case Orchestration.Supervisor.get_server(project_id) do
      {:ok, server} ->
        GenServer.cast(server, request)
    end
  end
end
