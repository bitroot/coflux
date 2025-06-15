defmodule Coflux.Orchestration do
  alias Coflux.Orchestration

  def get_workspaces(project_id) do
    call_server(project_id, :get_workspaces)
  end

  def create_workspace(project_id, name, base_id) do
    call_server(project_id, {:create_workspace, name, base_id})
  end

  def update_workspace(project_id, workspace_id, updates) do
    call_server(project_id, {:update_workspace, workspace_id, updates})
  end

  def pause_workspace(project_id, workspace_name) do
    call_server(project_id, {:pause_workspace, workspace_name})
  end

  def resume_workspace(project_id, workspace_name) do
    call_server(project_id, {:resume_workspace, workspace_name})
  end

  def archive_workspace(project_id, workspace_name) do
    call_server(project_id, {:archive_workspace, workspace_name})
  end

  def update_pool(project_id, workspace_name, pool_name, pool) do
    call_server(project_id, {:update_pool, workspace_name, pool_name, pool})
  end

  def stop_agent(project_id, workspace_name, agent_id) do
    call_server(project_id, {:stop_agent, workspace_name, agent_id})
  end

  def resume_agent(project_id, workspace_name, agent_id) do
    call_server(project_id, {:resume_agent, workspace_name, agent_id})
  end

  def register_manifests(project_id, workspace_name, manifests) do
    call_server(project_id, {:register_manifests, workspace_name, manifests})
  end

  def archive_module(project_id, workspace_name, module_name) do
    call_server(project_id, {:archive_module, workspace_name, module_name})
  end

  def get_workflow(project_id, workspace_name, module, target_name) do
    call_server(project_id, {:get_workflow, workspace_name, module, target_name})
  end

  def start_session(project_id, workspace_name, agent_id, provides, concurrency, pid) do
    call_server(
      project_id,
      {:start_session, workspace_name, agent_id, provides, concurrency, pid}
    )
  end

  def resume_session(project_id, session_id, pid) do
    call_server(project_id, {:resume_session, session_id, pid})
  end

  def declare_targets(project_id, session_id, targets) do
    call_server(project_id, {:declare_targets, session_id, targets})
  end

  def start_run(project_id, module, target, type, arguments, opts \\ []) do
    call_server(project_id, {:start_run, module, target, type, arguments, opts})
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

  def cancel_execution(project_id, execution_id) do
    call_server(project_id, {:cancel_execution, execution_id})
  end

  def rerun_step(project_id, step_id, workspace_name) do
    call_server(project_id, {:rerun_step, step_id, workspace_name})
  end

  def record_heartbeats(project_id, executions, session_id) do
    call_server(project_id, {:record_heartbeats, executions, session_id})
  end

  def notify_terminated(project_id, execution_ids) do
    call_server(project_id, {:notify_terminated, execution_ids})
  end

  def record_checkpoint(project_id, execution_id, arguments) do
    call_server(project_id, {:record_checkpoint, execution_id, arguments})
  end

  def record_result(project_id, execution_id, result) do
    call_server(project_id, {:record_result, execution_id, result})
  end

  def get_result(project_id, execution_id, from_execution_id, timeout_ms, request_id) do
    call_server(
      project_id,
      {:get_result, execution_id, from_execution_id, timeout_ms, request_id}
    )
  end

  def put_asset(project_id, execution_id, entries) do
    call_server(project_id, {:put_asset, execution_id, entries})
  end

  def get_asset(project_id, asset_id, opts \\ []) do
    call_server(project_id, {:get_asset, asset_id, opts})
  end

  def get_asset_by_external_id(project_id, asset_external_id) do
    call_server(project_id, {:get_asset_by_external_id, asset_external_id})
  end

  def record_logs(project_id, execution_id, messages) do
    call_server(project_id, {:record_logs, execution_id, messages})
  end

  def subscribe_workspaces(project_id, pid) do
    call_server(project_id, {:subscribe_workspaces, pid})
  end

  def subscribe_modules(project_id, workspace_id, pid) do
    call_server(project_id, {:subscribe_modules, workspace_id, pid})
  end

  def subscribe_module(project_id, module, workspace_id, pid) do
    call_server(project_id, {:subscribe_module, module, workspace_id, pid})
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

  def subscribe_workflow(project_id, module, target, workspace_id, pid) do
    call_server(project_id, {:subscribe_workflow, module, target, workspace_id, pid})
  end

  def subscribe_sensor(project_id, module, target, workspace_id, pid) do
    call_server(project_id, {:subscribe_sensor, module, target, workspace_id, pid})
  end

  def subscribe_run(project_id, run_id, pid) do
    call_server(project_id, {:subscribe_run, run_id, pid})
  end

  def subscribe_logs(project_id, run_id, pid) do
    call_server(project_id, {:subscribe_logs, run_id, pid})
  end

  def subscribe_targets(project_id, workspace_id, pid) do
    call_server(project_id, {:subscribe_targets, workspace_id, pid})
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
