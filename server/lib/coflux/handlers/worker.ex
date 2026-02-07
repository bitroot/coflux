defmodule Coflux.Handlers.Worker do
  @moduledoc """
  WebSocket handler for worker connections.

  Authentication is done via Sec-WebSocket-Protocol header using the format:
  `session.<base64-encoded-token>` (similar to the topics handler pattern).

  The client should request protocols like: ["session.dG9rZW4=", "v1"]
  The server echoes back "v1" on successful auth.

  The project is determined by COFLUX_PROJECT (if set) or extracted from the
  subdomain (if COFLUX_BASE_DOMAIN is set).
  """

  import Coflux.Handlers.Utils

  alias Coflux.{Orchestration, Version}

  @protocol_version "v1"

  def init(req, opts) do
    qs = :cowboy_req.parse_qs(req)
    expected_version = get_query_param(qs, "version")
    protocols = parse_websocket_protocols(req)

    case Version.check(expected_version) do
      :ok ->
        host = get_host(req)

        case resolve_project(host) do
          {:ok, project_id} ->
            workspace_name = get_query_param(qs, "workspace")
            session_token = extract_session_token(protocols)

            req = :cowboy_req.set_resp_header("sec-websocket-protocol", @protocol_version, req)
            {:cowboy_websocket, req, {project_id, workspace_name, session_token}}

          {:error, :not_configured} ->
            req = json_error_response(req, "not_configured", status: 500)
            {:ok, req, opts}

          {:error, :project_required} ->
            req = json_error_response(req, "project_required", status: 400)
            {:ok, req, opts}

          {:error, :invalid_host} ->
            req = json_error_response(req, "invalid_host", status: 400)
            {:ok, req, opts}
        end

      {:error, server_version, expected_version} ->
        req =
          json_error_response(req, "version_mismatch",
            status: 409,
            details: %{
              "server" => server_version,
              "expected" => expected_version
            }
          )

        {:ok, req, opts}
    end
  end

  def websocket_init({project_id, workspace_name, session_token}) do
    # TODO: monitor server?
    case Orchestration.resume_session(project_id, session_token, workspace_name, self()) do
      {:ok, external_id, execution_ids} ->
        {[session_message(external_id)],
         %{
           project_id: project_id,
           workspace_name: workspace_name,
           session_id: external_id,
           execution_ids: execution_ids
         }}

      {:error, :session_invalid} ->
        {[{:close, 4000, "session_invalid"}], nil}

      {:error, :workspace_mismatch} ->
        {[{:close, 4000, "workspace_mismatch"}], nil}
    end
  end

  def websocket_handle({:text, text}, state) do
    message = Jason.decode!(text)

    case message["request"] do
      "declare_targets" ->
        [targets] = message["params"]

        case Orchestration.declare_targets(
               state.project_id,
               state.session_id,
               parse_targets(targets)
             ) do
          :ok ->
            {[], state}
        end

      "register_group" ->
        [parent_id, group_id, name] = message["params"]
        parent_id = String.to_integer(parent_id)

        if is_recognised_execution?(parent_id, state) do
          case(
            Orchestration.register_group(
              state.project_id,
              parent_id,
              group_id,
              name
            )
          ) do
            :ok -> {[], state}
          end
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "submit" ->
        [
          module,
          target,
          type,
          arguments,
          parent_id,
          group_id,
          wait_for,
          cache,
          defer,
          memo,
          delay,
          retries,
          recurrent,
          requires
        ] = message["params"]

        parent_id = String.to_integer(parent_id)

        if is_recognised_execution?(parent_id, state) do
          case Orchestration.schedule_step(
                 state.project_id,
                 parent_id,
                 module,
                 target,
                 parse_type(type),
                 Enum.map(arguments, &parse_value/1),
                 group_id: group_id,
                 wait_for: wait_for,
                 cache: parse_cache(cache),
                 defer: parse_defer(defer),
                 memo: memo,
                 delay: delay || 0,
                 retries: parse_retries(retries),
                 recurrent: recurrent == true,
                 requires: requires
               ) do
            {:ok, _run_id, _step_id, execution_id, metadata} ->
              # Return execution_id with metadata for log references
              result = [
                Integer.to_string(execution_id),
                metadata.run_id,
                metadata.step_id,
                metadata.attempt,
                metadata.module,
                metadata.target
              ]

              {[success_message(message["id"], result)], state}

            {:error, error} ->
              {[error_message(message["id"], error)], state}
          end
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "record_heartbeats" ->
        [executions] = message["params"]
        executions = Map.new(executions, fn {k, v} -> {String.to_integer(k), v} end)

        if Enum.all?(Map.keys(executions), &is_recognised_execution?(&1, state)) do
          :ok =
            Orchestration.record_heartbeats(
              state.project_id,
              executions,
              state.session_id
            )

          {[], state}
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "notify_terminated" ->
        [execution_ids] = message["params"]
        execution_ids = Enum.map(execution_ids, &String.to_integer/1)

        # TODO: just ignore?
        if Enum.all?(execution_ids, &is_recognised_execution?(&1, state)) do
          :ok =
            Orchestration.notify_terminated(state.project_id, execution_ids)

          state =
            Map.update!(state, :execution_ids, &MapSet.difference(&1, MapSet.new(execution_ids)))

          {[], state}
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "put_result" ->
        [execution_id, value] = message["params"]
        execution_id = String.to_integer(execution_id)

        if is_recognised_execution?(execution_id, state) do
          :ok =
            Orchestration.record_result(
              state.project_id,
              execution_id,
              {:value, parse_value(value)}
            )

          {[], state}
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "put_error" ->
        [execution_id, error] = message["params"]
        execution_id = String.to_integer(execution_id)

        if is_recognised_execution?(execution_id, state) do
          {type, message, frames} = parse_error(error)

          :ok =
            Orchestration.record_result(
              state.project_id,
              execution_id,
              {:error, type, message, frames}
            )

          {[], state}
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "cancel" ->
        [execution_id] = message["params"]
        execution_id = String.to_integer(execution_id)

        case Orchestration.cancel_execution(state.project_id, state.workspace_name, execution_id) do
          :ok ->
            {[], state}

          {:error, :workspace_mismatch} ->
            {[{:close, 4000, "workspace_mismatch"}], nil}

          {:error, :not_found} ->
            {[], state}
        end

      "suspend" ->
        [execution_id, execute_after] = message["params"]
        execution_id = String.to_integer(execution_id)
        # TODO: validate execute_after

        if is_recognised_execution?(execution_id, state) do
          :ok =
            Orchestration.record_result(
              state.project_id,
              execution_id,
              {:suspended, execute_after, []}
            )

          {[], state}
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "get_result" ->
        [execution_id, from_execution_id, timeout_ms] = message["params"]
        execution_id = String.to_integer(execution_id)
        from_execution_id = String.to_integer(from_execution_id)

        if is_recognised_execution?(from_execution_id, state) do
          case Orchestration.get_result(
                 state.project_id,
                 execution_id,
                 from_execution_id,
                 timeout_ms,
                 message["id"]
               ) do
            {:ok, result} ->
              {[success_message(message["id"], compose_result(result))], state}

            :wait ->
              {[], state}
          end
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "put_asset" ->
        [execution_id, name, entries] = message["params"]
        execution_id = String.to_integer(execution_id)

        # TODO: validate

        if is_recognised_execution?(execution_id, state) do
          entries =
            Enum.map(entries, fn {path, [blob_key, size, metadata]} ->
              {path, blob_key, size, metadata}
            end)

          {:ok, external_id, metadata} =
            Orchestration.put_asset(state.project_id, execution_id, name, entries)

          result = [
            external_id,
            metadata.name,
            metadata.total_count,
            metadata.total_size
          ]

          {[success_message(message["id"], result)], state}
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "get_asset" ->
        [asset_external_id, from_execution_id] = message["params"]
        from_execution_id = String.to_integer(from_execution_id)

        if is_recognised_execution?(from_execution_id, state) do
          case Orchestration.get_asset(
                 state.project_id,
                 asset_external_id,
                 from_execution_id
               ) do
            {:ok, _name, entries} ->
              entries =
                Map.new(entries, fn {path, blob_key, size, metadata} ->
                  {path, [blob_key, size, metadata]}
                end)

              {[success_message(message["id"], entries)], state}

            {:error, error} ->
              {[error_message(message["id"], error)], state}
          end
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end
    end
  end

  def websocket_handle(_data, state) do
    {[], state}
  end

  def websocket_info(
        {:execute, execution_id, module, target, arguments, run_id, workspace_id},
        state
      ) do
    arguments = Enum.map(arguments, &compose_value/1)
    state = Map.update!(state, :execution_ids, &MapSet.put(&1, execution_id))

    {[
       command_message("execute", [
         Integer.to_string(execution_id),
         module,
         target,
         arguments,
         run_id,
         Integer.to_string(workspace_id)
       ])
     ], state}
  end

  def websocket_info({:result, request_id, result}, state) do
    {[success_message(request_id, compose_result(result))], state}
  end

  def websocket_info({:abort, execution_id}, state) do
    {[command_message("abort", [Integer.to_string(execution_id)])], state}
  end

  def websocket_info(:stop, state) do
    {[{:close, 4000, "workspace_not_found"}], state}
  end

  defp is_recognised_execution?(execution_id, state) do
    MapSet.member?(state.execution_ids, execution_id)
  end

  defp session_message(session_id) do
    {:text, Jason.encode!([0, session_id])}
  end

  defp command_message(command, params) do
    {:text, Jason.encode!([1, %{"command" => command, "params" => params}])}
  end

  defp success_message(id, result) do
    {:text, Jason.encode!([2, id, result])}
  end

  defp error_message(id, error) do
    {:text, Jason.encode!([3, id, error])}
  end

  defp parse_type(type) do
    case type do
      "workflow" -> :workflow
      "task" -> :task
    end
  end

  defp parse_frames(frames) do
    Enum.map(frames, fn [file, line, name, code] ->
      {file, line, name, code}
    end)
  end

  defp parse_error(error) do
    case error do
      nil ->
        nil

      [type, message, frames] ->
        {type, message, parse_frames(frames)}
    end
  end

  defp parse_references(references) do
    Enum.map(references, fn
      ["fragment", format, blob_key, size, metadata] ->
        {:fragment, format, blob_key, size, metadata}

      ["execution", execution_id | _rest] ->
        {:execution, to_id(execution_id)}

      ["asset", asset_external_id | _rest] ->
        {:asset, asset_external_id}
    end)
  end

  defp to_id(v) when is_integer(v), do: v
  defp to_id(v) when is_binary(v), do: String.to_integer(v)

  defp parse_value(value) do
    case value do
      ["raw", data, references] ->
        {:raw, data, parse_references(references)}

      ["blob", blob_key, size, references] ->
        {:blob, blob_key, size, parse_references(references)}
    end
  end

  def parse_targets(targets) do
    # TODO: validate
    Map.new(targets, fn {module_name, module_targets} ->
      {module_name,
       Map.new(module_targets, fn {type, target_names} ->
         {parse_type(type), target_names}
       end)}
    end)
  end

  def parse_cache(value) do
    if value do
      # TODO: validate
      %{
        params: Map.fetch!(value, "params"),
        max_age: Map.fetch!(value, "max_age"),
        namespace: Map.fetch!(value, "namespace"),
        version: Map.fetch!(value, "version")
      }
    end
  end

  def parse_defer(value) do
    if value do
      # TODO: validate
      %{params: Map.fetch!(value, "params")}
    end
  end

  def parse_retries(value) do
    if value do
      %{
        limit: Map.get(value, "limit"),
        delay_min: Map.fetch!(value, "delay_min"),
        delay_max: Map.fetch!(value, "delay_max")
      }
    end
  end

  defp compose_references(references) do
    Enum.map(references, fn
      {:fragment, format, blob_key, size, metadata} ->
        ["fragment", format, blob_key, size, metadata]

      {:execution, execution_id, metadata} ->
        [
          "execution",
          Integer.to_string(execution_id),
          metadata.run_id,
          metadata.step_id,
          metadata.attempt,
          metadata.module,
          metadata.target
        ]

      {:asset, external_id, {name, total_count, total_size, _entry}} ->
        ["asset", external_id, name, total_count, total_size]
    end)
  end

  defp compose_value(value) do
    # TODO: leave out size?
    case value do
      {:raw, data, references} ->
        ["raw", data, compose_references(references)]

      {:blob, blob_key, size, references} ->
        ["blob", blob_key, size, compose_references(references)]
    end
  end

  defp compose_result(result) do
    case result do
      {:error, type, message, _frames, nil} -> ["error", type, message]
      {:value, value} -> ["value", compose_value(value)]
      {:abandoned, nil} -> ["abandoned"]
      :cancelled -> ["cancelled"]
      :suspended -> ["suspended"]
    end
  end

  defp parse_websocket_protocols(req) do
    case :cowboy_req.parse_header("sec-websocket-protocol", req) do
      :undefined -> []
      protocols -> protocols
    end
  end

  defp extract_session_token(protocols) do
    Enum.find_value(protocols, nil, fn
      "session." <> encoded ->
        case Base.url_decode64(encoded, padding: false) do
          {:ok, token} -> token
          :error -> nil
        end

      _ ->
        nil
    end)
  end
end
