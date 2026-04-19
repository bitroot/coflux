defmodule Coflux.Handlers.Worker do
  @moduledoc """
  WebSocket handler for worker connections.

  Authentication is done via Sec-WebSocket-Protocol header using the format:
  `session.<base64-encoded-token>` (similar to the topics handler pattern).

  The client should request protocols like: ["session.dG9rZW4=", "v1"]
  The server echoes back "v1" on successful auth.

  The project is determined by COFLUX_PROJECT (if set), extracted from the
  subdomain (if COFLUX_PUBLIC_HOST starts with %), or from the X-Project header.
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
        case resolve_project(req) do
          {:ok, project_id} ->
            workspace_id = get_query_param(qs, "workspaceId")
            session_token = extract_session_token(protocols)

            req = :cowboy_req.set_resp_header("sec-websocket-protocol", @protocol_version, req)
            {:cowboy_websocket, req, {project_id, workspace_id, session_token}}

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

  def websocket_init({project_id, workspace_id, session_token}) do
    # TODO: monitor server?
    case Orchestration.resume_session(project_id, session_token, workspace_id, self()) do
      {:ok, external_id, external_execution_ids} ->
        {[session_message(external_id, external_execution_ids)],
         %{
           project_id: project_id,
           workspace_id: workspace_id,
           session_id: external_id,
           execution_ids: MapSet.new(external_execution_ids)
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
        [targets, concurrency] = message["params"]

        case Orchestration.declare_targets(
               state.project_id,
               state.session_id,
               parse_targets(targets),
               concurrency
             ) do
          :ok ->
            {[], state}
        end

      "session_draining" ->
        :ok = Orchestration.session_draining(state.project_id, state.session_id)
        {[], state}

      "register_group" ->
        [parent_id, group_id, name] = message["params"]

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
          | rest
        ] = message["params"]

        timeout = List.first(rest) || 0

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
                 requires: requires,
                 timeout: timeout
               ) do
            {:ok, _run_id, _step_id, execution_external_id, metadata} ->
              result = [
                execution_external_id,
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

      "started" ->
        [execution_id, metadata] = message["params"]

        if is_recognised_execution?(execution_id, state) do
          :ok =
            Orchestration.execution_started(
              state.project_id,
              execution_id,
              metadata
            )
        end

        {[], state}

      "define_metric" ->
        [execution_id, key, definition] = message["params"]

        if is_recognised_execution?(execution_id, state) do
          :ok =
            Orchestration.define_metric(
              state.project_id,
              execution_id,
              key,
              definition
            )
        end

        {[], state}

      "record_heartbeats" ->
        [executions] = message["params"]

        executions =
          Map.filter(executions, fn {id, _} -> is_recognised_execution?(id, state) end)

        if Enum.any?(executions) do
          :ok =
            Orchestration.record_heartbeats(
              state.project_id,
              executions,
              state.session_id
            )
        end

        {[], state}

      "notify_terminated" ->
        [execution_ids] = message["params"]

        execution_ids =
          Enum.filter(execution_ids, &is_recognised_execution?(&1, state))

        if Enum.any?(execution_ids) do
          :ok =
            Orchestration.notify_terminated(state.project_id, execution_ids)
        end

        state =
          Map.update!(state, :execution_ids, &MapSet.difference(&1, MapSet.new(execution_ids)))

        {[], state}

      "put_result" ->
        [execution_id, value] = message["params"]

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

      "stream_register" ->
        [execution_id, index] = message["params"]

        if is_recognised_execution?(execution_id, state) do
          case Orchestration.register_stream(state.project_id, execution_id, index) do
            :ok -> {[], state}
            # Idempotent — a duplicate register is harmless.
            {:error, :already_registered} -> {[], state}
            {:error, :not_found} -> {[{:close, 4000, "execution_invalid"}], nil}
          end
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "stream_append" ->
        [execution_id, index, sequence, value] = message["params"]

        if is_recognised_execution?(execution_id, state) do
          case Orchestration.append_stream_item(
                 state.project_id,
                 execution_id,
                 index,
                 sequence,
                 parse_value(value)
               ) do
            :ok ->
              {[], state}

            # Worker is trying to append to a stream the server has already
            # closed (e.g., owner execution was cancelled). Surfacing this to
            # the adapter would let it stop producing; for now, swallow so
            # the stream-close propagation to the worker (task #10) is the
            # canonical signal.
            {:error, :closed} ->
              {[], state}

            {:error, :not_registered} ->
              {[], state}

            {:error, :already_appended} ->
              {[], state}

            {:error, :not_found} ->
              {[{:close, 4000, "execution_invalid"}], nil}
          end
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "stream_close" ->
        [execution_id, index, error] = message["params"]

        if is_recognised_execution?(execution_id, state) do
          parsed_error =
            case parse_error(error) do
              nil -> nil
              {type, message, frames, _retryable} -> {type, message, frames}
            end

          case Orchestration.close_stream(
                 state.project_id,
                 execution_id,
                 index,
                 parsed_error
               ) do
            :ok -> {[], state}
            {:error, :already_closed} -> {[], state}
            {:error, :not_registered} -> {[], state}
            {:error, :not_found} -> {[{:close, 4000, "execution_invalid"}], nil}
          end
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "stream_subscribe" ->
        [
          subscription_id,
          consumer_execution_id,
          producer_execution_id,
          index,
          from_sequence,
          filter
        ] = message["params"]

        if is_recognised_execution?(consumer_execution_id, state) do
          case Orchestration.subscribe_stream(
                 state.project_id,
                 state.session_id,
                 subscription_id,
                 consumer_execution_id,
                 producer_execution_id,
                 index,
                 from_sequence,
                 filter
               ) do
            :ok ->
              {[], state}

            # If the stream doesn't exist yet (or producer vanished), push an
            # immediate close so the consumer doesn't wait forever.
            {:error, reason}
            when reason in [:stream_not_found, :producer_not_found, :already_subscribed] ->
              {[
                 command_message("stream_closed", [
                   consumer_execution_id,
                   subscription_id,
                   %{"type" => "Coflux.StreamNotFound", "message" => Atom.to_string(reason)}
                 ])
               ], state}

            {:error, :consumer_not_found} ->
              {[{:close, 4000, "execution_invalid"}], nil}
          end
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "stream_unsubscribe" ->
        [consumer_execution_id, subscription_id] = message["params"]

        :ok =
          Orchestration.unsubscribe_stream(
            state.project_id,
            state.session_id,
            consumer_execution_id,
            subscription_id
          )

        {[], state}

      "put_error" ->
        [execution_id, error] = message["params"]

        if is_recognised_execution?(execution_id, state) do
          {type, message, frames, retryable} = parse_error(error)

          :ok =
            Orchestration.record_result(
              state.project_id,
              execution_id,
              {:error, type, message, frames, retryable}
            )

          {[], state}
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "put_timeout" ->
        [execution_id | _] = message["params"]

        if is_recognised_execution?(execution_id, state) do
          :ok =
            Orchestration.record_result(
              state.project_id,
              execution_id,
              :timeout
            )

          {[], state}
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "cancel" ->
        [handles, from_execution_id] = message["params"]

        if is_recognised_execution?(from_execution_id, state) do
          case Orchestration.cancel(
                 state.project_id,
                 handles,
                 state.workspace_id,
                 from_execution_id
               ) do
            :ok ->
              {[success_message(message["id"], nil)], state}

            {:error, :workspace_not_found} ->
              {[{:close, 4000, "workspace_mismatch"}], nil}
          end
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "suspend" ->
        [execution_id, execute_after] = message["params"]
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

      "select" ->
        {handles, from_execution_id, timeout_ms, suspend, cancel_remaining} =
          case message["params"] do
            [h, feid, tms, s, cr] -> {h, feid, tms, s, cr}
            [h, feid, tms, s] -> {h, feid, tms, s, false}
          end

        if is_recognised_execution?(from_execution_id, state) do
          case Orchestration.select(
                 state.project_id,
                 handles,
                 from_execution_id,
                 timeout_ms,
                 suspend,
                 cancel_remaining,
                 message["id"]
               ) do
            {:ok, :timeout} ->
              {[success_message(message["id"], compose_select_result(:timeout))], state}

            {:ok, :suspended} ->
              # Respond with a null result to unblock the pending RPC before
              # the separate :abort command terminates the executor. The
              # worker kills the process regardless; sending this response
              # just clears the pending request so the dispatch loop can exit.
              {[success_message(message["id"], compose_select_result(:timeout))], state}

            {:ok, {idx, result}} ->
              {[success_message(message["id"], compose_select_result({idx, result}))], state}

            :wait ->
              {[], state}

            {:error, :execution_not_found} ->
              {[{:close, 4000, "execution_invalid"}], nil}

            {:error, :input_not_found} ->
              {[error_message(message["id"], "input_not_found")], state}
          end
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "submit_input" ->
        {execution_id, template, placeholders, schema_json, key, title, actions, initial,
         requires} =
          case message["params"] do
            [eid, t, p, s, k, ti, a, i, r] -> {eid, t, p, s, k, ti, a, i, r}
            [eid, t, p, s, k, ti, a, i] -> {eid, t, p, s, k, ti, a, i, nil}
          end

        if is_recognised_execution?(execution_id, state) do
          parsed_placeholders =
            Enum.map(placeholders || %{}, fn {placeholder, value} ->
              {placeholder, parse_value(value)}
            end)

          actions_json = if actions, do: Jason.encode!(actions)
          initial_json = if initial, do: Jason.encode!(initial)

          case Orchestration.submit_input(
                 state.project_id,
                 execution_id,
                 template,
                 parsed_placeholders,
                 schema_json,
                 key,
                 title,
                 actions_json,
                 initial_json,
                 requires
               ) do
            {:ok, input_external_id} ->
              {[success_message(message["id"], input_external_id)], state}

            {:error, {:invalid_schema, reason}} ->
              {[error_message(message["id"], "invalid_schema: #{reason}")], state}

            {:error, {:invalid_initial, reason}} ->
              {[error_message(message["id"], "invalid_initial: #{reason}")], state}

            {:error, :input_mismatch} ->
              {[error_message(message["id"], "input_mismatch")], state}
          end
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "put_asset" ->
        [execution_id, name, entries] = message["params"]

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
        {:execute, execution_external_id, module, target, arguments, run_id,
         workspace_external_id, timeout},
        state
      ) do
    arguments = Enum.map(arguments, &compose_value/1)

    state = Map.update!(state, :execution_ids, &MapSet.put(&1, execution_external_id))

    {[
       command_message("execute", [
         execution_external_id,
         module,
         target,
         arguments,
         run_id,
         workspace_external_id,
         timeout
       ])
     ], state}
  end

  def websocket_info({:result, request_id, payload}, state) do
    {[success_message(request_id, compose_select_result(payload))], state}
  end

  def websocket_info({:abort, execution_external_id}, state) do
    {[command_message("abort", [execution_external_id])], state}
  end

  def websocket_info({:stream_items, execution_external_id, subscription_id, items}, state) do
    # Items arrive in resolved form ([[sequence, value_tuple], ...]); compose
    # each value tuple to wire JSON here.
    encoded =
      Enum.map(items, fn [sequence, value] ->
        [sequence, compose_value(value)]
      end)

    {[command_message("stream_items", [execution_external_id, subscription_id, encoded])], state}
  end

  def websocket_info({:stream_closed, execution_external_id, subscription_id, error}, state) do
    {[command_message("stream_closed", [execution_external_id, subscription_id, error])], state}
  end

  def websocket_info(:stop, state) do
    {[{:close, 4000, "workspace_not_found"}], state}
  end

  defp is_recognised_execution?(execution_id, state) do
    MapSet.member?(state.execution_ids, execution_id)
  end

  defp session_message(session_id, execution_ids) do
    {:text, Jason.encode!([0, session_id, execution_ids])}
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
        {type, message, parse_frames(frames), nil}

      [type, message, frames, retryable] ->
        {type, message, parse_frames(frames), retryable}
    end
  end

  defp parse_references(references) do
    Enum.map(references, fn
      ["fragment", format, blob_key, size, metadata] ->
        {:fragment, format, blob_key, size, metadata}

      ["execution", execution_id | _rest] ->
        {:execution, execution_id}

      ["asset", asset_external_id | _rest] ->
        {:asset, asset_external_id}

      ["input", input_external_id | _rest] ->
        {:input, input_external_id}
    end)
  end

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
        backoff_min: Map.get(value, "backoff_min"),
        backoff_max: Map.get(value, "backoff_max")
      }
    end
  end

  defp compose_references(references) do
    Enum.map(references, fn
      {:fragment, format, blob_key, size, metadata} ->
        ["fragment", format, blob_key, size, metadata]

      {:execution, execution_external_id, {module, target}} ->
        ["execution", execution_external_id, module, target]

      {:asset, external_id, {name, total_count, total_size, _entry}} ->
        ["asset", external_id, name, total_count, total_size]

      {:input, input_external_id} ->
        ["input", input_external_id]
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

  # Compose a select result for the CLI. Returns a JSON-compatible map,
  # or `nil` when the select wait itself expired (no handle resolved).
  # Payload is one of:
  #   :timeout                       (wait expired; `nil` on the wire)
  #   {handle_index, result_detail}
  #     where result_detail is one of:
  #       {:value, value}
  #       {:error, type, message, frames, retry_id, retryable?}
  #       :cancelled | :dismissed | {:abandoned, _} | {:crashed, _} |
  #       {:timeout, _} | :suspended
  defp compose_select_result(:timeout) do
    nil
  end

  defp compose_select_result({idx, detail}) do
    base = %{"winner" => idx}

    case detail do
      {:value, value} ->
        Map.merge(base, %{"status" => "ok", "value" => compose_value(value)})

      {:error, type, message, frames, _retry_id, _retryable} ->
        Map.merge(base, %{
          "status" => "error",
          "error" => compose_error(type, message, frames)
        })

      {:error, type, message, frames, _retry_id} ->
        Map.merge(base, %{
          "status" => "error",
          "error" => compose_error(type, message, frames)
        })

      :cancelled ->
        Map.put(base, "status", "cancelled")

      :dismissed ->
        Map.put(base, "status", "dismissed")

      {:abandoned, _} ->
        Map.put(base, "status", "abandoned")

      {:crashed, _} ->
        Map.put(base, "status", "crashed")

      {:timeout, _} ->
        Map.put(base, "status", "timeout")

      :suspended ->
        # Should not reach the client: suspension is signalled via :abort
        # rather than as a winner status.
        Map.put(base, "status", "timeout")
    end
  end

  defp compose_error(type, message, frames) do
    traceback = format_traceback(frames)

    error = %{"type" => type, "message" => message}

    if traceback != "" do
      Map.put(error, "traceback", traceback)
    else
      error
    end
  end

  defp format_traceback(frames) when is_list(frames) do
    frames
    |> Enum.map(fn
      {file, line, name, code} ->
        "  File \"#{file}\", line #{line}, in #{name}\n    #{code}"

      _ ->
        ""
    end)
    |> Enum.reject(&(&1 == ""))
    |> Enum.join("\n")
  end

  defp format_traceback(_), do: ""

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
