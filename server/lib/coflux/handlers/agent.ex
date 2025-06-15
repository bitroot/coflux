defmodule Coflux.Handlers.Agent do
  import Coflux.Handlers.Utils

  alias Coflux.{Orchestration, Projects}

  def init(req, _opts) do
    qs = :cowboy_req.parse_qs(req)
    # TODO: validate
    project_id = get_query_param(qs, "project")
    session_id = get_query_param(qs, "session")
    workspace_name = get_query_param(qs, "workspace")
    agent_id = get_query_param(qs, "launch", &String.to_integer/1)
    provides = get_query_param(qs, "provides", &parse_provides/1)
    concurrency = get_query_param(qs, "concurrency", &String.to_integer/1) || 0

    {:cowboy_websocket, req,
     {project_id, session_id, workspace_name, agent_id, provides, concurrency}}
  end

  def websocket_init({project_id, session_id, workspace_name, agent_id, provides, concurrency}) do
    case Projects.get_project_by_id(Coflux.ProjectsServer, project_id) do
      {:ok, _} ->
        # TODO: authenticate
        # TODO: monitor server?
        case connect(project_id, session_id, workspace_name, agent_id, provides, concurrency) do
          {:ok, session_id, execution_ids} ->
            {[session_message(session_id)],
             %{
               project_id: project_id,
               session_id: session_id,
               execution_ids: execution_ids
             }}

          {:error, :workspace_invalid} ->
            {[{:close, 4000, "workspace_not_found"}], nil}

          {:error, :no_agent} ->
            {[{:close, 4000, "launch_invalid"}], nil}

          {:error, :no_session} ->
            {[{:close, 4000, "session_invalid"}], nil}
        end

      :error ->
        {[{:close, 4000, "project_not_found"}], nil}
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
          execute_after,
          retries,
          requires
        ] = message["params"]

        if is_recognised_execution?(parent_id, state) do
          case Orchestration.schedule_step(
                 state.project_id,
                 parent_id,
                 module,
                 target,
                 parse_type(type),
                 Enum.map(arguments, &parse_value/1),
                 group_id: group_id,
                 execute_after: execute_after,
                 wait_for: wait_for,
                 cache: parse_cache(cache),
                 defer: parse_defer(defer),
                 memo: memo,
                 retries: parse_retries(retries),
                 requires: requires
               ) do
            {:ok, _run_id, _step_id, execution_id} ->
              {[success_message(message["id"], execution_id)], state}

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

      "record_checkpoint" ->
        [execution_id, arguments] = message["params"]

        if is_recognised_execution?(execution_id, state) do
          arguments = Enum.map(arguments, &parse_value/1)

          :ok =
            Orchestration.record_checkpoint(
              state.project_id,
              execution_id,
              arguments
            )

          {[], state}
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "notify_terminated" ->
        [execution_ids] = message["params"]

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

        # TODO: restrict which executions can be cancelled?
        :ok = Orchestration.cancel_execution(state.project_id, execution_id)
        {[], state}

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

      "get_result" ->
        [execution_id, from_execution_id, timeout_ms] = message["params"]

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
        [execution_id, entries] = message["params"]

        # TODO: validate

        if is_recognised_execution?(execution_id, state) do
          entries =
            Enum.map(entries, fn {path, [blob_key, size, metadata]} ->
              {path, blob_key, size, metadata}
            end)

          {:ok, asset_id} =
            Orchestration.put_asset(state.project_id, execution_id, entries)

          {[success_message(message["id"], asset_id)], state}
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end

      "get_asset" ->
        [asset_id, from_execution_id] = message["params"]

        if is_recognised_execution?(from_execution_id, state) do
          case Orchestration.get_asset(state.project_id, asset_id, from_execution_id) do
            {:ok, entries} ->
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

      "log_messages" ->
        messages =
          Enum.reduce(
            message["params"],
            %{},
            fn [execution_id, timestamp, level, template, values], acc ->
              values = Map.new(values, fn {k, v} -> {k, parse_value(v)} end)
              message = {timestamp, parse_level(level), template, values}

              acc
              |> Map.put_new(execution_id, [])
              |> Map.update!(execution_id, &[message | &1])
            end
          )

        if Enum.all?(Map.keys(messages), &is_recognised_execution?(&1, state)) do
          Enum.each(messages, fn {execution_id, messages} ->
            Orchestration.record_logs(state.project_id, execution_id, Enum.reverse(messages))
          end)

          {[], state}
        else
          {[{:close, 4000, "execution_invalid"}], nil}
        end
    end
  end

  def websocket_handle(_data, state) do
    {[], state}
  end

  def websocket_info({:execute, execution_id, module, target, arguments}, state) do
    arguments = Enum.map(arguments, &compose_value/1)
    state = Map.update!(state, :execution_ids, &MapSet.put(&1, execution_id))
    {[command_message("execute", [execution_id, module, target, arguments])], state}
  end

  def websocket_info({:result, request_id, result}, state) do
    {[success_message(request_id, compose_result(result))], state}
  end

  def websocket_info({:abort, execution_id}, state) do
    {[command_message("abort", [execution_id])], state}
  end

  def websocket_info(:stop, state) do
    {[{:close, 4000, "workspace_not_found"}], state}
  end

  defp connect(project_id, session_id, workspace_name, agent_id, provides, concurrency) do
    if session_id do
      with {:ok, execution_ids} <- Orchestration.resume_session(project_id, session_id, self()) do
        {:ok, session_id, execution_ids}
      end
    else
      with {:ok, session_id} <-
             Orchestration.start_session(
               project_id,
               workspace_name,
               agent_id,
               provides,
               concurrency,
               self()
             ) do
        {:ok, session_id, MapSet.new()}
      end
    end
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
      "sensor" -> :sensor
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

      ["execution", execution_id] ->
        {:execution, execution_id}

      ["asset", asset_id] ->
        {:asset, asset_id}
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
        limit: Map.fetch!(value, "limit"),
        delay_min: Map.fetch!(value, "delay_min"),
        delay_max: Map.fetch!(value, "delay_max")
      }
    end
  end

  defp compose_references(references) do
    Enum.map(references, fn
      {:fragment, format, blob_key, size, metadata} ->
        ["fragment", format, blob_key, size, metadata]

      {:execution, execution_id} ->
        ["execution", execution_id]

      {:asset, asset_id} ->
        ["asset", asset_id]
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

  defp parse_level(level) do
    case level do
      0 -> :debug
      1 -> :stdout
      2 -> :info
      3 -> :stderr
      4 -> :warning
      5 -> :error
    end
  end

  defp parse_provides(value) do
    value
    |> String.split(";", trim: true)
    |> Enum.reduce(%{}, fn part, result ->
      [key, value] = String.split(part, ":", parts: 2)
      Map.update(result, key, [value], &[value | &1])
    end)
  end
end
