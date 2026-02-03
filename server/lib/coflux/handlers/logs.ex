defmodule Coflux.Handlers.Logs do
  @moduledoc """
  HTTP handler for the logs endpoint.

  - POST /logs - Accept batched log messages
  - GET /logs?run_id=X - Query logs (JSON) or subscribe (SSE)

  Project is resolved from the request (COFLUX_PROJECT env or subdomain).
  SSE is enabled when Accept: text/event-stream header is present.
  """

  import Coflux.Handlers.Utils

  alias Coflux.Logs

  def init(req, opts) do
    req = set_cors_headers(req)

    case :cowboy_req.method(req) do
      "OPTIONS" ->
        req = :cowboy_req.reply(204, req)
        {:ok, req, opts}

      "POST" ->
        handle_post(req, opts)

      "GET" ->
        handle_get(req, opts)

      _ ->
        req = json_error_response(req, "method_not_allowed", status: 405)
        {:ok, req, opts}
    end
  end

  ## POST /logs - Write log entries

  defp handle_post(req, opts) do
    host = get_host(req)

    case resolve_project(host) do
      {:ok, project_id} ->
        handle_post_with_project(req, opts, project_id)

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
  end

  defp handle_post_with_project(req, opts, project_id) do
    case read_json_body(req) do
      {:ok, body, req} ->
        with {:ok, messages} <- get_required(body, "messages"),
             {:ok, entries} <- parse_messages(messages) do
          # Route to per-project logs server (async, fire-and-forget)
          :ok = Logs.Server.write_logs(project_id, entries)
          req = json_response(req, 200, %{"ok" => true})
          {:ok, req, opts}
        else
          {:error, field, reason} ->
            req =
              json_error_response(req, "invalid_request",
                details: %{"field" => field, "reason" => reason}
              )

            {:ok, req, opts}
        end

      {:error, :invalid_json} ->
        req = json_error_response(req, "invalid_json")
        {:ok, req, opts}
    end
  end

  ## GET /logs - Query or subscribe to logs

  defp handle_get(req, opts) do
    host = get_host(req)

    case resolve_project(host) do
      {:ok, project_id} ->
        handle_get_with_project(req, opts, project_id)

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
  end

  defp handle_get_with_project(req, opts, project_id) do
    qs = :cowboy_req.parse_qs(req)
    accept = :cowboy_req.header("accept", req, "application/json")

    run_id = get_query_param(qs, "run")
    execution_id = get_query_param(qs, "execution", &String.to_integer/1)
    workspace_ids = parse_id_list(get_query_param(qs, "workspaces"))
    after_cursor = get_query_param(qs, "after")

    cond do
      is_nil(run_id) ->
        req = json_error_response(req, "run_required")
        {:ok, req, opts}

      String.contains?(accept, "text/event-stream") ->
        # SSE mode - subscribe and stream
        handle_sse(req, opts, project_id, run_id, execution_id, workspace_ids)

      true ->
        # JSON mode - query and return
        query_opts = [
          run_id: run_id,
          execution_id: execution_id,
          workspace_ids: workspace_ids,
          after: after_cursor
        ]

        case Logs.Server.query_logs(project_id, query_opts) do
          {:ok, entries, cursor} ->
            result = %{
              "logs" => Enum.map(entries, &format_entry/1),
              "cursor" => cursor
            }

            req = json_response(req, 200, result)
            {:ok, req, opts}

          {:error, reason} ->
            req =
              json_error_response(req, "query_failed", details: %{"reason" => inspect(reason)})

            {:ok, req, opts}
        end
    end
  end

  ## SSE Handling

  defp handle_sse(req, opts, project_id, run_id, execution_id, workspace_ids) do
    subscribe_opts =
      []
      |> then(fn o -> if execution_id, do: [{:execution_id, execution_id} | o], else: o end)
      |> then(fn o ->
        if workspace_ids != [], do: [{:workspace_ids, workspace_ids} | o], else: o
      end)

    case Logs.Server.subscribe(project_id, run_id, self(), subscribe_opts) do
      {:ok, ref, initial_entries} ->
        headers = %{
          "content-type" => "text/event-stream",
          "cache-control" => "no-cache",
          "connection" => "keep-alive"
        }

        req = :cowboy_req.stream_reply(200, headers, req)
        send_sse_data(req, Enum.map(initial_entries, &format_entry/1))
        {:cowboy_loop, req, %{ref: ref, project_id: project_id}}

      {:error, _reason} ->
        req = json_error_response(req, "subscription_failed", status: 500)
        {:ok, req, opts}
    end
  end

  def info({:logs, ref, entries}, req, %{ref: ref} = state) do
    if Enum.any?(entries) do
      send_sse_data(req, Enum.map(entries, &format_entry/1))
    end

    {:ok, req, state}
  end

  def info(_msg, req, state) do
    {:ok, req, state}
  end

  def terminate(_reason, _req, %{ref: ref, project_id: project_id}) do
    Logs.Server.unsubscribe(project_id, ref)
    :ok
  end

  def terminate(_reason, _req, _state) do
    :ok
  end

  ## Helpers

  defp get_required(body, field) do
    case Map.fetch(body, field) do
      {:ok, value} when not is_nil(value) -> {:ok, value}
      _ -> {:error, field, "required"}
    end
  end

  defp parse_messages(messages) when is_list(messages) do
    Enum.reduce_while(messages, {:ok, []}, fn msg, {:ok, acc} ->
      values = Map.get(msg, "values", %{})

      if valid_values?(values) do
        entry = %{
          run_id: Map.get(msg, "runId"),
          execution_id: parse_integer(Map.get(msg, "executionId")),
          workspace_id: parse_integer(Map.get(msg, "workspaceId")),
          timestamp: Map.get(msg, "timestamp"),
          level: Map.get(msg, "level"),
          template: Map.get(msg, "template"),
          values: values
        }

        {:cont, {:ok, [entry | acc]}}
      else
        {:halt, {:error, "values", "invalid structure"}}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      error -> error
    end
  end

  defp parse_messages(_), do: {:error, "messages", "must be an array"}

  defp parse_integer(value) when is_integer(value), do: value
  defp parse_integer(value) when is_binary(value), do: String.to_integer(value)
  defp parse_integer(nil), do: nil

  defp parse_id_list(nil), do: []
  defp parse_id_list(""), do: []

  defp parse_id_list(value) when is_binary(value) do
    value
    |> String.split(",")
    |> Enum.map(&String.to_integer/1)
  end

  # Validation functions - check structure but pass through as-is

  defp valid_values?(values) when is_map(values) do
    Enum.all?(values, fn {_key, value} -> valid_value?(value) end)
  end

  defp valid_values?(_), do: false

  defp valid_value?(%{"type" => "raw", "references" => refs}) when is_list(refs) do
    Enum.all?(refs, &valid_reference?/1)
  end

  defp valid_value?(%{"type" => "blob", "key" => key, "size" => size, "references" => refs})
       when is_binary(key) and is_integer(size) and is_list(refs) do
    Enum.all?(refs, &valid_reference?/1)
  end

  defp valid_value?(_), do: false

  defp valid_reference?(%{"type" => "execution", "executionId" => id})
       when is_binary(id) or is_integer(id),
       do: true

  defp valid_reference?(%{"type" => "asset", "assetId" => id})
       when is_binary(id) or is_integer(id),
       do: true

  defp valid_reference?(%{"type" => "fragment", "format" => f, "blobKey" => k, "size" => s})
       when is_binary(f) and is_binary(k) and is_integer(s),
       do: true

  defp valid_reference?(_), do: false

  defp format_entry(entry) do
    %{
      "executionId" => Integer.to_string(entry.execution_id),
      "workspaceId" => Integer.to_string(entry.workspace_id),
      "timestamp" => entry.timestamp,
      "level" => entry.level,
      "template" => entry.template,
      "values" => entry.values
    }
  end

  defp send_sse_data(req, logs) do
    message = "data: #{Jason.encode!(logs)}\n\n"
    :cowboy_req.stream_body(message, :nofin, req)
  end
end
