defmodule Coflux.Handlers.Metrics do
  @moduledoc """
  HTTP handler for the metrics endpoint.

  - POST /metrics - Accept batched metric entries
  - GET /metrics?run=X - Query metrics (JSON) or subscribe (SSE)

  Project is resolved from the request (COFLUX_PROJECT env or subdomain).
  SSE is enabled when Accept: text/event-stream header is present.
  """

  import Coflux.Handlers.Utils

  alias Coflux.{Auth, Metrics}

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

  ## POST /metrics - Write metric entries

  defp handle_post(req, opts) do
    host = get_host(req)

    with {:ok, project_id} <- resolve_project(host),
         {:ok, _access} <- Auth.check(get_token(req), project_id, host) do
      handle_post_with_project(req, opts, project_id)
    else
      {:error, :unauthorized} ->
        req = json_error_response(req, "unauthorized", status: 401)
        {:ok, req, opts}

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
        with {:ok, entries_data} <- get_required(body, "entries"),
             {:ok, entries} <- parse_entries(entries_data) do
          :ok = Metrics.Server.write_metrics(project_id, entries)
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

  ## GET /metrics - Query or subscribe to metrics

  defp handle_get(req, opts) do
    host = get_host(req)

    with {:ok, project_id} <- resolve_project(host),
         {:ok, _access} <- Auth.check(get_token(req), project_id, host) do
      handle_get_with_project(req, opts, project_id)
    else
      {:error, :unauthorized} ->
        req = json_error_response(req, "unauthorized", status: 401)
        {:ok, req, opts}

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
    execution_id = get_query_param(qs, "execution")
    workspace_ids = parse_string_list(get_query_param(qs, "workspaces"))
    keys = parse_string_list(get_query_param(qs, "keys"))
    after_cursor = get_query_param(qs, "after")
    from = parse_integer_param(get_query_param(qs, "from"))

    cond do
      is_nil(run_id) and is_nil(execution_id) ->
        req = json_error_response(req, "run_or_execution_required")
        {:ok, req, opts}

      String.contains?(accept, "text/event-stream") and run_id != nil ->
        handle_sse(req, opts, project_id, run_id, execution_id, workspace_ids, keys, from)

      true ->
        query_opts =
          []
          |> then(fn o -> if run_id, do: [{:run_id, run_id} | o], else: o end)
          |> then(fn o -> if execution_id, do: [{:execution_id, execution_id} | o], else: o end)
          |> then(fn o ->
            if workspace_ids != [], do: [{:workspace_ids, workspace_ids} | o], else: o
          end)
          |> then(fn o -> if keys != [], do: [{:keys, keys} | o], else: o end)
          |> then(fn o -> if after_cursor, do: [{:after, after_cursor} | o], else: o end)
          |> then(fn o -> if from, do: [{:from, from} | o], else: o end)

        case Metrics.Server.query_metrics(project_id, query_opts) do
          {:ok, entries, cursor} ->
            result = %{
              "metrics" => Enum.map(entries, &format_entry/1),
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

  defp handle_sse(req, opts, project_id, run_id, execution_id, workspace_ids, keys, from) do
    subscribe_opts =
      []
      |> then(fn o -> if execution_id, do: [{:execution_id, execution_id} | o], else: o end)
      |> then(fn o ->
        if workspace_ids != [], do: [{:workspace_ids, workspace_ids} | o], else: o
      end)
      |> then(fn o -> if keys != [], do: [{:keys, keys} | o], else: o end)
      |> then(fn o -> if from, do: [{:from, from} | o], else: o end)

    case Metrics.Server.subscribe(project_id, run_id, self(), subscribe_opts) do
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

  def info({:metrics, ref, entries}, req, %{ref: ref} = state) do
    if Enum.any?(entries) do
      send_sse_data(req, Enum.map(entries, &format_entry/1))
    end

    {:ok, req, state}
  end

  def info(_msg, req, state) do
    {:ok, req, state}
  end

  def terminate(_reason, _req, %{ref: ref, project_id: project_id}) do
    Metrics.Server.unsubscribe(project_id, ref)
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

  defp parse_entries(entries) when is_list(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, acc} ->
      parsed = %{
        run_id: Map.get(entry, "runId"),
        execution_id: Map.get(entry, "executionId"),
        workspace_id: Map.get(entry, "workspaceId"),
        key: Map.get(entry, "key"),
        value: Map.get(entry, "value"),
        at: Map.get(entry, "at")
      }

      if is_binary(parsed.run_id) and is_binary(parsed.execution_id) and
           is_binary(parsed.workspace_id) and is_binary(parsed.key) and
           is_number(parsed.value) and is_number(parsed.at) do
        {:cont, {:ok, [parsed | acc]}}
      else
        {:halt, {:error, "entries", "invalid entry structure"}}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      error -> error
    end
  end

  defp parse_entries(_), do: {:error, "entries", "must be an array"}

  defp parse_string_list(nil), do: []
  defp parse_string_list(""), do: []

  defp parse_string_list(value) when is_binary(value) do
    String.split(value, ",")
  end

  defp parse_integer_param(nil), do: nil

  defp parse_integer_param(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} -> int
      _ -> nil
    end
  end

  defp format_entry(entry) do
    %{
      "executionId" => entry.execution_id,
      "workspaceId" => entry.workspace_id,
      "key" => entry.key,
      "value" => entry.value,
      "at" => entry.at
    }
  end

  defp send_sse_data(req, metrics) do
    message = "data: #{Jason.encode!(metrics)}\n\n"
    :cowboy_req.stream_body(message, :nofin, req)
  end
end
