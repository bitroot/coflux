defmodule Coflux.Handlers.Topics.Rest do
  @moduledoc """
  REST handler for capturing topic snapshots.

  Uses Authorization header for authentication (unlike the WebSocket handler
  which uses Sec-WebSocket-Protocol).
  """

  import Coflux.Handlers.Utils

  alias Coflux.Auth
  alias Topical.Adapters.Cowboy.RestHandler

  def init(req, opts) do
    req = set_cors_headers(req)

    case :cowboy_req.method(req) do
      "OPTIONS" ->
        req = :cowboy_req.reply(204, req)
        {:ok, req, opts}

      "GET" ->
        token = get_token(req)
        host = get_host(req)

        with {:ok, project_id} <- resolve_project(host),
             {:ok, _access} <- Auth.check(token, project_id, host) do
          opts = Keyword.put(opts, :init, fn _req -> {:ok, %{project: project_id}} end)
          RestHandler.init(req, opts)
        else
          {:error, :not_configured} ->
            {:ok, json_error_response(req, "not_configured", status: 500), opts}

          {:error, :invalid_host} ->
            {:ok, json_error_response(req, "invalid_host", status: 400), opts}

          {:error, :project_required} ->
            {:ok, json_error_response(req, "project_required", status: 400), opts}

          {:error, :unauthorized} ->
            {:ok, json_error_response(req, "unauthorized", status: 401), opts}
        end

      _ ->
        req = :cowboy_req.reply(405, req)
        {:ok, req, opts}
    end
  end
end
