defmodule Coflux.Handlers.Topics do
  @moduledoc """
  WebSocket handler for topic subscriptions.

  Authentication is done via Sec-WebSocket-Protocol header using the format:
  `bearer.<base64-encoded-token>` (following the Kubernetes API convention).

  The client should request protocols like: ["bearer.dG9rZW4=", "v1"]
  The server echoes back "v1" on successful auth.

  The project is determined by COFLUX_PROJECT (if set) or extracted from the
  subdomain (if COFLUX_BASE_DOMAIN is set).

  At connection time, the token's workspace patterns are resolved against current
  workspaces to create a snapshot of allowed workspace IDs. This snapshot is used
  for the duration of the connection.
  """

  import Coflux.Handlers.Utils

  alias Topical.Adapters.Cowboy.WebsocketHandler, as: TopicalHandler
  alias Coflux.{Auth, Orchestration, Version}

  @protocol_version "v1"

  def init(req, opts) do
    qs = :cowboy_req.parse_qs(req)
    expected_version = get_query_param(qs, "version")
    protocols = parse_websocket_protocols(req)

    with {:ok, project_id} <- resolve_project(req),
         {:ok, req, allowed_workspace_ids} <- authenticate(req, protocols, project_id) do
      context = %{project: project_id, allowed_workspace_ids: allowed_workspace_ids}
      opts = Keyword.put(opts, :init, fn _req -> {:ok, context} end)

      case Version.check(expected_version) do
        :ok ->
          TopicalHandler.init(req, opts)

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
    else
      {:error, :not_configured} ->
        req = json_error_response(req, "not_configured", status: 500)
        {:ok, req, opts}

      {:error, :invalid_host} ->
        req = json_error_response(req, "invalid_host", status: 400)
        {:ok, req, opts}

      {:error, :project_required} ->
        req = json_error_response(req, "project_required", status: 400)
        {:ok, req, opts}

      {:error, :unauthorized} ->
        req = json_error_response(req, "unauthorized", status: 401)
        {:ok, req, opts}
    end
  end

  defp authenticate(req, protocols, project_id) do
    token =
      case extract_bearer_token(protocols) do
        {:ok, token} -> token
        :none -> nil
      end

    # Get current workspaces to resolve patterns
    {:ok, workspaces} = Orchestration.get_workspaces(project_id)

    case Auth.resolve_allowed_workspaces(token, project_id, workspaces) do
      {:ok, allowed_workspace_ids} ->
        req =
          if token do
            :cowboy_req.set_resp_header("sec-websocket-protocol", @protocol_version, req)
          else
            req
          end

        {:ok, req, allowed_workspace_ids}

      error ->
        error
    end
  end

  defp parse_websocket_protocols(req) do
    case :cowboy_req.parse_header("sec-websocket-protocol", req) do
      :undefined -> []
      protocols -> protocols
    end
  end

  defp extract_bearer_token(protocols) do
    Enum.find_value(protocols, :none, fn
      "bearer." <> encoded ->
        case Base.url_decode64(encoded, padding: false) do
          {:ok, token} -> {:ok, token}
          :error -> nil
        end

      _ ->
        nil
    end)
  end

  defdelegate websocket_handle(data, state), to: TopicalHandler
  defdelegate websocket_info(info, state), to: TopicalHandler
end
