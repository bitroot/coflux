defmodule Coflux.Handlers.Topics do
  import Coflux.Handlers.Utils

  alias Topical.Adapters.Cowboy.WebsocketHandler, as: TopicalHandler
  alias Coflux.Version

  def init(req, opts) do
    qs = :cowboy_req.parse_qs(req)
    expected_version = get_query_param(qs, "version")

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
  end

  defdelegate websocket_handle(data, state), to: TopicalHandler
  defdelegate websocket_info(info, state), to: TopicalHandler
end
