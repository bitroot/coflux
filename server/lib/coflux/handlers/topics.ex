defmodule Coflux.Handlers.Topics do
  import Coflux.Handlers.Utils

  alias Topical.Adapters.Cowboy.WebsocketHandler, as: TopicalHandler

  @version Mix.Project.config()[:version]
  @api_version (case Version.parse(@version) do
                  {:ok, %Version{major: 0, minor: minor}} -> "0.#{minor}"
                  {:ok, %Version{major: major}} -> "#{major}"
                  :error -> @version
                end)

  def init(req, opts) do
    case check_api_version(req) do
      :ok ->
        TopicalHandler.init(req, opts)

      {:error, expected_version} ->
        req =
          json_error_response(req, "version_mismatch",
            status: 409,
            details: %{
              "server" => @version,
              "expected" => expected_version
            }
          )

        {:ok, req, opts}
    end
  end

  defp check_api_version(req) do
    qs = :cowboy_req.parse_qs(req)

    case List.keyfind(qs, "apiVersion", 0) do
      nil ->
        :ok

      {"apiVersion", expected_version} ->
        if expected_version == @api_version do
          :ok
        else
          {:error, expected_version}
        end
    end
  end

  defdelegate websocket_handle(data, state), to: TopicalHandler
  defdelegate websocket_info(info, state), to: TopicalHandler
end
