defmodule Coflux.Handlers.WellKnown do
  import Coflux.Handlers.Utils

  alias Coflux.Version

  def init(req, opts) do
    req = handle(req, :cowboy_req.method(req))
    {:ok, req, opts}
  end

  defp handle(req, "GET") do
    json_response(req, %{"version" => Version.version(), "apiVersion" => Version.api_version()})
  end

  defp handle(req, _method) do
    json_error_response(req, "method_not_allowed", status: 405)
  end
end
