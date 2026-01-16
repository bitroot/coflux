defmodule Coflux.Handlers.WellKnown do
  import Coflux.Handlers.Utils

  def init(req, opts) do
    req = handle(req, :cowboy_req.method(req))
    {:ok, req, opts}
  end

  defp handle(req, "GET") do
    version = Application.spec(:coflux, :vsn) |> to_string()
    json_response(req, %{"version" => version})
  end

  defp handle(req, _method) do
    json_error_response(req, "method_not_allowed", status: 405)
  end
end
