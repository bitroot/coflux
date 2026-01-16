defmodule Coflux.Handlers.WellKnown do
  import Coflux.Handlers.Utils

  @version Mix.Project.config()[:version]
  @api_version (case Version.parse(@version) do
                  {:ok, %Version{major: 0, minor: minor}} -> "0.#{minor}"
                  {:ok, %Version{major: major}} -> "#{major}"
                  :error -> @version
                end)

  def init(req, opts) do
    req = handle(req, :cowboy_req.method(req))
    {:ok, req, opts}
  end

  defp handle(req, "GET") do
    json_response(req, %{"version" => @version, "apiVersion" => @api_version})
  end

  defp handle(req, _method) do
    json_error_response(req, "method_not_allowed", status: 405)
  end
end
