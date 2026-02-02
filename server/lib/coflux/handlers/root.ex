defmodule Coflux.Handlers.Root do
  alias Coflux.Config

  def init(req, opts) do
    host = :cowboy_req.host(req)
    port = :cowboy_req.port(req)
    studio_url = Config.studio_url()

    redirect_url = "#{studio_url}/setup?host=#{URI.encode_www_form("#{host}:#{port}")}"

    req = :cowboy_req.reply(302, %{"location" => redirect_url}, "", req)
    {:ok, req, opts}
  end
end
