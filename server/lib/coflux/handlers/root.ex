defmodule Coflux.Handlers.Root do
  def init(req, opts) do
    host = :cowboy_req.host(req)
    port = :cowboy_req.port(req)

    redirect_url =
      "https://studio.coflux.com/setup?host=#{URI.encode_www_form("#{host}:#{port}")}"

    req = :cowboy_req.reply(302, %{"location" => redirect_url}, "", req)
    {:ok, req, opts}
  end
end
