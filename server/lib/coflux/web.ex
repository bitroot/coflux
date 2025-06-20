defmodule Coflux.Web do
  @otp_app Mix.Project.config()[:app]

  alias Coflux.Handlers
  alias Topical.Adapters.Cowboy.WebsocketHandler, as: TopicalHandler

  def child_spec(opts) do
    port = Keyword.fetch!(opts, :port)
    trans_opts = %{socket_opts: [port: port]}
    proto_opts = %{env: %{dispatch: dispatch()}, connection_type: :supervisor}
    :ranch.child_spec(:http, :ranch_tcp, trans_opts, :cowboy_clear, proto_opts)
  end

  defp dispatch() do
    :cowboy_router.compile([
      {:_,
       [
         {"/blobs/:key", Handlers.Blobs, []},
         {"/worker", Handlers.Worker, []},
         {"/topics", TopicalHandler, registry: Coflux.TopicalRegistry},
         {"/api/[...]", Handlers.Api, []},
         {"/static/[...]", :cowboy_static, {:priv_dir, @otp_app, "static"}},
         {"/[...]", Handlers.Root, []}
       ]}
    ])
  end
end
