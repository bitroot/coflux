defmodule Coflux.Web do
  alias Coflux.Handlers

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
         {"/.well-known/com.coflux", Handlers.WellKnown, []},
         {"/blobs/:key", Handlers.Blobs, []},
         {"/logs", Handlers.Logs, []},
         {"/worker", Handlers.Worker, []},
         {"/topics", Handlers.Topics.WebSocket, registry: Coflux.TopicalRegistry},
         {"/topics/[...]", Handlers.Topics.Rest, registry: Coflux.TopicalRegistry},
         {"/api/[...]", Handlers.Api, []},
         {"/[...]", Handlers.Root, []}
       ]}
    ])
  end
end
