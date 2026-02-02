defmodule Coflux.Application do
  use Application

  alias Coflux.{Config, JwksStore, TokensStore, Orchestration, Logs, Topics}

  @impl true
  def start(_type, _args) do
    port = String.to_integer(System.get_env("PORT", "7777"))

    Config.init()

    children =
      [
        TokensStore,
        # TODO: separate launch supervisor per project? (and specify max_children?)
        {Task.Supervisor, name: Coflux.LauncherSupervisor},
        Orchestration.Supervisor,
        {Registry, keys: :unique, name: Coflux.Logs.Registry},
        Logs.Supervisor,
        {Topical, name: Coflux.TopicalRegistry, topics: topics()},
        {Coflux.Web, port: port}
      ] ++ auth_children()

    opts = [strategy: :one_for_one, name: Coflux.Supervisor]

    with {:ok, pid} <- Supervisor.start_link(children, opts) do
      IO.puts("Server started. Running on port #{port}.")
      {:ok, pid}
    end
  end

  # Start JWKS store only when using studio auth mode
  defp auth_children do
    case Config.auth_mode() do
      :studio -> [JwksStore]
      _ -> []
    end
  end

  defp topics() do
    [
      Topics.Sessions,
      Topics.Workspaces,
      Topics.Modules,
      Topics.Run,
      Topics.Workflow,
      Topics.Module,
      Topics.Pools,
      Topics.Pool,
      Topics.Search
    ]
  end
end
