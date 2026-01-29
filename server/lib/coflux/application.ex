defmodule Coflux.Application do
  use Application

  alias Coflux.{Config, Projects, Orchestration, Topics}
  alias Coflux.Auth.TokenStore

  @impl true
  def start(_type, _args) do
    port = String.to_integer(System.get_env("PORT", "7777"))

    Config.init()

    children =
      [
        TokenStore,
        {Projects, name: Coflux.ProjectsServer},
        # TODO: separate launch supervisor per project? (and specify max_children?)
        {Task.Supervisor, name: Coflux.LauncherSupervisor},
        Orchestration.Supervisor,
        {Topical, name: Coflux.TopicalRegistry, topics: topics()},
        {Coflux.Web, port: port}
      ]
      |> Enum.filter(& &1)

    opts = [strategy: :one_for_one, name: Coflux.Supervisor]

    with {:ok, pid} <- Supervisor.start_link(children, opts) do
      IO.puts("Server started. Running on port #{port}.")
      {:ok, pid}
    end
  end

  defp topics() do
    [
      Topics.Sessions,
      Topics.Projects,
      Topics.Workspaces,
      Topics.Modules,
      Topics.Run,
      Topics.Workflow,
      Topics.Logs,
      Topics.Module,
      Topics.Pools,
      Topics.Pool,
      Topics.Search
    ]
  end
end
