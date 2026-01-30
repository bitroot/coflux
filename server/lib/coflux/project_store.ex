defmodule Coflux.ProjectStore do
  @moduledoc """
  GenServer that owns the project whitelist ETS table.

  Only active when COFLUX_BASE_DOMAIN is configured (subdomain routing).
  Projects are loaded from $COFLUX_DATA_DIR/projects.json at startup.
  Reads go directly to ETS for performance.

  Project file format:
  ```json
  {
    "acme": {},
    "demo": {}
  }
  ```

  - Key: project name (string)
  - Value: reserved for future configuration (currently unused)
  """

  use GenServer

  alias Coflux.Utils

  @table :coflux_projects

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Checks if a project exists in the whitelist. Reads directly from ETS.

  Returns `true` if project is whitelisted, `false` otherwise.
  """
  def exists?(project) do
    case :ets.lookup(@table, project) do
      [{^project, _config}] -> true
      [] -> false
    end
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    table = :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])

    for {project, config} <- load_projects() do
      :ets.insert(@table, {project, config})
    end

    {:ok, %{table: table}}
  end

  defp load_projects do
    path = Utils.data_path("projects.json")

    if File.exists?(path) do
      path
      |> File.read!()
      |> Jason.decode!()
      |> Map.new(fn
        {project_name, config} when is_map(config) ->
          {project_name, config}

        {project_name, _} ->
          {project_name, %{}}
      end)
    else
      %{}
    end
  end
end
