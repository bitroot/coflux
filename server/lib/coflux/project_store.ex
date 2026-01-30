defmodule Coflux.ProjectStore do
  @moduledoc """
  GenServer that owns the project configuration ETS table.

  Projects are loaded from $COFLUX_DATA_DIR/projects.json at startup.
  If COFLUX_PROJECT is set, that project is added automatically.
  Reads go directly to ETS for performance.

  Project file format:
  ```json
  {
    "acme": {
      "tokens": ["<sha256-hash-1>", "<sha256-hash-2>"]
    },
    "demo": {}
  }
  ```

  - Key: project name (string)
  - tokens: controls access to the project:
    - missing/null: open access (no token auth)
    - [] (empty array): locked (no valid tokens)
    - ["hash1", ...]: restricted to listed token hashes (SHA-256, hex, lowercase)
  """

  use GenServer

  alias Coflux.{Config, Utils}

  @table :coflux_projects

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Checks if a project exists. Reads directly from ETS.

  Returns `true` if project exists, `false` otherwise.
  """
  def exists?(project) do
    case :ets.lookup(@table, project) do
      [{^project, _config}] -> true
      [] -> false
    end
  end

  @doc """
  Gets the list of valid token hashes for a project. Reads directly from ETS.

  Returns `{:ok, [hash1, hash2, ...]}` if project exists, `:error` otherwise.
  """
  def get_tokens(project) do
    case :ets.lookup(@table, project) do
      [{^project, config}] -> {:ok, config.tokens}
      [] -> :error
    end
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    table = :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])

    projects = load_projects()

    # Add COFLUX_PROJECT if set and not already in file (with open access)
    projects =
      case Config.project() do
        nil -> projects
        project_id -> Map.put_new(projects, project_id, %{tokens: nil})
      end

    for {project, config} <- projects do
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
      |> Map.new(fn {project_name, config} ->
        {project_name, parse_config(config)}
      end)
    else
      %{}
    end
  end

  defp parse_config(config) when is_map(config) do
    # nil/missing = open access, [] = locked, [...] = restricted
    tokens =
      case Map.get(config, "tokens") do
        nil -> nil
        list when is_list(list) -> list
        _ -> nil
      end

    %{tokens: tokens}
  end

  defp parse_config(_), do: %{tokens: nil}
end
