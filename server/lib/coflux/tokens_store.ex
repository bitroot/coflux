defmodule Coflux.TokensStore do
  @moduledoc """
  GenServer that owns the token configuration ETS table.

  Tokens are loaded from $COFLUX_DATA_DIR/tokens.json at startup.
  Reads go directly to ETS for performance.

  Token file format:
  ```json
  {
    "e3044207e9a26a59388c7224fc3f3c01...": {
      "projects": ["test1", "test2"],
      "workspaces": ["production"]
    },
    "479ccb088f9a5dce29dbff65996beac6...": {
      "projects": ["test1"],
      "workspaces": ["staging", "development/*"]
    }
  }
  ```

  - Key: SHA-256 hash of the token (hex, lowercase)
  - projects: list of project names the token can access
  - workspaces: list of workspace patterns the token can access
    - "production" - exact match
    - "staging/*" - matches "staging", "staging/feature1", etc.
  """

  use GenServer

  alias Coflux.Utils

  @table :coflux_tokens

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Gets the token configuration for a given token hash. Reads directly from ETS.

  Returns `{:ok, %{projects: [...], workspaces: [...]}}` if token exists, `:error` otherwise.
  """
  def get_token_config(token_hash) do
    case :ets.lookup(@table, token_hash) do
      [{^token_hash, config}] -> {:ok, config}
      [] -> :error
    end
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    table = :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])

    tokens = load_tokens()

    for {token_hash, config} <- tokens do
      :ets.insert(@table, {token_hash, config})
    end

    {:ok, %{table: table}}
  end

  defp load_tokens do
    path = Utils.data_path("tokens.json")

    if File.exists?(path) do
      path
      |> File.read!()
      |> Jason.decode!()
      |> Map.new(fn {token_hash, config} ->
        {token_hash, parse_config(config)}
      end)
    else
      %{}
    end
  end

  defp parse_config(config) when is_map(config) do
    projects =
      case Map.get(config, "projects") do
        list when is_list(list) -> list
        _ -> []
      end

    workspaces =
      case Map.get(config, "workspaces") do
        list when is_list(list) -> list
        _ -> []
      end

    %{projects: projects, workspaces: workspaces}
  end

  defp parse_config(_), do: %{projects: [], workspaces: []}
end
