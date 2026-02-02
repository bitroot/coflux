defmodule Coflux.TokensStore do
  @moduledoc """
  Configuration store for tokens.

  Tokens are loaded from $COFLUX_DATA_DIR/config/tokens.json at startup and
  automatically reloaded when the file changes. If the file is invalid,
  a warning is logged and the old configuration is kept.

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
    - "*" - matches all workspaces
    - "production" - exact match
    - "staging/*" - matches "staging", "staging/feature1", etc.
  """

  use Coflux.ConfigStore,
    filename: "tokens.json",
    table_key: :coflux_tokens_table

  @impl Coflux.ConfigStore
  def parse_entries(data) do
    Map.new(data, fn {token_hash, config} ->
      {token_hash, parse_config(config)}
    end)
  end

  @doc """
  Gets the token configuration for a given token hash. Reads directly from ETS.

  Returns `{:ok, %{projects: [...], workspaces: [...]}}` if token exists, `:error` otherwise.
  """
  def get_token_config(token_hash), do: get(token_hash)

  @doc """
  Returns true if any tokens are configured.
  """
  def has_tokens? do
    table = :persistent_term.get(@table_key)
    :ets.info(table, :size) > 0
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
