defmodule Coflux.TokensStore do
  @moduledoc """
  GenServer that owns the token configuration ETS table.

  Tokens are loaded from $COFLUX_DATA_DIR/tokens.json at startup and
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

  use GenServer

  require Logger

  alias Coflux.Utils

  @filename "tokens.json"
  @table_key :coflux_tokens_table

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Gets the token configuration for a given token hash. Reads directly from ETS.

  Returns `{:ok, %{projects: [...], workspaces: [...]}}` if token exists, `:error` otherwise.
  """
  def get_token_config(token_hash) do
    table = :persistent_term.get(@table_key)

    case :ets.lookup(table, token_hash) do
      [{^token_hash, config}] -> {:ok, config}
      [] -> :error
    end
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    path = Utils.data_path(@filename)
    dir = Path.dirname(path)

    # Create and populate initial table
    table = :ets.new(:coflux_tokens, [:set, :public, read_concurrency: true])
    load_into_table(table, path)
    :persistent_term.put(@table_key, table)

    # Start file watcher (optional - may not be available on all systems)
    watcher_pid = start_file_watcher(dir)

    {:ok, %{path: path, watcher_pid: watcher_pid}}
  end

  @impl true
  def handle_info({:file_event, _watcher_pid, {path, _events}}, state) do
    if Path.basename(path) == @filename do
      reload_tokens(state)
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info({:file_event, _watcher_pid, :stop}, state) do
    # Watcher stopped, try to restart it
    Logger.warning("File watcher stopped, restarting...")
    dir = Path.dirname(state.path)
    watcher_pid = start_file_watcher(dir)
    {:noreply, %{state | watcher_pid: watcher_pid}}
  end

  @impl true
  def handle_info({:delete_table, table}, state) do
    :ets.delete(table)
    {:noreply, state}
  end

  defp reload_tokens(state) do
    case load_and_validate(state.path) do
      {:ok, tokens} ->
        # Create and populate new table
        new_table = :ets.new(:coflux_tokens, [:set, :public, read_concurrency: true])

        for {token_hash, config} <- tokens do
          :ets.insert(new_table, {token_hash, config})
        end

        # Atomic swap
        old_table = :persistent_term.get(@table_key)
        :persistent_term.put(@table_key, new_table)

        # Delete old table after in-flight lookups complete
        Process.send_after(self(), {:delete_table, old_table}, :timer.seconds(1))

        {:noreply, state}

      {:error, reason} ->
        Logger.warning("Failed to reload #{@filename}: #{reason}, keeping old config")
        {:noreply, state}
    end
  end

  defp load_into_table(table, path) do
    case load_and_validate(path) do
      {:ok, tokens} ->
        for {token_hash, config} <- tokens do
          :ets.insert(table, {token_hash, config})
        end

      {:error, reason} ->
        Logger.warning("Failed to load #{@filename}: #{reason}")
    end
  end

  defp load_and_validate(path) do
    if File.exists?(path) do
      case File.read(path) do
        {:ok, content} ->
          case Jason.decode(content) do
            {:ok, data} when is_map(data) ->
              tokens =
                Map.new(data, fn {token_hash, config} ->
                  {token_hash, parse_config(config)}
                end)

              {:ok, tokens}

            {:ok, _} ->
              {:error, "expected JSON object"}

            {:error, %Jason.DecodeError{} = error} ->
              {:error, "invalid JSON: #{Exception.message(error)}"}
          end

        {:error, reason} ->
          {:error, "failed to read file: #{inspect(reason)}"}
      end
    else
      {:ok, %{}}
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

  defp start_file_watcher(dir) do
    case FileSystem.start_link(dirs: [dir]) do
      {:ok, watcher_pid} ->
        FileSystem.subscribe(watcher_pid)
        watcher_pid

      {:error, reason} ->
        Logger.warning("File watching not available for #{@filename}: #{inspect(reason)}")
        nil

      :ignore ->
        Logger.warning("File watching not available for #{@filename}")
        nil
    end
  end
end
