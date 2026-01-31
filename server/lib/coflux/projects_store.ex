defmodule Coflux.ProjectsStore do
  @moduledoc """
  GenServer that owns the project configuration ETS table.

  Projects are loaded from $COFLUX_DATA_DIR/projects.json at startup and
  automatically reloaded when the file changes. If the file is invalid,
  a warning is logged and the old configuration is kept.

  If COFLUX_PROJECT is set, that project is added automatically.

  Project file format:
  ```json
  {
    "test1": {},
    "test2": {}
  }
  ```

  - Key: project name (string)
  """

  use GenServer

  require Logger

  alias Coflux.{Config, Utils}

  @filename "projects.json"
  @table_key :coflux_projects_table

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Checks if a project exists. Reads directly from ETS.

  Returns `true` if project exists, `false` otherwise.
  """
  def exists?(project) do
    table = :persistent_term.get(@table_key)

    case :ets.lookup(table, project) do
      [{^project, _config}] -> true
      [] -> false
    end
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    path = Utils.data_path(@filename)
    dir = Path.dirname(path)

    # Create and populate initial table
    table = :ets.new(:coflux_projects, [:set, :public, read_concurrency: true])
    load_into_table(table, path)
    :persistent_term.put(@table_key, table)

    # Start file watcher (optional - may not be available on all systems)
    watcher_pid = start_file_watcher(dir)

    {:ok, %{path: path, watcher_pid: watcher_pid}}
  end

  @impl true
  def handle_info({:file_event, _watcher_pid, {path, _events}}, state) do
    if Path.basename(path) == @filename do
      reload_projects(state)
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

  defp reload_projects(state) do
    case load_and_validate(state.path) do
      {:ok, projects} ->
        # Add COFLUX_PROJECT if set
        projects = add_env_project(projects)

        # Create and populate new table
        new_table = :ets.new(:coflux_projects, [:set, :public, read_concurrency: true])

        for {project, config} <- projects do
          :ets.insert(new_table, {project, config})
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
      {:ok, projects} ->
        # Add COFLUX_PROJECT if set
        projects = add_env_project(projects)

        for {project, config} <- projects do
          :ets.insert(table, {project, config})
        end

      {:error, reason} ->
        Logger.warning("Failed to load #{@filename}: #{reason}")
    end
  end

  defp add_env_project(projects) do
    case Config.project() do
      nil -> projects
      project_id -> Map.put_new(projects, project_id, %{})
    end
  end

  defp load_and_validate(path) do
    if File.exists?(path) do
      case File.read(path) do
        {:ok, content} ->
          case Jason.decode(content) do
            {:ok, data} when is_map(data) ->
              projects =
                Map.new(data, fn {project_name, config} ->
                  {project_name, parse_config(config)}
                end)

              {:ok, projects}

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

  defp parse_config(config) when is_map(config), do: %{}
  defp parse_config(_), do: %{}

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
