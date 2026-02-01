defmodule Coflux.ConfigStore do
  @moduledoc """
  A behaviour and macro for creating configuration stores backed by ETS tables
  with file watching and atomic updates.

  ## Usage

      defmodule MyApp.ProjectsStore do
        use Coflux.ConfigStore,
          filename: "projects.json",
          table_key: :my_projects_table

        @impl Coflux.ConfigStore
        def parse_entries(data) do
          Map.new(data, fn {name, config} -> {name, config} end)
        end

        def exists?(project), do: has_key?(project)
      end

  ## Callbacks

  - `parse_entries/1` - Required. Transforms the raw JSON map into ETS entries.
    Called with an empty map if the file doesn't exist or fails to parse on
    initial load.
  """

  @doc """
  Transforms raw JSON data into a map of ETS entries.

  Called with the parsed JSON object (a map), or an empty map if the file
  doesn't exist or fails to parse during initial load.
  """
  @callback parse_entries(data :: map()) :: map()

  defmacro __using__(opts) do
    filename = Keyword.fetch!(opts, :filename)
    table_key = Keyword.fetch!(opts, :table_key)

    quote do
      use GenServer

      require Logger

      @behaviour Coflux.ConfigStore

      @filename unquote(filename)
      @table_key unquote(table_key)

      def start_link(opts \\ []) do
        GenServer.start_link(__MODULE__, opts, name: __MODULE__)
      end

      @doc false
      defp has_key?(key) do
        table = :persistent_term.get(@table_key)

        case :ets.lookup(table, key) do
          [{^key, _}] -> true
          [] -> false
        end
      end

      @doc false
      defp get(key) do
        table = :persistent_term.get(@table_key)

        case :ets.lookup(table, key) do
          [{^key, value}] -> {:ok, value}
          [] -> :error
        end
      end

      # GenServer callbacks

      @impl GenServer
      def init(_opts) do
        path = Coflux.Utils.data_path("config/#{@filename}")
        dir = Path.dirname(path)

        table = :ets.new(:config_store, [:set, :public, read_concurrency: true])
        load_into_table(table, path)
        :persistent_term.put(@table_key, table)

        watcher_pid = start_file_watcher(dir)

        {:ok, %{path: path, watcher_pid: watcher_pid}}
      end

      @impl GenServer
      def handle_info({:file_event, _watcher_pid, {event_path, _events}}, state) do
        if Path.basename(event_path) == @filename do
          reload(state)
        else
          {:noreply, state}
        end
      end

      @impl GenServer
      def handle_info({:file_event, _watcher_pid, :stop}, state) do
        Logger.warning("File watcher stopped for #{@filename}, restarting...")
        dir = Path.dirname(state.path)
        watcher_pid = start_file_watcher(dir)
        {:noreply, %{state | watcher_pid: watcher_pid}}
      end

      @impl GenServer
      def handle_info({:delete_table, table}, state) do
        :ets.delete(table)
        {:noreply, state}
      end

      defp reload(state) do
        case load_json_file(state.path) do
          {:ok, data} ->
            entries = parse_entries(data)
            new_table = :ets.new(:config_store, [:set, :public, read_concurrency: true])

            for {key, value} <- entries do
              :ets.insert(new_table, {key, value})
            end

            old_table = :persistent_term.get(@table_key)
            :persistent_term.put(@table_key, new_table)
            Process.send_after(self(), {:delete_table, old_table}, :timer.seconds(1))

            {:noreply, state}

          {:error, reason} ->
            Logger.warning("Failed to reload #{@filename}: #{reason}, keeping old config")
            {:noreply, state}
        end
      end

      defp load_into_table(table, path) do
        data =
          case load_json_file(path) do
            {:ok, data} ->
              data

            {:error, reason} ->
              Logger.warning("Failed to load #{@filename}: #{reason}")
              %{}
          end

        entries = parse_entries(data)

        for {key, value} <- entries do
          :ets.insert(table, {key, value})
        end
      end

      defp load_json_file(path) do
        if File.exists?(path) do
          case File.read(path) do
            {:ok, content} ->
              case Jason.decode(content) do
                {:ok, data} when is_map(data) ->
                  {:ok, data}

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
  end
end
