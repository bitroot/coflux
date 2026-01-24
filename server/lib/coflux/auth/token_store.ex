defmodule Coflux.Auth.TokenStore do
  @moduledoc """
  GenServer that owns the auth tokens ETS table.

  Tokens are loaded from $COFLUX_DATA_DIR/tokens.json at startup.
  Reads go directly to ETS for performance.
  """

  use GenServer

  defmodule TokenConfig do
    defstruct namespaces: [nil]
  end

  @table :coflux_auth_tokens

  alias Coflux.Utils

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Looks up a token by its hash. Reads directly from ETS.

  Returns `{:ok, %TokenConfig{}}` if found, `:error` otherwise.
  """
  def lookup(token_hash) do
    case :ets.lookup(@table, token_hash) do
      [{^token_hash, config}] -> {:ok, config}
      [] -> :error
    end
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    table = :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])

    for {hash, config} <- load_tokens() do
      :ets.insert(@table, {hash, config})
    end

    {:ok, %{table: table}}
  end

  defp load_tokens do
    path = Utils.data_path("tokens.json")

    if File.exists?(path) do
      path
      |> File.read!()
      |> Jason.decode!()
      |> Map.new(fn {hash, config} -> {hash, parse_config(config)} end)
    else
      %{}
    end
  end

  defp parse_config(config) do
    namespaces = Map.get(config, "namespaces", [nil])
    %TokenConfig{namespaces: namespaces}
  end
end
