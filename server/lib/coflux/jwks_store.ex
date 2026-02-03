defmodule Coflux.JwksStore do
  @moduledoc """
  GenServer that fetches and caches JWKS from Studio.

  Refreshes keys every hour or on cache miss.
  """

  use GenServer

  alias Coflux.Config

  @refresh_interval :timer.hours(1)
  @table_name :coflux_jwks

  # Client API

  def start_link(opts \\ []) do
    # Only start when Studio auth is enabled (namespaces configured)
    if Config.namespaces() do
      GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    else
      :ignore
    end
  end

  @doc """
  Get the public key for a given key ID.

  Returns `{:ok, key}` or `{:error, reason}`.
  """
  def get_key(kid) do
    case :ets.lookup(@table_name, kid) do
      [{^kid, key}] ->
        {:ok, key}

      [] ->
        # Try refreshing keys
        GenServer.call(__MODULE__, :refresh)

        case :ets.lookup(@table_name, kid) do
          [{^kid, key}] -> {:ok, key}
          [] -> {:error, :key_not_found}
        end
    end
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    # Create ETS table for storing keys
    :ets.new(@table_name, [:named_table, :set, :public, read_concurrency: true])

    # Fetch keys immediately
    send(self(), :refresh)

    # Schedule periodic refresh
    schedule_refresh()

    {:ok, %{}}
  end

  @impl true
  def handle_info(:refresh, state) do
    fetch_and_store_keys()
    schedule_refresh()
    {:noreply, state}
  end

  @impl true
  def handle_call(:refresh, _from, state) do
    fetch_and_store_keys()
    {:reply, :ok, state}
  end

  # Private functions

  defp schedule_refresh do
    Process.send_after(self(), :refresh, @refresh_interval)
  end

  defp fetch_and_store_keys do
    studio_url = Config.studio_url()
    jwks_url = "#{studio_url}/.well-known/jwks.json"

    with {:ok, %Req.Response{status: 200, body: %{"keys" => keys}}} when is_list(keys) <-
           Req.get(jwks_url) do
      Enum.each(keys, fn
        %{"kid" => kid} = jwk ->
          jose_jwk = JOSE.JWK.from_map(jwk)
          :ets.insert(@table_name, {kid, jose_jwk})

        _ ->
          :ok
      end)
    end
  end
end
