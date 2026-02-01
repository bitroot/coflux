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
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
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

    case Req.get(jwks_url) do
      {:ok, %Req.Response{status: 200, body: body}} ->
        case body do
          %{"keys" => keys} when is_list(keys) ->
            Enum.each(keys, fn key ->
              case key do
                %{"kid" => kid} = jwk ->
                  # Store the JWK as a JOSE.JWK struct for verification
                  jose_jwk = JOSE.JWK.from_map(jwk)
                  :ets.insert(@table_name, {kid, jose_jwk})

                _ ->
                  IO.warn("JWK missing 'kid' field")
              end
            end)

          _ ->
            IO.warn("Invalid JWKS response format")
        end

      {:ok, %Req.Response{status: status}} ->
        IO.warn("Failed to fetch JWKS: HTTP #{status}")

      {:error, reason} ->
        IO.warn("Failed to fetch JWKS: #{inspect(reason)}")
    end
  end
end
