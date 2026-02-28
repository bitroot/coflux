defmodule Coflux.JwksStore do
  @moduledoc """
  GenServer that fetches and caches JWKS from Studio.

  Keys are fetched lazily on first access and refreshed reactively:
  - On cache miss (handles key rotation)
  - When stale (>1 hour since last refresh, handles key revocation)
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
    {refreshed, refresh_result} =
      if stale?() do
        {true, GenServer.call(__MODULE__, :refresh)}
      else
        {false, :ok}
      end

    case :ets.lookup(@table_name, kid) do
      [{^kid, key}] ->
        {:ok, key}

      [] when not refreshed ->
        case GenServer.call(__MODULE__, :refresh) do
          :ok ->
            case :ets.lookup(@table_name, kid) do
              [{^kid, key}] -> {:ok, key}
              [] -> {:error, :key_not_found}
            end

          {:error, _} ->
            {:error, :jwks_unavailable}
        end

      [] when refresh_result != :ok ->
        {:error, :jwks_unavailable}

      [] ->
        {:error, :key_not_found}
    end
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    :ets.new(@table_name, [:named_table, :set, :public, read_concurrency: true])
    {:ok, %{}}
  end

  @impl true
  def handle_call(:refresh, _from, state) do
    result = fetch_and_replace_keys()
    {:reply, result, state}
  end

  # Private functions

  defp stale? do
    case :ets.lookup(@table_name, :last_refreshed) do
      [{:last_refreshed, ts}] ->
        System.monotonic_time(:millisecond) - ts > @refresh_interval

      [] ->
        true
    end
  end

  defp fetch_and_replace_keys do
    studio_url = Config.studio_url()
    jwks_url = "#{studio_url}/.well-known/jwks.json"

    case Req.get(jwks_url, retry: false) do
      {:ok, %Req.Response{status: 200, body: %{"keys" => keys}}} when is_list(keys) ->
        new_entries =
          for %{"kid" => kid} = jwk <- keys do
            {kid, JOSE.JWK.from_map(jwk)}
          end

        new_kids = MapSet.new(new_entries, fn {kid, _} -> kid end)

        old_kids =
          :ets.tab2list(@table_name)
          |> Enum.filter(fn {kid, _} -> is_binary(kid) end)
          |> MapSet.new(fn {kid, _} -> kid end)

        # Insert new/updated keys first (continuous availability)
        :ets.insert(
          @table_name,
          [{:last_refreshed, System.monotonic_time(:millisecond)} | new_entries]
        )

        # Then remove revoked keys
        old_kids
        |> MapSet.difference(new_kids)
        |> Enum.each(&:ets.delete(@table_name, &1))

        :ok

      {:ok, %Req.Response{status: status}} ->
        {:error, {:unexpected_status, status}}

      {:error, exception} ->
        {:error, exception}
    end
  end
end
