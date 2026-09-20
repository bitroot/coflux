defmodule Coflux.Launchers.AwsCredentials.Cache do
  @moduledoc """
  Holds credentials STS has issued until they expire.

  The process only owns the table: launcher tasks read and write it
  directly, so a lookup never waits on anything. Without the process (as
  under test) nothing is cached, and a lookup finds nothing.
  """

  use GenServer

  @table :coflux_aws_credentials

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Whatever is held under `key`, expired or not; `:none` otherwise."
  def get(key) do
    if table?() do
      case :ets.lookup(@table, key) do
        [{^key, _expires_at, credentials}] -> {:ok, credentials}
        [] -> :none
      end
    else
      :none
    end
  end

  @doc """
  Holds `credentials` under `key` until their `expires_at`, throwing out
  whatever else has expired by `now` while here, so entries for roles
  and identities no longer in use don't pile up.
  """
  def put(key, %{expires_at: expires_at} = credentials, now) do
    if table?() do
      :ets.select_delete(@table, [{{:_, :"$1", :_}, [{:<, :"$1", now}], [true]}])
      :ets.insert(@table, {key, expires_at, credentials})
    end

    :ok
  end

  defp table?, do: :ets.whereis(@table) != :undefined

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    {:ok, nil}
  end
end
