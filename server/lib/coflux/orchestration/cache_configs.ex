defmodule Coflux.Orchestration.CacheConfigs do
  import Coflux.Store

  alias Coflux.Orchestration.Utils

  def get_or_create_cache_config_id(db, cache) do
    hash = hash_cache_config(cache)

    case query_one(db, "SELECT id FROM cache_configs WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {id}} ->
        {:ok, id}

      {:ok, nil} ->
        insert_one(db, :cache_configs, %{
          hash: {:blob, hash},
          params: Utils.encode_params_list(cache.params),
          max_age: cache.max_age,
          namespace: cache.namespace,
          version: cache.version
        })
    end
  end

  def get_cache_config(db, cache_config_id) do
    case query_one(
           db,
           "SELECT params, max_age, namespace, version FROM cache_configs WHERE id = ?1",
           {cache_config_id}
         ) do
      {:ok, {params, max_age, namespace, version}} ->
        {:ok,
         %{
           params: Utils.decode_params_list(params),
           max_age: max_age,
           namespace: namespace,
           version: version
         }}
    end
  end

  defp hash_cache_config(cache) do
    parts = [
      Utils.encode_params_list(cache.params) || "",
      if(cache.max_age, do: Integer.to_string(cache.max_age), else: ""),
      cache.namespace || "",
      cache.version || ""
    ]

    :crypto.hash(:sha256, Enum.intersperse(parts, 0))
  end
end
