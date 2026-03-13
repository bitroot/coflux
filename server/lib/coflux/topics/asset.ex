defmodule Coflux.Topics.Asset do
  use Topical.Topic, route: ["asset", :asset_id]

  alias Coflux.Orchestration

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    asset_id = Map.fetch!(params, :asset_id)

    case Orchestration.get_asset(project_id, asset_id) do
      {:ok, name, entries} ->
        value = %{
          name: name,
          entries:
            Map.new(entries, fn {path, blob_key, size, metadata} ->
              {path,
               %{
                 blobKey: blob_key,
                 size: size,
                 metadata: metadata
               }}
            end)
        }

        {:ok, Topic.new(value, %{})}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end
end
