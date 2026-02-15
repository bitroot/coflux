defmodule Coflux.TopicUtils do
  def build_value(value) do
    case value do
      {:raw, data, references} ->
        %{
          type: "raw",
          data: data,
          references: build_references(references)
        }

      {:blob, key, size, references} ->
        %{
          type: "blob",
          key: key,
          size: size,
          references: build_references(references)
        }
    end
  end

  defp build_references(references) do
    Enum.map(references, fn
      {:fragment, format, blob_key, size, metadata} ->
        %{
          type: "fragment",
          format: format,
          blobKey: blob_key,
          size: size,
          metadata: metadata
        }

      {:execution, execution_external_id, {module, target}} ->
        %{
          type: "execution",
          executionId: execution_external_id,
          module: module,
          target: target
        }

      {:asset, asset_id, asset} ->
        %{
          type: "asset",
          assetId: asset_id,
          asset: build_asset(asset)
        }
    end)
  end

  def build_asset({name, total_count, total_size, entry}) do
    entry =
      case entry do
        {path, blob_key, size, metadata} ->
          %{path: path, blobKey: blob_key, size: size, metadata: metadata}

        nil ->
          nil
      end

    %{
      name: name,
      totalCount: total_count,
      totalSize: total_size,
      entry: entry
    }
  end

  def build_execution({ext_id, module, target}) do
    %{
      executionId: ext_id,
      module: module,
      target: target
    }
  end

  def build_principal(nil), do: nil

  def build_principal(%{type: type, external_id: external_id}) do
    %{type: type, externalId: external_id}
  end
end
