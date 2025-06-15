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

      {:execution, execution_id, execution} ->
        %{
          type: "execution",
          executionId: Integer.to_string(execution_id),
          execution: build_execution(execution)
        }

      {:asset, asset_id, asset} ->
        %{
          type: "asset",
          assetId: asset_id,
          asset: build_asset(asset)
        }
    end)
  end

  def build_asset({total_count, total_size, entry}) do
    entry =
      case entry do
        {path, blob_key, size, metadata} ->
          %{path: path, blobKey: blob_key, size: size, metadata: metadata}

        nil ->
          nil
      end

    %{
      totalCount: total_count,
      totalSize: total_size,
      entry: entry
    }
  end

  def build_execution(execution) do
    %{
      runId: execution.run_id,
      stepId: execution.step_id,
      attempt: execution.attempt,
      module: execution.module,
      target: execution.target
    }
  end
end
