defmodule Coflux.Handlers.Assets do
  import Coflux.Handlers.Utils

  alias Coflux.Utils
  alias Coflux.Orchestration

  @default_mime_type "application/octet-stream"

  def init(req, opts) do
    bindings = :cowboy_req.bindings(req)
    path = :cowboy_req.path_info(req)

    req
    |> set_cors_headers()
    |> handle(:cowboy_req.method(req), bindings[:project], bindings[:asset], path, opts)
  end

  defp handle(req, "GET", project_id, asset_id, path, opts) do
    asset_id = String.to_integer(asset_id)

    req =
      case Orchestration.get_asset(project_id, asset_id, load_metadata: true) do
        {:ok, entries} ->
          path = Enum.join(path, "/")

          entries
          |> Enum.find(fn {entry_path, _, _, _} -> entry_path == path end)
          |> case do
            nil ->
              not_found(req)

            {_, blob_key, size, metadata} ->
              # TODO: support other blob stores
              path = blob_path(blob_key)

              if File.exists?(path) do
                stream = File.stream!(path, 2048)
                content_type = Map.get(metadata, "type") || @default_mime_type

                stream_response(
                  req,
                  %{
                    "content-type" => content_type,
                    "content-length" => Integer.to_string(size)
                  },
                  stream
                )
              else
                not_found(req)
              end
          end

        {:error, _error} ->
          not_found(req)
      end

    {:ok, req, opts}
  end

  defp stream_response(req, headers, stream) do
    req = :cowboy_req.stream_reply(200, headers, req)

    stream
    |> Stream.each(fn part ->
      :cowboy_req.stream_body(part, :nofin, req)
    end)
    |> Stream.run()

    :cowboy_req.stream_body([], :fin, req)

    req
  end

  defp not_found(req) do
    :cowboy_req.reply(404, %{}, "Not found", req)
  end

  defp blob_path(<<a::binary-size(2), b::binary-size(2)>> <> c) do
    Utils.data_path("blobs/#{a}/#{b}/#{c}")
  end
end
