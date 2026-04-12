defmodule Coflux.Handlers.Blobs do
  import Coflux.Handlers.Utils

  alias Coflux.{Auth, Utils}

  def init(req, opts) do
    bindings = :cowboy_req.bindings(req)
    req = set_cors_headers(req)

    case :cowboy_req.method(req) do
      "OPTIONS" ->
        req = :cowboy_req.reply(204, req)
        {:ok, req, opts}

      method ->
        host = get_host(req)

        with {:ok, project_id} <- resolve_project(req),
             {:ok, _access} <- Auth.check(get_token(req), project_id, host) do
          handle(req, method, bindings[:key], opts)
        else
          {:error, :unauthorized} ->
            req = json_error_response(req, "unauthorized", status: 401)
            {:ok, req, opts}

          {:error, :not_configured} ->
            req = json_error_response(req, "not_configured", status: 500)
            {:ok, req, opts}

          {:error, :project_required} ->
            req = json_error_response(req, "project_required", status: 400)
            {:ok, req, opts}

          {:error, :invalid_host} ->
            req = json_error_response(req, "invalid_host", status: 400)
            {:ok, req, opts}
        end
    end
  end

  defp handle(req, "HEAD", key, opts) do
    exists = File.exists?(blob_path(key))
    status = if exists, do: 200, else: 404
    req = :cowboy_req.reply(status, %{}, req)
    {:ok, req, opts}
  end

  defp handle(req, "GET", key, opts) do
    case File.read(blob_path(key)) do
      {:ok, content} ->
        req = :cowboy_req.reply(200, %{}, content, req)
        {:ok, req, opts}

      {:error, :enoent} ->
        req = :cowboy_req.reply(404, %{}, "Not found", req)
        {:ok, req, opts}
    end
  end

  defp handle(req, "PUT", key, opts) do
    {:ok, temp_path} = Briefly.create()

    case File.open!(temp_path, [:write], &read_body(req, &1)) do
      {:ok, req, hash} ->
        req =
          if key == Base.encode16(hash, case: :lower) do
            path = blob_path(key)
            path |> Path.dirname() |> File.mkdir_p!()
            :ok = move_file(temp_path, path)
            :cowboy_req.reply(204, req)
          else
            json_error_response(req, "hash_mismatch")
          end

        {:ok, req, opts}
    end
  end

  defp blob_path(<<a::binary-size(2), b::binary-size(2)>> <> c) do
    Utils.data_path("blobs/#{a}/#{b}/#{c}")
  end

  defp read_body(req, file, hash \\ nil) do
    hash = hash || :crypto.hash_init(:sha256)

    case :cowboy_req.read_body(req) do
      {status, data, req} when status in [:ok, :more] ->
        case IO.binwrite(file, data) do
          :ok ->
            hash = :crypto.hash_update(hash, data)

            case status do
              :ok -> {:ok, req, :crypto.hash_final(hash)}
              :more -> read_body(req, file, hash)
            end
        end
    end
  end

  defp move_file(source, dest) do
    case File.rename(source, dest) do
      :ok -> :ok
      {:error, :exdev} -> File.cp(source, dest)
    end
  end
end
