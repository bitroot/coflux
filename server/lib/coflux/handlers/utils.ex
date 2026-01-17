defmodule Coflux.Handlers.Utils do
  @default_allowed_origins ["https://studio.coflux.com"]

  def set_cors_headers(req) do
    origin = :cowboy_req.header("origin", req, nil)
    allowed_origin = get_allowed_origin(origin)

    headers = %{
      "access-control-allow-methods" => "OPTIONS, GET, POST, PUT, PATCH, DELETE",
      "access-control-allow-headers" => "content-type,authorization,x-api-version",
      "access-control-max-age" => "86400"
    }

    headers =
      if allowed_origin do
        Map.put(headers, "access-control-allow-origin", allowed_origin)
      else
        headers
      end

    :cowboy_req.set_resp_headers(headers, req)
  end

  defp get_allowed_origin(nil), do: nil
  defp get_allowed_origin(""), do: nil

  defp get_allowed_origin(origin) do
    allowed_origins = get_allowed_origins()

    if Enum.any?(allowed_origins, &origin_matches?(origin, &1)) do
      origin
    else
      nil
    end
  end

  defp get_allowed_origins do
    case System.get_env("COFLUX_ALLOW_ORIGINS") do
      nil ->
        @default_allowed_origins

      value ->
        value
        |> String.split(",")
        |> Enum.map(&String.trim/1)
        |> Enum.reject(&(&1 == ""))
    end
  end

  defp origin_matches?(origin, pattern) do
    cond do
      origin == pattern ->
        true

      String.contains?(pattern, "*") ->
        wildcard_matches?(origin, pattern)

      true ->
        false
    end
  end

  defp wildcard_matches?(origin, pattern) do
    case String.split(pattern, "*", parts: 2) do
      [prefix, suffix] ->
        String.starts_with?(origin, prefix) &&
          String.ends_with?(origin, suffix) &&
          String.length(origin) > String.length(prefix) + String.length(suffix)

      _ ->
        false
    end
  end

  def json_response(req, status \\ 200, result) do
    :cowboy_req.reply(
      status,
      %{"content-type" => "application/json"},
      Jason.encode!(result),
      req
    )
  end

  def json_error_response(req, error, opts \\ []) do
    status = Keyword.get(opts, :status, 400)
    details = Keyword.get(opts, :details)
    result = %{"error" => error}
    result = if details, do: Map.put(result, "details", details), else: result
    json_response(req, status, result)
  end

  def read_json_body(req) do
    case :cowboy_req.read_body(req) do
      {:ok, data, req} ->
        with {:ok, result} <- Jason.decode(data) do
          {:ok, result, req}
        end
    end
  end

  defp default_parser(value) do
    if value do
      {:ok, value}
    else
      {:error, :missing}
    end
  end

  def read_arguments(req, required_specs, optional_specs \\ %{}) do
    {:ok, body, req} = read_json_body(req)

    {values, errors} =
      Enum.reduce(
        %{true: required_specs, false: optional_specs},
        {%{}, %{}},
        fn {required, specs}, {values, errors} ->
          Enum.reduce(specs, {values, errors}, fn {key, spec}, {values, errors} ->
            {field, parser} =
              case spec do
                {field, parser} -> {field, parser}
                field when is_binary(field) -> {field, &default_parser/1}
              end

            case Map.fetch(body, field) do
              {:ok, value} ->
                case parser.(value) do
                  {:ok, value} ->
                    {Map.put(values, key, value), errors}

                  {:error, error} ->
                    {values, merge_error(errors, key, error)}
                end

              :error ->
                if required do
                  {values, merge_error(errors, key, :required)}
                else
                  {values, errors}
                end
            end
          end)
        end
      )

    {:ok, values, errors, req}
  end

  defp merge_error(errors, key, error) do
    cond do
      is_map(error) ->
        Enum.reduce(error, errors, fn {k, error}, errors ->
          merge_error(errors, "#{key}.#{k}", error)
        end)

      is_binary(error) || is_atom(error) ->
        Map.put(errors, key, error)
    end
  end

  def get_query_param(qs, key, fun \\ nil) do
    case List.keyfind(qs, key, 0) do
      {^key, value} ->
        if fun do
          try do
            fun.(value)
          rescue
            ArgumentError ->
              nil
          end
        else
          value
        end

      nil ->
        nil
    end
  end
end
