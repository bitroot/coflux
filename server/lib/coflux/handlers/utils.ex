defmodule Coflux.Handlers.Utils do
  alias Coflux.Config

  @doc """
  Resolves the project from the hostname.

  Behavior depends on configuration:

  - Neither set: Returns `{:error, :not_configured}`
  - COFLUX_PROJECT only: Returns the configured project (any access method works)
  - COFLUX_BASE_DOMAIN only: Extracts project from subdomain (subdomain required)
  - Both set: Extracts from subdomain, but must match COFLUX_PROJECT
  """
  def resolve_project(hostname) do
    configured_project = Config.project()
    base_domain = Config.base_domain()

    # Strip port for subdomain matching
    host = hostname |> String.split(":") |> hd()

    case {configured_project, base_domain} do
      {nil, nil} ->
        {:error, :not_configured}

      {project_id, nil} ->
        {:ok, project_id}

      {_, base_domain} ->
        # Subdomain routing
        cond do
          host == base_domain ->
            {:error, :project_required}

          String.ends_with?(host, "." <> base_domain) ->
            project_id = String.replace_suffix(host, "." <> base_domain, "")

            if configured_project && project_id != configured_project do
              {:error, :project_mismatch}
            else
              {:ok, project_id}
            end

          true ->
            {:error, :invalid_host}
        end
    end
  end

  @doc """
  Gets the host from the request, including port if non-standard.
  """
  def get_host(req) do
    host = :cowboy_req.host(req)
    port = :cowboy_req.port(req)

    # Include port if it's non-standard
    case port do
      80 -> host
      443 -> host
      _ -> "#{host}:#{port}"
    end
  end

  @doc """
  Extracts a bearer token from the Authorization header.

  Returns the token string or nil if not found.
  """
  def get_token(req) do
    case :cowboy_req.header("authorization", req) do
      :undefined ->
        nil

      header ->
        case String.split(header, " ", parts: 2) do
          ["Bearer", token] -> String.trim(token)
          _ -> nil
        end
    end
  end

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
    Config.allowed_origins()
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
              {:ok, nil} when not required ->
                {values, errors}

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

    if Enum.empty?(errors) do
      {:ok, values, req}
    else
      {:error, errors, req}
    end
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

  @doc """
  Get all values for a query parameter (for repeated params like ?workspace=a&workspace=b).
  """
  def get_query_params(qs, key) do
    qs
    |> Enum.filter(fn {k, _v} -> k == key end)
    |> Enum.map(fn {_k, v} -> v end)
  end
end
