defmodule Coflux.Launchers.Aws do
  @moduledoc """
  What the AWS launchers share: credentials, request signing, and reading
  an error back out of a response.

  The APIs themselves differ in shape - ECS and CloudWatch Logs are JSON
  1.1 (every call a POST, the operation in a header), Lambda is REST-JSON
  (the operation in the path) - so each gets its own request function,
  over the same connection.
  """

  alias Coflux.Launchers.AwsCredentials

  # What every AWS API says when the credentials themselves are the
  # problem, as opposed to what they're allowed to do.
  @unauthorized_errors [
    "UnrecognizedClientException",
    "InvalidSignatureException",
    "SignatureDoesNotMatch",
    "ExpiredTokenException",
    "ExpiredToken",
    "InvalidClientTokenId",
    "InvalidAccessKeyId",
    "IncompleteSignature",
    "MissingAuthenticationToken"
  ]

  @doc "Whether an error type means the credentials weren't accepted."
  def unauthorized_error?(type), do: type in @unauthorized_errors

  @doc """
  A connection for calling one service in the region the pool names:
  credentials from the pool's secret or the server's surroundings, then
  through the pool's role if it names one.

  `default_endpoint` builds the service's endpoint for a region, for when
  the pool doesn't override it.
  """
  def build_conn(config, default_endpoint) do
    region = Map.fetch!(config, :region)

    with {:ok, credentials} <-
           AwsCredentials.resolve(static_credentials(config),
             region: region,
             role_arn: config[:role_arn],
             external_id: config[:role_external_id],
             req_options: Map.get(config, :req_options, [])
           ) do
      {:ok,
       %{
         region: region,
         endpoint: config[:endpoint] || default_endpoint.(region),
         credentials: credentials,
         # Extra options for `Req.request/1`, so a test can stub the API
         # without reaching AWS. Never set in normal operation.
         req_options: Map.get(config, :req_options, [])
       }}
    end
  end

  defp static_credentials(
         %{access_key_id: access_key_id, secret_access_key: secret_access_key} = config
       )
       when is_binary(access_key_id) and is_binary(secret_access_key) do
    %{access_key_id: access_key_id, secret_access_key: secret_access_key}
    |> maybe_put(:session_token, config[:session_token])
  end

  defp static_credentials(_config), do: nil

  @doc """
  A JSON 1.1 call: a POST to `endpoint`, with the operation named by
  `target`. Returns the decoded body, or `{:api, status, type, message}`
  for an error response.
  """
  def json_request(conn, service, region, endpoint, target, body) do
    request =
      [
        method: :post,
        url: endpoint,
        headers: [
          {"content-type", "application/x-amz-json-1.1"},
          {"x-amz-target", target}
        ],
        body: Jason.encode!(body),
        aws_sigv4: sigv4(conn, service, region),
        retry: false,
        decode_body: false
      ]
      |> Keyword.merge(conn.req_options)

    case Req.request(request) do
      {:ok, %{status: status, body: raw}} ->
        decoded = decode_json(raw)

        if status in 200..299 do
          {:ok, decoded}
        else
          {:error, {:api, status, error_type(decoded), message(decoded)}}
        end

      {:error, _exception} ->
        {:error, :request_failed}
    end
  end

  @doc """
  A REST-JSON call to `path` on the connection's endpoint. Returns the
  whole response - status, headers and undecoded body - for a success,
  since a REST API says as much in its headers as in its body,
  `{:api, status, type, message}` for an error, or `{:request_failed,
  exception}` for a request that got no response, with the exception
  saying why.

  `opts` may give `:query`, `:headers`, a raw `:body`, and
  `:receive_timeout`.
  """
  def rest_request(conn, service, method, path, opts \\ []) do
    request =
      [
        method: method,
        url: conn.endpoint <> path,
        params: Keyword.get(opts, :query, []),
        headers: Keyword.get(opts, :headers, []),
        body: Keyword.get(opts, :body, ""),
        aws_sigv4: sigv4(conn, service, conn.region),
        retry: false,
        decode_body: false
      ]
      |> maybe_put_keyword(:receive_timeout, Keyword.get(opts, :receive_timeout))
      |> maybe_put_keyword(:connect_options, Keyword.get(opts, :connect_options))
      |> Keyword.merge(conn.req_options)

    case Req.request(request) do
      {:ok, %{status: status} = response} when status in 200..299 ->
        {:ok, response}

      {:ok, %{status: status, body: raw} = response} ->
        decoded = decode_json(raw)
        type = header(response, "x-amzn-errortype") || error_type(decoded)
        {:error, {:api, status, clean_error_type(type), message(decoded)}}

      {:error, exception} ->
        {:error, {:request_failed, exception}}
    end
  end

  @doc "A response header's value, or nil."
  def header(%{headers: headers}, name) do
    case Map.get(headers, name) do
      [value | _] -> value
      value when is_binary(value) -> value
      _ -> nil
    end
  end

  defp sigv4(conn, service, region) do
    credentials = conn.credentials

    [
      service: service,
      region: region,
      access_key_id: credentials.access_key_id,
      secret_access_key: credentials.secret_access_key
    ]
    |> maybe_put_keyword(:token, credentials[:session_token])
  end

  defp decode_json(raw) do
    case Jason.decode(raw) do
      {:ok, map} when is_map(map) -> map
      _ -> %{}
    end
  end

  defp message(decoded), do: decoded["message"] || decoded["Message"]

  defp error_type(%{"__type" => type}) when is_binary(type), do: clean_error_type(type)
  defp error_type(_body), do: nil

  # The type comes back as e.g. "ClusterNotFoundException", or namespaced
  # as "com.amazonaws.ecs#ClusterNotFoundException", or with a suffix as
  # "ClusterNotFoundException:http://...".
  defp clean_error_type(nil), do: nil

  defp clean_error_type(type) do
    type
    |> String.split("#")
    |> List.last()
    |> String.split(":")
    |> hd()
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp maybe_put_keyword(keyword, _key, nil), do: keyword
  defp maybe_put_keyword(keyword, key, value), do: Keyword.put(keyword, key, value)
end
