defmodule Coflux.Launchers.AwsCredentials do
  @moduledoc """
  Finds AWS credentials for a launcher to sign requests with.

  Credentials configured on the pool win. Otherwise the server's own
  surroundings are searched the way the AWS SDKs do, in the same order:
  environment variables, then a web identity token (the service account
  token Kubernetes projects into a pod, when the server runs on EKS with
  a role for its service account), then the container credentials
  endpoint (an ECS task role, when the server itself runs on ECS), then
  the EC2 instance metadata service (an instance profile).

  Whatever is found, a pool can name a role to assume with it, so one
  server can launch into accounts its own identity doesn't reach.

  Credentials that STS issues - for an assumed role, or a web identity -
  are cached until shortly before they expire: a poll runs every few
  seconds per worker, and STS is neither local nor unthrottled. The rest
  are looked up on every call: those endpoints are link-local and quick,
  and getting their expiry right would cost more than the lookup.
  """

  alias Coflux.Launchers.AwsCredentials.Cache

  @container_credentials_host "http://169.254.170.2"
  @instance_metadata_endpoint "http://169.254.169.254"
  @instance_metadata_token_ttl "21600"

  @sts_version "2011-06-15"
  @role_session_name "coflux"

  # Cached STS credentials are replaced this long before they expire, so
  # a call made with them has time to finish.
  @refresh_margin_seconds 300

  # The local endpoints are on the link, or not there at all: a slow
  # answer means the latter, and a launcher task shouldn't sit on it.
  @connect_timeout_ms 1_000
  @receive_timeout_ms 2_000

  # STS is a real service, reached over the internet.
  @sts_connect_timeout_ms 5_000
  @sts_receive_timeout_ms 10_000

  @type t :: %{
          required(:access_key_id) => String.t(),
          required(:secret_access_key) => String.t(),
          required(:session_token) => String.t() | nil,
          optional(:expires_at) => integer()
        }

  @type error :: :credentials_missing | {:assume_role, String.t(), String.t() | nil}

  @doc """
  Resolves credentials, preferring `static` (a map with `:access_key_id`,
  `:secret_access_key` and optionally `:session_token`) when given.

  Options:

    * `:region` - whose STS endpoint to use, when a role is assumed.
    * `:role_arn` - a role to assume with the credentials found.
    * `:external_id` - what the role's trust policy expects, if anything.
    * `:req_options` - extra options for `Req.request/1`, so a test can
      stand in for the network.

  Fails with `:credentials_missing` when nothing is found, or
  `{:assume_role, code, message}` when STS refuses (the code is its own,
  `AccessDenied` say) or can't be reached (`request_failed`).
  """
  @spec resolve(map() | nil, keyword()) :: {:ok, t()} | {:error, error()}
  def resolve(static, opts \\ []) do
    static = static_credentials(static)

    case Keyword.get(opts, :role_arn) do
      nil ->
        base_credentials(static, opts)

      role_arn ->
        # Keyed by what the role is assumed with as well as the role: a
        # different identity may not be allowed to, or be given less.
        source = if static, do: {:static, static.access_key_id}, else: :surroundings
        key = {:assume_role, role_arn, Keyword.get(opts, :external_id), source}

        cached(key, fn ->
          with {:ok, base} <- base_credentials(static, opts) do
            assume_role(base, role_arn, opts)
          end
        end)
    end
  end

  defp static_credentials(
         %{access_key_id: access_key_id, secret_access_key: secret_access_key} = static
       )
       when is_binary(access_key_id) and is_binary(secret_access_key) do
    %{
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      session_token: blank_to_nil(Map.get(static, :session_token))
    }
  end

  defp static_credentials(_static), do: nil

  defp base_credentials(nil, opts), do: from_surroundings(opts)
  defp base_credentials(static, _opts), do: {:ok, static}

  # A source that isn't configured says `:none` and the next is tried;
  # one that is configured but fails is an error, not a reason to fall
  # through to something else, as the SDKs also treat it.
  defp from_surroundings(opts) do
    [&from_environment/1, &from_web_identity/1, &from_container/1, &from_instance_metadata/1]
    |> Enum.reduce_while({:error, :credentials_missing}, fn source, missing ->
      case source.(opts) do
        :none -> {:cont, missing}
        result -> {:halt, result}
      end
    end)
  end

  defp from_environment(_opts) do
    access_key_id = System.get_env("AWS_ACCESS_KEY_ID")
    secret_access_key = System.get_env("AWS_SECRET_ACCESS_KEY")

    if present?(access_key_id) and present?(secret_access_key) do
      {:ok,
       %{
         access_key_id: access_key_id,
         secret_access_key: secret_access_key,
         session_token: blank_to_nil(System.get_env("AWS_SESSION_TOKEN"))
       }}
    else
      :none
    end
  end

  # The token file is read on every refresh rather than once, since
  # what's in it is rotated by whoever projects it there.
  defp from_web_identity(opts) do
    token_file = System.get_env("AWS_WEB_IDENTITY_TOKEN_FILE")
    role_arn = System.get_env("AWS_ROLE_ARN")

    if present?(token_file) and present?(role_arn) do
      cached({:web_identity, role_arn, token_file}, fn ->
        case File.read(token_file) do
          {:ok, token} ->
            session_name = blank_to_nil(System.get_env("AWS_ROLE_SESSION_NAME"))

            sts_request(
              [
                {"Action", "AssumeRoleWithWebIdentity"},
                {"RoleArn", role_arn},
                {"RoleSessionName", session_name || @role_session_name},
                {"WebIdentityToken", String.trim(token)}
              ],
              nil,
              opts
            )

          {:error, reason} ->
            {:error,
             {:assume_role, "token_unreadable", "#{token_file}: #{:file.format_error(reason)}"}}
        end
      end)
    else
      :none
    end
  end

  defp from_container(_opts) do
    url =
      cond do
        present?(System.get_env("AWS_CONTAINER_CREDENTIALS_FULL_URI")) ->
          System.get_env("AWS_CONTAINER_CREDENTIALS_FULL_URI")

        present?(System.get_env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")) ->
          @container_credentials_host <> System.get_env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")

        true ->
          nil
      end

    if url do
      case request(:get, url, container_auth_headers()) do
        {:ok, body} -> parse_credentials(body)
        :error -> :none
      end
    else
      :none
    end
  end

  defp container_auth_headers do
    token =
      cond do
        present?(System.get_env("AWS_CONTAINER_AUTHORIZATION_TOKEN")) ->
          System.get_env("AWS_CONTAINER_AUTHORIZATION_TOKEN")

        present?(System.get_env("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")) ->
          case File.read(System.get_env("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")) do
            {:ok, contents} -> String.trim(contents)
            {:error, _} -> nil
          end

        true ->
          nil
      end

    if token, do: [{"authorization", token}], else: []
  end

  defp from_instance_metadata(_opts) do
    if System.get_env("AWS_EC2_METADATA_DISABLED") in ["true", "1"] do
      :none
    else
      base = System.get_env("AWS_EC2_METADATA_SERVICE_ENDPOINT") || @instance_metadata_endpoint
      credentials_url = base <> "/latest/meta-data/iam/security-credentials/"

      with {:ok, token} <-
             request(:put, base <> "/latest/api/token", [
               {"x-aws-ec2-metadata-token-ttl-seconds", @instance_metadata_token_ttl}
             ]),
           token_headers = [{"x-aws-ec2-metadata-token", token}],
           {:ok, roles} <- request(:get, credentials_url, token_headers),
           [role | _] <- roles |> String.split("\n", trim: true) |> Enum.map(&String.trim/1),
           {:ok, body} <- request(:get, credentials_url <> role, token_headers) do
        parse_credentials(body)
      else
        _ -> :none
      end
    end
  end

  defp parse_credentials(body) do
    case Jason.decode(body) do
      {:ok, %{"AccessKeyId" => access_key_id, "SecretAccessKey" => secret_access_key} = decoded}
      when is_binary(access_key_id) and is_binary(secret_access_key) ->
        {:ok,
         %{
           access_key_id: access_key_id,
           secret_access_key: secret_access_key,
           session_token: blank_to_nil(decoded["Token"])
         }}

      _ ->
        :none
    end
  end

  defp request(method, url, headers) do
    case Req.request(
           method: method,
           url: url,
           headers: headers,
           retry: false,
           decode_body: false,
           connect_options: [timeout: @connect_timeout_ms],
           receive_timeout: @receive_timeout_ms
         ) do
      {:ok, %{status: 200, body: body}} when is_binary(body) -> {:ok, body}
      _ -> :error
    end
  end

  # --- STS ---

  defp assume_role(base, role_arn, opts) do
    params = [
      {"Action", "AssumeRole"},
      {"RoleArn", role_arn},
      {"RoleSessionName", @role_session_name}
    ]

    params =
      case Keyword.get(opts, :external_id) do
        nil -> params
        external_id -> params ++ [{"ExternalId", external_id}]
      end

    sts_request(params, base, opts)
  end

  # STS speaks the AWS Query protocol: form-encoded parameters in a POST,
  # and XML back. The request is signed with `credentials`, or not at all
  # when there are none (a web identity token is its own authentication).
  defp sts_request(params, credentials, opts) do
    region = sts_region(opts)

    request =
      [
        method: :post,
        url: sts_endpoint(region),
        form: [{"Version", @sts_version} | params],
        retry: false,
        decode_body: false,
        connect_options: [timeout: @sts_connect_timeout_ms],
        receive_timeout: @sts_receive_timeout_ms
      ]
      |> sign(credentials, region)
      |> Keyword.merge(Keyword.get(opts, :req_options, []))

    case Req.request(request) do
      {:ok, %{status: 200, body: body}} when is_binary(body) ->
        parse_sts_credentials(body)

      {:ok, %{status: status, body: body}} ->
        body = if is_binary(body), do: body, else: ""
        code = xml_text(body, "Code") || "status_#{status}"
        {:error, {:assume_role, code, xml_text(body, "Message")}}

      {:error, _exception} ->
        {:error, {:assume_role, "request_failed", nil}}
    end
  end

  defp sign(request, nil, _region), do: request

  defp sign(request, credentials, region) do
    sigv4 = [
      service: "sts",
      region: region || "us-east-1",
      access_key_id: credentials.access_key_id,
      secret_access_key: credentials.secret_access_key
    ]

    sigv4 =
      case credentials[:session_token] do
        nil -> sigv4
        token -> Keyword.put(sigv4, :token, token)
      end

    Keyword.put(request, :aws_sigv4, sigv4)
  end

  # The pool's region, as the SDKs would use the one configured, or the
  # server's own; either issues tokens good everywhere. Without one the
  # global endpoint does.
  defp sts_region(opts) do
    Keyword.get(opts, :region) ||
      blank_to_nil(System.get_env("AWS_REGION")) ||
      blank_to_nil(System.get_env("AWS_DEFAULT_REGION"))
  end

  defp sts_endpoint(region) do
    blank_to_nil(System.get_env("AWS_ENDPOINT_URL_STS")) ||
      if region, do: "https://sts.#{region}.amazonaws.com/", else: "https://sts.amazonaws.com/"
  end

  # The response is a small, flat document, and the values wanted from it
  # never contain markup, so it isn't worth parsing properly.
  defp parse_sts_credentials(body) do
    with access_key_id when is_binary(access_key_id) <- xml_text(body, "AccessKeyId"),
         secret_access_key when is_binary(secret_access_key) <- xml_text(body, "SecretAccessKey"),
         session_token when is_binary(session_token) <- xml_text(body, "SessionToken"),
         expiration when is_binary(expiration) <- xml_text(body, "Expiration"),
         {:ok, expires_at, _offset} <- DateTime.from_iso8601(expiration) do
      {:ok,
       %{
         access_key_id: access_key_id,
         secret_access_key: secret_access_key,
         session_token: session_token,
         expires_at: DateTime.to_unix(expires_at)
       }}
    else
      _ -> {:error, {:assume_role, "unexpected_response", nil}}
    end
  end

  defp xml_text(body, element) do
    case Regex.run(~r{<#{element}>([^<]*)</#{element}>}, body, capture: :all_but_first) do
      [text] -> xml_unescape(text)
      nil -> nil
    end
  end

  defp xml_unescape(text) do
    text
    |> String.replace("&lt;", "<")
    |> String.replace("&gt;", ">")
    |> String.replace("&quot;", "\"")
    |> String.replace("&apos;", "'")
    |> String.replace("&amp;", "&")
  end

  # --- Cache ---

  # Two callers missing at once both fetch, and the second to finish
  # wins; either result is good, so that costs less than coordinating.
  defp cached(key, fetch) do
    now = System.system_time(:second)

    case Cache.get(key) do
      {:ok, %{expires_at: expires_at} = credentials}
      when expires_at - now > @refresh_margin_seconds ->
        {:ok, credentials}

      _ ->
        case fetch.() do
          {:ok, credentials} ->
            Cache.put(key, credentials, now)
            {:ok, credentials}

          other ->
            other
        end
    end
  end

  # --- Helpers ---

  defp present?(value), do: is_binary(value) and value != ""

  defp blank_to_nil(value), do: if(present?(value), do: value, else: nil)
end
