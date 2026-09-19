defmodule Coflux.Launchers.AwsCredentials do
  @moduledoc """
  Finds AWS credentials for a launcher to sign requests with.

  Credentials configured on the pool win. Otherwise the server's own
  surroundings are searched the way the AWS SDKs do, in the same order:
  environment variables, then the container credentials endpoint (an ECS
  task role, when the server itself runs on ECS), then the EC2 instance
  metadata service (an instance profile). Nothing is cached: the
  endpoints are link-local and quick, and a launcher call is rare enough
  that a lookup per call costs less than getting expiry right.
  """

  @container_credentials_host "http://169.254.170.2"
  @instance_metadata_endpoint "http://169.254.169.254"
  @instance_metadata_token_ttl "21600"

  # These endpoints are on the local link, or not there at all: a slow
  # answer means the latter, and a launcher task shouldn't sit on it.
  @connect_timeout_ms 1_000
  @receive_timeout_ms 2_000

  @type t :: %{
          access_key_id: String.t(),
          secret_access_key: String.t(),
          session_token: String.t() | nil
        }

  @doc """
  Resolves credentials, preferring `static` (a map with `:access_key_id`,
  `:secret_access_key` and optionally `:session_token`) when given.
  """
  @spec resolve(map() | nil) :: {:ok, t()} | {:error, :credentials_missing}
  def resolve(%{access_key_id: access_key_id, secret_access_key: secret_access_key} = static)
      when is_binary(access_key_id) and is_binary(secret_access_key) do
    {:ok,
     %{
       access_key_id: access_key_id,
       secret_access_key: secret_access_key,
       session_token: Map.get(static, :session_token)
     }}
  end

  def resolve(_static) do
    Enum.find_value(
      [&from_environment/0, &from_container/0, &from_instance_metadata/0],
      {:error, :credentials_missing},
      fn source ->
        case source.() do
          {:ok, credentials} -> {:ok, credentials}
          :none -> nil
        end
      end
    )
  end

  defp from_environment do
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

  defp from_container do
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

  defp from_instance_metadata do
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

  defp present?(value), do: is_binary(value) and value != ""

  defp blank_to_nil(value), do: if(present?(value), do: value, else: nil)
end
