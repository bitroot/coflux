defmodule Coflux.Launchers.AwsCredentialsTest do
  # The environment and the credentials cache are both global.
  use ExUnit.Case, async: false

  alias Coflux.Launchers.AwsCredentials
  alias Coflux.Launchers.AwsCredentials.Cache

  @static %{access_key_id: "AKIASTATIC", secret_access_key: "static-secret"}
  @role_arn "arn:aws:iam::123456789012:role/coflux-launcher"

  @env ~w(
    AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
    AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN AWS_ROLE_SESSION_NAME
    AWS_REGION AWS_DEFAULT_REGION AWS_ENDPOINT_URL_STS
  )

  # Whatever AWS configuration the machine running the tests has is
  # kept out of them, and put back afterwards.
  setup do
    saved = Map.new(@env, &{&1, System.get_env(&1)})
    Enum.each(@env, &System.delete_env/1)

    on_exit(fn ->
      Enum.each(saved, fn
        {name, nil} -> System.delete_env(name)
        {name, value} -> System.put_env(name, value)
      end)
    end)

    :ok
  end

  test "static credentials are used as they are when there's no role to assume" do
    assert {:ok,
            %{access_key_id: "AKIASTATIC", secret_access_key: "static-secret", session_token: nil}} =
             AwsCredentials.resolve(@static,
               region: "eu-west-1",
               req_options: [adapter: refuse()]
             )
  end

  describe "assuming a role" do
    test "calls STS signed with the credentials found, and uses what it issues" do
      assert {:ok, credentials} =
               AwsCredentials.resolve(Map.put(@static, :session_token, "static-token"),
                 region: "eu-west-1",
                 role_arn: @role_arn,
                 external_id: "team-42",
                 req_options: [adapter: respond(200, credentials_xml(in_seconds(3600)))]
               )

      assert %{
               access_key_id: "ASIAASSUMED",
               secret_access_key: "assumed-secret",
               session_token: "assumed-token"
             } = credentials

      assert_receive {:request, request}
      assert to_string(request.url) == "https://sts.eu-west-1.amazonaws.com/"

      assert URI.decode_query(request.body) == %{
               "Action" => "AssumeRole",
               "Version" => "2011-06-15",
               "RoleArn" => @role_arn,
               "RoleSessionName" => "coflux",
               "ExternalId" => "team-42"
             }

      assert [authorization] = Req.Request.get_header(request, "authorization")
      assert authorization =~ "AWS4-HMAC-SHA256 Credential=AKIASTATIC/"
      assert authorization =~ "/eu-west-1/sts/aws4_request"
      assert Req.Request.get_header(request, "x-amz-security-token") == ["static-token"]
    end

    test "leaves out the external ID when there isn't one" do
      assert {:ok, _} =
               AwsCredentials.resolve(@static,
                 region: "eu-west-1",
                 role_arn: @role_arn,
                 req_options: [adapter: respond(200, credentials_xml(in_seconds(3600)))]
               )

      assert_receive {:request, request}
      refute Map.has_key?(URI.decode_query(request.body), "ExternalId")
    end

    test "reports STS refusing with its own code and message" do
      body = """
      <ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
        <Error>
          <Type>Sender</Type>
          <Code>AccessDenied</Code>
          <Message>User &quot;AKIASTATIC&quot; is not authorized to perform: sts:AssumeRole</Message>
        </Error>
        <RequestId>req-1</RequestId>
      </ErrorResponse>
      """

      assert {:error,
              {:assume_role, "AccessDenied",
               "User \"AKIASTATIC\" is not authorized to perform: sts:AssumeRole"}} =
               AwsCredentials.resolve(@static,
                 region: "eu-west-1",
                 role_arn: @role_arn,
                 req_options: [adapter: respond(403, body)]
               )
    end

    test "reports STS being unreachable, or answering with something else" do
      unreachable = fn request -> {request, %Req.TransportError{reason: :econnrefused}} end

      assert {:error, {:assume_role, "request_failed", nil}} =
               AwsCredentials.resolve(@static,
                 region: "eu-west-1",
                 role_arn: @role_arn,
                 req_options: [adapter: unreachable]
               )

      assert {:error, {:assume_role, "unexpected_response", nil}} =
               AwsCredentials.resolve(@static,
                 region: "eu-west-1",
                 role_arn: @role_arn,
                 req_options: [adapter: respond(200, "<html>not sts</html>")]
               )

      assert {:error, {:assume_role, "status_503", nil}} =
               AwsCredentials.resolve(@static,
                 region: "eu-west-1",
                 role_arn: @role_arn,
                 req_options: [adapter: respond(503, "")]
               )
    end

    test "needs credentials to assume it with" do
      System.put_env("AWS_EC2_METADATA_DISABLED", "true")
      on_exit(fn -> System.delete_env("AWS_EC2_METADATA_DISABLED") end)

      assert {:error, :credentials_missing} =
               AwsCredentials.resolve(nil,
                 region: "eu-west-1",
                 role_arn: @role_arn,
                 req_options: [adapter: refuse()]
               )
    end
  end

  describe "a web identity in the environment" do
    @describetag :tmp_dir

    setup %{tmp_dir: dir} do
      token_file = Path.join(dir, "token")
      File.write!(token_file, "eyJ.web.identity\n")
      System.put_env("AWS_WEB_IDENTITY_TOKEN_FILE", token_file)
      System.put_env("AWS_ROLE_ARN", @role_arn)
      {:ok, token_file: token_file}
    end

    test "is exchanged with STS, unsigned" do
      assert {:ok, %{access_key_id: "ASIAASSUMED", session_token: "assumed-token"}} =
               AwsCredentials.resolve(nil,
                 region: "eu-west-1",
                 req_options: [adapter: respond(200, credentials_xml(in_seconds(3600)))]
               )

      assert_receive {:request, request}

      assert URI.decode_query(request.body) == %{
               "Action" => "AssumeRoleWithWebIdentity",
               "Version" => "2011-06-15",
               "RoleArn" => @role_arn,
               "RoleSessionName" => "coflux",
               "WebIdentityToken" => "eyJ.web.identity"
             }

      assert Req.Request.get_header(request, "authorization") == []
    end

    test "is named by AWS_ROLE_SESSION_NAME, and uses the server's own region" do
      System.put_env("AWS_ROLE_SESSION_NAME", "coflux-server")
      System.put_env("AWS_REGION", "us-east-2")

      assert {:ok, _} =
               AwsCredentials.resolve(nil,
                 req_options: [adapter: respond(200, credentials_xml(in_seconds(3600)))]
               )

      assert_receive {:request, request}
      assert to_string(request.url) == "https://sts.us-east-2.amazonaws.com/"
      assert %{"RoleSessionName" => "coflux-server"} = URI.decode_query(request.body)
    end

    test "is then what a pool's role is assumed with" do
      assert {:ok, _} =
               AwsCredentials.resolve(nil,
                 region: "eu-west-1",
                 role_arn: "arn:aws:iam::210987654321:role/other-account",
                 req_options: [adapter: respond(200, credentials_xml(in_seconds(3600)))]
               )

      assert_receive {:request, first}
      assert %{"Action" => "AssumeRoleWithWebIdentity"} = URI.decode_query(first.body)

      assert_receive {:request, second}

      assert %{
               "Action" => "AssumeRole",
               "RoleArn" => "arn:aws:iam::210987654321:role/other-account"
             } =
               URI.decode_query(second.body)

      assert [authorization] = Req.Request.get_header(second, "authorization")
      assert authorization =~ "Credential=ASIAASSUMED/"
    end

    test "that can't be read is an error rather than something to look past", %{
      token_file: token_file
    } do
      File.rm!(token_file)

      assert {:error, {:assume_role, "token_unreadable", message}} =
               AwsCredentials.resolve(nil, region: "eu-west-1", req_options: [adapter: refuse()])

      assert message =~ token_file
    end

    test "gives way to static credentials on the pool" do
      assert {:ok, %{access_key_id: "AKIASTATIC"}} =
               AwsCredentials.resolve(@static,
                 region: "eu-west-1",
                 req_options: [adapter: refuse()]
               )
    end
  end

  describe "with the cache running" do
    setup do
      start_supervised!(Cache)
      :ok
    end

    test "credentials from STS are reused until shortly before they expire" do
      opts = [
        region: "eu-west-1",
        role_arn: @role_arn,
        req_options: [adapter: respond(200, credentials_xml(in_seconds(3600)))]
      ]

      assert {:ok, first} = AwsCredentials.resolve(@static, opts)
      assert {:ok, ^first} = AwsCredentials.resolve(@static, opts)

      assert_receive {:request, _}
      refute_received {:request, _}
    end

    test "credentials about to expire are replaced" do
      opts = [
        region: "eu-west-1",
        role_arn: @role_arn,
        req_options: [adapter: respond(200, credentials_xml(in_seconds(60)))]
      ]

      assert {:ok, _} = AwsCredentials.resolve(@static, opts)
      assert {:ok, _} = AwsCredentials.resolve(@static, opts)

      assert_receive {:request, _}
      assert_receive {:request, _}
    end

    test "the role, its external ID and what it's assumed with each key an entry" do
      adapter = respond(200, credentials_xml(in_seconds(3600)))
      opts = [region: "eu-west-1", role_arn: @role_arn, req_options: [adapter: adapter]]

      assert {:ok, _} = AwsCredentials.resolve(@static, opts)
      assert {:ok, _} = AwsCredentials.resolve(@static, Keyword.put(opts, :external_id, "x"))

      assert {:ok, _} =
               AwsCredentials.resolve(
                 @static,
                 Keyword.put(opts, :role_arn, "arn:aws:iam::123456789012:role/another")
               )

      assert {:ok, _} = AwsCredentials.resolve(%{@static | access_key_id: "AKIAOTHER"}, opts)

      # Each of those was a first call; these aren't.
      assert {:ok, _} = AwsCredentials.resolve(@static, opts)
      assert {:ok, _} = AwsCredentials.resolve(@static, Keyword.put(opts, :external_id, "x"))

      for _ <- 1..4, do: assert_receive({:request, _})
      refute_received {:request, _}
    end

    test "a refusal isn't kept" do
      opts = [region: "eu-west-1", role_arn: @role_arn]

      assert {:error, {:assume_role, "status_500", nil}} =
               AwsCredentials.resolve(@static, opts ++ [req_options: [adapter: respond(500, "")]])

      assert {:ok, _} =
               AwsCredentials.resolve(
                 @static,
                 opts ++ [req_options: [adapter: respond(200, credentials_xml(in_seconds(3600)))]]
               )
    end

    @tag :tmp_dir
    test "a web identity's credentials are kept too", %{tmp_dir: dir} do
      token_file = Path.join(dir, "token")
      File.write!(token_file, "eyJ.web.identity")
      System.put_env("AWS_WEB_IDENTITY_TOKEN_FILE", token_file)
      System.put_env("AWS_ROLE_ARN", @role_arn)

      opts = [
        region: "eu-west-1",
        req_options: [adapter: respond(200, credentials_xml(in_seconds(3600)))]
      ]

      assert {:ok, first} = AwsCredentials.resolve(nil, opts)
      assert {:ok, ^first} = AwsCredentials.resolve(nil, opts)

      assert_receive {:request, _}
      refute_received {:request, _}
    end
  end

  # --- Helpers ---

  # A stand-in for STS that answers every request the same way, and
  # tells the test what it was asked.
  defp respond(status, body) do
    test = self()

    fn request ->
      send(test, {:request, request})
      {request, Req.Response.new(status: status, body: body)}
    end
  end

  defp refuse do
    fn _request -> flunk("no request was expected") end
  end

  defp credentials_xml(expiration) do
    """
    <AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
      <AssumeRoleResult>
        <Credentials>
          <AccessKeyId>ASIAASSUMED</AccessKeyId>
          <SecretAccessKey>assumed-secret</SecretAccessKey>
          <SessionToken>assumed-token</SessionToken>
          <Expiration>#{expiration}</Expiration>
        </Credentials>
        <AssumedRoleUser>
          <AssumedRoleId>AROAEXAMPLE:coflux</AssumedRoleId>
          <Arn>arn:aws:sts::123456789012:assumed-role/coflux-launcher/coflux</Arn>
        </AssumedRoleUser>
      </AssumeRoleResult>
      <ResponseMetadata>
        <RequestId>req-1</RequestId>
      </ResponseMetadata>
    </AssumeRoleResponse>
    """
  end

  defp in_seconds(seconds) do
    DateTime.utc_now()
    |> DateTime.add(seconds, :second)
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end
end
