defmodule Coflux.Config do
  @moduledoc """
  Caches configuration values read from environment variables at startup.

  Uses persistent_term for fast reads without copying.

  ## Project Configuration

  At least one of the following must be configured:

  - **COFLUX_PROJECT**: Restricts the server to a single project. All requests
    are routed to this project. Supports any access method including IP addresses.

  - **COFLUX_BASE_DOMAIN**: Enables subdomain-based project routing. The project
    is extracted from the subdomain (e.g., `acme.example.com` → project "acme").
    Requires subdomain access - direct IP or base domain access is not allowed.

  When both are set, subdomain routing is used but only the configured project
  is allowed.

  ## Authentication Configuration

  Authentication methods are enabled based on configuration:

  - **Super token**: Set `COFLUX_SUPER_TOKEN` for a single token with full access
  - **Database tokens**: Tokens stored in the project database (created via API)
  - **Studio auth (JWT)**: Enabled if `COFLUX_NAMESPACES` is set

  Multiple auth methods can be enabled simultaneously. The auth method is determined
  by token format: JWTs (containing dots) use Studio auth, other tokens check
  super token first, then database tokens.

  - **COFLUX_REQUIRE_AUTH**: Whether authentication is required (default: "true").
    Set to "false" (case insensitive) to allow anonymous requests.
  - **COFLUX_SUPER_TOKEN**: A super token with full access (can create other tokens)
  - **COFLUX_SECRET**: Server secret for signing API tokens. Required for API token
    support. Should be a long random string, kept consistent across restarts.
  - **COFLUX_NAMESPACES**: Comma-separated list of team IDs allowed for Studio auth
  - **COFLUX_STUDIO_URL**: Studio URL for JWKS (default: https://studio.coflux.com)
  """

  @doc """
  Loads all config from environment variables. Call once at startup.
  """
  def init do
    :persistent_term.put(:coflux_data_dir, parse_data_dir())
    :persistent_term.put(:coflux_project, System.get_env("COFLUX_PROJECT"))
    :persistent_term.put(:coflux_base_domain, System.get_env("COFLUX_BASE_DOMAIN"))
    :persistent_term.put(:coflux_allowed_origins, parse_allowed_origins())
    :persistent_term.put(:coflux_require_auth, parse_require_auth())
    :persistent_term.put(:coflux_namespaces, parse_namespaces())
    :persistent_term.put(:coflux_studio_url, parse_studio_url())
    :persistent_term.put(:coflux_super_token_hash, parse_super_token())
    :persistent_term.put(:coflux_secret, parse_secret())
    :ok
  end

  @doc """
  Returns the data directory path.
  """
  def data_dir do
    :persistent_term.get(:coflux_data_dir)
  end

  @doc """
  Returns the configured project name, or nil if not set.

  When set, restricts the server to only serve this project.
  """
  def project do
    :persistent_term.get(:coflux_project)
  end

  @doc """
  Returns the base domain for subdomain-based routing, or nil if not set.

  When set, the project is extracted from the request's subdomain.
  """
  def base_domain do
    :persistent_term.get(:coflux_base_domain)
  end

  @doc """
  Returns the list of allowed CORS origins.
  """
  def allowed_origins do
    :persistent_term.get(:coflux_allowed_origins)
  end

  @doc """
  Returns whether authentication is required.

  When true (default), requests must provide valid credentials.
  When false, anonymous requests are allowed (but auth still works if provided).
  """
  def require_auth? do
    :persistent_term.get(:coflux_require_auth)
  end

  @doc """
  Returns the list of allowed namespace (team) IDs for studio auth mode.

  Returns nil if no namespaces are configured (allows any namespace).
  """
  def namespaces do
    :persistent_term.get(:coflux_namespaces)
  end

  @doc """
  Returns the Studio URL for fetching JWKS.
  """
  def studio_url do
    :persistent_term.get(:coflux_studio_url)
  end

  @doc """
  Returns the hashed super token, or nil if not configured.

  The super token (set via COFLUX_SUPER_TOKEN) provides full access to the system
  and can be used to create other tokens. It is stored as a SHA-256 hash.
  """
  def super_token_hash do
    :persistent_term.get(:coflux_super_token_hash)
  end

  @doc """
  Returns the server secret, or nil if not configured.

  This secret is used to derive per-project signing keys for API tokens.
  Set via COFLUX_SECRET environment variable. API tokens are only supported
  when this is configured.
  """
  def secret do
    :persistent_term.get(:coflux_secret)
  end

  defp parse_data_dir do
    System.get_env("COFLUX_DATA_DIR", Path.join(File.cwd!(), "data"))
  end

  @default_allowed_origins ["https://studio.coflux.com"]

  defp parse_allowed_origins do
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

  defp parse_require_auth do
    case System.get_env("COFLUX_REQUIRE_AUTH") do
      nil -> true
      value -> String.downcase(value) != "false"
    end
  end

  defp parse_namespaces do
    case System.get_env("COFLUX_NAMESPACES") do
      nil ->
        nil

      value ->
        value
        |> String.split(",")
        |> Enum.map(&String.trim/1)
        |> Enum.reject(&(&1 == ""))
        |> MapSet.new()
    end
  end

  @default_studio_url "https://studio.coflux.com"

  defp parse_studio_url do
    System.get_env("COFLUX_STUDIO_URL", @default_studio_url)
  end

  defp parse_super_token do
    case System.get_env("COFLUX_SUPER_TOKEN") do
      nil -> nil
      "" -> nil
      token -> hash_token(token)
    end
  end

  defp parse_secret do
    case System.get_env("COFLUX_SECRET") do
      nil -> nil
      "" -> nil
      secret -> secret
    end
  end

  defp hash_token(token) do
    :crypto.hash(:sha256, token) |> Base.encode16(case: :lower)
  end
end
