defmodule Coflux.Config do
  @moduledoc """
  Caches configuration values read from environment variables at startup.

  Uses persistent_term for fast reads without copying.

  ## Project Configuration

  - **COFLUX_PROJECT**: Restricts the server to a single project. All requests
    are routed to this project. Supports any access method including IP addresses.

  - **COFLUX_PUBLIC_HOST**: The public-facing host for the server. Prefix with
    `%` as a placeholder for the project ID to enable subdomain-based routing.

    Examples:
    - `%.example.com:7777` — multi-project subdomain routing
    - `%.localhost:7777` — local dev with multiple projects
    - `myserver.internal:7777` — single-project, no subdomain routing

    When the placeholder is present, the project is extracted from the subdomain
    of incoming requests. When combined with COFLUX_PROJECT, subdomain routing
    is used but only the configured project is accepted.

    When not set, falls back to `localhost:PORT`.

  ## Authentication Configuration

  Authentication methods are enabled based on configuration:

  - **Super token**: Set `COFLUX_SUPER_TOKEN_HASH` (SHA-256 hex) for a single token with full access
  - **Service tokens**: Tokens for programmatic access, stored in the project database
  - **Studio auth (JWT)**: Enabled if `COFLUX_STUDIO_TEAMS` is set

  Multiple auth methods can be enabled simultaneously. The auth method is determined
  by token format: JWTs (containing dots) use Studio auth, other tokens check
  super token first, then database tokens.

  - **COFLUX_REQUIRE_AUTH**: Whether authentication is required (default: "true").
    Set to "false" (case insensitive) to allow anonymous requests.
  - **COFLUX_SUPER_TOKEN_HASH**: SHA-256 hex hash of a super token with full access
  - **COFLUX_SECRET**: Server secret for signing service tokens. Required for service
    token support. Should be a long random string, kept consistent across restarts.
  - **COFLUX_STUDIO_TEAMS**: Comma-separated list of team IDs allowed for Studio auth
  - **COFLUX_STUDIO_URL**: Studio URL for JWKS (default: https://studio.coflux.com)
  """

  @doc """
  Loads all config from environment variables. Call once at startup.
  """
  def init do
    :persistent_term.put(:coflux_data_dir, parse_data_dir())
    :persistent_term.put(:coflux_port, String.to_integer(System.get_env("PORT", "7777")))
    :persistent_term.put(:coflux_project, System.get_env("COFLUX_PROJECT"))
    :persistent_term.put(:coflux_public_host, parse_public_host())
    :persistent_term.put(:coflux_allowed_origins, parse_allowed_origins())
    :persistent_term.put(:coflux_require_auth, parse_require_auth())
    :persistent_term.put(:coflux_studio_teams, parse_studio_teams())
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
  Returns the parsed public host configuration, or nil if not set.

  When set with a leading `%` (e.g., `%.example.com:7777`), returns
  `{:template, suffix}` for subdomain-based routing. Without a `%` prefix,
  returns the literal host string.
  """
  def public_host do
    :persistent_term.get(:coflux_public_host)
  end

  @doc """
  Returns the base domain for subdomain-based routing, or nil.

  Derived from COFLUX_PUBLIC_HOST when it starts with `%`.
  For example, `%.example.com:7777` yields base domain `example.com`.
  """
  def base_domain do
    case public_host() do
      {:template, suffix} ->
        suffix
        |> String.trim_leading(".")
        |> String.split(":")
        |> hd()

      _ ->
        nil
    end
  end

  @doc """
  Returns the host that workers should use to connect to this server.

  When COFLUX_PUBLIC_HOST starts with `%`, the project ID is substituted.
  Otherwise the literal host is returned. Falls back to `localhost:PORT`
  when not configured.
  """
  def server_host(project_id \\ nil) do
    port_suffix =
      case :persistent_term.get(:coflux_port) do
        port when port in [80, 443] -> ""
        port -> ":#{port}"
      end

    case public_host() do
      {:template, suffix} ->
        "#{project_id}#{suffix}"

      host when is_binary(host) ->
        host

      nil ->
        "localhost#{port_suffix}"
    end
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
  Returns the set of allowed team IDs for Studio auth.

  Returns nil if not configured (Studio auth disabled).
  """
  def studio_teams do
    :persistent_term.get(:coflux_studio_teams)
  end

  @doc """
  Returns the Studio URL for fetching JWKS.
  """
  def studio_url do
    :persistent_term.get(:coflux_studio_url)
  end

  @doc """
  Returns the super token hash, or nil if not configured.

  Set via COFLUX_SUPER_TOKEN_HASH (a SHA-256 hex digest). The raw token never
  needs to exist on the server — only the hash is stored.
  """
  def super_token_hash do
    :persistent_term.get(:coflux_super_token_hash)
  end

  @doc """
  Returns the server secret, or nil if not configured.

  This secret is used to derive per-project signing keys for service tokens.
  Set via COFLUX_SECRET environment variable. Service tokens are only supported
  when this is configured.
  """
  def secret do
    :persistent_term.get(:coflux_secret)
  end

  defp parse_public_host do
    case System.get_env("COFLUX_PUBLIC_HOST") do
      nil -> nil
      "" -> nil
      "%" <> suffix -> {:template, suffix}
      host -> host
    end
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

  defp parse_studio_teams do
    case System.get_env("COFLUX_STUDIO_TEAMS") do
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
    case System.get_env("COFLUX_SUPER_TOKEN_HASH") do
      nil -> nil
      "" -> nil
      hash -> hash
    end
  end

  defp parse_secret do
    case System.get_env("COFLUX_SECRET") do
      nil -> nil
      "" -> nil
      secret -> secret
    end
  end
end
