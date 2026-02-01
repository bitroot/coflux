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

  - **COFLUX_AUTH_MODE**: Authentication mode - "token" (default), "studio", or "none"
  - **COFLUX_NAMESPACES**: Comma-separated list of team IDs allowed (for studio mode)
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
    :persistent_term.put(:coflux_auth_mode, parse_auth_mode())
    :persistent_term.put(:coflux_namespaces, parse_namespaces())
    :persistent_term.put(:coflux_studio_url, parse_studio_url())
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
  Returns the authentication mode (:token, :studio, or :none).

  - :token (default) - All requests require valid token from tokens.json
  - :studio - Validates JWTs issued by Studio
  - :none - No authentication required
  """
  def auth_mode do
    :persistent_term.get(:coflux_auth_mode)
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

  defp parse_auth_mode do
    case System.get_env("COFLUX_AUTH_MODE") do
      "none" -> :none
      "studio" -> :studio
      _ -> :token
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
end
