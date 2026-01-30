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
  """

  @doc """
  Loads all config from environment variables. Call once at startup.
  """
  def init do
    :persistent_term.put(:coflux_data_dir, parse_data_dir())
    :persistent_term.put(:coflux_auth_mode, parse_auth_mode())
    :persistent_term.put(:coflux_project, System.get_env("COFLUX_PROJECT"))
    :persistent_term.put(:coflux_base_domain, System.get_env("COFLUX_BASE_DOMAIN"))
    :persistent_term.put(:coflux_allowed_origins, parse_allowed_origins())
    :ok
  end

  @doc """
  Returns the data directory path.
  """
  def data_dir do
    :persistent_term.get(:coflux_data_dir)
  end

  @doc """
  Returns the auth mode: `:none` (default) or `:token`.
  """
  def auth_mode do
    :persistent_term.get(:coflux_auth_mode)
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

  defp parse_data_dir do
    System.get_env("COFLUX_DATA_DIR", Path.join(File.cwd!(), "data"))
  end

  defp parse_auth_mode do
    case System.get_env("COFLUX_AUTH_MODE", "none") do
      "none" -> :none
      "token" -> :token
      other -> raise "Invalid COFLUX_AUTH_MODE: #{inspect(other)}. Must be \"none\" or \"token\"."
    end
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
end
