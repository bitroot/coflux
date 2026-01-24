defmodule Coflux.Config do
  @moduledoc """
  Caches configuration values read from environment variables at startup.

  Uses persistent_term for fast reads without copying.
  """

  @doc """
  Loads all config from environment variables. Call once at startup.
  """
  def init do
    :persistent_term.put(:coflux_data_dir, parse_data_dir())
    :persistent_term.put(:coflux_auth_mode, parse_auth_mode())
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
  Returns the auth mode: `:none` or `:token` (default).
  """
  def auth_mode do
    :persistent_term.get(:coflux_auth_mode)
  end

  @doc """
  Returns the base domain for namespace resolution, or nil if not set.
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
    case System.get_env("COFLUX_AUTH_MODE", "token") do
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
