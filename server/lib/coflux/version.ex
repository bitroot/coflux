defmodule Coflux.Version do
  @version Mix.Project.config()[:version]
  @api_version (case Version.parse(@version) do
                  {:ok, %Version{major: 0, minor: minor}} -> "0.#{minor}"
                  {:ok, %Version{major: major}} -> "#{major}"
                  :error -> @version
                end)

  def version, do: @version
  def api_version, do: @api_version

  def check(nil), do: :ok
  def check(@api_version), do: :ok
  def check(expected), do: {:error, @version, expected}
end
