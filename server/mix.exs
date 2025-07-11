defmodule Coflux.MixProject do
  use Mix.Project

  @version String.trim(File.read!("VERSION"))

  def project do
    [
      app: :coflux,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Coflux.Application, []}
    ]
  end

  defp deps do
    [
      {:cowboy, "~> 2.9"},
      {:exqlite, "~> 0.13"},
      {:jason, "~> 1.4"},
      {:topical, "~> 0.2"},
      {:briefly, "~> 0.5.0"},
      {:unzip, "~> 0.12.0"},
      {:mime, "~> 2.0"},
      {:req, "~> 0.5"}
    ]
  end
end
