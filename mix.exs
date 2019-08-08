defmodule Pgxreplay.MixProject do
  use Mix.Project

  def project do
    [
      app: :pgxreplay,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
      {:apex, "~> 1.1"},
      {:jason, "1.1.2"},
      {:observer_cli, "~> 1.5"},
      {:postgrex, "0.14.1"}
    ]
  end
end
