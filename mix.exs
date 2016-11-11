defmodule MongoEcto.Mixfile do
  use Mix.Project

  def project do
    [app: :mongo_ecto,
     version: "0.1.6",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end


  def application do
    [applications: [:logger, :mongodb, :poolboy]]
  end


  defp deps do
    [{:ecto, "~> 2.0.0"},
     {:mongodb, ">= 0.0.0"},
     {:poolboy, ">= 0.0.0"}]
  end
end
