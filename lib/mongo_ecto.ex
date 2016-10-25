defmodule MongoEcto do
    @moduledoc """
    Connection pool for native MongoDb connections.
    """

    use Mongo.Pool, name: __MODULE__, adapter: Mongo.Pool.Poolboy

    def start(opts), do: MongoEcto.start_link(opts)
end
