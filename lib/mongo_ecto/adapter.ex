defmodule MongoEcto.Adapter do
    @behaviour Ecto.Adapter

    @doc false
    defmacro __before_compile__(env) do
        config = Module.get_attribute(env.module, :config)
        pool = Keyword.get(config, :pool, DBConnection.Poolboy)
        pool_name = pool_name(env.module, config)
        norm_config = normalize_config(config)
        quote do
            @doc false
            def __pool__ do
                {unquote(pool_name), unquote(Macro.escape(norm_config))}
            end
            defoverridable [__pool__: 0]
        end
    end

    @pool_timeout 5_000
    @timeout 15_000

    defp normalize_config(config) do
        config
        |> Keyword.delete(:name)
        |> Keyword.put_new(:timeout, @timeout)
        |> Keyword.put_new(:pool_timeout, @pool_timeout)
    end

    defp pool_name(module, config) do
        Keyword.get(config, :pool_name, default_pool_name(module, config))
    end

    defp default_pool_name(repo, config) do
        Module.concat(Keyword.get(config, :name, repo), Pool)
    end

    @doc false
    def application, do: :mongo_ecto


    @doc false
    def child_spec(repo, opts) do
        {pool_name, pool_opts} =
            case Keyword.fetch(opts, :pool) do
                {:ok, pool} ->
                    {pool_name(repo, opts), opts}
                _ ->
                    repo.__pool__
            end
        opts = [name: pool_name] ++ Keyword.delete(opts, :pool) ++ pool_opts

        Mongo.child_spec(opts)
    end

end