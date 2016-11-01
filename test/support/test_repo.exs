defmodule MongoEcto.TestRepo do
    conf = Application.get_env(:mongo_ecto, Repo)
    MongoEcto.Repo.start_link(conf)
end