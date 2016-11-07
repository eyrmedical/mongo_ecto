Code.require_file "./support/test_tools.ex", __DIR__
Code.require_file "./support/test_repo.exs", __DIR__

#tests use `accounts` table so we need to create it if not exists
if MongoEcto.TestTools.findCollection("accounts") == nil do
    Mongo.run_command(MongoEcto, %{"create" => "accounts"})
end

ExUnit.start()