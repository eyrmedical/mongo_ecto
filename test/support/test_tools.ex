defmodule MongoEcto.TestTools do
    def findCollection(name) do
        result = Mongo.run_command(MongoEcto, %{"listCollections" => true})
        %{"cursor" => %{"firstBatch" => collections}} = result
        Enum.find collections, fn
            (%{"name" => ^name}) -> true;
            (_) -> false
        end
    end
end