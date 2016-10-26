# Setup
1. Add `{:mongo_ecto, git: "https://github.com/eyrmedical/mongo_ecto.git"}` to **mix.exs** under `deps` function, add `:poolboy` and `:mongodb` in your application list.
2. Run `mix deps.get && mix deps.compile`.
3. Add `MongoEcto.Repo` into your supervision tree, on Phoenix it should look like: `worker(MongoEcto.Repo, [[database: "eyr", hostname: "localhost"]])`.

# Usage
`MongoEcto.Repo` implements most of the `Ecto.Repo` functions with exception to transactions, mass operations (**insert_all, update_all etc**) and system calls defined for the `Ecto.Repo` Behaviour.

You can use `MongoEcto.Repo` in the same way you are using [Ecto.Repo](https://hexdocs.pm/ecto/Ecto.Repo.html)

The main difference is that you should use [Mongo queries](https://docs.mongodb.com/v3.2/tutorial/query-documents/) in form of `map()` instead of `Ecto.Query` keyword clause.

* `[] = MongoEcto.Repo.all(schema)`
* `[] = MongoEcto.Repo.all(schema, %{
    "$query": %{
        "$or": [
            %{account_id: account1_id},
            %{account_id: account2_id}
        ]
    }
})`
* `schema_record = MongoEcto.Repo.get(schema, id)`
* `schema_record = MongoEcto.Repo.one(schema, %{field: value})`
* `{:ok, schema_record} = MongoEcto.Repo.insert(changeset)`
* `schema_record = MongoEcto.Repo.insert!(changeset)`
* `schema_record = MongoEcto.Repo.delete!(changeset)`

# Diclaimer
This module is in it's early beta, use on your own risk.
