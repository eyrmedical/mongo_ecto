# Setup
1. Add `{:mongodb, "0.1.1"}, {:poolboy, ">= 0.0.0"}, {:mongo_ecto, git: "https://github.com/eyrmedical/mongo_ecto.git", tag: "v0.1.7-beta"}` to **mix.exs** under `deps` function, add `:poolboy` and `:mongodb` in your application list.
2. Run `mix deps.get && mix deps.compile`.
3. Add `MongoEcto.Repo` into your supervision tree, on Phoenix it should look like: `worker(MongoEcto.Repo, [[database: "eyr", hostname: "localhost"]])`.
4. In **web.exs** either remove `import Ecto.Changeset` completly if you plan to use different database endpoints, or replace it with `import Ecto.Changeset, except: [unique_constraint: 2]` if you plan to use **MongoEcto** as single repo for all your schemas.

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

# Model
* Add `use MongoEcto.Model, :model` to your Mongo models.
* Define `@collection_name` with the name of the related mongo collection.
* Define `@primary_key {:id, :binary_id, autogenerate: true}` to comply Mongo ObjectId UID.
* Use `embedded_schema` instead of `schema` to define fields.

Helper functions `field_timestamp/2` and `now_timestamp/0` can be used to set current timestamp, like: `Schema.field_timestamp(record, :inserted_at)`

# Diclaimer
This module is in it's early beta, use on your own risk.
