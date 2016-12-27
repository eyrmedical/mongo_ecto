defmodule MongoEcto.Repo.EmbeddedSchemaTest do
    require MongoEcto.Repo, as: TestRepo
    use ExUnit.Case, async: true

    defmodule AccountInterest do
        use Ecto.Schema
        use MongoEcto.Model, :model

        embedded_schema do
            field :interest, :string
            field :active, :boolean, default: true
        end
    end

    defmodule Account do
        use Ecto.Schema
        use MongoEcto.Model, :model

        @collection_name "accounts"
        @primary_key {:id, :binary_id, autogenerate: true}

        embedded_schema do
            field :nickname, :string
            field :email, :string

            embeds_many :interests, AccountInterest
            embeds_one :main_interest, AccountInterest
        end
    end

    setup do
        on_exit fn ->
            Mongo.delete_many(MongoEcto, "accounts", %{})
        end
    end

    
    test "creation of embedded schema model" do
        interests = [
            %AccountInterest{interest: "Football", active: true},
            %AccountInterest{interest: "Hunting", active: true}
        ]
        main_interest = %AccountInterest{interest: "Fishing", active: true}

        account = %Account{nickname: "Tester", email: "embed_test@gmail.com"}
        changeset = account
        |> Ecto.Changeset.change
        |> Ecto.Changeset.put_embed(:interests, interests)
        |> Ecto.Changeset.put_embed(:main_interest, main_interest)
        assert TestRepo.insert!(changeset)
        inserted_account = TestRepo.one(Account, %{email: "embed_test@gmail.com"})
        assert inserted_account
        assert inserted_account.interests == interests
        assert inserted_account.main_interest == main_interest
    end
end
