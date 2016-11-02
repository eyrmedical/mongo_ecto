defmodule MongoEcto.Repo.ValidateTest do
    require MongoEcto.Repo, as: TestRepo
    use ExUnit.Case, async: true

    defmodule Account do
        use Ecto.Schema
        use MongoEcto.Model, :model

        @collection_name "accounts"
        @primary_key {:id, :binary_id, autogenerate: true}

        embedded_schema do
            field :nickname, :string
            field :email, :string
        end
    end

    defmodule NoPkAccount do
        use Ecto.Schema
        use MongoEcto.Model, :model

        @collection_name "accounts"
        @primary_key false

        embedded_schema do
            field :nickname, :string
            field :email, :string
        end
    end


     test "all returns list" do
        assert is_list(TestRepo.all(Account))
        query = %{"$query": %{"$or": [%{account_id: "777"}, %{account_id: "778"}]}}
        assert is_list(TestRepo.all(Account, query))
     end

    test "get raises exception with incorrect mongo id" do
        message = "Field is not correct Mongo Id"
        assert_raise Ecto.InvalidMongoIdError, message, fn ->
            TestRepo.get(Account, 456)
        end
    end

    test "get returns nil when nothing found" do
      assert TestRepo.get(Account, "ffffffffffffffffffffffff") == nil
      assert TestRepo.get(Account, nil) == nil
    end

    test "validates get!" do
        message = "no results found"
        assert_raise Ecto.NoResultsError, message, fn ->
            TestRepo.get!(Account, "ffffffffffffffffffffffff")
        end
        assert_raise Ecto.NoResultsError, message, fn ->
            TestRepo.get!(Account, nil)
        end
    end

    test "get_by returns nil when nothing found" do
        assert TestRepo.get_by(Account, %{"email" => "abc"}) == nil
        assert TestRepo.get_by(Account, %{"nonexistent" => "abc"}) == nil
    end

    test "validates get_by!" do
        message = "no results found"
        assert_raise Ecto.NoResultsError, message, fn ->
            TestRepo.get_by!(Account, %{"email" => "abc"})
        end
        assert_raise Ecto.NoResultsError, message, fn ->
            TestRepo.get_by!(Account, %{"nonexistent" => "abc"})
        end
    end

    test "one returns nil when nothing found" do
        assert TestRepo.one(Account, %{"email" => "abc"}) == nil
        assert TestRepo.one(Account, %{"nonexistent" => "abc"}) == nil
    end

    test "validates one!" do
        message = "no results found"
        assert_raise Ecto.NoResultsError, message, fn ->
            TestRepo.one!(Account, %{"email" => "abc"})
        end
        assert_raise Ecto.NoResultsError, message, fn ->
            TestRepo.one!(Account, %{"nonexistent" => "abc"})
        end
    end

    test "delete raises exception without id" do
        account = %NoPkAccount{}
        changeset = Ecto.Changeset.change(account, %{})
        assert_raise Ecto.NoPrimaryKeyFieldError, fn ->
             TestRepo.delete(account)
        end
        assert_raise Ecto.NoPrimaryKeyFieldError, fn ->
            TestRepo.delete(changeset)
                   end
    end

    test "delete raises exception with nil id" do
        account = %Account{}
        changeset = Ecto.Changeset.change(account, %{})
        assert_raise Ecto.NoPrimaryKeyValueError, fn ->
             TestRepo.delete(account)
        end
        assert_raise Ecto.NoPrimaryKeyValueError, fn ->
            TestRepo.delete(changeset)
        end
    end

    test "delete nonexistent entity return error" do
        account = %Account{id: "ffffffffffffffffffffffff"}
        changeset = Ecto.Changeset.change(account, %{})
        assert match? {:error, %Ecto.Changeset{errors: [id: {"failed to delete record", []}]}},
            TestRepo.delete(account)
        assert match? {:error, %Ecto.Changeset{errors: [id: {"failed to delete record", []}]}},
            TestRepo.delete(changeset)
    end

    test "validates delete!" do
        account = %Account{id: "ffffffffffffffffffffffff"}
        changeset = Ecto.Changeset.change(account, %{})
        assert_raise Ecto.InvalidChangesetError, fn ->
            TestRepo.delete!(account)
        end
        assert_raise Ecto.InvalidChangesetError, fn ->
            TestRepo.delete!(changeset)
        end
    end

    test "insert invalid changeset returns error" do
         account = %Account{}
         changeset = account |> Ecto.Changeset.cast(%{}, [:email], [])
         assert {:error, changeset} == TestRepo.insert(changeset)
    end

     test "update raises exception without id" do
        account = %NoPkAccount{}
        changeset = Ecto.Changeset.change(account, %{})
        assert_raise Ecto.NoPrimaryKeyFieldError, fn ->
            TestRepo.update(changeset)
        end
    end

    test "update raises exception with nil id" do
        account = %Account{}
        changeset = Ecto.Changeset.change(account, %{})
        assert_raise Ecto.NoPrimaryKeyValueError, fn ->
            TestRepo.update(changeset)
        end
    end

    test "update invalid changeset returns error" do
         account = %Account{id: "ffffffffffffffffffffffff"}
         changeset = account |> Ecto.Changeset.cast(%{}, [:email], [])
         assert {:error, changeset} == TestRepo.update(changeset)
    end

    test "update! invalid changeset raises exception" do
        account = %Account{id: "ffffffffffffffffffffffff"}
        changeset = account |> Ecto.Changeset.cast(%{}, [:email], [])
        assert_raise Ecto.InvalidChangesetError, fn ->
            TestRepo.update!(changeset)
        end
    end
end