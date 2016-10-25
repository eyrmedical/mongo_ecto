defmodule MongoEcto.Repo do
    @moduledoc """
    Define Repo-like functions to import in Mongo models.
    """

    require Logger

    alias Ecto.Changeset

    @type mongo_bson_id :: %BSON.ObjectId{}
    @type mongo_string_id :: << _ :: 192 >> 
    @type mongo_binary_id :: << _ :: 96 >>
    @type mongo_id :: mongo_string_id | mongo_binary_id | mongo_bson_id
    @type mongo_object_result :: {:ok, Ecto.Schema.t} | {:error, Ecto.Changeset.t}
    @type mongo_changeset :: Ecto.Changeset.t
    @type mongo_record :: Ecto.Schema.t
    @type mongo_object :: mongo_changeset | mongo_record
    @type mongo_query :: map()
    @type mongo_options :: Keyword.t
    @type mongo_preload :: [atom()]
    @type mongo_schema :: Ecto.Schema.t
    @type mongo_assoc :: %Ecto.Association.HasThrough{} | %Ecto.Association.Has{}


    def start_link(opts) do
        MongoEcto.start(opts)
    end
    @doc """
    Get all records by query.
    """
    @spec all(mongo_schema, mongo_query) :: [mongo_record] | []
    def all(schema, query \\ %{}) do
        all(schema, query, [])
    end
    @spec all(mongo_schema, mongo_query, mongo_options) :: [mongo_record] | []
    def all(schema, query, options) do
        all(schema, query, options, [])
    end
    @spec all(mongo_schema, mongo_query, mongo_options, mongo_preload) :: [mongo_record] | []
    def all(schema, query, options, preload) do
        mongo_raw_data = query(schema.collection_name, query, options)
        Enum.map(mongo_raw_data, &(cursor_record_to_struct(schema, &1, preload)))
    end


    @doc """
    Get record by id.
    """
    @spec get(mongo_schema, mongo_id) :: mongo_record | nil | no_return
    def get(_schema, nil) do
        nil
    end
    def get(schema, id) do
        id = to_mongo_id(id)
        get_by(schema, %{_id: id})
    end

    @doc """
    Get record by id, raise if not found.
    """
    @spec get!(mongo_schema, mongo_id) :: mongo_record | no_return
    def get!(schema, id) do
        raise_if_no_results get(schema, id)
    end


    @doc """
    Get record by query.
    """
    @spec get_by(mongo_schema, mongo_query) :: mongo_record | nil | no_return
    def get_by(schema, query \\ %{}) do
        result = all(schema, query)
        case result do
            [record | _] -> record
            _ -> nil
        end
    end

    @doc """
    Get record by query, raise if not found.
    """
    @spec get_by!(mongo_schema, mongo_query) :: mongo_record | no_return
    def get_by!(schema, query \\ %{}) do
        raise_if_no_results get_by(schema, query)
    end


    @doc """
    Get single record by query.
    """
    @spec one(mongo_schema, mongo_query) :: mongo_record | nil | no_return
    def one(schema, query \\ %{}) do
        result = all(schema, query)
        case result do
            [record | []] -> record
            [_record | _] -> raise(Ecto.MultipleResultsError, message: "expected one record, but got more")
            _ -> nil
        end
    end

    @doc """
    Get single record by query, raise if not found.
    """
    @spec one!(mongo_schema, mongo_query) :: mongo_record | no_return
    def one!(schema, query \\ %{}) do
        raise_if_no_results one(schema, query)
    end


 @doc """
    Add new record.
    """
    @spec insert(mongo_object) :: {:ok, mongo_record} | {:error, mongo_changeset}
    def insert(%{valid?: true, data: %{__struct__: schema}} = changeset) do
        new_record = changeset
        |> schema.field_timestamp(:inserted_at)
        |> schema.field_timestamp(:updated_at)
        |> schema.apply_changes

        new_record_map = Map.from_struct(new_record)
        result = Mongo.insert_one(MongoEcto, schema.collection_name, new_record_map)
        case result do
            {:ok, %{inserted_id: bson_id}} ->
                inserted_record = inserted_record_to_struct(new_record_map, mongo_id_to_string(bson_id))
                {:ok, inserted_record}
            {:error, mongo_error} ->
                Logger.error fn -> "Mongo Insert Error: " <> inspect(mongo_error) end
                changeset = Changeset.add_error(changeset, :id, "failed to create new record")
                {:error, changeset}
            _ ->
                {:ok, new_record}
        end
    end
    def insert(%{valid?: false} = changeset) do
        {:error, changeset}
    end
    def insert(record) do
        changeset = Ecto.Changeset.change(record)
        insert(changeset)
    end

    @doc """
    Add new record, raise in case of error.
    """
    @spec insert!(mongo_object) :: mongo_record | no_return
    def insert!(changeset) do
        raise_if_changeset_errors insert(changeset), "insert"
    end


    @doc """
    Update existing record.
    """
    @spec update(mongo_object) :: {:ok, mongo_record} | {:error, mongo_changeset}
    def update(%{
        valid?: true,
        id: record_id,
        data: %{__struct__: schema}
    } = changeset) when is_bitstring(record_id) do
        updated_record = changeset
        |> schema.field_timestamp(:updated_at)
        |> schema.apply_changes

        bson_record_id = to_mongo_id(record_id)
        result = Mongo.replace_one(
            MongoEcto,
            schema.collection_name,
            %{_id: bson_record_id},
            Map.from_struct(updated_record)
        )
        case result do
            {:ok, %Mongo.UpdateResult{matched_count: 1, modified_count: 1, upserted_id: nil}} ->
                {:ok, updated_record}
            {:error, mongo_error} ->
                Logger.error fn -> "Mongo Update Error: " <> inspect(mongo_error) end
                changeset = Changeset.add_error(changeset, :id, "failed to update existing record")
                {:error, changeset}
            _ ->
                {:ok, updated_record}
        end
    end
    def update(%{valid?: false} = changeset) do
        {:error, changeset}
    end
    def update(%{id: _record_id} = record) do
        changeset = Ecto.Changeset.change(record)
        update(changeset)
    end
    def update(changeset) do
        changeset = Changeset.add_error(changeset, :id, "record doesn't existing in a database")
        {:error, changeset}
    end

    @doc """
    Update existing record, raise in case of error.
    """
    @spec update!(mongo_object) :: mongo_record | no_return
    def update!(changeset) do
        raise_if_changeset_errors update(changeset), "update"
    end


    @doc """
    Update existing record.
    """
    @spec insert_or_update(mongo_object) :: {:ok, mongo_record} | {:error, mongo_changeset}
    def insert_or_update(%{valid?: true, id: record_id} = changeset) when is_bitstring(record_id) do
        update(changeset)
    end
    def insert_or_update(%{valid?: true} = changeset) do
        insert(changeset)
    end
    def insert_or_update(%{valid?: false} = changeset) do
        {:error, changeset}
    end
    def insert_or_update(record) do
        changeset = Ecto.Changeset.change(record)
        insert_or_update(changeset)
    end

    @doc """
    Update existing record, raise in case of error.
    """
    @spec insert_or_update!(mongo_object) :: mongo_record | no_return
    def insert_or_update!(changeset) do
        raise_if_changeset_errors insert_or_update(changeset), "upsert"
    end


    @doc """
    Delete existing record.
    """
    @spec delete(mongo_object) :: {:ok, mongo_record} | {:error, mongo_changeset}
    def delete(%{data: %{__struct__: schema}} = changeset) do
        record = schema.apply_changes(changeset)
        delete(record)
    end
    def delete(%{id: record_id, __struct__: schema} = record) when is_bitstring(record_id) do
        record_id = to_mongo_id(record_id)
        result = Mongo.delete_one(MongoEcto, schema.collection_name, %{"_id" => record_id})
        case result do
            {:ok, %Mongo.DeleteResult{deleted_count: 1}} -> 
                {:ok, record}
            _ ->
                changeset = record
                |> Changeset.change
                |> Changeset.add_error(:id, "failed to delete record")
                {:error, changeset}
        end
    end
    def delete(record) do
        changeset = record
        |> Changeset.change
        |> Changeset.add_error(:id, "record doesn't existing in a database")
        {:error, changeset}
    end

    @doc """
    Delete existing record, raise in case of error.
    """
    @spec delete!(mongo_object) :: mongo_record | no_return
    def delete!(record) do
        raise_if_changeset_errors delete(record), "delete"
    end


    @doc """
    Load connected records from another schema.
    """
    @spec preload(mongo_record | nil, atom() | [atom()]) :: mongo_record
    def preload(nil, _key) do
        nil
    end
    def preload(record, keys) when is_list(keys) do
        Enum.reduce keys, record, &(preload &2, &1)
    end
    def preload(record, key) do
        assoc = record.__struct__.__schema__(:association, key)
        load_assoc(record, assoc)
    end


    @doc """
    Encode binary id to a string value.
    """
    @spec mongo_id_to_string(%BSON.ObjectId{} | binary()) :: String.t
    def mongo_id_to_string(%BSON.ObjectId{value: bson_id}) do
        mongo_id_to_string(bson_id)
    end
    def mongo_id_to_string(mongo_id) do
        Base.encode16(mongo_id, case: :lower)
    end


    # Load HasThrough assoc relationship
    @spec load_assoc(mongo_record, mongo_assoc) :: mongo_record
    defp load_assoc(record, %Ecto.Association.HasThrough{
        cardinality: :many,
        relationship: :child,
        through: [direct_child, direct_grandc]
    }) do
        record = preload(record, direct_child)
        children = Map.get(record, direct_child)
        children = Enum.map children, &(preload(&1, direct_grandc))
        all_grandc = Enum.reduce children, [], fn(child, acc) ->
            acc ++ Map.get(child, direct_grandc)
        end
        
        record
        |> Map.put(direct_child, children)
        |> Map.put(direct_grandc, all_grandc)
    end
    # Load HasMany assoc relationship
    defp load_assoc(record, %Ecto.Association.Has{
        cardinality: :many,
        relationship: :child,
        field: assoc_field,
        owner_key: parent_key,
        related_key: child_key,
        related: childSchema
    }) do
        query = Map.put(%{}, child_key, Map.get(record, parent_key))
        children = all(childSchema, query)
        Map.put(record, assoc_field, children)
    end
    defp load_assoc(record, %Ecto.Association.BelongsTo{
        cardinality: :one,
        relationship: :parent,
        field: assoc_field,
        owner_key: parent_key,
        related_key: child_key,
        related: parentSchema
    }) do
        parent = get(parentSchema, Map.get(record, parent_key))
        Map.put(record, assoc_field, parent)
    end
    defp load_assoc(_record, assoc) do
        Logger.error fn ->
            "Mongo Preload Error: association was not implemeneted, yet" <> inspect(assoc)
        end
        raise Ecto.SubQueryError, message: "association was not implemented"
    end
    

    # Convert all changeset errors into a single string value.
    @spec changeset_errors_to_string(mongo_changeset) :: String.t
    defp changeset_errors_to_string(changeset) do
        Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
            Enum.reduce(opts, msg, fn {key, value}, _acc ->
                String.replace(msg, "%{#{key}}", to_string(value))
            end)
        end)
    end

    # Raise Ecto.NoResultsError if there is no results
    @spec raise_if_no_results(mongo_record | nil) :: mongo_record | no_return
    defp raise_if_no_results(result) do
        case result do
            nil -> raise Ecto.NoResultsError, message: "no results found"
            record -> record
        end
    end

    # Convert changeset errors, by raising Ecto.InvalidChangesetError
    # Useful for bang (!) functions
    @spec raise_if_changeset_errors(mongo_object_result, String.t) :: mongo_object | no_return
    defp raise_if_changeset_errors(result, action) do
        case result do
            {:ok, record} ->
                record
            {:error, changeset} ->
                Logger.error fn ->
                    "Mongo #{action} Error: " <> changeset_errors_to_string(changeset)
                end
                raise Ecto.InvalidChangesetError, message: "failed to #{action} record"
            _ ->
                raise Ecto.InvalidChangesetError, message: "failed to #{action} record"
        end
    end

    # Ensure that Mongo id is converted to a proper BSON object.
    @spec to_mongo_id(mongo_id) :: mongo_bson_id
    defp to_mongo_id(%BSON.ObjectId{} = id) do
        id
    end
    defp to_mongo_id(<< _ :: size(192)>> = string_id) do
        binary_id = Base.decode16!(string_id, case: :lower)
        to_mongo_id(binary_id)
    end
    defp to_mongo_id(<< _ :: size(96)>> = binary_id) do
        %BSON.ObjectId{value: binary_id}
    end
    defp to_mongo_id(id) do
        BSON.Decoder.decode(id)
    end


    # Query mongo for data
    @spec query(String.t, mongo_query, mongo_options) :: [map()]
    defp query(collection_name, query, options) do
        mongo_cursor = Mongo.find(MongoEcto, collection_name, query, options)
        mongo_cursor
        |> Enum.to_list
    end


    # Ensure that returned map() record is the struct of the schema type.
    @spec cursor_record_to_struct(mongo_schema, map(), mongo_preload) :: mongo_record
    defp cursor_record_to_struct(schema, %{"_id" => %BSON.ObjectId{}} = model, preload \\ []) do
        model = for {key, val} <- model, into: %{} do
            case val do
                %BSON.ObjectId{value: binary_id} -> {:id, mongo_id_to_string(binary_id)}
                %BSON.DateTime{utc: ts} -> {String.to_atom(key), timestamp_to_datetime(ts)}
                _ -> {String.to_atom(key), val}
            end
        end
        record = Kernel.struct(schema, model)
        preload(record, preload)
    end

    # Ensure that inserted record is properly set.
    @spec inserted_record_to_struct(map(), mongo_string_id) :: mongo_record
    defp inserted_record_to_struct(record, id) do
        record = Map.put(record, :id, id)
        for {key, val} <- record, into: %{} do
            case val do
                %BSON.DateTime{utc: ts} -> {key, timestamp_to_datetime(ts)}
                _ -> {key, val}
            end
        end
    end

    # Convert integer timestamp to %Ecto.Datetime{}
    @spec timestamp_to_datetime(integer()) :: %Ecto.DateTime{}
    defp timestamp_to_datetime(timestamp) do
        epoch = :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
        datetime = :calendar.gregorian_seconds_to_datetime(epoch + div(timestamp, 1000))
        usec = rem(timestamp, 1000) * 1000
        %{Ecto.DateTime.from_erl(datetime) | usec: usec}
    end
end
