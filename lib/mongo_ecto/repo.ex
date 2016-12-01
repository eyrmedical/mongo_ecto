defmodule MongoEcto.Repo do
    @moduledoc """
    Define Repo-like functions to import in Mongo models.
    """

    require Logger

    alias Ecto.Changeset

    @type mongo_bson_id :: %BSON.ObjectId{}
    @type mongo_string_id :: << _ :: 192 >> 
    @type mongo_binary_id :: << _ :: 96 >>
    @type mongo_id :: mongo_string_id | mongo_binary_id | mongo_bson_id | String.t | integer
    @type mongo_object_result :: {:ok, Ecto.Schema.t} | {:error, Ecto.Changeset.t}
    @type mongo_changeset :: Ecto.Changeset.t
    @type mongo_record :: Ecto.Schema.t
    @type mongo_dirty_record :: Ecto.Schema.t
    @type mongo_object :: mongo_changeset | mongo_record
    @type mongo_query :: map()
    @type mongo_options :: Keyword.t
    @type mongo_preload :: [atom()]
    @type mongo_schema :: Ecto.Schema.t
    @type mongo_assoc :: %Ecto.Association.HasThrough{} | %Ecto.Association.Has{}
    @type mongo_datatype :: mongo_bson_id | %BSON.DateTime{}


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
    def get(_schema, nil), do: nil
    def get(schema, id) do
        if is_mongo_id(id) do
            id = to_mongo_id(id)
            get_by(schema, %{_id: id})
        else
            raise Ecto.InvalidMongoIdError
        end
    end


    @doc """
    Get record by id, raise if not found.
    """
    @spec get!(mongo_schema, mongo_id) :: mongo_record | no_return
    def get!(schema, nil), do: raise %Ecto.NoResultsError{message: "no results found"}
    def get!(schema, id) do
        raise_if_no_results schema, get(schema, id)
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
        raise_if_no_results schema, get_by(schema, query)
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
        raise_if_no_results schema, one(schema, query)
    end


    @doc """
    Add new record.
    """
    @spec insert(mongo_object) :: {:ok, mongo_record} | {:error, mongo_changeset}
    def insert(%Changeset{valid?: true, data: %{__struct__: schema}} = changeset) do
        new_record = changeset
        |> autogenerate(:autogenerate)
        |> schema.apply_changes

        foreign_keys = get_foreign_keys(new_record)
        new_record_map = Map.from_struct(new_record)
        |> convert_types_with(&to_mongo_type/1)
        |> convert_types_for(foreign_keys, &to_mongo_id/1)

        result = Mongo.insert_one(MongoEcto, schema.collection_name, new_record_map)
        case result do
            {:ok, %{inserted_id: bson_id}} ->
                inserted_record = Map.put(new_record, :id, mongo_id_to_string(bson_id))
                {:ok, inserted_record}
            {:error, mongo_error} ->
                Logger.error fn -> "Mongo Insert Error: " <> inspect(mongo_error) end
                changeset = Changeset.add_error(changeset, :id, "failed to create new record")
                {:error, changeset}
            _ ->
                {:ok, new_record}
        end
    end
    def insert(%Changeset{valid?: false} = changeset) do
        {:error, changeset}
    end
    def insert(record) do
        changeset = Changeset.change(record)
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
    @spec update(mongo_changeset) :: {:ok, mongo_record} | {:error, mongo_changeset}
    def update(%Changeset{valid?: true, data: %{__struct__: schema, id: record_id}} = changeset)
        when is_bitstring(record_id) do
        record_to_update = changeset
        |> autogenerate(:autoupdate)
        |> schema.apply_changes

        foreign_keys = get_foreign_keys(record_to_update)
        to_update = Map.from_struct(record_to_update)
        |> convert_types_with(&to_mongo_type/1)
        |> convert_types_for(foreign_keys, &to_mongo_id/1)

        bson_record_id = to_mongo_id(record_id)
        result = Mongo.replace_one(
            MongoEcto,
            schema.collection_name,
            %{_id: bson_record_id},
            to_update
        )
        case result do
            {:ok, %Mongo.UpdateResult{matched_count: 1, modified_count: 1, upserted_id: nil}} ->
                {:ok, Map.put(record_to_update, :id, record_id)}
            {:error, mongo_error} ->
                Logger.error fn -> "Mongo Update Error: " <> inspect(mongo_error) end
                changeset = Changeset.add_error(changeset, :id, "failed to update existing record")
                {:error, changeset}
            _ ->
                {:ok, Map.put(record_to_update, :id, record_id)}
        end
    end
    def update(%Changeset{valid?: false} = changeset) do
        {:error, changeset}
    end
    def update(%Changeset{data: %{id: nil} = record} = _changeset) do
         raise Ecto.NoPrimaryKeyValueError, struct: record
    end
    def update(%Changeset{data: %{__struct__: schema}} = _changeset) do
        raise Ecto.NoPrimaryKeyFieldError, schema: schema
    end


    @doc """
    Update existing record, raise in case of error.
    """
    @spec update!(mongo_changeset) :: mongo_record | no_return
    def update!(%Changeset{} = changeset) do
        raise_if_changeset_errors update(changeset), "update"
    end


    @doc """
    Update existing record.
    """
    @spec insert_or_update(mongo_changeset) :: {:ok, mongo_record} | {:error, mongo_changeset}
    def insert_or_update(%Changeset{valid?: true, data: %{id: record_id}} = changeset)
        when is_bitstring(record_id) do
        update(changeset)
    end
    def insert_or_update(%Changeset{valid?: true} = changeset) do
        insert(changeset)
    end
    def insert_or_update(%Changeset{valid?: false} = changeset) do
        {:error, changeset}
    end


    @doc """
    Update existing record, raise in case of error.
    """
    @spec insert_or_update!(mongo_changeset) :: mongo_record | no_return
    def insert_or_update!(changeset) do
        raise_if_changeset_errors insert_or_update(changeset), "upsert"
    end


    @doc """
    Delete existing record.
    """
    @spec delete(mongo_object) :: {:ok, mongo_record} | {:error, mongo_changeset}
    def delete(%Changeset{data: %{__struct__: schema, id: id}} = changeset) do
        record = schema.apply_changes(changeset)
        delete(Map.put(record, :id, id))
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
    def delete(%{id: nil, __struct__: _schema} = record) do
        raise Ecto.NoPrimaryKeyValueError, struct: record
    end
    def delete(%{__struct__: schema} = _record) do
        raise Ecto.NoPrimaryKeyFieldError, schema: schema
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
    def mongo_id_to_string(<< _ :: size(96)>> = mongo_id) do
        Base.encode16(mongo_id, case: :lower)
    end
    def mongo_id_to_string(mongo_id) when is_bitstring(mongo_id) do
        mongo_id
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
        related_key: _child_key,
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
    @spec raise_if_no_results(mongo_schema, mongo_record | nil) :: mongo_record | no_return
    defp raise_if_no_results(schema, result) do
        case result do
            nil -> raise Ecto.NoResultsError.exception([queryable: schema])
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
                    "Mongo #{action} Error: " <> inspect changeset_errors_to_string(changeset)
                end
                raise Ecto.InvalidChangesetError, [action: action, changeset: changeset]
            _ ->
                raise Ecto.InvalidChangesetError, action: action
        end
    end


    #Generates automatic gereted fields (such as updated_at, inserted_at etc.)
    @spec autogenerate(mongo_changeset, atom()) :: mongo_changeset
    defp autogenerate(%Changeset{data: %{__struct__: schema}, changes: changes} = changeset, action) do
        new_changes = Enum.reduce schema.__schema__(action), changes, fn
            {k, {mod, fun, args}}, acc ->
                Map.put_new(acc, k, apply(mod, fun, args))
        end
        Ecto.Changeset.change(changeset, new_changes)
    end


    #Checks if id is a correct mongo_id type
    @spec is_mongo_id(mongo_id) :: boolean()
    defp is_mongo_id(id) do
        convertable_to_mongo_id(id) or is_mongo_bson_id(id)
    end


    #Checks if id convertable to BSON.ObjectId
    @spec convertable_to_mongo_id(mongo_id) :: boolean()
    defp convertable_to_mongo_id(mongo_id) when is_bitstring(mongo_id), do: true
    defp convertable_to_mongo_id(mongo_id) when is_integer(mongo_id), do: true
    defp convertable_to_mongo_id(<< _ :: size(192)>>), do: true
    defp convertable_to_mongo_id(<< _ :: size(96)>>), do: true
    defp convertable_to_mongo_id(_id), do: false


    #Checks if id is a BSON.ObjectId type
    @spec is_mongo_bson_id(mongo_id) :: boolean()
    defp is_mongo_bson_id(%BSON.ObjectId{} = _id), do: true
    defp is_mongo_bson_id(_id), do: false


    # Ensure that Mongo id is converted to a proper BSON object.
    @spec to_mongo_id(mongo_id) :: mongo_bson_id
    defp to_mongo_id(nil), do: nil
    defp to_mongo_id(%BSON.ObjectId{} = id), do: id
    defp to_mongo_id(<< _ :: size(192)>> = string_id) when is_bitstring(string_id) do
        binary_id = Base.decode16!(string_id, case: :lower)
        to_mongo_id(binary_id)
    end
    defp to_mongo_id(<< _ :: size(96)>> = binary_id) do
        %BSON.ObjectId{value: binary_id}
    end
    defp to_mongo_id(string_id) when is_bitstring(string_id) do
        string_id
    end
    defp to_mongo_id(integer_id) when is_integer(integer_id) do
        integer_id
    end


    # Query mongo for data.
    @spec query(String.t, mongo_query, mongo_options) :: [map()]
    defp query(collection_name, query, options) do
        mongo_cursor = Mongo.find(MongoEcto, collection_name, normalise_query_map(query), options)
        mongo_cursor
        |> Enum.to_list
    end

    # Normalise query to fit MongoDb.
    @spec normalise_query_map(mongo_query) :: mongo_query
    defp normalise_query_map(query) do
        Enum.reduce query, %{}, &normalise_query_chunk/2
    end

    # Normalise query chunk
    @spec normalise_query_chunk({atom(), any()}, mongo_query) :: mongo_query
    defp normalise_query_chunk({key, %BSON.ObjectId{} = value}, acc) do
        Map.put acc, key, value
    end
    defp normalise_query_chunk({key, %{__struct__: _} = value}, acc) do
        Map.put acc, key, value
    end
    defp normalise_query_chunk({key, value}, acc) when is_map(value) do
        Map.put acc, key, normalise_query_map(value)
    end
    defp normalise_query_chunk({key, << _ :: size(96)>> = value}, acc) do
        Map.put acc, key, to_mongo_id(value)
    end
    defp normalise_query_chunk({key, << _ :: size(192)>> = value}, acc) do
        Map.put acc, key, to_mongo_id(value)
    end
    defp normalise_query_chunk({key, value}, acc) do
        Map.put acc, key, value
    end


    # Ensure that returned map() record is the struct of the schema type.
    @spec cursor_record_to_struct(mongo_schema, map(), mongo_preload) :: mongo_record
    defp cursor_record_to_struct(schema,
        %{"_id" => %BSON.ObjectId{value: id}} = model,
        preload) do
        cursor_record_to_struct(schema, id, model, preload)
    end
    defp cursor_record_to_struct(schema, %{"_id" => id} = model, preload) do
        cursor_record_to_struct(schema, id, model, preload)
    end
    @spec cursor_record_to_struct(mongo_schema, String.t, map(), mongo_preload) :: mongo_record
    def cursor_record_to_struct(schema, id, model, preload) do
        model = Map.delete(model, "_id")
        model = for {key, val} <- model, into: %{} do
            atom_key = String.to_atom(key)
            case val do
                %BSON.ObjectId{value: binary_id} ->
                    {atom_key, mongo_id_to_string(binary_id)}
                %BSON.DateTime{utc: ts} ->
                    case schema.__schema__(:type, atom_key) do
                        Ecto.Date ->
                            {atom_key, timestamp_to_date(ts)}
                        _ ->
                            {atom_key, timestamp_to_datetime(ts)}
                    end
                _ ->
                    {atom_key, val}
            end
        end
        model = Map.put(model, :id, mongo_id_to_string(id))
        record = Kernel.struct(schema, model)
        preload(record, preload)
    end


    # Convert schema types to specific mongo type
    @spec get_foreign_keys(mongo_record) :: [atom()]
    defp get_foreign_keys(%{__struct__: schema}) do
        schema.__schema__(:associations)|> Enum.reduce([], fn(assoc, acc) ->
            case schema.__schema__(:association, assoc) do
                %Ecto.Association.BelongsTo{owner_key: key} -> [key|acc]
                _ -> acc
            end
        end)
    end


    # Convert schema types to specific mongo types
    @spec convert_types_with(mongo_schema, fun()) :: mongo_schema
    defp convert_types_with(record, f) do
        record |> Map.new(fn({attr, val}) -> {attr, f.(val)} end)
    end


    # Convert specified types in schema to specific mongo types
    @spec convert_types_for(mongo_schema, [atom()], fun()) :: mongo_schema
    defp convert_types_for(record, fields, f) do
        Enum.reduce fields, record, fn(field, acc) ->
            Map.update!(acc, field, f)
        end
    end


    # Convert type to specific mongo type
    @spec to_mongo_type(any()) :: mongo_datatype
    defp to_mongo_type(%Ecto.Date{} = dt), do: ecto_date_to_mongo(dt)
    defp to_mongo_type(%Ecto.DateTime{} = dt), do: ecto_datetime_to_mongo(dt)
    defp to_mongo_type(type), do: type


    # Convert integer timestamp to %Ecto.Datetime{}
    @spec timestamp_to_datetime(integer()) :: %Ecto.DateTime{}
    defp timestamp_to_datetime(timestamp) do
        epoch = :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
        datetime = :calendar.gregorian_seconds_to_datetime(epoch + div(timestamp, 1000))
        usec = rem(timestamp, 1000) * 1000
        %{Ecto.DateTime.from_erl(datetime) | usec: usec}
    end


    # Convert integer timestamp to %Ecto.Date{}
    @spec timestamp_to_date(integer()) :: %Ecto.Date{}
    defp timestamp_to_date(timestamp) do
        epoch = :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
        {date, _} = :calendar.gregorian_seconds_to_datetime(epoch + div(timestamp, 1000))
        Ecto.Date.from_erl(date)
    end

    # Convert %Ecto.Date{} to BSON.DateTime object
    @spec ecto_date_to_mongo(%Ecto.Date{}) :: %BSON.DateTime{}
    defp ecto_date_to_mongo(ecto_timestamp = %Ecto.Date{}) do
        {:ok, date} = Ecto.Date.dump(ecto_timestamp)
        BSON.DateTime.from_datetime({date, {0, 0, 0, 0}})
    end

    # Convert %Ecto.Datetime{} to BSON.DateTime object
    @spec ecto_datetime_to_mongo(%Ecto.DateTime{}) :: %BSON.DateTime{}
    defp ecto_datetime_to_mongo(ecto_timestamp = %Ecto.DateTime{}) do
        {:ok, datetime} = Ecto.DateTime.dump(ecto_timestamp)
        BSON.DateTime.from_datetime(datetime)
    end
end
