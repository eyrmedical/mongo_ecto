defmodule MongoEcto.Model do
    @moduledoc """
    Defines boilerplate to use native Mongo models.
    """

    def model do
        quote do    
            alias MongoEcto.Model.Helpers

            Module.register_attribute __MODULE__, :collection_name, accumulate: false
            @before_compile unquote(__MODULE__)

            def apply_changes(model) do
                Helpers.apply_changes(model)
            end

            def unique_constraint(model, field) do
                Helpers.unique_constraint(model, field)
            end


            @epoch :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})

            def now_timestamp do
                Helpers.now_timestamp()
            end

            def field_timestamp(changeset, field) do
                Helpers.field_timestamp(changeset, field)
            end
        end
    end

    defmacro __using__(which) when is_atom(which) do
        apply(__MODULE__, which, [])
    end

    defmacro __before_compile__(_env) do
        quote do
            def collection_name, do: @collection_name
        end
    end
end

defmodule MongoEcto.Model.Helpers do
    import Ecto.Changeset, except: [unique_constraint: 2]

    @type mongo_changeset :: MongoEcto.Repo.mongo_changeset
    @type mongo_record :: MongoEcto.Repo.mongo_record
    @type mongo_query :: MongoEcto.Repo.mongo_query


    @doc """
    apply_changes doesn't remove virtual fields from struct.
    We have to run this method manually, before persisting record.
    """
    @spec apply_changes(mongo_changeset) :: mongo_record
    def apply_changes(%{data: %{__struct__: schema}} = changeset) do
        record = Ecto.Changeset.apply_changes(changeset)
        fields = schema.__schema__(:fields)
        drop_fields = (Map.keys(record) -- (fields ++ [:__struct__])) ++ [:id]
        Map.drop record, drop_fields
    end


    @doc """
    We override the default unique_constraint, 
    because it doesn't support embedded_schema.
    """
    @spec unique_constraint(mongo_changeset, atom() | [atom()] | mongo_query) :: mongo_changeset
    def unique_constraint(%{data: %{__struct__: _schema}} = changeset, fields) when is_list(fields) do
        query = Enum.reduce fields, %{}, fn(field, acc) ->
            value = Ecto.Changeset.get_field(changeset, field)
            case value do
                nil -> acc
                value -> Map.put(acc, field, value)
            end
        end
        
        # No sense to run an empty query
        if Enum.count(query) != Enum.count(fields) do
            changeset
        else
            unique_constraint(changeset, query)
        end
    end
    def unique_constraint(%{data: %{__struct__: _schema}} = changeset, field) when is_atom(field) do
        value = Ecto.Changeset.get_field(changeset, field)
        case value do
            nil -> changeset
            value ->
                query = Map.put(%{}, field, value)
                unique_constraint(changeset, query)
        end
    end
    def unique_constraint(%{data: %{__struct__: schema}} = changeset, query) do
        result = MongoEcto.Repo.get_by(schema, query)
        case result do
            nil -> changeset
            _ ->
                fields = Map.keys(query)
                Enum.reduce fields, changeset, fn(field, changeset) ->
                    Ecto.Changeset.add_error(changeset, field, "unique constraint has failed")
                end
        end
    end


    @epoch :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})

    @doc """
    Get current timestamp in BSON format suitable for MongoDb.
    """
    @spec now_timestamp :: %BSON.DateTime{}
    def now_timestamp do
        greg_secs = :calendar.datetime_to_gregorian_seconds(:calendar.universal_time())
        %BSON.DateTime{utc: (greg_secs - @epoch) * 1000}
    end

    @doc """
    Add current timestamp in some field.
    """
    @spec field_timestamp(mongo_changeset | mongo_record, atom()) :: mongo_changeset
    def field_timestamp(changeset, field) do
        change(changeset, Map.put(%{}, field, now_timestamp()))
    end
end
