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
            
            def put_embed(changeset, name, value, opts \\ []) do
                Helpers.put_embed(changeset, name, value, opts)
            end

            def unique_constraint(model, field) do
                Helpers.unique_constraint(model, field)
            end

            @epoch :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})

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
            if Module.get_attribute(__MODULE__, :collection_name) do
                def collection_name, do: @collection_name
            end
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
    We override the default put_embed,
    because it doesn't map :replace actions to :delete
    """
    @spec put_embed(mongo_changeset, atom(), any(), Keyword.t) :: mongo_changeset
    def put_embed(%{data: %{__struct__: schema}} = changeset, name, value, opts) do
        changeset = Ecto.Changeset.put_embed(changeset, name, value, opts)
        case schema.__schema__(:embed, name) do
            %Ecto.Embedded{cardinality: :many, on_replace: replace_action} ->
                Map.update(changeset, :changes, %{}, fn(changes) ->
                    Map.update(changes, name, [], fn(values) ->
                        Enum.map(values, fn(value) ->
                            case value do
                                %{action: :replace} ->
                                    Map.put(value, :action, replace_action)
                                _ ->
                                    value
                            end
                        end)
                    end)
                end)
            _ ->
                changeset
        end
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
        case {result, schema.__schema__(:primary_key)} do
            {nil, _} -> changeset
            {_, []} -> changeset
            {_, primary_keys} ->
                if Map.take(changeset.data, primary_keys) == Map.take(result, primary_keys) do
                    changeset
                else
                    fields = Map.keys(query)
                    Enum.reduce fields, changeset, fn(field, changeset) ->
                        Ecto.Changeset.add_error(changeset, field, "unique constraint has failed")
                    end
                end
        end
    end


    @doc """
    Add current timestamp in some field.
    """
    @spec field_timestamp(mongo_changeset | mongo_record, atom()) :: mongo_changeset
    def field_timestamp(changeset, field) do
        change(changeset, Map.put(%{}, field, Ecto.DateTime.utc))
    end
end
