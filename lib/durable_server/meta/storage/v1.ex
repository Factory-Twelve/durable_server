defmodule DurableServer.Meta.Storage.V1 do
  @moduledoc false

  alias DurableServer.Meta
  alias DurableServer.Meta.{ExternalAtom, ExternalIdentity}
  alias DurableServer.SafeETF
  alias DurableServer.SafeETF.{UnresolvedAtom, UnresolvedIdentity}

  @version 1
  @field_contract [
    vsn: :scalar,
    module: :nullable_atom,
    permanent: :scalar,
    pid: {:identity, :pid},
    status: :scalar,
    sticky_placement: :scalar,
    sticky_placement_history: :scalar,
    supervisor: :nullable_atom,
    task_supervisor: :nullable_atom,
    dynamic_supervisor: :nullable_atom,
    node_ref: :scalar,
    node_str: :scalar,
    last_heartbeat_at: :scalar,
    crash_history: :scalar,
    restart_attempt_node: :scalar,
    restart_attempt_time: :scalar,
    restart_attempt_ttl: :scalar,
    init_from_ref: {:identity, :reference},
    init_from_pid: {:identity, :pid}
  ]
  @fields Keyword.keys(@field_contract)
  @field_type_by_name Map.new(@field_contract)
  @field_by_name Map.new(@fields, &{Atom.to_string(&1), &1})
  @field_names Map.keys(@field_by_name)
  @field_policies @field_contract
                  |> Enum.flat_map(fn
                    {field, :nullable_atom} -> [{Atom.to_string(field), :nullable_atom}]
                    {field, {:identity, kind}} -> [{Atom.to_string(field), kind}]
                    {_field, :scalar} -> []
                  end)
                  |> Map.new()

  @external_atom_marker "$durable_external_atom"
  @external_identity_marker "$durable_external_identity"

  def load_binary(binary) when is_binary(binary) do
    with {:ok, named_fields} <- SafeETF.decode_map(binary, @field_policies) do
      {:ok, load_named!(named_fields)}
    end
  end

  def load_term(term) when is_map(term) do
    named_fields =
      Enum.reduce(term, %{}, fn {key, value}, fields ->
        name = storage_key_name!(key)

        if Map.has_key?(fields, name) do
          raise ArgumentError, "duplicate metadata storage key: #{inspect(name)}"
        end

        Map.put(fields, name, value)
      end)

    load_named!(named_fields)
  end

  def load_term(_term), do: raise(ArgumentError, "invalid meta storage term")

  def dump(%Meta{vsn: @version} = meta) do
    meta
    |> Map.from_struct()
    |> Map.take(@fields)
    |> Map.new(fn {field, value} -> {field, dump_field(field, value)} end)
  end

  def dump(%Meta{vsn: version}) do
    raise ArgumentError, "unsupported metadata schema version: #{inspect(version)}"
  end

  def fields, do: @fields

  defp storage_key_name!(key) when is_atom(key), do: Atom.to_string(key)
  defp storage_key_name!(key) when is_binary(key), do: key

  defp storage_key_name!(key),
    do: raise(ArgumentError, "invalid metadata storage key: #{inspect(key)}")

  defp load_named!(named_fields) do
    version = Map.get(named_fields, "vsn", :missing)

    if version != @version do
      raise ArgumentError, "unsupported metadata schema version: #{inspect(version)}"
    end

    unknown_fields = Map.keys(named_fields) -- @field_names

    if unknown_fields != [] do
      raise ArgumentError,
            "metadata schema version 1 contains unknown keys: #{inspect(unknown_fields)}"
    end

    Map.new(named_fields, fn {name, value} ->
      field = Map.fetch!(@field_by_name, name)
      field_type = Map.fetch!(@field_type_by_name, field)
      {field, load_field(field, field_type, value)}
    end)
  end

  defp load_field(field, {:identity, expected_kind}, %UnresolvedIdentity{
         kind: kind,
         node: node,
         etf: etf
       }) do
    if kind == expected_kind do
      ExternalIdentity.new(kind, node, etf)
    else
      raise ArgumentError, "invalid opaque identity kind for #{inspect(field)}"
    end
  end

  defp load_field(_field, :nullable_atom, %UnresolvedAtom{name: name}),
    do: ExternalAtom.new(name)

  defp load_field(
         field,
         {:identity, expected_kind},
         %{@external_identity_marker => kind, "node" => node, "etf" => etf} = identity
       )
       when map_size(identity) == 3 do
    if kind == Atom.to_string(expected_kind) do
      ExternalIdentity.new(expected_kind, node, etf)
    else
      raise ArgumentError, "invalid opaque identity kind for #{inspect(field)}"
    end
  end

  defp load_field(
         _field,
         :nullable_atom,
         %{@external_atom_marker => name} = atom
       )
       when map_size(atom) == 1,
       do: ExternalAtom.new(name)

  defp load_field(_field, _field_type, value), do: value

  defp dump_field(field, %ExternalIdentity{kind: kind, node: node, etf: etf}) do
    {:identity, ^kind} = Map.fetch!(@field_type_by_name, field)
    %{@external_identity_marker => Atom.to_string(kind), "node" => node, "etf" => etf}
  end

  defp dump_field(field, %ExternalAtom{name: name}) do
    :nullable_atom = Map.fetch!(@field_type_by_name, field)
    %{@external_atom_marker => name}
  end

  defp dump_field(_field, value), do: value
end
