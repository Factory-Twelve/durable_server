defmodule DurableServer.Meta.Storage.ObjectStoreLegacy do
  @moduledoc false

  alias DurableServer.Meta
  alias DurableServer.Meta.{ExternalAtom, ExternalIdentity}
  alias DurableServer.Meta.Storage.V1

  @version_magic 131
  @map_ext 116
  @atom_utf8_ext 118
  @small_atom_utf8_ext 119

  # The 0.1.4 reader requires native identity and atom terms. Assemble the ETF
  # directly so opaque names never need to become atoms in the encoding VM.
  def dump_binary(%Meta{} = meta) do
    fields = V1.fields()

    entries =
      Enum.map(fields, fn field ->
        [encode_native(field), encode_field(field, Map.fetch!(meta, field))]
      end)

    IO.iodata_to_binary([<<@version_magic, @map_ext, length(fields)::unsigned-big-32>>, entries])
  end

  defp encode_field(field, %ExternalIdentity{kind: kind, etf: etf} = identity) do
    if V1.field_type(field) == {:identity, kind} and ExternalIdentity.valid?(identity) do
      strip_version(etf)
    else
      raise ArgumentError,
            "unsupported external identity in object-store metadata field #{inspect(field)}"
    end
  end

  defp encode_field(field, %ExternalAtom{name: name} = atom) do
    if V1.field_type(field) == :nullable_atom and ExternalAtom.valid?(atom) do
      encode_atom(name)
    else
      raise ArgumentError,
            "unsupported external atom in object-store metadata field #{inspect(field)}"
    end
  end

  defp encode_field(field, value) do
    reject_nested_wrapper!(value, field)
    encode_native(value)
  end

  defp encode_atom(name) when byte_size(name) <= 255,
    do: <<@small_atom_utf8_ext, byte_size(name), name::binary>>

  defp encode_atom(name),
    do: <<@atom_utf8_ext, byte_size(name)::unsigned-big-16, name::binary>>

  defp encode_native(term) do
    term
    |> :erlang.term_to_binary()
    |> strip_version()
  end

  defp strip_version(<<@version_magic, term::binary>>), do: term

  defp reject_nested_wrapper!(%type{}, field)
       when type in [ExternalAtom, ExternalIdentity] do
    raise ArgumentError,
          "unsupported nested external wrapper in object-store metadata field #{inspect(field)}"
  end

  defp reject_nested_wrapper!([], _field), do: :ok

  defp reject_nested_wrapper!([head | tail], field) do
    reject_nested_wrapper!(head, field)
    reject_nested_wrapper!(tail, field)
  end

  defp reject_nested_wrapper!(tuple, field) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> Enum.each(&reject_nested_wrapper!(&1, field))
  end

  defp reject_nested_wrapper!(map, field) when is_map(map) do
    Enum.each(map, fn {key, value} ->
      reject_nested_wrapper!(key, field)
      reject_nested_wrapper!(value, field)
    end)
  end

  defp reject_nested_wrapper!(_value, _field), do: :ok
end
