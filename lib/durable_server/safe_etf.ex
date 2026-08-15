defmodule DurableServer.SafeETF.UnresolvedIdentity do
  @moduledoc false

  @enforce_keys [:kind, :node, :etf]
  defstruct [:kind, :node, :etf]
end

defmodule DurableServer.SafeETF.UnresolvedAtom do
  @moduledoc false

  @enforce_keys [:name]
  defstruct [:name]
end

defmodule DurableServer.SafeETF do
  @moduledoc false

  alias DurableServer.SafeETF.{UnresolvedAtom, UnresolvedIdentity}

  @max_collection_items 4_096
  @max_nesting_depth 32
  @max_reference_id_words 5
  @max_atom_bytes 255
  @max_utf8_atom_bytes 1_020
  @max_term_bytes 65_536
  @max_legacy_reference_first_id 0x0003FFFF
  @max_legacy_creation 3

  # External Term Format tags accepted by the metadata storage schema.
  @version_magic 131
  @new_pid_ext 88
  @newer_reference_ext 90
  @small_integer_ext 97
  @integer_ext 98
  @atom_ext 100
  @reference_ext 101
  @pid_ext 103
  @nil_ext 106
  @list_ext 108
  @binary_ext 109
  @small_big_ext 110
  @large_big_ext 111
  @new_reference_ext 114
  @small_atom_ext 115
  @map_ext 116
  @atom_utf8_ext 118
  @small_atom_utf8_ext 119

  @pid_tags [@pid_ext, @new_pid_ext]
  @reference_tags [@reference_ext, @new_reference_ext, @newer_reference_ext]

  def valid_atom_name?(name) when is_binary(name) do
    String.valid?(name) and byte_size(name) <= @max_utf8_atom_bytes and
      length(String.codepoints(name)) <= 255
  end

  def valid_atom_name?(_name), do: false

  @doc """
  Decodes a bounded ETF map without creating atoms.

  Keys are returned as their binary atom names. Field policies may preserve an
  unknown-node PID/reference or unknown atom as an explicit unresolved value;
  all other values must decode with `binary_to_term/2` in safe mode.
  """
  def decode_map(
        <<@version_magic, @map_ext, count::unsigned-big-32, rest::binary>>,
        field_policies
      )
      when count <= @max_collection_items and is_map(field_policies) do
    with {:ok, decoded, <<>>} <- decode_entries(rest, count, field_policies, %{}) do
      {:ok, decoded}
    else
      _error -> {:error, "malformed external term"}
    end
  end

  def decode_map(_binary, _field_policies), do: {:error, "malformed external term"}

  def external_identity_node(<<@version_magic, tag, rest::binary>> = binary, kind) do
    with true <- tag in identity_tags(kind),
         true <- complete_term?(binary),
         {:ok, node_name} <- identity_node_atom(rest, tag) do
      {:ok, node_name}
    else
      _error -> :error
    end
  end

  def external_identity_node(_binary, _kind), do: :error

  def decode_external_identity(binary, kind) when is_binary(binary) do
    with {:ok, _node_name} <- external_identity_node(binary, kind),
         {identity, used} when used == byte_size(binary) <-
           :erlang.binary_to_term(binary, [:safe, :used]) do
      {:ok, identity}
    else
      _error -> :error
    end
  rescue
    _error -> :error
  end

  defp decode_entries(rest, 0, _field_policies, decoded), do: {:ok, decoded, rest}

  defp decode_entries(binary, count, field_policies, decoded) do
    with {:ok, key_name, rest} <- take_atom(binary),
         false <- Map.has_key?(decoded, key_name),
         {:ok, value, rest} <- take_term(rest),
         {:ok, decoded_value} <- decode_value(value, Map.get(field_policies, key_name, :safe)) do
      decode_entries(rest, count - 1, field_policies, Map.put(decoded, key_name, decoded_value))
    end
  end

  defp decode_value(value, :pid), do: decode_identity(value, :pid, @pid_tags)
  defp decode_value(value, :reference), do: decode_identity(value, :reference, @reference_tags)

  defp decode_value(value, :nullable_atom) do
    case decode_existing(value) do
      {:ok, decoded} ->
        {:ok, decoded}

      :error ->
        with {:ok, name, <<>>} <- take_atom(value),
             true <- unknown_existing_atom?(name) do
          {:ok, %UnresolvedAtom{name: name}}
        else
          _error -> :error
        end
    end
  end

  defp decode_value(value, :safe), do: decode_existing(value)

  defp decode_identity(value, kind, tags) do
    case decode_existing(value) do
      {:ok, decoded} ->
        {:ok, decoded}

      :error ->
        with <<tag, rest::binary>> <- value,
             true <- tag in tags,
             {:ok, node_name} <- identity_node_atom(rest, tag),
             true <- unknown_existing_atom?(node_name) do
          {:ok,
           %UnresolvedIdentity{
             kind: kind,
             node: node_name,
             etf: <<@version_magic, value::binary>>
           }}
        else
          _error -> :error
        end
    end
  end

  defp decode_existing(term) do
    binary = <<@version_magic, term::binary>>

    case :erlang.binary_to_term(binary, [:safe, :used]) do
      {decoded, used} when used == byte_size(binary) -> {:ok, decoded}
      _other -> :error
    end
  rescue
    _error -> :error
  end

  defp identity_tags(:pid), do: @pid_tags
  defp identity_tags(:reference), do: @reference_tags
  defp identity_tags(_kind), do: []

  defp complete_term?(<<@version_magic, term::binary>>) do
    match?({:ok, <<>>}, skip_term(term, 0))
  end

  defp identity_node_atom(<<_id_words::unsigned-big-16, rest::binary>>, tag)
       when tag in [@new_reference_ext, @newer_reference_ext],
       do: atom_name(rest)

  defp identity_node_atom(rest, _tag), do: atom_name(rest)

  defp atom_name(binary) do
    case take_atom(binary) do
      {:ok, name, _rest} -> {:ok, name}
      :error -> :error
    end
  end

  defp unknown_existing_atom?(name) do
    _atom = String.to_existing_atom(name)
    false
  rescue
    ArgumentError -> true
  end

  defp take_term(binary) do
    with {:ok, rest} <- skip_term(binary, 0) do
      consumed = byte_size(binary) - byte_size(rest)
      <<term::binary-size(consumed), ^rest::binary>> = binary
      {:ok, term, rest}
    end
  end

  defp skip_term(_binary, depth) when depth > @max_nesting_depth, do: :error
  defp skip_term(<<@small_integer_ext, _value, rest::binary>>, _depth), do: {:ok, rest}

  defp skip_term(<<@integer_ext, _value::signed-big-32, rest::binary>>, _depth),
    do: {:ok, rest}

  defp skip_term(<<@nil_ext, rest::binary>>, _depth), do: {:ok, rest}

  defp skip_term(<<@binary_ext, size::unsigned-big-32, rest::binary>>, _depth)
       when size <= @max_term_bytes,
       do: drop_bytes(rest, size)

  defp skip_term(<<@small_big_ext, size, _sign, rest::binary>>, _depth),
    do: drop_bytes(rest, size)

  defp skip_term(<<@large_big_ext, size::unsigned-big-32, _sign, rest::binary>>, _depth)
       when size <= @max_term_bytes,
       do: drop_bytes(rest, size)

  defp skip_term(<<@list_ext, count::unsigned-big-32, rest::binary>>, depth)
       when count <= @max_collection_items do
    with {:ok, rest} <- skip_terms(rest, count, depth + 1) do
      skip_term(rest, depth + 1)
    end
  end

  defp skip_term(<<@map_ext, count::unsigned-big-32, rest::binary>>, depth)
       when count <= @max_collection_items,
       do: skip_terms(rest, count * 2, depth + 1)

  defp skip_term(<<@pid_ext, rest::binary>>, _depth), do: skip_legacy_pid(rest)
  defp skip_term(<<@new_pid_ext, rest::binary>>, _depth), do: skip_new_pid(rest)
  defp skip_term(<<@reference_ext, rest::binary>>, _depth), do: skip_legacy_reference(rest)

  defp skip_term(<<@new_reference_ext, id_words::unsigned-big-16, rest::binary>>, _depth)
       when id_words > 0 and id_words <= @max_reference_id_words,
       do: skip_new_reference(rest, id_words)

  defp skip_term(<<@newer_reference_ext, id_words::unsigned-big-16, rest::binary>>, _depth)
       when id_words <= @max_reference_id_words,
       do: skip_newer_reference(rest, id_words)

  defp skip_term(binary, _depth), do: skip_atom(binary)

  defp skip_terms(rest, 0, _depth), do: {:ok, rest}

  defp skip_terms(binary, count, depth) do
    with {:ok, rest} <- skip_term(binary, depth) do
      skip_terms(rest, count - 1, depth)
    end
  end

  defp skip_legacy_pid(binary) do
    with {:ok, rest} <- skip_atom(binary),
         <<_id::unsigned-big-32, _serial::unsigned-big-32, creation, tail::binary>> <- rest,
         true <- creation <= @max_legacy_creation do
      {:ok, tail}
    end
  end

  defp skip_new_pid(binary) do
    with {:ok, rest} <- skip_atom(binary),
         <<_id::unsigned-big-32, _serial::unsigned-big-32, _creation::unsigned-big-32,
           tail::binary>> <- rest do
      {:ok, tail}
    end
  end

  defp skip_legacy_reference(binary) do
    with {:ok, rest} <- skip_atom(binary),
         <<id::unsigned-big-32, creation, tail::binary>> <- rest,
         true <- id <= @max_legacy_reference_first_id,
         true <- creation <= @max_legacy_creation do
      {:ok, tail}
    end
  end

  defp skip_new_reference(binary, id_words) do
    with {:ok, rest} <- skip_atom(binary),
         <<creation, first_id::unsigned-big-32, _remaining_ids::binary-size((id_words - 1) * 4),
           tail::binary>> <- rest,
         true <- creation <= @max_legacy_creation,
         true <- first_id <= @max_legacy_reference_first_id do
      {:ok, tail}
    end
  end

  defp skip_newer_reference(binary, id_words) do
    with {:ok, rest} <- skip_atom(binary),
         <<_creation::unsigned-big-32, _ids::binary-size(id_words * 4), tail::binary>> <- rest do
      {:ok, tail}
    end
  end

  defp take_atom(<<@atom_ext, size::unsigned-big-16, rest::binary>>)
       when size <= @max_atom_bytes do
    take_atom_payload(rest, size, :latin1)
  end

  defp take_atom(<<@small_atom_ext, size, rest::binary>>),
    do: take_atom_payload(rest, size, :latin1)

  defp take_atom(<<@atom_utf8_ext, size::unsigned-big-16, rest::binary>>)
       when size <= @max_utf8_atom_bytes do
    take_atom_payload(rest, size, :utf8)
  end

  defp take_atom(<<@small_atom_utf8_ext, size, rest::binary>>),
    do: take_atom_payload(rest, size, :utf8)

  defp take_atom(_binary), do: :error

  defp take_atom_payload(rest, size, encoding) when byte_size(rest) >= size do
    <<encoded_name::binary-size(size), tail::binary>> = rest

    with {:ok, name} <- normalize_atom_name(encoded_name, encoding) do
      {:ok, name, tail}
    end
  end

  defp take_atom_payload(_rest, _size, _encoding), do: :error

  defp normalize_atom_name(name, :utf8) do
    if valid_atom_name?(name), do: {:ok, name}, else: :error
  end

  defp normalize_atom_name(name, :latin1) do
    normalized = :unicode.characters_to_binary(name, :latin1, :utf8)
    if valid_atom_name?(normalized), do: {:ok, normalized}, else: :error
  rescue
    _error -> :error
  end

  defp skip_atom(binary) do
    case take_atom(binary) do
      {:ok, _name, rest} -> {:ok, rest}
      :error -> :error
    end
  end

  defp drop_bytes(binary, size) when byte_size(binary) >= size do
    <<_bytes::binary-size(size), rest::binary>> = binary
    {:ok, rest}
  end

  defp drop_bytes(_binary, _size), do: :error
end
