defmodule DurableServer.SafeETFTest do
  use ExUnit.Case, async: false

  alias DurableServer.SafeETF
  alias DurableServer.SafeETF.{UnresolvedAtom, UnresolvedIdentity}

  test "preserves every supported unknown-node identity tag without creating atoms" do
    cases = [
      {:pid, "pid",
       fn atom ->
         <<103, atom::binary, 0::unsigned-big-32, 0::unsigned-big-32, 0>>
       end},
      {:pid, "pid",
       fn atom ->
         <<88, atom::binary, 0::unsigned-big-32, 0::unsigned-big-32, 0::unsigned-big-32>>
       end},
      {:reference, "ref",
       fn atom ->
         <<101, atom::binary, 0::unsigned-big-32, 0>>
       end},
      {:reference, "ref",
       fn atom ->
         <<114, 1::unsigned-big-16, atom::binary, 0, 0::unsigned-big-32>>
       end},
      {:reference, "ref",
       fn atom ->
         <<90, 1::unsigned-big-16, atom::binary, 0::unsigned-big-32, 0::unsigned-big-32>>
       end}
    ]

    for {kind, field, value_builder} <- cases do
      atom_name = "safe_etf_unknown_#{System.unique_integer([:positive, :monotonic])}"
      value = value_builder.(small_atom(atom_name))
      binary = map_with(field, value)

      assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end

      assert {:ok, %{^field => unresolved}} =
               SafeETF.decode_map(binary, %{field => kind})

      assert %UnresolvedIdentity{kind: ^kind, node: ^atom_name, etf: <<131, ^value::binary>>} =
               unresolved

      assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end
    end
  end

  test "preserves unknown nullable atoms explicitly" do
    atom_name = "safe_etf_unknown_atom_#{System.unique_integer([:positive, :monotonic])}"
    binary = map_with("module", small_atom(atom_name))

    assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end

    assert {:ok, %{"module" => %UnresolvedAtom{name: ^atom_name}}} =
             SafeETF.decode_map(binary, %{"module" => :nullable_atom})

    assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end
  end

  test "enforces atom tag encoding and OTP atom length semantics" do
    latin1_name = <<233>>

    assert {:ok, %{"module" => %UnresolvedAtom{name: "é"}}} =
             SafeETF.decode_map(
               map_with("module", <<115, 1, latin1_name::binary>>),
               %{"module" => :nullable_atom}
             )

    for invalid_atom <- [
          <<119, 1, 255>>,
          <<118, 512::unsigned-big-16, String.duplicate("é", 256)::binary>>
        ] do
      assert {:error, "malformed external term"} =
               SafeETF.decode_map(
                 map_with("module", invalid_atom),
                 %{"module" => :nullable_atom}
               )
    end
  end

  test "rejects duplicate top-level keys" do
    key = small_atom("status")

    binary =
      <<131, 116, 2::unsigned-big-32, key::binary, small_atom("running")::binary, key::binary,
        small_atom("crashed")::binary>>

    assert {:error, "malformed external term"} = SafeETF.decode_map(binary, %{})
  end

  test "rejects identities outside OTP semantic ranges" do
    node = small_atom("safe_etf_unknown_node_#{System.unique_integer([:positive])}")

    invalid_identities = [
      {:pid, <<103, node::binary, 0::unsigned-big-32, 0::unsigned-big-32, 4>>},
      {:reference, <<101, node::binary, 0x00040000::unsigned-big-32, 0>>},
      {:reference, <<101, node::binary, 0::unsigned-big-32, 4>>},
      {:reference,
       <<114, 6::unsigned-big-16, node::binary, 0,
         :binary.copy(<<0::unsigned-big-32>>, 6)::binary>>},
      {:reference, <<114, 1::unsigned-big-16, node::binary, 4, 0::unsigned-big-32>>},
      {:reference, <<114, 1::unsigned-big-16, node::binary, 0, 0x00040000::unsigned-big-32>>}
    ]

    for {kind, identity} <- invalid_identities do
      assert {:error, "malformed external term"} =
               SafeETF.decode_map(map_with("identity", identity), %{"identity" => kind})
    end
  end

  test "accepts full-width modern PID and newer-reference identity fields" do
    node_name = "safe_etf_modern_node_#{System.unique_integer([:positive, :monotonic])}"
    node = small_atom(node_name)

    legacy_pid =
      <<103, node::binary, 0xF0000001::unsigned-big-32, 0x80000002::unsigned-big-32, 0>>

    zero_word_reference = <<90, 0::unsigned-big-16, node::binary, 0xFFFFFFFF::unsigned-big-32>>

    identities = [
      {:pid, legacy_pid},
      {:pid,
       <<88, node::binary, 0xF0000001::unsigned-big-32, 0x80000002::unsigned-big-32,
         0xFFFFFFFF::unsigned-big-32>>},
      {:reference, zero_word_reference},
      {:reference,
       <<90, 1::unsigned-big-16, node::binary, 0xFFFFFFFF::unsigned-big-32,
         0x80000001::unsigned-big-32>>}
    ]

    for {kind, identity} <- identities do
      assert {:ok, %{"identity" => unresolved}} =
               SafeETF.decode_map(map_with("identity", identity), %{"identity" => kind})

      assert %UnresolvedIdentity{
               kind: ^kind,
               node: ^node_name,
               etf: <<131, ^identity::binary>>
             } = unresolved
    end

    assert is_pid(:erlang.binary_to_term(<<131, legacy_pid::binary>>))
    assert is_reference(:erlang.binary_to_term(<<131, zero_word_reference::binary>>))
  end

  test "counts UTF-8 atom limits by Unicode code points" do
    prefix = "safe_etf_#{System.unique_integer([:positive, :monotonic])}_"
    valid_name = prefix <> String.duplicate("\u0301", 255 - length(String.codepoints(prefix)))
    invalid_name = valid_name <> "\u0301"
    valid_atom = utf8_atom(valid_name)
    invalid_atom = utf8_atom(invalid_name)

    assert {:ok, %{"module" => %UnresolvedAtom{name: ^valid_name}}} =
             SafeETF.decode_map(map_with("module", valid_atom), %{"module" => :nullable_atom})

    valid_pid =
      <<103, valid_atom::binary, 0xFFFFFFFF::unsigned-big-32, 0::unsigned-big-32, 0>>

    assert {:ok, %{"identity" => %UnresolvedIdentity{node: ^valid_name}}} =
             SafeETF.decode_map(map_with("identity", valid_pid), %{"identity" => :pid})

    assert is_pid(:erlang.binary_to_term(<<131, valid_pid::binary>>))

    assert {:error, "malformed external term"} =
             SafeETF.decode_map(map_with("module", invalid_atom), %{
               "module" => :nullable_atom
             })

    invalid_pid = <<103, invalid_atom::binary, 0::unsigned-big-32, 0::unsigned-big-32, 0>>

    assert {:error, "malformed external term"} =
             SafeETF.decode_map(map_with("identity", invalid_pid), %{"identity" => :pid})
  end

  test "keeps current-node identities native" do
    <<131, pid_term::binary>> = :erlang.term_to_binary(self())

    assert {:ok, %{"pid" => pid}} =
             SafeETF.decode_map(map_with("pid", pid_term), %{"pid" => :pid})

    assert pid == self()
  end

  test "rejects every truncation of a valid map" do
    binary = map_with("status", small_atom("running"))

    for size <- 0..(byte_size(binary) - 1) do
      assert {:error, "malformed external term"} =
               binary
               |> binary_part(0, size)
               |> SafeETF.decode_map(%{})
    end
  end

  test "enforces recursive depth and collection count bounds" do
    assert {:ok, %{"value" => _value}} =
             SafeETF.decode_map(map_with("value", nested_list(32)), %{})

    assert {:error, "malformed external term"} =
             SafeETF.decode_map(map_with("value", nested_list(33)), %{})

    max_list =
      <<108, 4_096::unsigned-big-32, :binary.copy(<<97, 1>>, 4_096)::binary, 106>>

    assert {:ok, %{"value" => value}} = SafeETF.decode_map(map_with("value", max_list), %{})
    assert length(value) == 4_096

    oversized_list = <<108, 4_097::unsigned-big-32, 106>>

    assert {:error, "malformed external term"} =
             SafeETF.decode_map(map_with("value", oversized_list), %{})
  end

  defp map_with(field, value) do
    <<131, 116, 1::unsigned-big-32, small_atom(field)::binary, value::binary>>
  end

  defp nested_list(depth) do
    Enum.reduce(1..depth, <<106>>, fn _index, tail ->
      <<108, 1::unsigned-big-32, 97, 1, tail::binary>>
    end)
  end

  defp small_atom(name) when byte_size(name) <= 255 do
    <<119, byte_size(name), name::binary>>
  end

  defp utf8_atom(name) when byte_size(name) <= 1_020 do
    <<118, byte_size(name)::unsigned-big-16, name::binary>>
  end
end
