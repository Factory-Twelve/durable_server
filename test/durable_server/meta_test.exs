defmodule DurableServer.MetaTest do
  use ExUnit.Case, async: false

  alias DurableServer.Meta
  alias DurableServer.Meta.{ExternalAtom, ExternalIdentity}
  alias DurableServer.Meta.Storage.V1
  alias DurableServer.StoredState

  @context %{key: "server-key", prefix: "tenant/"}
  @max_encoded_bytes 87_384
  @max_binary_bytes 65_536

  test "round-trips valid metadata through the hardened decoder" do
    placement = [%{env_var: "FLY_REGION", value: "yyz"}, %{env_var: :any, value: :any}]

    meta = %Meta{
      module: __MODULE__,
      permanent: true,
      pid: self(),
      status: :running,
      supervisor: :meta_test_supervisor,
      task_supervisor: :meta_test_tasks,
      dynamic_supervisor: :meta_test_dynamic,
      node_ref: 123,
      node_str: to_string(node()),
      last_heartbeat_at: 456,
      sticky_placement: placement,
      sticky_placement_history: [%{at: 450, placement: placement}],
      crash_history: [
        %{timestamp: 400, reason: "boom", node_ref: 123},
        %{timestamp: 300, reason: "older crash"}
      ],
      restart_attempt_node: "node@example",
      restart_attempt_time: 460,
      restart_attempt_ttl: 560,
      init_from_ref: make_ref(),
      init_from_pid: self()
    }

    decoded = meta |> Meta.encode_to_binary() |> Meta.decode_from_binary(@context)

    assert decoded == %{meta | key: @context.key, prefix: @context.prefix}
  end

  test "V1 declares every persisted metadata field exactly once" do
    expected_fields =
      %Meta{}
      |> Map.from_struct()
      |> Map.keys()
      |> Kernel.--([:key, :prefix])
      |> Enum.sort()

    assert Enum.sort(V1.fields()) == expected_fields
    assert length(V1.fields()) == length(Enum.uniq(V1.fields()))
  end

  test "opaque modules resolve only through loaded application manifests" do
    assert ExternalAtom.resolve_module(%ExternalAtom{name: "Elixir.DurableServer"}) ==
             DurableServer

    assert ExternalAtom.resolve_module(%ExternalAtom{name: "not_a_release_module"}) == nil

    assert %Meta{module: DurableServer} =
             Meta.resolve_module(%Meta{module: %ExternalAtom{name: "Elixir.DurableServer"}})
  end

  test "producer rejects invalid shape and oversized external terms" do
    assert_raise ArgumentError, ~r/invalid metadata field :crash_history/, fn ->
      Meta.encode_to_binary(%Meta{status: :running, crash_history: [:invalid]})
    end

    assert_raise ArgumentError, ~r/invalid metadata field :crash_history/, fn ->
      Meta.to_storage_term(%Meta{status: :running, crash_history: [:invalid]})
    end

    assert_raise ArgumentError, ~r/metadata external term exceeds 65536 byte limit/, fn ->
      Meta.encode_to_binary(%Meta{
        status: :stopped_graceful,
        node_str: :binary.copy("x", @max_binary_bytes)
      })
    end

    invalid_external_identity = %ExternalIdentity{
      kind: :pid,
      node: "wrong@node",
      etf: :erlang.term_to_binary(self())
    }

    assert_raise ArgumentError, ~r/invalid metadata field :pid/, fn ->
      Meta.encode_to_binary(%Meta{status: :running, pid: invalid_external_identity})
    end

    assert_raise ArgumentError, ~r/invalid metadata field :module/, fn ->
      Meta.encode_to_binary(%Meta{
        status: :running,
        module: %ExternalAtom{name: :binary.copy("a", 256)}
      })
    end
  end

  test "rejects duplicate normalized storage keys" do
    assert_raise ArgumentError, ~r/duplicate metadata storage key/, fn ->
      Meta.from_storage_term(%{:status => :running, "status" => :crashed}, @context)
    end
  end

  test "rejects encoded and decoded size limit violations" do
    assert_invalid_metadata(:binary.copy("A", @max_encoded_bytes + 1), ~r/87384 byte limit/)

    oversized_binary = <<131>> <> :binary.copy(<<0>>, @max_binary_bytes)
    assert byte_size(oversized_binary) == @max_binary_bytes + 1
    assert_invalid_metadata(Base.encode64(oversized_binary), ~r/65536 byte limit/)
  end

  test "rejects invalid Base64, compressed, malformed, and trailing external terms" do
    valid = :erlang.term_to_binary(%{status: :running})

    for encoded <- [
          "not base64!",
          Base.encode64(<<0>>),
          Base.encode64(<<131, 80, 0, 0, 0, 1, 0>>),
          Base.encode64(<<131, 255>>),
          metadata_with_raw_value(
            "module",
            <<118, 256::unsigned-big-16, :binary.copy("a", 256)::binary>>
          )
          |> Base.encode64(),
          Base.encode64(valid <> <<0>>)
        ] do
      assert_invalid_metadata(encoded)
    end
  end

  test "rejects non-map, missing status, and unknown metadata keys" do
    for term <- [
          [:not, :a, :map],
          %{module: __MODULE__},
          %{status: :running, future_field: "unrecognized"},
          %{status: :running, key: "runtime-only"},
          %{status: :running, prefix: "runtime-only/"}
        ] do
      assert_invalid_term(term)
    end
  end

  test "rejects unsupported metadata schema versions" do
    missing_version = :erlang.term_to_binary(%{status: :stopped_graceful})

    assert_invalid_metadata(
      Base.encode64(missing_version),
      ~r/unsupported metadata schema version: :missing/
    )

    for version <- [0, 2] do
      assert_invalid_term(
        %{status: :running, vsn: version},
        ~r/unsupported metadata schema version: #{version}/
      )
    end

    assert {:error, %ArgumentError{message: message}} =
             StoredState.from_storage_term(%StoredState{
               vsn: 1,
               state: %{},
               meta: %Meta{vsn: 2, status: :running}
             })

    assert message =~ "invalid metadata field :vsn"
  end

  test "rejects every invalid scalar field shape" do
    invalid_fields = [
      module: "not-an-atom",
      permanent: :yes,
      pid: :not_a_pid,
      pid: <<131, 97, 1>>,
      status: :unknown_status,
      key: 1,
      prefix: 1,
      supervisor: "not-an-atom",
      task_supervisor: "not-an-atom",
      dynamic_supervisor: "not-an-atom",
      node_ref: [],
      node_str: :not_a_binary,
      last_heartbeat_at: "now",
      restart_attempt_node: :not_a_binary,
      restart_attempt_time: "now",
      restart_attempt_ttl: "later",
      init_from_ref: :not_a_reference,
      init_from_ref: <<131, 97, 1>>,
      init_from_pid: :not_a_pid
    ]

    for {field, value} <- invalid_fields do
      assert_invalid_term(Map.put(%{status: :running}, field, value), ~r/#{field}/)
    end
  end

  test "rejects malformed placement and placement-history entries" do
    invalid_fields = [
      sticky_placement: %{},
      sticky_placement: [%{env_var: "FLY_REGION"}],
      sticky_placement: [%{env_var: "FLY_REGION", value: "yyz", extra: true}],
      sticky_placement: [%{env_var: :region, value: "yyz"}],
      sticky_placement: [%{env_var: "FLY_REGION", value: 1}],
      sticky_placement_history: [%{at: "now", placement: nil}],
      sticky_placement_history: [%{at: 1}],
      sticky_placement_history: [%{at: 1, placement: nil, extra: true}],
      sticky_placement_history: [%{at: 1, placement: [%{env_var: "X"}]}]
    ]

    for {field, value} <- invalid_fields do
      assert_invalid_term(Map.put(%{status: :running}, field, value), ~r/#{field}/)
    end
  end

  test "rejects malformed crash-history entries" do
    invalid_histories = [
      [:not_a_map],
      [%{timestamp: 1}],
      [%{timestamp: 1, reason: "boom", extra: true}],
      [%{timestamp: "now", reason: "boom"}],
      [%{timestamp: 1, reason: :boom}],
      [%{timestamp: 1, reason: "boom", node_ref: []}]
    ]

    for crash_history <- invalid_histories do
      assert_invalid_term(%{status: :running, crash_history: crash_history}, ~r/crash_history/)
    end
  end

  test "rejects improper metadata lists with controlled field errors" do
    valid_placement = %{env_var: "FLY_REGION", value: "yyz"}
    valid_placement_history = %{at: 1, placement: [valid_placement]}
    valid_crash = %{timestamp: 1, reason: "boom"}

    for {field, value} <- [
          sticky_placement: [valid_placement | :improper_tail],
          sticky_placement_history: [valid_placement_history | :improper_tail],
          crash_history: [valid_crash | :improper_tail]
        ] do
      assert_invalid_term(Map.put(%{status: :running}, field, value), ~r/#{field}/)
    end
  end

  test "rejects executable external terms with a controlled error" do
    executable = :erlang.term_to_binary(%{status: :running, module: fn -> :unsafe end})
    term = %{"vsn" => 1, "state" => %{}, "meta" => Base.encode64(executable)}

    assert {:error, %ArgumentError{}} = StoredState.from_object_store_term(term)
  end

  test "malformed opaque identity wrappers return controlled envelope errors" do
    malformed_identity = %{
      "$durable_external_identity" => "pid",
      "node" => 123,
      "etf" => :erlang.term_to_binary(self())
    }

    encoded_meta =
      %{vsn: 1, status: :stopped_graceful, pid: malformed_identity}
      |> :erlang.term_to_binary()
      |> Base.encode64()

    envelope = %{
      "$durable_server_stored_state" => 1,
      "vsn" => 1,
      "state" => %{},
      "meta" => encoded_meta
    }

    assert {:error, %ArgumentError{message: message}} =
             StoredState.from_storage_term(envelope)

    assert message =~ "invalid persisted metadata"
  end

  test "rejects incomplete or mismatched lock ownership" do
    valid_owner = %{
      vsn: 1,
      status: :running,
      pid: self(),
      node_str: to_string(node()),
      node_ref: 1
    }

    for invalid <- [
          %{valid_owner | pid: nil},
          %{valid_owner | node_str: nil},
          %{valid_owner | node_str: ""},
          %{valid_owner | node_str: "other@node"},
          %{valid_owner | node_ref: nil},
          %{valid_owner | node_ref: ""},
          %{valid_owner | node_ref: -1}
        ] do
      assert_invalid_term(invalid, ~r/invalid metadata lock owner/)
    end

    assert %Meta{} =
             valid_owner
             |> :erlang.term_to_binary()
             |> Base.encode64()
             |> Meta.decode_from_binary(@context)

    assert %Meta{node_ref: "legacy-ref"} =
             valid_owner
             |> Map.put(:node_ref, "legacy-ref")
             |> :erlang.term_to_binary()
             |> Base.encode64()
             |> Meta.decode_from_binary(@context)
  end

  test "preserves unknown nullable atoms explicitly without growing the atom table" do
    for field <- ["module", "supervisor", "task_supervisor", "dynamic_supervisor"] do
      atom_name =
        "durable_meta_unknown_#{field}_#{System.unique_integer([:positive, :monotonic])}"

      encoded = metadata_with_raw_value(field, small_atom(atom_name)) |> Base.encode64()
      assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end
      atom_count = :erlang.system_info(:atom_count)

      decoded = Meta.decode_from_binary(encoded, @context)

      assert Map.fetch!(decoded, String.to_existing_atom(field)) == %ExternalAtom{name: atom_name}
      assert :erlang.system_info(:atom_count) == atom_count
      assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end
    end
  end

  test "preserves the maximum UTF-8 atom name and rejects longer names" do
    max_name = String.duplicate("é", 255)
    max_atom = <<118, byte_size(max_name)::unsigned-big-16, max_name::binary>>
    encoded = metadata_with_raw_value("module", max_atom) |> Base.encode64()

    assert_raise ArgumentError, fn -> String.to_existing_atom(max_name) end

    assert %Meta{module: %ExternalAtom{name: ^max_name}} =
             Meta.decode_from_binary(encoded, @context)

    assert_raise ArgumentError, fn -> String.to_existing_atom(max_name) end

    oversized_name = String.duplicate("é", 256)
    oversized_atom = <<118, byte_size(oversized_name)::unsigned-big-16, oversized_name::binary>>
    oversized_encoded = metadata_with_raw_value("module", oversized_atom) |> Base.encode64()

    assert_invalid_metadata(oversized_encoded, ~r/malformed external term/)
  end

  test "preserves unknown-node PIDs and references opaquely without growing atoms" do
    cases = [
      {"pid", fn atom -> <<103, atom::binary, 0::unsigned-big-32, 0::unsigned-big-32, 0>> end},
      {"init_from_pid",
       fn atom -> <<103, atom::binary, 0::unsigned-big-32, 0::unsigned-big-32, 0>> end},
      {"init_from_ref", fn atom -> <<101, atom::binary, 0::unsigned-big-32, 0>> end}
    ]

    for {field, value_builder} <- cases do
      atom_name =
        "durable_meta_unknown_#{field}_#{System.unique_integer([:positive, :monotonic])}"

      atom = small_atom(atom_name)
      encoded = metadata_with_raw_value(field, value_builder.(atom)) |> Base.encode64()

      assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end
      atom_count = :erlang.system_info(:atom_count)

      decoded = Meta.decode_from_binary(encoded, @context)
      field_atom = String.to_existing_atom(field)
      external_identity = Map.fetch!(decoded, field_atom)

      assert %ExternalIdentity{node: ^atom_name, etf: opaque_etf} = external_identity
      assert :erlang.system_info(:atom_count) == atom_count
      assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end

      redecoded = decoded |> Meta.encode_to_binary() |> Meta.decode_from_binary(@context)
      assert Map.fetch!(redecoded, field_atom) == external_identity

      _node_atom = String.to_atom(atom_name)
      native_identity = :erlang.binary_to_term(opaque_etf, [:safe])
      assert Meta.identity_equal?(external_identity, native_identity)

      if field in ["pid", "init_from_pid"] do
        assert Meta.resolve_pid(external_identity) == native_identity
      end
    end
  end

  defp assert_invalid_term(term, message \\ nil) do
    term = if is_map(term), do: Map.put_new(term, :vsn, 1), else: term

    term
    |> :erlang.term_to_binary()
    |> Base.encode64()
    |> assert_invalid_metadata(message)
  end

  defp assert_invalid_metadata(encoded, message \\ nil)

  defp assert_invalid_metadata(encoded, nil) do
    assert_raise ArgumentError, fn -> Meta.decode_from_binary(encoded, @context) end
  end

  defp assert_invalid_metadata(encoded, message) do
    assert_raise ArgumentError, message, fn -> Meta.decode_from_binary(encoded, @context) end
  end

  defp metadata_with_raw_value(field, raw_value) do
    <<131, 116, 3::unsigned-big-32, small_atom("vsn")::binary, 97, 1,
      small_atom("status")::binary, small_atom("stopped_graceful")::binary,
      small_atom(field)::binary, raw_value::binary>>
  end

  defp small_atom(name) when byte_size(name) <= 255 do
    <<119, byte_size(name), name::binary>>
  end
end
