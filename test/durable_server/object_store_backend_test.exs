defmodule DurableServer.ObjectStoreBackendTest do
  use ExUnit.Case, async: true

  alias DurableServer.Backends.ObjectStore, as: ObjectStoreBackend
  alias DurableServer.Meta.{ExternalAtom, ExternalIdentity}
  alias DurableServer.Meta.Storage.V1
  alias DurableServer.{Meta, ObjectStore, StorageBackend, StoredState}

  test "adopts an exact same-owner write after an ambiguous conditional conflict" do
    attempted = stored_state(%{value: 2})
    store = object_store_with_conflict_then_get(attempted)
    backend = StorageBackend.new(ObjectStoreBackend, store)

    assert {:ok, %{body: ^attempted, etag: "committed-etag"}} =
             StorageBackend.put_object(backend, "server/one", attempted,
               etag: "old-etag",
               max_retries: 0
             )
  end

  test "does not adopt a different body owned by the same boot" do
    attempted = stored_state(%{value: 2})
    persisted = stored_state(%{value: 1})
    store = object_store_with_conflict_then_get(persisted)
    backend = StorageBackend.new(ObjectStoreBackend, store)

    assert {:error, :conflict} =
             StorageBackend.put_object(backend, "server/one", attempted,
               etag: "old-etag",
               max_retries: 0
             )
  end

  test "does not adopt an exact body without a complete boot owner" do
    attempted =
      stored_state(%{value: 2}, %{
        status: :stopped_graceful,
        pid: nil,
        node_ref: nil,
        node_str: nil
      })

    store = object_store_with_conflict_then_get(attempted)
    backend = StorageBackend.new(ObjectStoreBackend, store)

    assert {:error, :conflict} =
             StorageBackend.put_object(backend, "server/one", attempted,
               etag: "old-etag",
               max_retries: 0
             )
  end

  test "classifies malformed near-envelopes as persisted corruption" do
    backend = StorageBackend.new(ObjectStoreBackend, %ObjectStore{})

    malformed = %{
      "$durable_server_stored_state" => 1,
      "vsn" => 1,
      "state" => %{},
      "meta" => "invalid",
      "extra" => true
    }

    assert {:error, {:invalid_persisted_state, %ArgumentError{message: message}}} =
             StorageBackend.decode(backend, JSON.encode!(malformed))

    assert message == "malformed stored-state envelope"
  end

  test "preserves user maps that contain legacy envelope keys plus application data" do
    backend = StorageBackend.new(ObjectStoreBackend, %ObjectStore{})

    user_state = %{
      "vsn" => 7,
      "state" => %{"nested" => true},
      "meta" => %{"source" => "application"},
      "extra" => true
    }

    assert {:ok, ^user_state} = StorageBackend.decode(backend, JSON.encode!(user_state))
  end

  test "writes the exact DurableServer 0.1.4 object-store envelope" do
    stored = stored_state(%{"value" => 2})
    encoded_meta = Meta.encode_to_object_store_binary(stored.meta)

    assert StoredState.to_object_store_term(stored) == %{
             "vsn" => 1,
             "state" => %{"value" => 2},
             "meta" => encoded_meta
           }

    backend = StorageBackend.new(ObjectStoreBackend, %ObjectStore{})
    assert {:ok, encoded} = StorageBackend.encode(backend, stored)

    assert JSON.decode!(encoded) == %{
             "vsn" => 1,
             "state" => %{"value" => 2},
             "meta" => encoded_meta
           }

    legacy_meta = encoded_meta |> Base.decode64!() |> :erlang.binary_to_term()

    assert legacy_meta ==
             stored.meta
             |> Map.from_struct()
             |> Map.drop([:key, :prefix])

    assert Enum.sort(Map.keys(legacy_meta)) == Enum.sort(V1.fields())

    assert {:ok, %StoredState{vsn: 1, state: %{"value" => 2}, meta: decoded_meta}} =
             StorageBackend.decode(backend, encoded)

    assert decoded_meta == stored.meta
  end

  test "rewrites opaque metadata as native ETF for old object-store readers" do
    atom_prefix = "object_store_#{System.unique_integer([:positive, :monotonic])}_"

    atom_name =
      atom_prefix <>
        String.duplicate("\u0301", 255 - length(String.codepoints(atom_prefix)))

    node_name = "durable_server_object_store_legacy_foreign@compat.invalid"

    assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end
    assert_raise ArgumentError, fn -> String.to_existing_atom(node_name) end

    external_atom = ExternalAtom.new(atom_name)

    external_pid =
      ExternalIdentity.new(
        :pid,
        node_name,
        <<131, 103, small_atom(node_name)::binary, 0::unsigned-big-32, 0::unsigned-big-32, 0>>
      )

    external_reference =
      ExternalIdentity.new(
        :reference,
        node_name,
        <<131, 101, small_atom(node_name)::binary, 0::unsigned-big-32, 0>>
      )

    meta = %Meta{
      module: external_atom,
      pid: external_pid,
      status: :stopped_graceful,
      init_from_ref: external_reference,
      init_from_pid: external_pid
    }

    stored = %StoredState{vsn: 1, state: %{"value" => 2}, meta: meta}

    ekv_meta = stored |> StoredState.to_storage_term() |> Map.fetch!("meta")
    ekv_term = ekv_meta |> Base.decode64!() |> :erlang.binary_to_term([:safe])

    assert ekv_term.module == %{"$durable_external_atom" => atom_name}

    assert ekv_term.pid == %{
             "$durable_external_identity" => "pid",
             "node" => node_name,
             "etf" => external_pid.etf
           }

    backend = StorageBackend.new(ObjectStoreBackend, %ObjectStore{})
    assert {:ok, encoded} = StorageBackend.encode(backend, stored)
    object_store_meta = encoded |> JSON.decode!() |> Map.fetch!("meta")

    assert %Meta{
             module: ^external_atom,
             pid: ^external_pid,
             init_from_ref: ^external_reference,
             init_from_pid: ^external_pid
           } = Meta.decode_from_binary(object_store_meta, %{key: nil, prefix: nil})

    assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end
    assert_raise ArgumentError, fn -> String.to_existing_atom(node_name) end

    legacy_term = object_store_meta |> Base.decode64!() |> :erlang.binary_to_term()
    old_reader_meta = struct!(Meta, legacy_term)

    assert Enum.sort(Map.keys(legacy_term)) == Enum.sort(V1.fields())
    assert old_reader_meta.module |> Atom.to_string() == atom_name
    assert is_pid(old_reader_meta.pid)
    assert old_reader_meta.pid |> node() |> Atom.to_string() == node_name
    assert is_reference(old_reader_meta.init_from_ref)
    assert old_reader_meta.init_from_ref |> node() |> Atom.to_string() == node_name
    assert is_pid(old_reader_meta.init_from_pid)
    refute is_map(old_reader_meta.module)
    refute is_map(old_reader_meta.pid)
    refute is_map(old_reader_meta.init_from_ref)
    refute is_map(old_reader_meta.init_from_pid)
  end

  test "object-store decoder accepts the EKV discriminator envelope" do
    stored = stored_state(%{"value" => 2})
    backend = StorageBackend.new(ObjectStoreBackend, %ObjectStore{})
    encoded = JSON.encode!(StoredState.to_storage_term(stored))

    assert {:ok, %StoredState{vsn: 1, state: %{"value" => 2}, meta: decoded_meta}} =
             StorageBackend.decode(backend, encoded)

    assert decoded_meta == stored.meta
  end

  defp stored_state(state, owner_overrides \\ %{}) do
    meta =
      struct!(
        Meta,
        Map.merge(
          %{
            module: __MODULE__,
            permanent: false,
            pid: self(),
            status: :running,
            node_ref: 123,
            node_str: to_string(node())
          },
          owner_overrides
        )
      )

    %StoredState{vsn: 1, state: state, meta: meta}
  end

  defp object_store_with_conflict_then_get(%StoredState{} = persisted) do
    {:ok, encoded} = ObjectStoreBackend.encode(%ObjectStore{}, persisted)
    responses = start_supervised!({Agent, fn -> [:conflict, {:persisted, encoded}] end})

    adapter = fn request ->
      response =
        Agent.get_and_update(responses, fn
          [:conflict | rest] ->
            {%Req.Response{status: 412}, rest}

          [{:persisted, body} | rest] ->
            {%Req.Response{
               status: 200,
               headers: %{"etag" => ["committed-etag"]},
               body: body
             }, rest}
        end)

      {request, response}
    end

    ObjectStore.new(
      bucket: "test-bucket",
      access_key_id: "test-access-key",
      secret_access_key: "test-secret-key",
      s3_endpoint: "http://s3.test",
      default_region: "us-east-1",
      req_opts: [adapter: adapter, retry: false]
    )
  end

  defp small_atom(name) when byte_size(name) <= 255 do
    <<119, byte_size(name), name::binary>>
  end
end
