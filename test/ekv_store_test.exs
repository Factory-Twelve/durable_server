defmodule DurableServer.EKVStoreTest do
  use ExUnit.Case, async: false

  alias DurableServer.Backends.EKVStore
  alias DurableServer.{Meta, StorageBackend, StoredState}

  @table :durable_server_ekv_store_test_state
  @origin "0123456789abcdef"
  @max_timestamp 9_223_372_036_854_775_807

  defmodule FakeEKVSupervisor do
    def get_config(_name),
      do: %{cluster_size: 1, mode: :member, node_id: "0123456789abcdef"}
  end

  defmodule OversizedNodeIdEKVSupervisor do
    def get_config(_name), do: %{cluster_size: 1, node_id: :binary.copy("n", 256)}
  end

  defmodule FakeEKV do
    @table :durable_server_ekv_store_test_state
    @origin "0123456789abcdef"

    def keys(name, prefix), do: next(name, :keys, [{prefix, {1, @origin}}])
    def scan(name, prefix), do: next(name, :scan, [{prefix, "value", {1, @origin}}])
    def lookup(name, _key), do: next(name, :lookup, nil)
    def row_state(name, _key, _opts), do: next(name, :row_state, {:ok, :absent})
    def put(name, _key, _value, _opts), do: next(name, :put, {:ok, {2, @origin}})
    def update(name, _key, _fun, _opts), do: next(name, :update, {:ok, :updated, {3, @origin}})
    def get(name, _key, _opts), do: next(name, :get, :ok)
    def delete(name, _key, _opts), do: next(name, :delete, {:ok, {4, @origin}})
    def subscribe(name, _prefix), do: next(name, :subscribe, :ok)
    def unsubscribe(name, _prefix), do: next(name, :unsubscribe, :ok)

    defp next(name, op, default) do
      case :ets.lookup(@table, {name, op}) do
        [{{^name, ^op}, [step | rest]}] ->
          :ets.insert(@table, {{name, op}, rest})
          apply_step(step)

        _ ->
          default
      end
    end

    defp apply_step({:return, value}), do: value
    defp apply_step({:exit, reason}), do: exit(reason)

    defp apply_step({:raise, message}) when is_binary(message) do
      raise RuntimeError, message
    end
  end

  setup_all do
    case :ets.whereis(@table) do
      :undefined -> :ets.new(@table, [:named_table, :public, :set])
      _ -> :ok
    end

    :ok
  end

  setup do
    name = :"ekv_store_test_#{System.unique_integer([:positive, :monotonic])}"

    backend_opts = [
      name: name,
      cas_retries: 2,
      backoff: {0, 0},
      timeout: 50,
      ekv_mod: FakeEKV,
      ekv_supervisor_mod: FakeEKVSupervisor
    ]

    {:ok, backend} = StorageBackend.init_backend(EKVStore, backend_opts)

    on_exit(fn ->
      for op <- [
            :keys,
            :scan,
            :lookup,
            :row_state,
            :put,
            :update,
            :get,
            :delete,
            :subscribe,
            :unsubscribe
          ] do
        :ets.delete(@table, {name, op})
      end
    end)

    {:ok, backend: backend, name: name}
  end

  test "ensure_ready rejects node IDs that cannot fit bounded ETags" do
    {:ok, backend} =
      StorageBackend.init_backend(EKVStore,
        name: :oversized_node_id_test,
        ekv_mod: FakeEKV,
        ekv_supervisor_mod: OversizedNodeIdEKVSupervisor
      )

    assert {:error, {:ekv_node_id_exceeds_etag_limit, 255}} =
             StorageBackend.ensure_ready(backend)
  end

  test "consistent get retries transient exits and consistent read failures", %{
    backend: backend,
    name: name
  } do
    :ets.insert(
      @table,
      {{name, :get},
       [
         {:exit, :timeout},
         {:raise, "EKV: consistent read failed: :quorum_timeout"},
         {:return, :ok}
       ]}
    )

    :ets.insert(@table, {{name, :lookup}, [{:return, {"value", {11, @origin}}}]})

    assert {:ok, %{body: "value"}} = StorageBackend.get_object(backend, "key", consistent: true)
  end

  test "put_object retries transient exits on latest-update path", %{backend: backend, name: name} do
    :ets.insert(
      @table,
      {{name, :update},
       [
         {:exit, {:timeout, {GenServer, :call, []}}},
         {:return, {:ok, :ignored, {12, @origin}}}
       ]}
    )

    assert {:ok, %{body: "value"}} =
             StorageBackend.put_object(backend, "key", "value", max_retries: 1)
  end

  test "put_object retries transient exits on expected-vsn path", %{
    backend: backend,
    name: name
  } do
    etag = encode_etag({13, @origin})

    :ets.insert(
      @table,
      {{name, :put},
       [
         {:exit, {:shutdown, {:timeout, {GenServer, :call, []}}}},
         {:return, {:ok, {14, @origin}}}
       ]}
    )

    assert {:ok, %{body: "value"}} =
             StorageBackend.put_object(backend, "key", "value", etag: etag, max_retries: 1)
  end

  test "accepts timestamp boundaries and emits canonical bounded ETags", %{
    backend: backend,
    name: name
  } do
    versions = [
      {0, "1"},
      {@max_timestamp, @origin},
      {1, :binary.copy("n", 255)}
    ]

    for {timestamp, origin} = version <- versions do
      input_etag = encode_etag(version)
      :ets.insert(@table, {{name, :put}, [{:return, {:ok, {timestamp, origin}}}]})

      assert {:ok, %{etag: output_etag, body: "value"}} =
               StorageBackend.put_object(backend, "key", "value",
                 etag: input_etag,
                 max_retries: 0
               )

      assert byte_size(output_etag) <= 366
      assert output_etag == input_etag
    end
  end

  test "producer rejects EKV versions outside the contracted shape", %{
    backend: backend,
    name: name
  } do
    invalid_versions = [
      {-1, @origin},
      {@max_timestamp + 1, @origin},
      {1, :node@fake},
      {1, ""},
      {1, :binary.copy("a", 256)}
    ]

    for version <- invalid_versions do
      :ets.insert(@table, {{name, :lookup}, [{:return, {"value", version}}]})

      assert {:error, :invalid_ekv_version} = StorageBackend.get_object(backend, "key")
    end
  end

  test "list routes corrupt persisted versions through the configured error policy", %{
    backend: backend,
    name: name
  } do
    corrupt_vsn = {-1, @origin}
    :ets.insert(@table, {{name, :keys}, [{:return, [{"bad", corrupt_vsn}]}]})

    assert [] =
             backend
             |> StorageBackend.list_all_objects_stream("",
               error_handler: fn reason ->
                 send(self(), {:list_error, reason})
                 :continue
               end
             )
             |> Enum.to_list()

    assert_receive {:list_error, {:decode_failed, "bad", :invalid_ekv_version}}

    :ets.insert(
      @table,
      {{name, :scan},
       [{:return, [{"bad", "value", corrupt_vsn}, {"good", "value", {1, @origin}}]}]}
    )

    assert [%{key: "good", body: "value"}] =
             backend
             |> StorageBackend.list_all_objects_stream("",
               include_objects: true,
               error_handler: fn _reason -> :continue end
             )
             |> Enum.to_list()
  end

  test "EKV encoding rejects metadata its decoder would reject", %{backend: backend} do
    stored_state = %StoredState{
      vsn: 1,
      state: %{},
      meta: %Meta{status: :running, crash_history: [:invalid]}
    }

    assert {:error, %ArgumentError{message: message}} =
             StorageBackend.encode(backend, stored_state)

    assert message =~ "invalid metadata field :crash_history"
  end

  test "EKV keeps metadata in a bounded binary envelope", %{backend: backend} do
    stored_state = %StoredState{
      vsn: 1,
      state: %{counter: 1},
      meta: %Meta{status: :stopped_graceful}
    }

    assert {:ok, %{"meta" => encoded_meta} = encoded} =
             StorageBackend.encode(backend, stored_state)

    assert is_binary(encoded_meta)
    refute String.contains?(:erlang.term_to_binary(encoded), "Elixir.DurableServer.Meta")

    assert {:ok, %StoredState{} = decoded} = StorageBackend.decode(backend, encoded)
    assert decoded.meta.status == :stopped_graceful

    oversized_meta = Base.encode64(<<131, :binary.copy(<<0>>, 65_536)::binary>>)
    corrupted = %{"vsn" => 1, "state" => %{}, "meta" => oversized_meta}

    assert {:error, {:invalid_persisted_state, %ArgumentError{message: message}}} =
             StorageBackend.decode(backend, corrupted)

    assert message =~ "invalid persisted metadata"
  end

  test "EKV rejects the legacy native metadata envelope", %{backend: backend} do
    legacy = %{vsn: 1, state: %{}, meta: %{vsn: 1, status: :stopped_graceful}}

    assert {:error, {:invalid_persisted_state, %ArgumentError{message: message}}} =
             StorageBackend.decode(backend, legacy)

    assert message == "legacy native metadata envelope is not accepted"
  end

  test "EKV classifies malformed near-envelopes as persisted corruption", %{backend: backend} do
    malformed_terms = [
      %{
        "$durable_server_stored_state" => 1,
        "vsn" => 1,
        "state" => %{},
        "meta" => "invalid",
        "extra" => true
      },
      %{"vsn" => 1, "state" => %{}, "meta" => 123},
      %{"$durable_server_stored_state" => 2, "vsn" => 1, "state" => %{}, "meta" => ""}
    ]

    for term <- malformed_terms do
      assert {:error, {:invalid_persisted_state, %ArgumentError{message: message}}} =
               StorageBackend.decode(backend, term)

      assert message == "malformed stored-state envelope"
    end
  end

  test "rejects malformed and noncanonical ETags before put or delete CAS", %{
    backend: backend,
    name: name
  } do
    valid_binary = :erlang.term_to_binary({1, @origin})

    invalid_etags = [
      123,
      "not-base64!",
      :binary.copy("A", 367),
      encode_binary(:binary.copy(<<0>>, 275)),
      encode_binary(<<131, 80, 0, 0, 0, 1, 0>>),
      encode_binary(<<131, 255>>),
      encode_binary(valid_binary <> <<0>>),
      Base.url_encode64(valid_binary, padding: true),
      encode_etag(:not_a_tuple),
      encode_etag({1}),
      encode_etag({1, @origin, :extra}),
      encode_etag({-1, @origin}),
      encode_etag({@max_timestamp + 1, @origin}),
      encode_etag({1.0, @origin}),
      encode_etag({1, :node@fake}),
      encode_etag({1, []}),
      encode_etag({1, %{}}),
      encode_etag({1, ""}),
      encode_etag({1, :binary.copy("a", 256)}),
      encode_binary(noncanonical_etf(@origin)),
      encode_etag({1, fn -> :unsafe end})
    ]

    for etag <- invalid_etags do
      assert_rejected_before_cas(backend, name, etag)
    end
  end

  test "unknown atom ETags do not grow the atom table", %{backend: backend, name: name} do
    warmup = <<131, 104, 2, 97, 1, small_atom("node@fake")::binary>> |> encode_binary()
    assert {:error, :conflict} = StorageBackend.put_object(backend, "key", "value", etag: warmup)

    atom_name = "durable_ekv_unknown_#{System.unique_integer([:positive, :monotonic])}"
    unknown_etf = <<131, 104, 2, 97, 1, small_atom(atom_name)::binary>>
    etag = encode_binary(unknown_etf)

    assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end
    atom_count = :erlang.system_info(:atom_count)

    assert_rejected_before_cas(backend, name, etag)

    assert :erlang.system_info(:atom_count) == atom_count
    assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end
  end

  defp assert_rejected_before_cas(backend, name, etag) do
    put_steps = [{:return, {:ok, {2, @origin}}}]
    delete_steps = [{:return, {:ok, {3, @origin}}}]
    :ets.insert(@table, {{name, :put}, put_steps})
    :ets.insert(@table, {{name, :delete}, delete_steps})

    assert {:error, :conflict} =
             StorageBackend.put_object(backend, "key", "value", etag: etag, max_retries: 0)

    assert {:error, :conflict} = StorageBackend.delete_object(backend, "key", etag: etag)
    assert :ets.lookup(@table, {name, :put}) == [{{name, :put}, put_steps}]
    assert :ets.lookup(@table, {name, :delete}) == [{{name, :delete}, delete_steps}]
  end

  defp encode_etag(term), do: term |> :erlang.term_to_binary() |> encode_binary()
  defp encode_binary(binary), do: Base.url_encode64(binary, padding: false)

  defp noncanonical_etf(origin) do
    <<131, 104, 2, 98, 1::signed-big-32, 109, byte_size(origin)::unsigned-big-32, origin::binary>>
  end

  defp small_atom(name) when byte_size(name) <= 255 do
    <<119, byte_size(name), name::binary>>
  end
end
