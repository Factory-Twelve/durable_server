defmodule DurableServer.ObjectStoreBackendTest do
  use ExUnit.Case, async: true

  alias DurableServer.Backends.ObjectStore, as: ObjectStoreBackend
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
            node_str: "test@node"
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
end
