defmodule DurableServer.MirrorStoreTest do
  use ExUnit.Case, async: true

  alias DurableServer.Backends.MirrorStore
  alias DurableServer.StorageBackend

  defmodule RaceBackend do
    @behaviour StorageBackend

    @impl true
    def init_backend(opts), do: {:ok, %{state: Map.new(opts)}}

    @impl true
    def ensure_ready(_state), do: :ok

    @impl true
    def get_object(%{table: table}, key, _opts) do
      case :ets.lookup(table, key) do
        [{^key, object}] -> {:ok, object}
        [] -> {:error, :not_found}
      end
    end

    @impl true
    def list_all_objects_stream(_state, _prefix, _opts), do: []

    @impl true
    def put_object(state, key, body, _opts) do
      maybe_pause(state, key, :put)
      object = %{body: body, etag: next_etag()}
      :ets.insert(state.table, {key, object})
      {:ok, object}
    end

    @impl true
    def delete_object(%{table: table}, key) do
      :ets.delete(table, key)
      :ok
    end

    @impl true
    def try_claim(state, key, body) do
      maybe_pause(state, key, :try_claim)
      object = %{body: body, etag: next_etag()}

      if :ets.insert_new(state.table, {key, object}) do
        {:ok, {:claimed, object.etag}}
      else
        {:error, :already_claimed}
      end
    end

    @impl true
    def update_object(_state, _key, _update_fn, _opts), do: {:error, :unsupported}

    @impl true
    def encode(_state, data), do: {:ok, data}

    @impl true
    def decode(_state, data), do: {:ok, data}

    defp maybe_pause(%{pause_owner: owner, pause_key: key}, key, operation)
         when is_pid(owner) do
      ref = make_ref()
      send(owner, {:promotion_paused, self(), ref, operation})

      receive do
        {:continue_promotion, ^ref} -> :ok
      end
    end

    defp maybe_pause(_state, _key, _operation), do: :ok

    defp next_etag do
      System.unique_integer([:positive, :monotonic])
      |> Integer.to_string()
    end
  end

  test "fallback promotion cannot overwrite a concurrent preferred-backend create" do
    key = "promotion-race"
    primary_table = :ets.new(:mirror_primary, [:set, :public])
    secondary_table = :ets.new(:mirror_secondary, [:set, :public])

    primary =
      StorageBackend.new(RaceBackend, %{
        table: primary_table,
        pause_owner: self(),
        pause_key: key
      })

    direct_primary = StorageBackend.new(RaceBackend, %{table: primary_table})
    secondary = StorageBackend.new(RaceBackend, %{table: secondary_table})

    assert {:ok, _object} = StorageBackend.put_object(secondary, key, "fallback-value")

    mirror =
      StorageBackend.new(MirrorStore, %{
        primary: primary,
        secondary: secondary,
        read_preference: :primary,
        write_target: :primary,
        fallback_reads: true,
        promote_on_fallback: true,
        mirror_writes: false,
        mirror_mode: :best_effort,
        secondary_required: false
      })

    read_task = Task.async(fn -> StorageBackend.get_object(mirror, key) end)

    assert_receive {:promotion_paused, promotion_pid, ref, operation}
    assert operation in [:put, :try_claim]

    assert {:ok, %{body: "newer-preferred-value"}} =
             StorageBackend.put_object(direct_primary, key, "newer-preferred-value")

    send(promotion_pid, {:continue_promotion, ref})

    assert {:ok, %{body: "newer-preferred-value"}} = Task.await(read_task)

    assert {:ok, %{body: "newer-preferred-value"}} =
             StorageBackend.get_object(direct_primary, key)
  end
end
