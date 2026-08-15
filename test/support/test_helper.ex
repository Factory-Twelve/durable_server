defmodule DurableServer.TestHelper do
  @moduledoc """
  Test helpers for DurableServer tests.
  """

  alias DurableServer.Meta.ExternalIdentity
  alias DurableServer.ObjectStore

  @lock_owner_statuses [:running, :crashed, :permanently_crashed, :cordoned]

  @doc """
  Returns the default object store config for testing as a keyword list.
  """
  def test_object_store_opts(opts \\ []) do
    Keyword.merge(
      [
        access_key_id: "test",
        secret_access_key: "test",
        s3_endpoint: "http://localhost:4566",
        iam_endpoint: "http://localhost:4566",
        default_region: "us-east-1",
        bucket: "durable-test-bucket"
      ],
      opts
    )
  end

  @doc """
  Creates an ObjectStore configured for testing.

  Uses environment variables or defaults suitable for LocalStack.
  """
  def test_object_store(opts \\ []) do
    ObjectStore.new(test_object_store_opts(opts))
  end

  def normalize_test_lock_owner(%{status: status, pid: pid, node_str: node_str} = meta)
      when status in @lock_owner_statuses and is_pid(pid) and is_binary(node_str) do
    if to_string(node(pid)) == node_str do
      meta
    else
      Map.put(meta, :pid, external_pid_for_node(node_str))
    end
  end

  def normalize_test_lock_owner(meta), do: meta

  def external_pid_for_node(node_str)
      when is_binary(node_str) and byte_size(node_str) <= 255 do
    node_atom = <<119, byte_size(node_str), node_str::binary>>
    etf = <<131, 103, node_atom::binary, 0::unsigned-big-32, 0::unsigned-big-32, 0>>
    ExternalIdentity.new(:pid, node_str, etf)
  end
end
