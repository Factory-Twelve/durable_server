defmodule DurableServer.ObjectStoreRetryTest do
  use ExUnit.Case, async: true

  alias DurableServer.ObjectStore

  test "put retries transient Req failures within its deadline" do
    adapter =
      adapter([
        %Req.Response{status: 503},
        %Req.TransportError{reason: :timeout},
        %Req.Response{status: 200, headers: %{"etag" => ["retry-etag"]}}
      ])

    assert {:ok, %{etag: "retry-etag", body: "heartbeat"}} =
             ObjectStore.put_object(store(adapter), "__nodes/test@localhost", "heartbeat",
               max_retries: 10,
               timeout: 1_000
             )
  end

  test "put does not retry permanent Req responses" do
    responses_key = make_ref()
    Process.put(responses_key, [%Req.Response{status: 400}, :unexpected_retry])

    assert {:error, %Req.Response{status: 400}} =
             ObjectStore.put_object(
               store(adapter(responses_key)),
               "__nodes/test@localhost",
               "heartbeat",
               max_retries: 10,
               timeout: 1_000
             )

    assert Process.get(responses_key) == [:unexpected_retry]
  end

  defp adapter(responses) when is_list(responses) do
    responses_key = make_ref()
    Process.put(responses_key, responses)
    adapter(responses_key)
  end

  defp adapter(responses_key) do
    fn request ->
      case Process.get(responses_key) do
        [response_or_error | rest] ->
          Process.put(responses_key, rest)
          {request, response_or_error}

        [] ->
          raise "unexpected request"
      end
    end
  end

  defp store(adapter) do
    ObjectStore.new(
      bucket: "test-bucket",
      access_key_id: "test-access-key",
      secret_access_key: "test-secret-key",
      s3_endpoint: "http://s3.test",
      default_region: "us-east-1",
      req_opts: [adapter: adapter, retry_delay: 0, retry_log_level: false]
    )
  end
end
