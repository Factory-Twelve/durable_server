defmodule DurableServer.ObjectStoreSecurityTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import SweetXml

  alias DurableServer.ObjectStore
  alias DurableServer.StorageBackend

  defmodule SensitiveBackend do
    @behaviour StorageBackend

    @impl true
    def init_backend(opts) do
      {:ok,
       %{
         state: %{
           table: :ets.new(__MODULE__, [:set, :public]),
           secret: Keyword.fetch!(opts, :secret)
         },
         defaults: %{
           discovery_interval_ms: 60_000,
           heartbeat_interval_ms: 10_000,
           heartbeat_tracking_mode: :poll,
           heartbeat_reconcile_interval_ms: 10_000
         }
       }}
    end

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
    def list_all_objects_stream(%{table: table}, prefix, _opts) do
      table
      |> :ets.tab2list()
      |> Stream.filter(fn {key, _object} -> String.starts_with?(key, prefix) end)
      |> Stream.map(fn {key, object} -> Map.put(object, :key, key) end)
    end

    @impl true
    def put_object(%{table: table}, key, body, _opts) do
      object = %{body: body, etag: Integer.to_string(System.unique_integer([:positive]))}
      :ets.insert(table, {key, object})
      {:ok, object}
    end

    @impl true
    def delete_object(%{table: table}, key) do
      :ets.delete(table, key)
      :ok
    end

    @impl true
    def try_claim(%{table: table}, key, body) do
      object = %{body: body, etag: Integer.to_string(System.unique_integer([:positive]))}

      if :ets.insert_new(table, {key, object}) do
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
  end

  test "supervisor startup logs omit backend state and init_info" do
    backend_secret = "backend-secret-#{System.unique_integer([:positive])}"
    init_secret = "init-secret-#{System.unique_integer([:positive])}"
    unique = System.unique_integer([:positive, :monotonic])
    supervisor_name = :"redacted_supervisor_#{unique}"

    log =
      capture_log([level: :debug], fn ->
        start_supervised!(
          {DurableServer.Supervisor,
           name: supervisor_name,
           prefix: "redacted/#{unique}/",
           backend: {SensitiveBackend, secret: backend_secret},
           init_info: %{credential: init_secret},
           graceful_shutdown_timeout_ms: 100}
        )
      end)

    refute log =~ backend_secret
    refute log =~ init_secret
  end

  test "CreateAccessKey parsing never logs returned credentials" do
    access_key_id = "access-id-#{System.unique_integer([:positive])}"
    secret = "secret-key-#{System.unique_integer([:positive])}"

    xml = """
    <CreateAccessKeyResponse>
      <AccessKey><AccessKeyId>#{access_key_id}</AccessKeyId><SecretAccessKey>#{secret}</SecretAccessKey></AccessKey>
    </CreateAccessKeyResponse>
    """

    log =
      capture_log([level: :debug], fn ->
        assert {:ok,
                %{access_key_id: ^access_key_id, secret_access_key: ^secret, user_name: "user"}} =
                 ObjectStore.__parse_create_access_key_response__(xml, "user")
      end)

    refute log =~ access_key_id
    refute log =~ secret
  end

  test "IAM XML parsing rejects DTDs and never expands an external entity" do
    secret = "iam-xxe-secret-#{System.unique_integer([:positive, :monotonic])}"

    path =
      Path.join(System.tmp_dir!(), "durable-server-xxe-#{System.unique_integer([:positive])}")

    File.write!(path, secret)
    on_exit(fn -> File.rm(path) end)

    xml = """
    <?xml version="1.0"?>
    <!DOCTYPE response [<!ENTITY xxe SYSTEM "file://#{path}">]>
    <CreateAccessKeyResponse><AccessKeyId>&xxe;</AccessKeyId></CreateAccessKeyResponse>
    """

    assert {:error, :invalid_xml} = ObjectStore.__parse_iam_xml__(xml)

    assert {:ok, document} =
             ObjectStore.__parse_iam_xml__(
               "<CreateAccessKeyResponse><AccessKeyId>safe-id</AccessKeyId></CreateAccessKeyResponse>"
             )

    assert xpath(document, ~x"//AccessKeyId/text()"s) == "safe-id"
  end
end
