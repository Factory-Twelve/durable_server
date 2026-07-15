defmodule DurableServer.ObjectStoreSecurityTest do
  use ExUnit.Case, async: true

  import SweetXml

  alias DurableServer.ObjectStore

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
