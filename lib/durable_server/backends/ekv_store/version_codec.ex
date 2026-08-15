defmodule DurableServer.Backends.EKVStore.VersionCodec do
  @moduledoc false

  @max_timestamp 9_223_372_036_854_775_807
  @max_origin_bytes 255
  @max_binary_bytes 19 + @max_origin_bytes
  @max_encoded_bytes div(@max_binary_bytes * 4 + 2, 3)

  @type version :: {non_neg_integer(), binary()}

  @spec encode(term()) :: {:ok, binary()} | {:error, :invalid_ekv_version}
  def encode(version) do
    if valid_version?(version) do
      {:ok,
       version
       |> :erlang.term_to_binary()
       |> Base.url_encode64(padding: false)}
    else
      {:error, :invalid_ekv_version}
    end
  end

  @spec decode(term()) :: {:ok, version()} | :error
  def decode(etag) when is_binary(etag) and byte_size(etag) <= @max_encoded_bytes do
    with {:ok, binary} <- Base.url_decode64(etag, padding: false),
         :ok <- validate_binary(binary),
         {:ok, version} <- decode_term(binary),
         true <- valid_version?(version),
         true <- :erlang.term_to_binary(version) == binary,
         true <- Base.url_encode64(binary, padding: false) == etag do
      {:ok, version}
    else
      _ -> :error
    end
  rescue
    _ -> :error
  end

  def decode(_etag), do: :error

  @spec decode_delete_etag(term()) :: {:ok, version()} | {:error, :conflict}
  def decode_delete_etag(etag) do
    case decode(etag) do
      {:ok, version} -> {:ok, version}
      :error -> {:error, :conflict}
    end
  end

  @spec validate_config(map()) ::
          :ok
          | {:error, :ekv_node_id_missing_for_etag}
          | {:error, {:ekv_node_id_exceeds_etag_limit, pos_integer()}}
  def validate_config(%{mode: :client}), do: :ok

  def validate_config(%{node_id: node_id}) do
    origin = if is_integer(node_id), do: Integer.to_string(node_id), else: node_id

    if valid_origin?(origin) do
      :ok
    else
      {:error, {:ekv_node_id_exceeds_etag_limit, @max_origin_bytes}}
    end
  end

  def validate_config(_config), do: {:error, :ekv_node_id_missing_for_etag}

  defp validate_binary(<<131, 80, _compressed_size::32, _rest::binary>>), do: :error

  defp validate_binary(<<131, _rest::binary>> = binary)
       when byte_size(binary) <= @max_binary_bytes,
       do: :ok

  defp validate_binary(_binary), do: :error

  defp decode_term(binary) do
    case :erlang.binary_to_term(binary, [:safe, :used]) do
      {version, used} when used == byte_size(binary) -> {:ok, version}
      {_version, _used} -> :error
    end
  rescue
    _ -> :error
  end

  defp valid_version?({timestamp, origin}) do
    is_integer(timestamp) and timestamp >= 0 and timestamp <= @max_timestamp and
      valid_origin?(origin)
  end

  defp valid_version?(_version), do: false

  defp valid_origin?(origin)
       when is_binary(origin) and byte_size(origin) >= 1 and
              byte_size(origin) <= @max_origin_bytes,
       do: true

  defp valid_origin?(_origin), do: false
end
