defmodule DurableServer.StoredState do
  alias DurableServer.Meta

  @envelope_marker "$durable_server_stored_state"
  @envelope_version 1

  defstruct key: nil,
            prefix: nil,
            state: nil,
            meta: nil,
            vsn: nil,
            etag: nil

  def to_storage_term(%__MODULE__{vsn: vsn, state: state, meta: %Meta{} = meta}) do
    %{
      @envelope_marker => @envelope_version,
      "vsn" => vsn,
      "state" => state,
      "meta" => Meta.encode_to_binary(meta)
    }
  end

  # Object-store writes retain the DurableServer 0.1.4 envelope so old nodes
  # can read records written during a rolling upgrade. EKV uses the explicit
  # discriminator emitted by to_storage_term/1.
  def to_object_store_term(%__MODULE__{vsn: vsn, state: state, meta: %Meta{} = meta}) do
    %{
      "vsn" => vsn,
      "state" => state,
      "meta" => Meta.encode_to_object_store_binary(meta)
    }
  end

  def from_storage_term(%__MODULE__{} = stored_state) do
    with {:ok, meta} <- normalize_meta(stored_state.meta) do
      {:ok,
       %__MODULE__{
         vsn: stored_state.vsn,
         state: stored_state.state,
         meta: meta
       }}
    end
  end

  def from_storage_term(
        %{
          @envelope_marker => @envelope_version,
          "vsn" => vsn,
          "state" => state,
          "meta" => meta_binary
        } = term
      )
      when map_size(term) == 4 and is_binary(meta_binary),
      do: decode_binary_meta(vsn, state, meta_binary)

  # Object-store envelopes remain readable by DurableServer 0.1.4 and are
  # still emitted by to_object_store_term/1 for rolling-upgrade compatibility.
  def from_storage_term(%{"vsn" => vsn, "state" => state, "meta" => meta_binary} = term)
      when map_size(term) == 3 and is_binary(meta_binary),
      do: decode_binary_meta(vsn, state, meta_binary)

  def from_storage_term(%{vsn: _vsn, state: _state, meta: _meta} = term)
      when map_size(term) == 3,
      do: {:error, ArgumentError.exception("legacy native metadata envelope is not accepted")}

  def from_storage_term(term) when is_map(term) do
    if malformed_envelope?(term) do
      {:error, ArgumentError.exception("malformed stored-state envelope")}
    else
      :not_stored_state
    end
  end

  def from_storage_term(_), do: :not_stored_state

  def from_object_store_term(term), do: from_storage_term(term)

  defp normalize_meta(%Meta{} = meta) do
    {:ok, Meta.normalize_runtime(meta, %{key: nil, prefix: nil})}
  rescue
    error -> {:error, error}
  end

  defp normalize_meta(meta_term) when is_map(meta_term) do
    {:ok, Meta.from_storage_term(meta_term, %{key: nil, prefix: nil})}
  rescue
    error -> {:error, error}
  end

  defp normalize_meta(other) do
    {:error, ArgumentError.exception("invalid stored meta term: #{inspect(other)}")}
  end

  defp decode_binary_meta(vsn, state, meta_binary) do
    {:ok,
     %__MODULE__{
       vsn: vsn,
       state: state,
       meta: Meta.decode_from_binary(meta_binary, %{key: nil, prefix: nil})
     }}
  rescue
    error -> {:error, error}
  end

  defp malformed_envelope?(term) do
    Map.has_key?(term, @envelope_marker) or
      exact_legacy_envelope_shape?(term, ["vsn", "state", "meta"]) or
      exact_legacy_envelope_shape?(term, [:vsn, :state, :meta])
  end

  defp exact_legacy_envelope_shape?(term, keys) do
    map_size(term) == length(keys) and Enum.all?(keys, &Map.has_key?(term, &1))
  end
end
