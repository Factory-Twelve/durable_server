defmodule DurableServer.Meta do
  # represents the object metadata in storage
  alias DurableServer.Meta
  alias DurableServer.Meta.{ExternalAtom, ExternalIdentity}
  alias DurableServer.Meta.Storage.V1

  defstruct vsn: 1,
            module: nil,
            permanent: false,
            pid: nil,
            status: :stopped_graceful,
            key: nil,
            prefix: nil,
            sticky_placement: nil,
            sticky_placement_history: [],
            supervisor: nil,
            task_supervisor: nil,
            dynamic_supervisor: nil,
            node_ref: nil,
            node_str: nil,
            last_heartbeat_at: nil,
            crash_history: [],
            restart_attempt_node: nil,
            restart_attempt_time: nil,
            restart_attempt_ttl: nil,
            init_from_ref: nil,
            init_from_pid: nil

  @stopped_graceful :stopped_graceful
  @stopped_permanent :stopped_permanent
  @running :running
  @crashed :crashed
  @permanently_crashed :permanently_crashed
  @deleting :deleting
  @cordoned :cordoned

  @max_metadata_binary_bytes 65_536
  @max_metadata_base64_bytes div(@max_metadata_binary_bytes + 2, 3) * 4
  @max_collection_items 4_096

  @statuses [
    @stopped_graceful,
    @stopped_permanent,
    @running,
    @crashed,
    @permanently_crashed,
    @deleting,
    @cordoned
  ]

  @lock_owner_statuses [@running, @crashed, @permanently_crashed, @cordoned]

  def decode_from_binary(meta_str, %{key: key, prefix: prefix}) when is_binary(meta_str) do
    with :ok <- validate_encoded_size(meta_str),
         {:ok, binary} <- decode_base64(meta_str),
         :ok <- validate_binary_format(binary),
         {:ok, term} <- V1.load_binary(binary) do
      build_meta(term, key, prefix)
    else
      {:error, reason} ->
        raise ArgumentError, to_string(reason)
    end
  rescue
    error in ArgumentError ->
      raise ArgumentError, "invalid persisted metadata: #{Exception.message(error)}"
  end

  defp validate_encoded_size(meta_str) do
    if byte_size(meta_str) <= @max_metadata_base64_bytes do
      :ok
    else
      {:error, "encoded value exceeds #{@max_metadata_base64_bytes} byte limit"}
    end
  end

  defp decode_base64(meta_str) do
    case Base.decode64(meta_str) do
      {:ok, binary} -> {:ok, binary}
      :error -> {:error, "invalid base64"}
    end
  end

  # This encoder has never emitted compressed ETF. Rejecting it prevents a tiny
  # persisted value from expanding without a useful pre-decode size bound.
  defp validate_binary_format(<<131, 80, _compressed_size::32, _rest::binary>>),
    do: {:error, "compressed external terms are not accepted"}

  defp validate_binary_format(<<131, _rest::binary>> = binary) do
    if byte_size(binary) <= @max_metadata_binary_bytes do
      :ok
    else
      {:error, "decoded value exceeds #{@max_metadata_binary_bytes} byte limit"}
    end
  end

  defp validate_binary_format(_binary), do: {:error, "invalid external term"}

  def encode_to_binary(%Meta{} = meta) do
    {_storage_term, binary} = validated_storage_term(meta)

    encoded = Base.encode64(binary)
    validate_produced_size!(encoded, @max_metadata_base64_bytes, "encoded metadata")
    encoded
  end

  def normalize_runtime(%Meta{} = meta, %{key: key, prefix: prefix}) do
    validate_storage_term!(Map.from_struct(meta))
    %{meta | key: key, prefix: prefix}
  end

  def from_storage_term(meta_map, %{key: key, prefix: prefix}) when is_map(meta_map) do
    meta_map
    |> V1.load_term()
    |> build_meta(key, prefix)
  end

  def from_storage_term(_term, _context) do
    raise ArgumentError, "invalid meta storage term"
  end

  defp build_meta(storage_term, key, prefix) do
    validate_storage_term!(storage_term)
    %{struct!(Meta, storage_term) | key: key, prefix: prefix}
  end

  defp validate_storage_term!(meta) do
    unless Map.has_key?(meta, :status) do
      raise ArgumentError, "metadata is missing required field :status"
    end

    validate_field!(meta, :vsn, &(&1 == 1), "schema version 1")
    validate_field!(meta, :module, &valid_nullable_atom?/1, "an atom or unresolved atom")
    validate_field!(meta, :permanent, &is_boolean/1, "a boolean")
    validate_field!(meta, :pid, &valid_pid_identity?/1, "a pid, opaque pid, or nil")
    validate_field!(meta, :status, &(&1 in @statuses), "a supported status")
    validate_field!(meta, :key, &(is_nil(&1) or is_binary(&1)), "a binary or nil")
    validate_field!(meta, :prefix, &(is_nil(&1) or is_binary(&1)), "a binary or nil")

    validate_field!(
      meta,
      :sticky_placement,
      &valid_sticky_placement?/1,
      "nil or a list of exact placement entries"
    )

    validate_field!(
      meta,
      :sticky_placement_history,
      &valid_sticky_placement_history?/1,
      "a list of exact placement-history entries"
    )

    validate_field!(meta, :supervisor, &valid_nullable_atom?/1, "an atom or unresolved atom")

    validate_field!(
      meta,
      :task_supervisor,
      &valid_nullable_atom?/1,
      "an atom or unresolved atom"
    )

    validate_field!(
      meta,
      :dynamic_supervisor,
      &valid_nullable_atom?/1,
      "an atom or unresolved atom"
    )

    validate_field!(
      meta,
      :node_ref,
      &(is_nil(&1) or is_integer(&1) or is_binary(&1)),
      "an integer, binary, or nil"
    )

    validate_field!(meta, :node_str, &(is_nil(&1) or is_binary(&1)), "a binary or nil")

    validate_field!(
      meta,
      :last_heartbeat_at,
      &(is_nil(&1) or is_integer(&1)),
      "an integer or nil"
    )

    validate_field!(
      meta,
      :crash_history,
      &valid_crash_history?/1,
      "a list of exact crash-history entries"
    )

    validate_field!(
      meta,
      :restart_attempt_node,
      &(is_nil(&1) or is_binary(&1)),
      "a binary or nil"
    )

    validate_field!(
      meta,
      :restart_attempt_time,
      &(is_nil(&1) or is_integer(&1)),
      "an integer or nil"
    )

    validate_field!(
      meta,
      :restart_attempt_ttl,
      &(is_nil(&1) or is_integer(&1)),
      "an integer or nil"
    )

    validate_field!(
      meta,
      :init_from_ref,
      &valid_reference_identity?/1,
      "a reference, opaque reference, or nil"
    )

    validate_field!(
      meta,
      :init_from_pid,
      &valid_pid_identity?/1,
      "a pid, opaque pid, or nil"
    )

    validate_lock_owner!(meta)

    :ok
  end

  defp validate_lock_owner!(meta) do
    if lock_owner_required?(meta) and not valid_lock_owner?(meta) do
      raise ArgumentError,
            "invalid metadata lock owner: lock-bearing statuses require a PID on node_str and a non-negative integer or non-empty legacy binary node_ref"
    end
  end

  defp validate_field!(meta, key, predicate, expected) do
    value = Map.get(meta, key, Map.fetch!(metadata_defaults(), key))

    unless predicate.(value) do
      raise ArgumentError, "invalid metadata field #{inspect(key)}: expected #{expected}"
    end
  end

  defp metadata_defaults, do: Map.from_struct(%Meta{})

  defp valid_sticky_placement?(nil), do: true

  defp valid_sticky_placement?(placement),
    do: valid_proper_bounded_list?(placement, &valid_sticky_placement_entry?/1)

  defp valid_sticky_placement_entry?(entry) when is_map(entry) do
    exact_keys?(entry, [:env_var, :value]) and
      (is_binary(entry.env_var) or entry.env_var == :any) and
      (is_binary(entry.value) or is_nil(entry.value) or entry.value == :any)
  end

  defp valid_sticky_placement_entry?(_entry), do: false

  defp valid_sticky_placement_history?(history),
    do: valid_proper_bounded_list?(history, &valid_sticky_placement_history_entry?/1)

  defp valid_sticky_placement_history_entry?(%{at: at, placement: placement} = entry) do
    exact_keys?(entry, [:at, :placement]) and is_integer(at) and
      valid_sticky_placement?(placement)
  end

  defp valid_sticky_placement_history_entry?(_entry), do: false

  defp valid_crash_history?(history),
    do: valid_proper_bounded_list?(history, &valid_crash_entry?/1)

  defp valid_crash_entry?(%{timestamp: timestamp, reason: reason} = entry) do
    optional_exact_keys?(entry, [:timestamp, :reason], [:node_ref]) and is_integer(timestamp) and
      is_binary(reason) and valid_node_ref?(Map.get(entry, :node_ref))
  end

  defp valid_crash_entry?(_entry), do: false

  defp valid_node_ref?(node_ref),
    do: is_nil(node_ref) or is_integer(node_ref) or is_binary(node_ref)

  defp valid_proper_bounded_list?(list, predicate),
    do: valid_proper_bounded_list?(list, predicate, @max_collection_items)

  defp valid_proper_bounded_list?([], _predicate, _remaining), do: true

  defp valid_proper_bounded_list?([entry | rest], predicate, remaining) when remaining > 0 do
    predicate.(entry) and valid_proper_bounded_list?(rest, predicate, remaining - 1)
  end

  defp valid_proper_bounded_list?(_list, _predicate, _remaining), do: false

  defp valid_nullable_atom?(value) when is_atom(value), do: true
  defp valid_nullable_atom?(%ExternalAtom{} = atom), do: ExternalAtom.valid?(atom)
  defp valid_nullable_atom?(_value), do: false

  defp valid_pid_identity?(identity) do
    is_nil(identity) or is_pid(identity) or valid_external_identity?(identity, :pid)
  end

  defp valid_reference_identity?(identity) do
    is_nil(identity) or is_reference(identity) or valid_external_identity?(identity, :reference)
  end

  defp valid_external_identity?(%ExternalIdentity{kind: kind} = identity, kind),
    do: ExternalIdentity.valid?(identity)

  defp valid_external_identity?(_identity, _kind), do: false

  defp exact_keys?(map, keys) do
    map_size(map) == length(keys) and Enum.all?(keys, &Map.has_key?(map, &1))
  end

  defp optional_exact_keys?(map, required_keys, optional_keys) do
    Enum.all?(required_keys, &Map.has_key?(map, &1)) and
      Enum.all?(Map.keys(map), &(&1 in required_keys or &1 in optional_keys))
  end

  defp validate_produced_size!(binary, max_bytes, label) do
    if byte_size(binary) > max_bytes do
      raise ArgumentError, "#{label} exceeds #{max_bytes} byte limit"
    end
  end

  def to_storage_term(%Meta{} = meta) do
    {storage_term, _binary} = validated_storage_term(meta)
    storage_term
  end

  defp validated_storage_term(%Meta{} = meta) do
    validate_storage_term!(Map.from_struct(meta))
    storage_term = V1.dump(meta)

    binary = :erlang.term_to_binary(storage_term)
    validate_produced_size!(binary, @max_metadata_binary_bytes, "metadata external term")
    {storage_term, binary}
  end

  def resolve_pid(pid) when is_pid(pid), do: pid

  def resolve_pid(%ExternalIdentity{kind: :pid} = identity),
    do: ExternalIdentity.resolve(identity)

  def resolve_pid(_identity), do: nil

  def identity_equal?(left, right), do: ExternalIdentity.equal?(left, right)

  def valid_lock_owner?(%Meta{} = meta), do: valid_lock_owner?(Map.from_struct(meta))

  def valid_lock_owner?(meta) when is_map(meta) do
    if lock_owner_required?(meta) do
      pid = metadata_value(meta, :pid)
      node_str = metadata_value(meta, :node_str)
      node_ref = metadata_value(meta, :node_ref)

      is_binary(node_str) and node_str != "" and valid_lock_node_ref?(node_ref) and
        identity_node(pid) == node_str
    else
      true
    end
  end

  def valid_lock_owner?(_term), do: false

  def resolve_module(%Meta{} = meta), do: %{meta | module: resolve_module_atom(meta.module)}

  def lock_owner(%Meta{} = meta), do: resolve_pid(meta.pid) || :noproc

  defp resolve_module_atom(atom) when is_atom(atom), do: atom

  defp resolve_module_atom(%ExternalAtom{} = atom),
    do: ExternalAtom.resolve_module(atom) || atom

  defp lock_owner_required?(meta),
    do: metadata_value(meta, :status) in @lock_owner_statuses

  defp metadata_value(meta, key),
    do: Map.get(meta, key, Map.fetch!(metadata_defaults(), key))

  defp identity_node(pid) when is_pid(pid), do: pid |> node() |> to_string()
  defp identity_node(%ExternalIdentity{kind: :pid, node: node_str}), do: node_str
  defp identity_node(_identity), do: nil

  defp valid_lock_node_ref?(node_ref) when is_integer(node_ref), do: node_ref >= 0
  defp valid_lock_node_ref?(node_ref) when is_binary(node_ref), do: node_ref != ""
  defp valid_lock_node_ref?(_node_ref), do: false

  def running?(%Meta{} = meta) do
    meta.status == @running
  end

  def stopped_permanently?(%Meta{} = meta) do
    meta.status == @stopped_permanent
  end

  def currently_restarting?(%Meta{} = meta) do
    current_time = System.system_time(:millisecond)
    meta.restart_attempt_ttl && current_time < meta.restart_attempt_ttl
  end

  def last_heartbeat_within_ms(%Meta{} = meta, ms) do
    # check both the server's last heartbeat and the node's heartbeat timestamp
    # use whichever is more recent to avoid false-positive orphan claims
    current_time = System.system_time(:millisecond)
    node_timestamp = lookup_node_heartbeat_timestamp(meta)

    # use the most recent timestamp between server and node heartbeats
    most_recent_heartbeat =
      case {meta.last_heartbeat_at, node_timestamp} do
        {nil, nil} -> nil
        {server_ts, nil} -> server_ts
        {nil, node_ts} -> node_ts
        {server_ts, node_ts} -> max(server_ts, node_ts)
      end

    most_recent_heartbeat && current_time - most_recent_heartbeat < ms
  end

  # Lookup the node's heartbeat timestamp from the ETS cache
  # Returns {:ok, timestamp} if found with matching node_ref, or :not_found
  defp lookup_node_heartbeat_timestamp(%Meta{
         supervisor: supervisor,
         node_str: node_str,
         node_ref: expected_node_ref
       })
       when is_atom(supervisor) and is_binary(node_str) and is_integer(expected_node_ref) do
    table_name = :"durable_server_heartbeats_#{supervisor}"

    case :ets.lookup(table_name, node_str) do
      [{^node_str, ^expected_node_ref, timestamp, _region, _capacity, _resources, _env_vars}] ->
        # node found and node_ref matches - this is the current incarnation
        timestamp

      [{^node_str, _different_node_ref, _timestamp, _region, _capacity, _resources, _env_vars}] ->
        # node found but node_ref doesn't match - this is a different incarnation
        # don't use this stale data
        nil

      [] ->
        # node not found in ETS cache
        nil
    end
  end

  # Fallback for when node_ref is not an integer or fields are missing
  defp lookup_node_heartbeat_timestamp(_meta), do: nil

  def restart_attempt_expired?(%Meta{} = meta) do
    meta.restart_attempt_ttl && System.system_time(:millisecond) > meta.restart_attempt_ttl
  end

  def crashed?(%Meta{} = meta), do: meta.status == @crashed

  def stopped_graceful?(%Meta{} = meta), do: meta.status == @stopped_graceful

  def deleting?(%Meta{} = meta), do: meta.status == @deleting

  def cordoned?(%Meta{} = meta), do: meta.status == @cordoned

  def permanently_crashed?(%Meta{} = meta), do: meta.status == @permanently_crashed

  def put_status(%Meta{} = meta, status) when status in @statuses do
    %{meta | status: status}
  end

  def put_crash_history(%Meta{} = meta, history) when is_list(history) do
    %{meta | crash_history: history}
  end

  def clear_restart_attempt(%Meta{} = meta) do
    %{meta | restart_attempt_node: nil, restart_attempt_time: nil, restart_attempt_ttl: nil}
  end

  def put_restart_attempt(%Meta{} = meta, %{
        restart_attempt_node: node_str,
        ttl_ms: ttl_ms
      })
      when is_binary(node_str) and is_integer(ttl_ms) do
    current_time = System.system_time(:millisecond)

    %{
      meta
      | restart_attempt_node: to_string(node_str),
        restart_attempt_time: current_time,
        restart_attempt_ttl: current_time + ttl_ms
    }
  end
end
