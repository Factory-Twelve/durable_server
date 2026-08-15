defmodule DurableServer.Meta.ExternalIdentity do
  @moduledoc false

  alias DurableServer.SafeETF

  @enforce_keys [:kind, :node, :etf]
  defstruct [:kind, :node, :etf]

  def new(kind, node, etf) do
    identity = %__MODULE__{kind: kind, node: node, etf: etf}

    if valid?(identity) do
      identity
    else
      raise ArgumentError, "invalid opaque external identity"
    end
  end

  def valid?(%__MODULE__{kind: kind, node: node, etf: etf})
      when kind in [:pid, :reference] and is_binary(node) and is_binary(etf),
      do: SafeETF.external_identity_node(etf, kind) == {:ok, node}

  def valid?(%__MODULE__{}), do: false

  def resolve(%__MODULE__{kind: kind, node: expected_node, etf: etf}) do
    case SafeETF.decode_external_identity(etf, kind) do
      {:ok, identity} ->
        if identity_node(identity) == expected_node, do: identity, else: nil

      :error ->
        nil
    end
  end

  def equal?(%__MODULE__{} = left, %__MODULE__{} = right), do: left == right

  def equal?(%__MODULE__{} = external, native) do
    case resolve(external) do
      nil -> false
      resolved -> resolved == native
    end
  end

  def equal?(native, %__MODULE__{} = external), do: equal?(external, native)
  def equal?(left, right), do: left == right

  defp identity_node(pid) when is_pid(pid), do: pid |> node() |> Atom.to_string()

  defp identity_node(reference) when is_reference(reference),
    do: reference |> node() |> Atom.to_string()
end

defmodule DurableServer.Meta.ExternalAtom do
  @moduledoc false

  alias DurableServer.SafeETF

  @enforce_keys [:name]
  defstruct [:name]

  def new(name) do
    atom = %__MODULE__{name: name}

    if valid?(atom) do
      atom
    else
      raise ArgumentError, "invalid opaque atom name"
    end
  end

  def valid?(%__MODULE__{name: name}) when is_binary(name) do
    SafeETF.valid_atom_name?(name)
  end

  def valid?(%__MODULE__{}), do: false

  # Release application manifests are the deployment-owned allowlist for
  # persisted module names. They contain module atoms without requiring this
  # decoder to intern data-controlled atoms.
  def resolve_module(%__MODULE__{name: name}) do
    :application.loaded_applications()
    |> Enum.find_value(fn {application, _description, _version} ->
      application
      |> Application.spec(:modules)
      |> List.wrap()
      |> Enum.find(&(Atom.to_string(&1) == name))
    end)
  end
end
