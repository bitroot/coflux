defmodule Coflux.Topics.Diff do
  @moduledoc """
  Emits the topic updates that turn one projected value into another.

  Maps are walked key by key so a change deep in an entry becomes one `set`
  at that path; anything else that differs is set wholesale. A `nil` new
  value *at the given path* unsets the entry, so a projection that returns
  `nil` for something the model no longer holds removes it from the topic;
  a field inside an entry that becomes `nil` is set to `nil`, as any other
  value would be. At the root there is no entry to unset, so the topic's
  value becomes `nil` instead. Structs never appear in projected values,
  but are treated as leaves just in case.
  """

  alias Topical.Topic

  def apply(topic, _path, nil, nil), do: topic

  def apply(topic, [], _old, nil), do: Topic.set(topic, [], nil)

  def apply(topic, path, _old, nil) do
    {parent, [key]} = Enum.split(path, -1)
    Topic.unset(topic, parent, key)
  end

  def apply(topic, path, old, new) do
    diff(topic, path, old, new)
  end

  defp diff(topic, path, old, new)
       when is_map(old) and is_map(new) and not is_struct(old) and not is_struct(new) do
    topic =
      old
      |> Map.keys()
      |> Enum.reject(&Map.has_key?(new, &1))
      |> Enum.reduce(topic, fn key, topic -> Topic.unset(topic, path, key) end)

    Enum.reduce(new, topic, fn {key, value}, topic ->
      case Map.fetch(old, key) do
        {:ok, ^value} -> topic
        {:ok, previous} -> diff(topic, path ++ [key], previous, value)
        :error -> Topic.set(topic, path ++ [key], value)
      end
    end)
  end

  defp diff(topic, path, _old, new) do
    Topic.set(topic, path, new)
  end
end
