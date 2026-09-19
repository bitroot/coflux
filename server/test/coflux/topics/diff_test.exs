defmodule Coflux.Topics.DiffTest do
  use ExUnit.Case, async: true

  alias Coflux.Topics.Diff
  alias Topical.Topic

  defp updates(topic), do: Enum.map(topic.updates, &Tuple.delete_at(&1, 0))

  test "a nil projection at the path unsets the entry" do
    topic = Topic.new(%{"a" => %{x: 1}})
    topic = Diff.apply(topic, ["a"], %{x: 1}, nil)
    assert updates(topic) == [{[], "a"}]
  end

  test "a field becoming nil inside an entry is set to nil, not unset" do
    topic = Topic.new(%{"a" => %{x: 1, active: "running"}})
    topic = Diff.apply(topic, ["a"], %{x: 1, active: "running"}, %{x: 1, active: nil})
    assert updates(topic) == [{["a", :active], nil}]
  end

  test "nothing to do when neither side holds the entry" do
    topic = Topic.new(%{})
    assert Diff.apply(topic, ["a"], nil, nil) == topic
  end

  test "a nil projection at the root sets the value rather than unsetting an entry" do
    topic = Topic.new(%{x: 1})
    topic = Diff.apply(topic, [], %{x: 1}, nil)
    assert updates(topic) == [{[], nil}]
    assert topic.value == nil
  end

  test "keys that disappear from an entry are unset and new ones set" do
    topic = Topic.new(%{"a" => %{x: 1, y: 2}})
    topic = Diff.apply(topic, ["a"], %{x: 1, y: 2}, %{x: 1, z: 3})
    assert Enum.sort(updates(topic)) == Enum.sort([{["a"], :y}, {["a", :z], 3}])
  end
end
