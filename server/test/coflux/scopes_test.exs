defmodule Coflux.ScopesTest do
  use ExUnit.Case, async: true

  alias Coflux.Scopes

  describe "covers?/2" do
    test "a scope covers itself and everything under it" do
      assert Scopes.covers?("development", "development")
      assert Scopes.covers?("development", "development/joe")
      assert Scopes.covers?("development", "development/joe/feature-1")
      assert Scopes.covers?("development/joe", "development/joe/feature-1")
    end

    test "the root scope covers every workspace" do
      assert Scopes.covers?("", "development")
      assert Scopes.covers?("", "development/joe")
    end

    test "names are compared a segment at a time" do
      refute Scopes.covers?("development", "development-2")
      refute Scopes.covers?("development", "development2/joe")
      refute Scopes.covers?("development/joe", "development/joel")
    end

    test "a scope doesn't cover what's above it, or a sibling" do
      refute Scopes.covers?("development/joe", "development")
      refute Scopes.covers?("development", "")
      refute Scopes.covers?("development/joe", "development/sam")
      refute Scopes.covers?("development", "production")
    end

    test "the scope of a pattern granting nothing covers nothing" do
      refute Scopes.covers?(:never, "development")
      refute Scopes.covers?(:never, "")
    end
  end

  describe "from_pattern/1" do
    test "a bare name and a wildcard grant the same subtree" do
      assert Scopes.from_pattern("development") == "development"
      assert Scopes.from_pattern("development/*") == "development"
      assert Scopes.from_pattern("development/joe/*") == "development/joe"
    end

    test "a wildcard grant includes the workspace it names" do
      scope = Scopes.from_pattern("development/*")

      assert Scopes.covers?(scope, "development")
      assert Scopes.covers?(scope, "development/joe")
    end

    test "'*' grants the root scope" do
      assert Scopes.from_pattern("*") == ""
      assert Scopes.covers?(Scopes.from_pattern("*"), "anything/at/all")
    end

    test "a pattern that names nothing grants nothing, not everything" do
      assert Scopes.from_pattern("") == :never
      assert Scopes.from_pattern("/*") == :never
      assert Scopes.from_pattern(nil) == :never
      assert Scopes.from_pattern(["development"]) == :never
    end
  end

  describe "valid_pattern?/1" do
    test "accepts what names a scope, rejects what doesn't" do
      assert Scopes.valid_pattern?("*")
      assert Scopes.valid_pattern?("development")
      assert Scopes.valid_pattern?("development/*")

      refute Scopes.valid_pattern?("")
      refute Scopes.valid_pattern?("/*")
      refute Scopes.valid_pattern?(nil)
    end
  end
end
