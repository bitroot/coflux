defmodule Coflux.ScopesTest do
  use ExUnit.Case, async: true

  alias Coflux.Scopes

  describe "covers?/2" do
    test "an exact scope selects that workspace and no other" do
      assert Scopes.covers?("development", "development")
      refute Scopes.covers?("development", "development/joe")
      refute Scopes.covers?("development", "development-2")
    end

    test "a prefix scope selects what's under it, at any depth" do
      assert Scopes.covers?("development/*", "development/joe")
      assert Scopes.covers?("development/*", "development/joe/feature-1")
    end

    test "a prefix scope doesn't select the workspace it names" do
      refute Scopes.covers?("development/*", "development")
    end

    test "prefixes stop at a segment boundary" do
      refute Scopes.covers?("development/*", "development-2/joe")
      refute Scopes.covers?("dev/*", "devops/thing")
    end

    test "'*' selects every workspace" do
      assert Scopes.covers?("*", "development")
      assert Scopes.covers?("*", "development/joe/feature-1")
    end

    test "covering a workspace and everything under it takes both scopes" do
      scopes = ["development", "development/*"]

      assert Scopes.covers_any?(scopes, "development")
      assert Scopes.covers_any?(scopes, "development/joe")
      refute Scopes.covers_any?(scopes, "development-2")
    end
  end

  describe "contains?/2" do
    test "a scope contains itself" do
      assert Scopes.contains?("development", "development")
      assert Scopes.contains?("development/*", "development/*")
      assert Scopes.contains?("*", "*")
    end

    test "'*' contains everything, and nothing else contains '*'" do
      assert Scopes.contains?("*", "development")
      assert Scopes.contains?("*", "development/*")

      refute Scopes.contains?("development", "*")
      refute Scopes.contains?("development/*", "*")
    end

    test "a prefix contains the scopes beneath it" do
      assert Scopes.contains?("development/*", "development/joe")
      assert Scopes.contains?("development/*", "development/joe/*")
      assert Scopes.contains?("development/*", "development/joe/feature-1")
    end

    test "a prefix doesn't contain the workspace it names, which it doesn't select" do
      refute Scopes.contains?("development/*", "development")
    end

    test "an exact scope contains only itself" do
      refute Scopes.contains?("development", "development/joe")
      refute Scopes.contains?("development", "development/*")
    end

    test "containment stops at a segment boundary" do
      refute Scopes.contains?("dev/*", "devops/*")
      refute Scopes.contains?("dev/*", "devops")
    end

    # The distinction that matters: holding one workspace is not holding a
    # scope that reaches others.
    test "covering a workspace is not containing a scope that names it" do
      assert Scopes.covers?("staging", "staging")
      refute Scopes.contains?("staging", "staging/*")
    end
  end

  describe "valid?/1" do
    test "accepts scopes" do
      assert Scopes.valid?("*")
      assert Scopes.valid?("development")
      assert Scopes.valid?("development/joe")
      assert Scopes.valid?("development/*")
      assert Scopes.valid?("development/joe/*")
    end

    test "rejects what isn't one" do
      refute Scopes.valid?("")
      refute Scopes.valid?("/*")
      refute Scopes.valid?("development/")
      refute Scopes.valid?("not valid!")
      refute Scopes.valid?(nil)
      refute Scopes.valid?(["development"])
    end
  end

  describe "specificity/1" do
    test "the nearest scope wins: exact, then longer prefix, then '*'" do
      scopes = ["*", "development/*", "development/joe/*", "development/joe/feature-1"]

      assert Enum.max_by(scopes, &Scopes.specificity/1) == "development/joe/feature-1"

      assert scopes
             |> Enum.reject(&(&1 == "development/joe/feature-1"))
             |> Enum.max_by(&Scopes.specificity/1) == "development/joe/*"
    end

    test "an exact scope beats a longer prefix" do
      assert Scopes.specificity("development/joe") >
               Scopes.specificity("development/joe/very/long/*")
    end
  end
end
