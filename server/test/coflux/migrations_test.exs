defmodule Coflux.MigrationsTest do
  use ExUnit.Case, async: true

  alias Coflux.Store.Migrations
  alias Exqlite.Sqlite3

  describe "run/2" do
    test "evaluates orchestration migrations" do
      {:ok, db} = Sqlite3.open(":memory:")

      assert :ok = Migrations.run(db, "orchestration")

      :ok = Sqlite3.close(db)
    end

    test "evaluates admin migrations" do
      {:ok, db} = Sqlite3.open(":memory:")

      assert :ok = Migrations.run(db, "admin")

      :ok = Sqlite3.close(db)
    end
  end
end
