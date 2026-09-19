defmodule Coflux.AdminTokensTest do
  use ExUnit.Case, async: true

  alias Coflux.Admin.Tokens
  alias Coflux.Orchestration.Principals
  alias Coflux.Store
  alias Coflux.Store.Migrations
  alias Exqlite.Sqlite3

  # Creating a token signs it with the server secret.
  setup_all do
    previous = :persistent_term.get(:coflux_secret, nil)
    :persistent_term.put(:coflux_secret, "test-secret")
    on_exit(fn -> :persistent_term.put(:coflux_secret, previous) end)
    :ok
  end

  # An orchestration database as it was before tokens moved: at version 5,
  # with a token, its principal, and a user principal that created it.
  defp legacy_orchestration_db do
    {:ok, db} = Sqlite3.open(":memory:")
    :ok = Migrations.run(db, "orchestration", up_to: 5)

    {:ok, user_id} = Store.insert_one(db, :principals, %{user_external_id: "user-1"})

    {:ok, token_id} =
      Store.insert_one(db, :tokens, %{
        external_id: "tok1",
        token_hash: "hash1",
        name: "CI",
        workspaces: ~s(["staging"]),
        created_by: user_id,
        created_at: 1000
      })

    {:ok, _} = Store.insert_one(db, :principals, %{token_id: token_id})
    db
  end

  defp admin_db do
    {:ok, db} = Sqlite3.open(":memory:")
    :ok = Migrations.run(db, "admin")
    db
  end

  test "migrating rebuilds principals to name tokens by external id" do
    db = legacy_orchestration_db()
    :ok = Migrations.run(db, "orchestration")

    {:ok, rows} =
      Store.query(
        db,
        "SELECT user_external_id, token_external_id FROM principals ORDER BY id",
        {}
      )

    assert rows == [{"user-1", nil}, {nil, "tok1"}]
    assert {:ok, {"token", "tok1"}} = Principals.get_principal(db, 2)
  end

  test "importing moves tokens to the admin store and drops the table" do
    db = legacy_orchestration_db()
    :ok = Migrations.run(db, "orchestration")
    admin = admin_db()

    :ok = Tokens.import_legacy(db, admin)

    assert {:ok, [token]} = Tokens.list_tokens(admin)
    assert token.external_id == "tok1"
    assert token.name == "CI"
    assert token.workspaces == ["staging"]
    assert token.created_by == %{type: "user", external_id: "user-1"}
    assert token.revoked_at == nil

    assert {:ok, nil} =
             Store.query_one(
               db,
               "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'tokens'",
               {}
             )

    # Nothing left to import, and nothing duplicated.
    :ok = Tokens.import_legacy(db, admin)
    assert {:ok, [_]} = Tokens.list_tokens(admin)
  end

  test "a token is checked in the admin store and attributed in the orchestration store" do
    {:ok, db} = Sqlite3.open(":memory:")
    :ok = Migrations.run(db, "orchestration")
    admin = admin_db()

    {:ok, created} =
      Tokens.create_token(admin, "proj", "deploy", %{type: "user", external_id: "user-1"},
        workspaces: ["prod"]
      )

    "cflx_" <> rest = created.token
    random_hex = binary_part(rest, 0, 32)
    hash = :crypto.hash(:sha256, random_hex) |> Base.encode16(case: :lower)

    assert {:ok, %{external_id: external_id, workspaces: ["prod"]}} =
             Tokens.check_token(admin, hash)

    assert external_id == created.external_id

    # The principal is made on first sight and found afterwards.
    {:ok, principal_id} = Principals.ensure_token(db, external_id)
    assert {:ok, ^principal_id} = Principals.ensure_token(db, external_id)

    assert {:ok, {"token", ^external_id}} = Principals.get_principal(db, principal_id)

    {:ok, ^external_id} = Tokens.revoke_token(admin, created.id)
    assert {:error, :not_found} = Tokens.check_token(admin, hash)
  end
end
