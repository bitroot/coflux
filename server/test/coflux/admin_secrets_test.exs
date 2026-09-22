defmodule Coflux.AdminSecretsTest do
  use ExUnit.Case, async: true

  alias Coflux.Admin.Secrets
  alias Coflux.Store
  alias Coflux.Store.Migrations
  alias Exqlite.Sqlite3

  @project "proj"
  @by %{type: "user", external_id: "user-1"}

  setup do
    {:ok, db} = Sqlite3.open(":memory:")
    :ok = Migrations.run(db, "admin")
    {:ok, db: db}
  end

  test "a value round-trips, and setting it again replaces it and bumps the version", %{db: db} do
    assert {:ok, %{version: 1}} = Secrets.set(db, @project, "*", "api-key", "first", @by)
    assert {:ok, "first"} = Secrets.resolve(db, @project, "development", "api-key")

    assert {:ok, %{version: 2, updated_by: @by}} =
             Secrets.set(db, @project, "*", "api-key", "second", @by)

    assert {:ok, "second"} = Secrets.resolve(db, @project, "development", "api-key")

    assert {:ok, [%{name: "api-key", workspaces: "*", version: 2}]} = Secrets.list(db)
    assert :ok = Secrets.delete(db, "*", "api-key")
    assert {:error, :not_found} = Secrets.delete(db, "*", "api-key")
    assert {:error, :not_found} = Secrets.resolve(db, @project, "development", "api-key")
  end

  test "the nearest scope wins, and production's secrets don't reach development", %{db: db} do
    {:ok, _} = Secrets.set(db, @project, "*", "key", "project-wide", nil)
    {:ok, _} = Secrets.set(db, @project, "development/*", "key", "for-development", nil)
    {:ok, _} = Secrets.set(db, @project, "development/joe/*", "key", "under-joe", nil)
    {:ok, _} = Secrets.set(db, @project, "development/joe", "key", "for-joe", nil)
    {:ok, _} = Secrets.set(db, @project, "production", "key", "for-production", nil)

    # An exact scope beats a prefix, and a longer prefix beats a shorter.
    assert {:ok, "for-joe"} = Secrets.resolve(db, @project, "development/joe", "key")
    assert {:ok, "under-joe"} = Secrets.resolve(db, @project, "development/joe/feature", "key")
    assert {:ok, "for-development"} = Secrets.resolve(db, @project, "development/sam", "key")
    assert {:ok, "project-wide"} = Secrets.resolve(db, @project, "staging", "key")
    assert {:ok, "for-production"} = Secrets.resolve(db, @project, "production", "key")

    # `development/*` doesn't select `development` itself.
    assert {:ok, "project-wide"} = Secrets.resolve(db, @project, "development", "key")

    # A prefix is a path prefix, not a string prefix.
    assert {:ok, "project-wide"} = Secrets.resolve(db, @project, "developments", "key")

    {:ok, _} = Secrets.set(db, @project, "production", "prod-only", "x", nil)
    assert {:error, :not_found} = Secrets.resolve(db, @project, "development/joe", "prod-only")
  end

  test "a ciphertext is bound to its row", %{db: db} do
    {:ok, _} = Secrets.set(db, @project, "*", "a", "value-a", nil)
    {:ok, _} = Secrets.set(db, @project, "*", "b", "value-b", nil)

    {:ok, _} =
      Store.query(
        db,
        """
        UPDATE secrets SET (nonce, ciphertext) =
          (SELECT nonce, ciphertext FROM secrets WHERE name = 'a')
        WHERE name = 'b'
        """,
        {}
      )

    assert {:ok, "value-a"} = Secrets.resolve(db, @project, "x", "a")
    assert {:error, :undecryptable} = Secrets.resolve(db, @project, "x", "b")
    assert {:error, :undecryptable} = Secrets.resolve(db, "other-project", "x", "a")
  end

  test "a launcher config is given the values it names", %{db: db} do
    {:ok, _} = Secrets.set(db, @project, "*", "k8s-token", "bearer-xyz", nil)
    {:ok, _} = Secrets.set(db, @project, "*", "openai", "sk-123", nil)

    {:ok, _} =
      Secrets.set(
        db,
        @project,
        "*",
        "aws",
        ~s({"Version": 1, "AccessKeyId": "AKIA1", "SecretAccessKey": "s3cr3t", "SessionToken": "tok"}),
        nil
      )

    launcher = %{
      type: :kubernetes,
      token_secret: "k8s-token",
      env: %{"PLAIN" => "1"},
      env_secrets: %{"OPENAI_API_KEY" => "openai"}
    }

    assert Secrets.references(launcher) == ["k8s-token", "openai"]
    assert :ok = Secrets.check_references(db, "dev", launcher)

    assert {:ok, resolved} = Secrets.resolve_launcher(db, @project, "dev", launcher)
    assert resolved.token == "bearer-xyz"
    assert resolved.env == %{"PLAIN" => "1", "OPENAI_API_KEY" => "sk-123"}

    ecs = %{type: :ecs, credentials_secret: "aws"}
    assert {:ok, resolved} = Secrets.resolve_launcher(db, @project, "dev", ecs)
    assert resolved.access_key_id == "AKIA1"
    assert resolved.secret_access_key == "s3cr3t"
    assert resolved.session_token == "tok"

    missing = %{type: :kubernetes, token_secret: "nope", env_secrets: %{"X" => "openai"}}
    assert {:error, {:secrets_not_found, ["nope"]}} = Secrets.check_references(db, "dev", missing)

    assert {:error, {:secret_not_found, "nope"}} =
             Secrets.resolve_launcher(db, @project, "dev", missing)

    {:ok, _} = Secrets.set(db, @project, "*", "not-json", "plain text", nil)

    assert {:error, {:secret_invalid, "not-json"}} =
             Secrets.resolve_launcher(db, @project, "dev", %{credentials_secret: "not-json"})
  end
end

defmodule Coflux.AdminSecretsWithoutServerSecretTest do
  # Takes the server secret away, which every other test's fixtures read,
  # so it can't run alongside them.
  use ExUnit.Case, async: false

  alias Coflux.Admin.Secrets
  alias Coflux.Store.Migrations
  alias Exqlite.Sqlite3

  setup do
    previous = :persistent_term.get(:coflux_secret, nil)
    :persistent_term.put(:coflux_secret, nil)
    on_exit(fn -> :persistent_term.put(:coflux_secret, previous) end)

    {:ok, db} = Sqlite3.open(":memory:")
    :ok = Migrations.run(db, "admin")
    {:ok, db: db}
  end

  test "without a server secret nothing can be stored or read", %{db: db} do
    assert {:error, :no_secret} = Secrets.set(db, "proj", "*", "x", "v", nil)
    assert {:error, :no_secret} = Secrets.resolve(db, "proj", "dev", "x")
  end
end
