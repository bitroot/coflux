defmodule Coflux.CatalogTest do
  use ExUnit.Case, async: true

  alias Coflux.Orchestration.{Catalog, Results, Runs}
  alias Coflux.Store.Migrations
  alias Exqlite.Sqlite3

  @prod 1
  @dev 2

  setup do
    {:ok, db} = Sqlite3.open(":memory:")
    :ok = Migrations.run(db, "orchestration")

    create_workspace(db, @prod, "prod")
    create_workspace(db, @dev, "dev")
    create_run(db, 1, "r1")
    create_step(db, 1, 1, 0)
    create_run(db, 2, "r2")
    create_step(db, 2, 2, 0)

    create_value(db, 1)
    create_value(db, 2)
    create_value(db, 3)

    {:ok, db: db}
  end

  defp prod_chain, do: [@prod]
  defp dev_chain, do: [@dev, @prod]

  describe "validate_path/1" do
    test "accepts slash-separated segments" do
      assert :ok = Catalog.validate_path("datasets/customers")
      assert :ok = Catalog.validate_path("a")
      assert :ok = Catalog.validate_path("a-b_c.d/e")
    end

    test "rejects empty, dotted, absolute and reserved forms" do
      for path <- ["", "/a", "a/", "a//b", "a/../b", ".", "a@1", "a:b", "a b", nil, 1] do
        assert {:error, :invalid_path} = Catalog.validate_path(path), inspect(path)
      end
    end
  end

  describe "publish/7" do
    test "numbers versions per path, across workspaces", %{db: db} do
      assert {:ok, %{number: 1, workspace_id: @prod}, true} = publish(db, "m", @prod, 1)
      assert {:ok, %{number: 2}, true} = publish(db, "m", @prod, 2)
      # dev's publish takes the next number on the path, not a number of its own
      assert {:ok, %{number: 3, workspace_id: @dev}, true} = publish(db, "m", @dev, 3)
      # prod's next continues past dev's, leaving prod a gap at 3
      assert {:ok, %{number: 4}, true} = publish(db, "m", @prod, 1)
      # another path has its own numbering
      assert {:ok, %{number: 1}, true} = publish(db, "other", @prod, 1)
    end

    test "is a no-op when the visible head already holds the value", %{db: db} do
      {:ok, first, true} = publish(db, "m", @prod, 1)
      assert {:ok, ^first, false} = publish(db, "m", @prod, 1)
      # dev sees prod's head, so publishing the same value from dev writes nothing
      assert {:ok, ^first, false} = publish(db, "m", @dev, 1)
      # a different value is a new version, and the old one again is another
      assert {:ok, %{number: 2}, true} = publish(db, "m", @prod, 2)
      assert {:ok, %{number: 3}, true} = publish(db, "m", @prod, 1)
    end
  end

  describe "get_head/5" do
    test "is the newest visible version, whichever workspace published it", %{db: db} do
      {:ok, _, true} = publish(db, "m", @prod, 1)
      {:ok, dev_v2, true} = publish(db, "m", @dev, 2)
      assert {:ok, ^dev_v2} = Catalog.get_head(db, "m", dev_chain(), nil, nil)
      # prod doesn't see dev's
      assert {:ok, %{number: 1}} = Catalog.get_head(db, "m", prod_chain(), nil, nil)
      # a later prod publish overtakes dev's own, from dev's point of view
      {:ok, prod_v3, true} = publish(db, "m", @prod, 3)
      assert {:ok, ^prod_v3} = Catalog.get_head(db, "m", dev_chain(), nil, nil)
      assert {:ok, nil} = Catalog.get_head(db, "missing", dev_chain(), nil, nil)
    end

    test "applies the pin before the chain", %{db: db} do
      {:ok, v1, true} = publish(db, "m", @prod, 1)
      {:ok, dev_v2, true} = publish(db, "m", @dev, 2)

      # Pinned before dev's publish: falls through to prod's, rather than
      # seeing an empty path because the nearest workspace's row is too new.
      assert {:ok, ^v1} = Catalog.get_head(db, "m", dev_chain(), v1.id, nil)
      assert {:ok, ^dev_v2} = Catalog.get_head(db, "m", dev_chain(), dev_v2.id, nil)
      assert {:ok, nil} = Catalog.get_head(db, "m", dev_chain(), 0, nil)
    end

    test "exempts the reader's own run from the pin", %{db: db} do
      create_execution(db, 1, 1, 1, @prod)
      create_execution(db, 2, 2, 1, @prod)
      {:ok, ref1} = Runs.create_execution_ref_for(db, 1)
      {:ok, ref2} = Runs.create_execution_ref_for(db, 2)

      {:ok, v1, true} = Catalog.publish(db, "m", @prod, prod_chain(), 1, ref1, nil)
      {:ok, v2, true} = Catalog.publish(db, "m", @prod, prod_chain(), 2, ref2, nil)

      # Pinned at v1: v2 is invisible to a run that didn't publish it...
      assert {:ok, ^v1} = Catalog.get_head(db, "m", prod_chain(), v1.id, "r1")
      # ...and visible to the run that did.
      assert {:ok, ^v2} = Catalog.get_head(db, "m", prod_chain(), v1.id, "r2")
    end
  end

  describe "get_version/3 and visible?/2" do
    test "finds a number wherever it was published", %{db: db} do
      {:ok, _, true} = publish(db, "m", @prod, 1)
      {:ok, dev_v2, true} = publish(db, "m", @dev, 2)

      assert {:ok, ^dev_v2} = Catalog.get_version(db, "m", 2)
      assert Catalog.visible?(dev_v2, dev_chain())
      refute Catalog.visible?(dev_v2, prod_chain())
      assert {:ok, nil} = Catalog.get_version(db, "m", 3)
    end
  end

  describe "get_next/4" do
    test "is the smallest visible number above the cursor, ignoring any pin", %{db: db} do
      {:ok, _, true} = publish(db, "m", @prod, 1)
      {:ok, _, true} = publish(db, "m", @dev, 2)
      {:ok, prod_v3, true} = publish(db, "m", @prod, 3)

      assert {:ok, %{number: 1}} = Catalog.get_next(db, "m", prod_chain(), 0)
      # prod skips dev's 2
      assert {:ok, ^prod_v3} = Catalog.get_next(db, "m", prod_chain(), 1)
      assert {:ok, %{number: 2}} = Catalog.get_next(db, "m", dev_chain(), 1)
      assert {:ok, nil} = Catalog.get_next(db, "m", dev_chain(), 3)
    end
  end

  describe "listing" do
    test "list_heads/3 gives one head per visible path under a prefix", %{db: db} do
      {:ok, _, true} = publish(db, "models/a", @prod, 1)
      {:ok, _, true} = publish(db, "models/a", @prod, 2)
      {:ok, _, true} = publish(db, "models/b", @dev, 3)
      {:ok, _, true} = publish(db, "data/x", @prod, 1)

      assert {:ok, [%{path: "data/x"}, %{path: "models/a", number: 2}, %{path: "models/b"}]} =
               Catalog.list_heads(db, dev_chain(), nil)

      assert {:ok, [%{path: "models/a", number: 2}]} =
               Catalog.list_heads(db, prod_chain(), "models/")

      # LIKE wildcards in the prefix are literal
      assert {:ok, []} = Catalog.list_heads(db, prod_chain(), "models/%")
    end

    test "list_versions/5 pages newest-first through what the chain can see", %{db: db} do
      {:ok, _, true} = publish(db, "m", @prod, 1)
      {:ok, _, true} = publish(db, "m", @dev, 2)
      {:ok, _, true} = publish(db, "m", @prod, 3)
      {:ok, _, true} = publish(db, "m", @prod, 1)

      assert {:ok, [%{number: 4}, %{number: 3}]} = Catalog.list_versions(db, "m", prod_chain(), 2)
      assert {:ok, [%{number: 1}]} = Catalog.list_versions(db, "m", prod_chain(), 2, 3)

      assert {:ok, [%{number: 4}, %{number: 3}, %{number: 2}, %{number: 1}]} =
               Catalog.list_versions(db, "m", dev_chain(), 10)
    end
  end

  describe "reads and waits" do
    test "record_read/3 is idempotent and surfaces per run", %{db: db} do
      create_execution(db, 1, 1, 1, @prod)
      {:ok, v1, true} = publish(db, "m", @prod, 1)

      assert {:ok, true} = Catalog.record_read(db, 1, v1.id)
      assert {:ok, false} = Catalog.record_read(db, 1, v1.id)
      assert {:ok, [{1, ^v1}]} = Catalog.get_reads_for_run(db, 1)
      assert {:ok, []} = Catalog.get_reads_for_run(db, 2)
    end

    test "record_wait/4 keeps one row per path, taking the latest number", %{db: db} do
      create_execution(db, 1, 1, 1, @prod)
      :ok = Catalog.record_wait(db, 1, "m", 2)
      :ok = Catalog.record_wait(db, 1, "m", 5)
      :ok = Catalog.record_wait(db, 1, "n", 0)
      assert {:ok, waits} = Catalog.get_waits(db, 1)
      assert Enum.sort(waits) == [{"m", 5}, {"n", 0}]
      assert {:ok, run_waits} = Catalog.get_waits_for_run(db, 1)
      assert Enum.sort(run_waits) == [{1, "m", 5}, {1, "n", 0}]
    end

    test "get_publishes_for_run/2 attributes versions to the publishing attempt", %{db: db} do
      create_execution(db, 1, 1, 2, @prod)
      {:ok, ref} = Runs.create_execution_ref_for(db, 1)
      {:ok, v1, true} = Catalog.publish(db, "m", @prod, prod_chain(), 1, ref, nil)
      assert {:ok, [{{0, 2}, ^v1}]} = Catalog.get_publishes_for_run(db, "r1")
      assert {:ok, []} = Catalog.get_publishes_for_run(db, "r2")
    end
  end

  describe "resolve_pin/2" do
    # Step 1 of run 1; attempts are created as needed. Versions are
    # published between attempts so the clock moves.

    test "a first attempt takes the clock", %{db: db} do
      {:ok, v1, true} = publish(db, "m", @prod, 1)
      create_execution(db, 1, 1, 1, @prod)
      assert {:ok, seq} = Catalog.resolve_pin(db, 1)
      assert seq == v1.id
    end

    test "a later attempt inherits the previous attempt's pin", %{db: db} do
      {:ok, v1, true} = publish(db, "m", @prod, 1)
      create_execution(db, 1, 1, 1, @prod)
      assign(db, 1, v1.id)
      {:ok, _v2, true} = publish(db, "m", @prod, 2)

      # a retry, a plain re-run, or the resumption of a suspension
      complete(db, 1, :suspended)
      create_execution(db, 2, 1, 2, @prod)
      assert {:ok, pin} = Catalog.resolve_pin(db, 2)
      assert pin == v1.id
    end

    test "inherits from the latest assigned attempt, skipping one never assigned", %{db: db} do
      {:ok, v1, true} = publish(db, "m", @prod, 1)
      create_execution(db, 1, 1, 1, @prod)
      assign(db, 1, v1.id)
      # attempt 2 was cancelled while pending
      create_execution(db, 2, 1, 2, @prod)
      complete(db, 2, :cancelled)
      {:ok, _v2, true} = publish(db, "m", @prod, 2)

      create_execution(db, 3, 1, 3, @prod)
      assert {:ok, pin} = Catalog.resolve_pin(db, 3)
      assert pin == v1.id
    end

    test "an attempt gated on a catalog wait takes the clock", %{db: db} do
      {:ok, v1, true} = publish(db, "m", @prod, 1)
      create_execution(db, 1, 1, 1, @prod)
      assign(db, 1, v1.id)
      complete(db, 1, :suspended)
      {:ok, v2, true} = publish(db, "m", @prod, 2)

      create_execution(db, 2, 1, 2, @prod)
      :ok = Catalog.record_wait(db, 2, "m", 1)
      assert {:ok, pin} = Catalog.resolve_pin(db, 2)
      assert pin == v2.id
    end

    test "the iteration after a recurrence takes the clock", %{db: db} do
      {:ok, v1, true} = publish(db, "m", @prod, 1)
      create_execution(db, 1, 1, 1, @prod)
      assign(db, 1, v1.id)
      complete(db, 1, :recurred)
      {:ok, v2, true} = publish(db, "m", @prod, 2)

      create_execution(db, 2, 1, 2, @prod)
      assert {:ok, pin} = Catalog.resolve_pin(db, 2)
      assert pin == v2.id
    end

    test "a run's override applies to attempts with nothing to inherit", %{db: db} do
      {:ok, v1, true} = publish(db, "m", @prod, 1)
      {:ok, v2, true} = publish(db, "m", @prod, 2)
      :ok = Sqlite3.execute(db, "UPDATE runs SET catalog_sequence = #{v1.id} WHERE id = 1")

      create_execution(db, 1, 1, 1, @prod)
      assert {:ok, pin} = Catalog.resolve_pin(db, 1)
      assert pin == v1.id

      # a re-run with a chosen snapshot, and then a plain re-run of *that*
      create_execution(db, 2, 1, 2, @prod)
      :ok = Sqlite3.execute(db, "UPDATE executions SET catalog_sequence = #{v2.id} WHERE id = 2")
      assert {:ok, pin} = Catalog.resolve_pin(db, 2)
      assert pin == v2.id
      assign(db, 2, v2.id)

      create_execution(db, 3, 1, 3, @prod)
      assert {:ok, pin} = Catalog.resolve_pin(db, 3)
      assert pin == v2.id
    end

    test "a catalog wait sees past the run's override", %{db: db} do
      {:ok, v1, true} = publish(db, "m", @prod, 1)
      :ok = Sqlite3.execute(db, "UPDATE runs SET catalog_sequence = #{v1.id} WHERE id = 1")
      create_execution(db, 1, 1, 1, @prod)
      assign(db, 1, v1.id)
      complete(db, 1, :suspended)
      {:ok, v2, true} = publish(db, "m", @prod, 2)

      create_execution(db, 2, 1, 2, @prod)
      :ok = Catalog.record_wait(db, 2, "m", 1)
      assert {:ok, pin} = Catalog.resolve_pin(db, 2)
      assert pin == v2.id
    end
  end

  test "current_sequence/1 is the highest id", %{db: db} do
    assert {:ok, 0} = Catalog.current_sequence(db)
    {:ok, v, true} = publish(db, "m", @prod, 1)
    assert {:ok, seq} = Catalog.current_sequence(db)
    assert seq == v.id
  end

  # --- helpers ---

  defp publish(db, path, workspace_id, value_id) do
    chain = if workspace_id == @dev, do: dev_chain(), else: prod_chain()
    Catalog.publish(db, path, workspace_id, chain, value_id, nil, nil)
  end

  defp create_workspace(db, id, external_id) do
    :ok =
      Sqlite3.execute(
        db,
        "INSERT INTO workspaces (id, external_id) VALUES (#{id}, '#{external_id}')"
      )
  end

  defp create_run(db, id, external_id) do
    :ok =
      Sqlite3.execute(
        db,
        "INSERT INTO runs (id, external_id, created_at) VALUES (#{id}, '#{external_id}', 0)"
      )
  end

  defp create_step(db, id, run_id, number) do
    :ok =
      Sqlite3.execute(db, """
      INSERT INTO steps (
        id, number, run_id, module, target, type, priority, wait_for,
        retry_limit, retry_backoff_min_ms, retry_backoff_max_ms, created_at
      )
      VALUES (#{id}, #{number}, #{run_id}, 'module', 'target', 0, 0, 0, 0, 0, 0, 0)
      """)
  end

  defp create_execution(db, id, step_id, attempt, workspace_id) do
    :ok =
      Sqlite3.execute(db, """
      INSERT INTO executions (id, step_id, attempt, workspace_id, created_at)
      VALUES (#{id}, #{step_id}, #{attempt}, #{workspace_id}, 0)
      """)
  end

  defp assign(db, execution_id, catalog_sequence) do
    :ok =
      Sqlite3.execute(db, """
      INSERT INTO assignments (execution_id, session_id, created_at, catalog_sequence)
      VALUES (#{execution_id}, 1, 0, #{catalog_sequence})
      """)
  end

  defp complete(db, execution_id, kind) do
    :ok =
      Sqlite3.execute(db, """
      INSERT INTO completions (execution_id, kind, created_at)
      VALUES (#{execution_id}, #{Results.atom_kind(kind)}, 0)
      """)
  end

  defp create_value(db, id) do
    :ok =
      Sqlite3.execute(db, """
      INSERT INTO values_ (id, hash, content) VALUES (#{id}, X'0#{id}', X'0#{id}')
      """)
  end
end
