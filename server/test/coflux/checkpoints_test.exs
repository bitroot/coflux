defmodule Coflux.CheckpointsTest do
  use ExUnit.Case, async: true

  alias Coflux.Orchestration.Checkpoints
  alias Coflux.Store.Migrations
  alias Exqlite.Sqlite3

  @base_ws 1
  @child_ws 2

  setup do
    {:ok, db} = Sqlite3.open(":memory:")
    :ok = Migrations.run(db, "orchestration")

    create_workspace(db, @base_ws, "base")
    create_workspace(db, @child_ws, "child")
    create_run(db, 1, "r1")
    create_step(db, 1, 1, 0)

    {:ok, db: db}
  end

  # The chain a base-workspace execution sees, and the chain a child sees.
  defp base_chain, do: [@base_ws]
  defp child_chain, do: [@child_ws, @base_ws]

  defp val(data), do: {:raw, data, []}

  describe "get_effective/4" do
    test "is empty when nothing has been written", %{db: db} do
      assert {:ok, %{}} = Checkpoints.get_effective(db, 1, base_chain())
    end

    test "returns what the latest attempt recorded", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      set(db, 1, 1, %{"cursor" => val(5)})

      assert {:ok, %{"cursor" => {:raw, 5, []}}} = Checkpoints.get_effective(db, 1, base_chain())
    end

    test "bounds the lookup to attempts below before_attempt", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      create_execution(db, 2, 1, 2, @base_ws)
      set(db, 1, 1, %{"cursor" => val(5)})
      set(db, 2, 2, %{"cursor" => val(9)})

      # What attempt 2 started with, rather than what it ended with.
      assert {:ok, %{"cursor" => {:raw, 5, []}}} =
               Checkpoints.get_effective(db, 1, base_chain(), 2)

      assert {:ok, %{"cursor" => {:raw, 9, []}}} = Checkpoints.get_effective(db, 1, base_chain())
    end

    test "distinguishes a stored null from a reset", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      set(db, 1, 1, %{"cursor" => val(nil)})

      assert {:ok, entries} = Checkpoints.get_effective(db, 1, base_chain())
      assert Map.has_key?(entries, "cursor")
      assert entries["cursor"] == {:raw, nil, []}

      create_execution(db, 2, 1, 2, @base_ws)
      reset(db, 2, 2, ["cursor"])

      assert {:ok, entries} = Checkpoints.get_effective(db, 1, base_chain())
      refute Map.has_key?(entries, "cursor")
    end
  end

  describe "apply_delta/7 carry-forward" do
    test "preserves names the latest attempt didn't write", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      set(db, 1, 1, %{"cursor" => val(5), "batch" => val("a")})

      create_execution(db, 2, 1, 2, @base_ws)
      set(db, 2, 2, %{"cursor" => val(9)})

      assert {:ok, entries} = Checkpoints.get_effective(db, 1, base_chain())
      assert entries["cursor"] == {:raw, 9, []}
      assert entries["batch"] == {:raw, "a", []}
    end

    test "writes a complete snapshot for each attempt", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      set(db, 1, 1, %{"cursor" => val(5), "batch" => val("a")})

      create_execution(db, 2, 1, 2, @base_ws)
      set(db, 2, 2, %{"cursor" => val(9)})

      # Attempt 2 carries `batch` forward rather than leaving it to a merge.
      assert {:ok, rows} = Checkpoints.get_rows_for_execution(db, 2)
      assert Enum.map(rows, fn {name, _, _} -> name end) == ["batch", "cursor"]
    end

    test "only carries forward once per execution", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      set(db, 1, 1, %{"cursor" => val(5)})

      create_execution(db, 2, 1, 2, @base_ws)
      reset(db, 2, 2, ["cursor"])
      # A later write in the same execution must not re-materialise the
      # previous snapshot and undo the reset.
      set(db, 2, 2, %{"batch" => val("a")})

      assert {:ok, entries} = Checkpoints.get_effective(db, 1, base_chain())
      refute Map.has_key?(entries, "cursor")
      assert entries["batch"] == {:raw, "a", []}
    end
  end

  describe "reset" do
    test "a reset name falls back to absent in the next attempt", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      set(db, 1, 1, %{"cursor" => val(5)})

      create_execution(db, 2, 1, 2, @base_ws)
      reset(db, 2, 2, ["cursor"])

      create_execution(db, 3, 1, 3, @base_ws)
      set(db, 3, 3, %{"other" => val(1)})

      assert {:ok, entries} = Checkpoints.get_effective(db, 1, base_chain())
      refute Map.has_key?(entries, "cursor")
    end

    test "resetting every name does not resurrect the pre-reset state", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      set(db, 1, 1, %{"cursor" => val(5), "batch" => val("a")})

      create_execution(db, 2, 1, 2, @base_ws)
      reset(db, 2, 2, ["cursor", "batch"])

      # Attempt 2's row-set is all tombstones, which must still count as
      # "touched checkpoints" — otherwise the read falls back to attempt 1.
      assert {:ok, %{}} = Checkpoints.get_effective(db, 1, base_chain())
    end

    test "resetting a name that was never set is recorded", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      reset(db, 1, 1, ["cursor"])

      assert {:ok, [{"cursor", nil, _}]} = Checkpoints.get_rows_for_execution(db, 1)
      assert {:ok, %{}} = Checkpoints.get_effective(db, 1, base_chain())
    end
  end

  describe "workspace scoping" do
    test "a child reads the base's state", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      set(db, 1, 1, %{"cursor" => val(5)})

      assert {:ok, %{"cursor" => {:raw, 5, []}}} = Checkpoints.get_effective(db, 1, child_chain())
    end

    test "a child's writes do not affect the base", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      set(db, 1, 1, %{"cursor" => val(5)})

      create_execution(db, 2, 1, 2, @child_ws)
      set(db, 2, 2, %{"cursor" => val(9)}, child_chain())

      assert {:ok, %{"cursor" => {:raw, 5, []}}} = Checkpoints.get_effective(db, 1, base_chain())
      assert {:ok, %{"cursor" => {:raw, 9, []}}} = Checkpoints.get_effective(db, 1, child_chain())
    end

    test "the nearest workspace wins outright", %{db: db} do
      create_execution(db, 1, 1, 1, @child_ws)
      set(db, 1, 1, %{"cursor" => val(9)}, child_chain())

      # A later base-workspace attempt must not override the child's own
      # state — inheritance is a fallback, not a merge.
      create_execution(db, 2, 1, 2, @base_ws)
      set(db, 2, 2, %{"cursor" => val(5)})

      assert {:ok, %{"cursor" => {:raw, 9, []}}} = Checkpoints.get_effective(db, 1, child_chain())
    end
  end

  describe "ordering" do
    test "a write from an older attempt does not affect the current state", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      create_execution(db, 2, 1, 2, @base_ws)

      set(db, 2, 2, %{"cursor" => val(9)})
      # A stale worker flushing after being superseded.
      set(db, 1, 1, %{"cursor" => val(5)})

      assert {:ok, %{"cursor" => {:raw, 9, []}}} = Checkpoints.get_effective(db, 1, base_chain())
    end
  end

  describe "get_execution_snapshots/5" do
    test "reports what an attempt started with and what it left", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      create_execution(db, 2, 1, 2, @base_ws)
      set(db, 1, 1, %{"cursor" => val(5)})
      set(db, 2, 2, %{"cursor" => val(9)})

      assert {:ok, {before, after_}} =
               Checkpoints.get_execution_snapshots(db, 2, 1, base_chain(), 2)

      assert before == %{"cursor" => {:raw, 5, []}}
      assert after_ == %{"cursor" => {:raw, 9, []}}
    end

    test "an attempt that didn't write leaves the state as it found it", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      create_execution(db, 2, 1, 2, @base_ws)
      set(db, 1, 1, %{"cursor" => val(5)})

      assert {:ok, {before, after_}} =
               Checkpoints.get_execution_snapshots(db, 2, 1, base_chain(), 2)

      assert before == %{"cursor" => {:raw, 5, []}}
      assert after_ == before
    end

    test "an attempt that reset everything is distinct from one that didn't write", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      create_execution(db, 2, 1, 2, @base_ws)
      set(db, 1, 1, %{"cursor" => val(5)})
      reset(db, 2, 2, ["cursor"])

      assert {:ok, {before, after_}} =
               Checkpoints.get_execution_snapshots(db, 2, 1, base_chain(), 2)

      assert before == %{"cursor" => {:raw, 5, []}}
      assert after_ == %{}
    end

    test "the first attempt starts from nothing", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      set(db, 1, 1, %{"cursor" => val(5)})

      assert {:ok, {%{}, after_}} =
               Checkpoints.get_execution_snapshots(db, 1, 1, base_chain(), 1)

      assert after_ == %{"cursor" => {:raw, 5, []}}
    end

    test "a child workspace attempt starts from the base's state", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      create_execution(db, 2, 1, 2, @child_ws)
      set(db, 1, 1, %{"cursor" => val(5)})
      set(db, 2, 2, %{"cursor" => val(9)}, child_chain())

      assert {:ok, {before, after_}} =
               Checkpoints.get_execution_snapshots(db, 2, 1, child_chain(), 2)

      assert before == %{"cursor" => {:raw, 5, []}}
      assert after_ == %{"cursor" => {:raw, 9, []}}
    end
  end

  describe "get_snapshot_execution_ids/2" do
    test "returns the latest checkpoint-bearing execution per workspace", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)
      create_execution(db, 2, 1, 2, @base_ws)
      create_execution(db, 3, 1, 3, @child_ws)

      set(db, 1, 1, %{"cursor" => val(1)})
      set(db, 2, 2, %{"cursor" => val(2)})
      set(db, 3, 3, %{"cursor" => val(3)}, child_chain())

      assert {:ok, ids} = Checkpoints.get_snapshot_execution_ids(db, 1)
      assert Enum.sort(ids) == [2, 3]
    end

    test "is empty when no checkpoints exist", %{db: db} do
      create_execution(db, 1, 1, 1, @base_ws)

      assert {:ok, []} = Checkpoints.get_snapshot_execution_ids(db, 1)
    end
  end

  ## Helpers

  defp set(db, execution_id, attempt, entries, chain \\ nil) do
    {:ok, _} =
      Checkpoints.apply_delta(db, execution_id, 1, chain || base_chain(), attempt, entries, [])
  end

  defp reset(db, execution_id, attempt, names, chain \\ nil) do
    {:ok, _} =
      Checkpoints.apply_delta(db, execution_id, 1, chain || base_chain(), attempt, %{}, names)
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
        retry_limit, retry_backoff_min, retry_backoff_max, created_at
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
end
