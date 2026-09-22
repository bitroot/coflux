defmodule Coflux.ConcurrencyPermitsTest do
  use ExUnit.Case, async: true

  alias Coflux.Orchestration.Runs
  alias Coflux.Store.Migrations
  alias Exqlite.Sqlite3

  @workspace 1

  # A permit is held from the moment an assignment is written until the
  # completion is. Nothing records that directly — it's derived from those two
  # tables, which is what lets the scheduler's in-memory ledger be rebuilt at
  # boot without ever leaking a permit.
  #
  # A step can declare two keys — its task's and its group's — and a holder
  # reports both, so the ledger counts it against each.

  setup do
    {:ok, db} = Sqlite3.open(":memory:")
    :ok = Migrations.run(db, "orchestration")

    create_workspace(db, @workspace, "ws1")
    create_session(db, 1, @workspace)
    create_run(db, 1, "r1")

    {:ok, db: db}
  end

  test "is empty when nothing has been assigned", %{db: db} do
    create_step(db, 1, 1, 0, "abc")
    create_execution(db, 1, 1, 1)

    assert {:ok, []} = Runs.get_held_concurrency_permits(db)
  end

  test "reports an assigned, uncompleted execution", %{db: db} do
    create_step(db, 1, 1, 0, "abc")
    create_execution(db, 1, 1, 1)
    assign(db, 1)

    assert {:ok, [{1, @workspace, "abc", nil}]} = Runs.get_held_concurrency_permits(db)
  end

  test "drops an execution once it completes", %{db: db} do
    create_step(db, 1, 1, 0, "abc")
    create_execution(db, 1, 1, 1)
    assign(db, 1)
    complete(db, 1)

    assert {:ok, []} = Runs.get_held_concurrency_permits(db)
  end

  test "ignores steps that declare no key", %{db: db} do
    create_step(db, 1, 1, 0, nil)
    create_execution(db, 1, 1, 1)
    assign(db, 1)

    assert {:ok, []} = Runs.get_held_concurrency_permits(db)
  end

  test "counts each holder of a shared key", %{db: db} do
    create_run(db, 2, "r2")
    create_step(db, 1, 1, 0, "abc")
    create_step(db, 2, 2, 0, "abc")
    create_execution(db, 1, 1, 1)
    create_execution(db, 2, 2, 1)
    assign(db, 1)
    assign(db, 2)

    assert {:ok, permits} = Runs.get_held_concurrency_permits(db)
    assert Enum.sort(permits) == [{1, @workspace, "abc", nil}, {2, @workspace, "abc", nil}]
  end

  test "reports a step limited only by its group", %{db: db} do
    create_step(db, 1, 1, 0, nil, "r1:1:1/0")
    create_execution(db, 1, 1, 1)
    assign(db, 1)

    assert {:ok, [{1, @workspace, nil, "r1:1:1/0"}]} = Runs.get_held_concurrency_permits(db)
  end

  test "reports both keys of a step under both limits", %{db: db} do
    create_step(db, 1, 1, 0, "abc", "r1:1:1/0")
    create_execution(db, 1, 1, 1)
    assign(db, 1)

    assert {:ok, [{1, @workspace, "abc", "r1:1:1/0"}]} = Runs.get_held_concurrency_permits(db)
  end

  # A retry is a new execution of the same step: the failed attempt's
  # completion releases, and the successor only holds once it's assigned.
  test "follows attempts of the same step", %{db: db} do
    create_step(db, 1, 1, 0, "abc")
    create_execution(db, 1, 1, 1)
    assign(db, 1)
    complete(db, 1)
    create_execution(db, 2, 1, 2)

    assert {:ok, []} = Runs.get_held_concurrency_permits(db)

    assign(db, 2)

    assert {:ok, [{2, @workspace, "abc", nil}]} = Runs.get_held_concurrency_permits(db)
  end

  defp create_workspace(db, id, external_id) do
    :ok =
      Sqlite3.execute(
        db,
        "INSERT INTO workspaces (id, external_id) VALUES (#{id}, '#{external_id}')"
      )
  end

  defp create_session(db, id, workspace_id) do
    :ok =
      Sqlite3.execute(db, """
      INSERT INTO sessions (id, external_id, workspace_id, created_at)
      VALUES (#{id}, 's#{id}', #{workspace_id}, 0)
      """)
  end

  defp create_run(db, id, external_id) do
    :ok =
      Sqlite3.execute(
        db,
        "INSERT INTO runs (id, external_id, created_at) VALUES (#{id}, '#{external_id}', 0)"
      )
  end

  defp create_step(db, id, run_id, number, concurrency_key, group_key \\ nil) do
    key = if concurrency_key, do: "x'#{Base.encode16(concurrency_key)}'", else: "NULL"
    limit = if concurrency_key, do: 1, else: 0
    group_key_sql = if group_key, do: "'#{group_key}'", else: "NULL"
    group_limit = if group_key, do: 1, else: 0

    :ok =
      Sqlite3.execute(db, """
      INSERT INTO steps (
        id, number, run_id, module, target, type, priority, wait_for,
        retry_limit, retry_backoff_min_ms, retry_backoff_max_ms,
        concurrency_key, concurrency_limit, group_key, group_limit, created_at
      )
      VALUES (
        #{id}, #{number}, #{run_id}, 'module', 'target', 0, 0, 0, 0, 0, 0,
        #{key}, #{limit}, #{group_key_sql}, #{group_limit}, 0
      )
      """)
  end

  defp create_execution(db, id, step_id, attempt) do
    :ok =
      Sqlite3.execute(db, """
      INSERT INTO executions (id, step_id, attempt, workspace_id, created_at)
      VALUES (#{id}, #{step_id}, #{attempt}, #{@workspace}, 0)
      """)
  end

  defp assign(db, execution_id) do
    :ok =
      Sqlite3.execute(db, """
      INSERT INTO assignments (execution_id, session_id, created_at)
      VALUES (#{execution_id}, 1, 0)
      """)
  end

  defp complete(db, execution_id) do
    :ok =
      Sqlite3.execute(db, """
      INSERT INTO completions (execution_id, kind, created_at)
      VALUES (#{execution_id}, 0, 0)
      """)
  end
end
