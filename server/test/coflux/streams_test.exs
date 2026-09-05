defmodule Coflux.StreamsTest do
  use ExUnit.Case, async: true

  alias Coflux.Orchestration.{Results, Streams}
  alias Coflux.Store.Migrations
  alias Exqlite.Sqlite3

  @base_ws 1
  @child_ws 2
  @step 1

  setup do
    {:ok, db} = Sqlite3.open(":memory:")
    :ok = Migrations.run(db, "orchestration")

    create_workspace(db, @base_ws, "base")
    create_workspace(db, @child_ws, "child")
    create_run(db, 1, "r1")
    create_step(db, @step, 1, 0)

    {:ok, db: db}
  end

  defp val(data), do: {:raw, data, []}

  describe "register/7" do
    test "opens a new stream per position with step-allocated indexes", %{db: db} do
      create_execution(db, 1, @step, 1, @base_ws)

      assert {:ok, %{index: 0, head: -1, continued: false} = first} = register(db, 1, 0)
      assert {:ok, %{index: 1, head: -1, continued: false} = second} = register(db, 1, 1)
      assert first.id != second.id

      assert {:ok, ^first} =
               Streams.register(db, @step, @base_ws, 1, 0, 0, nil) |> strip_created()
    end

    test "resumes a stream paused by a suspend, continuing the sequence", %{db: db} do
      create_execution(db, 1, @step, 1, @base_ws)
      {:ok, %{id: stream_id}} = register(db, 1, 0)
      {:ok, _} = Streams.append_item(db, stream_id, 1, 0, val("a"))
      {:ok, _} = Streams.append_item(db, stream_id, 1, 1, val("b"))
      suspend(db, 1, 2)

      create_execution(db, 2, @step, 2, @base_ws)
      assert {:ok, %{id: ^stream_id, index: 0, head: 1, continued: true}} = register(db, 2, 0)

      assert {:ok, _} = Streams.append_item(db, stream_id, 2, 2, val("c"))

      assert {:ok, [{0, _, _}, {1, _, _}, {2, _, _}]} =
               Streams.get_stream_items(db, stream_id, 0, 10)

      assert {:ok, {0, nil}} = Streams.get_config(db, stream_id)
      assert {:ok, 2} = Streams.get_producer(db, stream_id)
    end

    test "the latest registration's config is the one in force", %{db: db} do
      create_execution(db, 1, @step, 1, @base_ws)
      {:ok, %{id: stream_id}} = Streams.register(db, @step, @base_ws, 1, 0, 0, nil)
      suspend(db, 1, 2)

      create_execution(db, 2, @step, 2, @base_ws)

      {:ok, %{id: ^stream_id, continued: true}} =
        Streams.register(db, @step, @base_ws, 2, 0, 5, 1000)

      assert {:ok, {5, 1000}} = Streams.get_config(db, stream_id)
    end

    test "does not continue a stream whose producer is still live", %{db: db} do
      create_execution(db, 1, @step, 1, @base_ws)
      {:ok, %{id: first}} = register(db, 1, 0)

      create_execution(db, 2, @step, 2, @base_ws)
      assert {:ok, %{index: 1, continued: false} = second} = register(db, 2, 0)
      assert second.id != first
    end

    test "does not continue a closed stream", %{db: db} do
      create_execution(db, 1, @step, 1, @base_ws)
      {:ok, %{id: first}} = register(db, 1, 0)
      {:ok, _} = Streams.close_stream(db, first, 1, :complete)
      suspend(db, 1, 2)

      create_execution(db, 2, @step, 2, @base_ws)
      assert {:ok, %{index: 1, continued: false} = second} = register(db, 2, 0)
      assert second.id != first
    end

    test "does not continue a stream from another workspace", %{db: db} do
      create_execution(db, 1, @step, 1, @base_ws)
      {:ok, %{id: first}} = register(db, 1, 0)
      suspend(db, 1, 2)

      create_execution(db, 2, @step, 2, @child_ws)

      assert {:ok, %{index: 1, continued: false} = second} =
               Streams.register(db, @step, @child_ws, 2, 0, 0, nil)

      assert second.id != first
    end

    test "matches paused streams by the opener's registration position", %{db: db} do
      create_execution(db, 1, @step, 1, @base_ws)
      {:ok, %{id: s0}} = register(db, 1, 0)
      {:ok, %{id: s1}} = register(db, 1, 1)
      {:ok, %{id: s2}} = register(db, 1, 2)
      {:ok, _} = Streams.close_stream(db, s1, 1, :complete)
      suspend(db, 1, 2)

      create_execution(db, 2, @step, 2, @base_ws)
      assert {:ok, %{id: ^s0, continued: true}} = register(db, 2, 0)
      assert {:ok, %{index: 3, continued: false} = fresh} = register(db, 2, 1)
      assert fresh.id != s1
      assert {:ok, %{id: ^s2, continued: true}} = register(db, 2, 2)
    end

    test "survives a resuming execution that suspends before reaching later positions", %{db: db} do
      create_execution(db, 1, @step, 1, @base_ws)
      {:ok, %{id: s0}} = register(db, 1, 0)
      {:ok, %{id: s1}} = register(db, 1, 1)
      suspend(db, 1, 2)

      create_execution(db, 2, @step, 2, @base_ws)
      {:ok, %{id: ^s0, continued: true}} = register(db, 2, 0)
      suspend(db, 2, 3)

      create_execution(db, 3, @step, 3, @base_ws)
      assert {:ok, %{id: ^s0, continued: true}} = register(db, 3, 0)
      assert {:ok, %{id: ^s1, continued: true}} = register(db, 3, 1)
    end
  end

  describe "append_item/5" do
    test "rejects appends from an execution that isn't registered", %{db: db} do
      create_execution(db, 1, @step, 1, @base_ws)
      create_execution(db, 2, @step, 2, @base_ws)
      {:ok, %{id: stream_id}} = register(db, 1, 0)

      assert {:error, :not_producer} = Streams.append_item(db, stream_id, 2, 0, val("x"))
    end

    test "rejects appends after closure and duplicate sequences", %{db: db} do
      create_execution(db, 1, @step, 1, @base_ws)
      {:ok, %{id: stream_id}} = register(db, 1, 0)
      {:ok, _} = Streams.append_item(db, stream_id, 1, 0, val("x"))

      assert {:error, :already_appended} = Streams.append_item(db, stream_id, 1, 0, val("y"))
      {:ok, _} = Streams.close_stream(db, stream_id, 1, :complete)
      assert {:error, :closed} = Streams.append_item(db, stream_id, 1, 1, val("z"))
      assert {:error, :already_closed} = Streams.close_stream(db, stream_id, 1, :complete)
    end
  end

  describe "open streams" do
    test "distinguishes the step's open streams from an execution's own", %{db: db} do
      create_execution(db, 1, @step, 1, @base_ws)
      {:ok, %{id: s0}} = register(db, 1, 0)
      {:ok, %{id: s1}} = register(db, 1, 1)
      suspend(db, 1, 2)

      # The pending successor never registered anything.
      create_execution(db, 2, @step, 2, @base_ws)

      assert {:ok, [^s0, ^s1]} = Streams.get_open_stream_ids_for_step(db, @step, @base_ws)
      assert {:ok, []} = Streams.get_open_stream_ids_for_step(db, @step, @child_ws)
      assert {:ok, []} = Streams.get_open_stream_ids_for_execution(db, 2)
      assert {:ok, [^s0, ^s1]} = Streams.get_open_stream_ids_for_execution(db, 1)
    end

    test "summarises closures by the execution that wrote them", %{db: db} do
      create_execution(db, 1, @step, 1, @base_ws)
      {:ok, %{id: s0}} = register(db, 1, 0)
      {:ok, %{id: s1}} = register(db, 1, 1)
      {:ok, _} = Streams.close_stream(db, s0, 1, :timeout)
      {:ok, _} = Streams.close_stream(db, s1, 1, {:errored, "E", "boom", []})

      assert {:ok, %{timed_out: true, errored: error_id}} =
               Streams.get_closure_summary_for_execution(db, 1)

      assert is_integer(error_id)

      assert {:ok, %{timed_out: false, errored: nil}} =
               Streams.get_closure_summary_for_execution(db, 2)

      assert {:ok, {:errored, {"E", "boom", []}, 1, _}} = Streams.get_stream_closure(db, s1)
    end
  end

  describe "refs" do
    test "resolves a stream by its key and creates a stable ref", %{db: db} do
      create_execution(db, 1, @step, 1, @base_ws)
      {:ok, %{id: stream_id}} = register(db, 1, 0)

      assert {:ok, ^stream_id} = Streams.get_stream_id_by_key(db, "r1", 0, 0)
      assert {:error, :not_found} = Streams.get_stream_id_by_key(db, "r1", 0, 1)
      assert {:ok, ref_id} = Streams.create_stream_ref_for(db, stream_id)
      assert {:ok, ^ref_id} = Streams.create_stream_ref_for(db, stream_id)
      assert {:ok, {"r1", 0, 0, "module", "target"}} = Streams.get_stream_ref(db, ref_id)
      assert {:ok, id} = Streams.record_dependency(db, 1, ref_id)
      assert is_integer(id)
      assert {:ok, nil} = Streams.record_dependency(db, 1, ref_id)
      assert {:ok, %{1 => [^ref_id]}} = Streams.get_run_dependencies(db, 1)
    end
  end

  # --- helpers ---

  defp register(db, execution_id, position) do
    Streams.register(db, @step, @base_ws, execution_id, position, 0, nil) |> strip_created()
  end

  defp strip_created({:ok, registration}), do: {:ok, Map.delete(registration, :created_at)}

  defp suspend(db, execution_id, successor_id) do
    {:ok, _} = Results.record_completion(db, execution_id, :suspended, successor_id: nil)
    _ = successor_id
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
