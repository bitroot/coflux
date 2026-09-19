defmodule Coflux.EpochsTest do
  # Not async: the data directory is global.
  use ExUnit.Case, async: false

  alias Coflux.Store
  alias Coflux.Store.Epochs
  alias Exqlite.Sqlite3

  setup do
    dir =
      Path.join(
        System.tmp_dir!(),
        "coflux-epochs-test-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    previous = :persistent_term.get(:coflux_data_dir, nil)
    :persistent_term.put(:coflux_data_dir, dir)

    on_exit(fn ->
      if previous do
        :persistent_term.put(:coflux_data_dir, previous)
      else
        :persistent_term.erase(:coflux_data_dir)
      end

      File.rm_rf!(dir)
    end)

    {:ok, dir: dir}
  end

  describe "rotate/2" do
    test "leaves an archive the next epoch can't corrupt", %{dir: dir} do
      {:ok, epochs} = Epochs.open("proj", "orchestration")

      :ok = Sqlite3.execute(Epochs.active_db(epochs), "CREATE TABLE probe (value TEXT)")

      Store.with_transaction(Epochs.active_db(epochs), fn ->
        {:ok, _} =
          Store.insert_many(
            Epochs.active_db(epochs),
            "probe",
            {:value},
            Enum.map(1..500, &{"row-#{&1}"})
          )
      end)

      {:ok, epochs, archived_db} = Epochs.rotate(epochs, "20260101_0001")

      # What `Epoch.copy_config/2` does on every rotation: a write
      # transaction open on the new epoch while the archived one is read.
      # The new epoch's journal sits at the path the archived database used
      # to occupy, so a connection still opened against that path would
      # take it for its own — roll those pages into the archive, and
      # truncate the archive to the length the journal header records.
      Store.with_transaction(Epochs.active_db(epochs), fn ->
        :ok =
          Sqlite3.execute(
            Epochs.active_db(epochs),
            "CREATE TABLE carried_over (value TEXT)"
          )

        assert {:ok, [{500}]} = Store.query(archived_db, "SELECT COUNT(*) FROM probe")
      end)

      :ok = Epochs.close(epochs)

      # Read the archive back the way a later lookup would: a fresh
      # connection, opened on demand.
      path = Path.join([dir, "projects", "proj", "orchestration", "20260101_0001.sqlite"])
      {:ok, db} = Sqlite3.open(path)

      assert {:ok, [{"ok"}]} = Store.query(db, "PRAGMA integrity_check")
      assert {:ok, [{500}]} = Store.query(db, "SELECT COUNT(*) FROM probe")

      :ok = Sqlite3.close(db)
    end

    test "the new epoch starts empty and migrated", %{dir: dir} do
      {:ok, epochs} = Epochs.open("proj", "orchestration")
      :ok = Sqlite3.execute(Epochs.active_db(epochs), "CREATE TABLE probe (value TEXT)")

      {:ok, epochs, _archived_db} = Epochs.rotate(epochs, "20260101_0001")

      assert {:ok, []} =
               Store.query(
                 Epochs.active_db(epochs),
                 "SELECT name FROM sqlite_master WHERE name = 'probe'"
               )

      assert {:ok, [{"runs"}]} =
               Store.query(
                 Epochs.active_db(epochs),
                 "SELECT name FROM sqlite_master WHERE name = 'runs'"
               )

      :ok = Epochs.close(epochs)
      assert File.exists?(Path.join([dir, "projects", "proj", "orchestration.sqlite"]))
    end
  end
end
