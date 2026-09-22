defmodule Coflux.FleetTest do
  use ExUnit.Case, async: true

  alias Coflux.Orchestration.Server.Fleet

  describe "pool_hosts_module?/2" do
    test "a name covers the module itself" do
      assert Fleet.pool_hosts_module?(["myapp"], "myapp")
    end

    test "a name covers its submodules, as discovery imports them" do
      assert Fleet.pool_hosts_module?(["myapp"], "myapp.workflows")
      assert Fleet.pool_hosts_module?(["myapp"], "myapp.jobs.nightly")
    end

    test "a name covers only whole components" do
      refute Fleet.pool_hosts_module?(["myapp"], "myapp2")
      refute Fleet.pool_hosts_module?(["myapp.work"], "myapp.workflows")
    end

    test "a submodule name doesn't reach its parent" do
      refute Fleet.pool_hosts_module?(["myapp.workflows"], "myapp")
    end

    test "any name in the list will do" do
      assert Fleet.pool_hosts_module?(["other", "myapp.tasks"], "myapp.tasks.io")
      refute Fleet.pool_hosts_module?(["other", "myapp.tasks"], "myapp.workflows")
    end

    test "no modules is no restriction" do
      assert Fleet.pool_hosts_module?([], "anything.at.all")
    end
  end

  describe "worker_args/1" do
    test "a pool's modules are the worker's arguments" do
      assert Fleet.worker_args(%{modules: ["myapp", "other"]}) == ["myapp", "other"]
    end

    test "a pool with no modules tells the worker to host everything" do
      assert Fleet.worker_args(%{modules: []}) == ["--all-modules"]
    end
  end
end
