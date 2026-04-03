defmodule Coflux.Metrics.Supervisor do
  @moduledoc """
  DynamicSupervisor for per-project metric servers.

  Manages a pool of Metrics.Server processes, one per active project.
  Servers are started on-demand when metrics are written or subscribed to.
  """

  use DynamicSupervisor

  alias Coflux.Metrics.Server

  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Get or start a metrics server for the given project.

  Returns {:ok, pid} if successful.
  """
  def get_server(project_id) do
    case Registry.lookup(Coflux.Metrics.Registry, project_id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        start_server(project_id)
    end
  end

  defp start_server(project_id) do
    spec = {Server, project_id: project_id}

    case DynamicSupervisor.start_child(__MODULE__, spec) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}
    end
  end
end
