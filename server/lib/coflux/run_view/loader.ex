defmodule Coflux.RunView.Loader do
  @moduledoc """
  Subscribes a topic process to a run and builds the view from the snapshot.
  """

  alias Coflux.Orchestration
  alias Coflux.RunView

  @doc """
  Returns `{:ok, view, run, parent, fetch}` with the view showing the
  workspace the run started in plus the one being viewed, or
  `{:error, :not_found}`. The view holds structure only; `fetch` loads the
  detail `RunView.missing_details/3` asks for (see `RunView.Sync.load/4`).
  """
  def load(project_id, run_id, workspace_id, pid) do
    case Orchestration.subscribe_run(project_id, run_id, pid) do
      {:error, :not_found} ->
        {:error, :not_found}

      {:ok, run, parent, steps, _ref} ->
        fetch = fn request ->
          case Orchestration.get_run_details(project_id, run_id, request) do
            {:ok, details} -> details
            {:error, :not_found} -> %{executions: %{}, steps: %{}}
          end
        end

        run_workspace_id =
          steps
          |> Map.values()
          |> Enum.reject(& &1.parent_id)
          |> Enum.min_by(& &1.created_at)
          |> Map.fetch!(:executions)
          |> Map.values()
          |> Enum.min_by(& &1.created_at)
          |> Map.fetch!(:workspace_id)

        workspace_ids = Enum.uniq([run_workspace_id, workspace_id])
        {:ok, RunView.new(run, steps, workspace_ids), run, parent, fetch}
    end
  end
end
