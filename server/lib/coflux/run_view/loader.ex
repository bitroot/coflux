defmodule Coflux.RunView.Loader do
  @moduledoc """
  Subscribes a topic process to a run and builds the view from the snapshot.
  """

  alias Coflux.Events.RunCreated
  alias Coflux.Orchestration
  alias Coflux.RunView

  @doc """
  Returns `{:ok, view, fetch}` with the view showing the workspace the run
  started in plus the one being viewed, or `{:error, :not_found}`. The view
  holds structure only; `fetch` loads the detail `RunView.missing_details/3`
  asks for (see `RunView.Sync.load/4`).
  """
  def load(project_id, run_id, workspace_id, pid) do
    case Orchestration.subscribe(project_id, {:run, run_id}, pid) do
      {:error, :not_found} ->
        {:error, :not_found}

      {:ok, events, _ref} ->
        fetch = fn request ->
          case Orchestration.get_run_details(project_id, run_id, request) do
            {:ok, events} -> events
            {:error, :not_found} -> []
          end
        end

        workspace_ids =
          [run_workspace_id(events), workspace_id]
          |> Enum.reject(&is_nil/1)
          |> Enum.uniq()

        {:ok, RunView.new(events, workspace_ids), fetch}
    end
  end

  # The workspace the run started in, which `RunCreated` already carries.
  defp run_workspace_id(events) do
    case Enum.find(events, &match?(%RunCreated{}, &1)) do
      %RunCreated{workspace: workspace} -> workspace
      nil -> nil
    end
  end
end
