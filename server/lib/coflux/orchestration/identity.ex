defmodule Coflux.Orchestration.Identity do
  @moduledoc """
  Resolves an execution's identity, as events carry it, from the database:
  every id external, plus the step's module, target and type and the run's
  root target. One query; resolved from `db` rather than server state so it
  is correct against archived epochs too.
  """

  alias Coflux.Orchestration.{Ids, Runs, Utils}

  def execution(db, execution_id) do
    case Runs.get_execution_identity(db, execution_id) do
      {:ok, {run, step, attempt, workspace, module, target, type, root_module, root_target}} ->
        {:ok,
         %{
           execution: Ids.execution(run, step, attempt),
           run: run,
           step: step,
           attempt: attempt,
           workspace: workspace,
           module: module,
           target: target,
           type: Utils.decode_step_type(type),
           root_module: root_module,
           root_target: root_target
         }}

      {:ok, nil} ->
        {:error, :not_found}
    end
  end
end
