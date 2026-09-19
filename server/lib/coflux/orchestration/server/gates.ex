defmodule Coflux.Orchestration.Server.Gates do
  @moduledoc """
  The queue's answer to "why isn't this running?": whatever the execution
  is waiting on, plus each concurrency gate that's holding it back, in the
  form the queue topic renders.

  Every kind is represented. The answer is wrong if a gate it can't name is
  dropped, so an unresolvable one keeps its type and loses only its
  identifier.
  """

  alias Coflux.Orchestration.{Ids, Inputs, Runs, Streams}

  @doc "The gates one execution is waiting on: its pending dependencies and its concurrency gates."
  def for_execution(state, execution_id) do
    pending = Map.get(state.pending_dependencies, execution_id, MapSet.new())
    describe(state.db, pending) ++ Map.get(state.concurrency_gated, execution_id, [])
  end

  @doc """
  The gates of every execution in the workspace that is waiting on
  something, keyed by execution external id. Gated executions belong here
  as much as blocked ones do: both are executions the queue is showing as
  not running for a reason.

  The ledgers are project-wide, so the workspace is filtered in SQL: one
  query narrows every blocked or gated execution in the project to the
  ones this workspace's queue can show, and only those are described.
  """
  def for_workspace(state, workspace_id) do
    execution_ids =
      Enum.uniq(Map.keys(state.pending_dependencies) ++ Map.keys(state.concurrency_gated))

    {:ok, keys} =
      Runs.get_execution_keys_in_workspace(state.db, execution_ids, workspace_id)

    Map.new(keys, fn {execution_id, {r, s, a}} ->
      {Ids.execution(r, s, a), for_execution(state, execution_id)}
    end)
  end

  @doc "Renders a set of pending dependency keys as gates."
  def describe(db, pending_dependency_ids) do
    Enum.map(pending_dependency_ids, fn
      {:execution, dependency_id} ->
        case Runs.get_execution_key(db, dependency_id) do
          {:ok, {r, s, a}} -> %{type: "execution", executionId: Ids.execution(r, s, a)}
          {:error, _} -> %{type: "execution", executionId: nil}
        end

      {:input, input_id} ->
        case Inputs.get_input_run_and_number(db, input_id) do
          {:ok, run_ext_id, number} ->
            %{type: "input", inputId: Ids.input(run_ext_id, number)}

          {:error, _} ->
            %{type: "input", inputId: nil}
        end

      {:stream, stream_id, sequence} ->
        case Streams.get_stream(db, stream_id) do
          {:ok, stream} ->
            %{
              type: "stream",
              stepId: Ids.step(stream.run_external_id, stream.step_number),
              index: stream.index,
              module: stream.module,
              target: stream.target,
              sequence: sequence
            }

          {:error, :not_found} ->
            %{
              type: "stream",
              stepId: nil,
              index: nil,
              module: nil,
              target: nil,
              sequence: sequence
            }
        end

      {:catalog, _workspace_id, path, number} ->
        %{type: "catalog", path: path, number: number}
    end)
  end
end
