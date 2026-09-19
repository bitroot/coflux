defmodule Coflux.Orchestration.Snapshots do
  @moduledoc """
  The initial events for a subscription key: what a topic folds before it
  starts receiving live events.

  A snapshot must be *fold-equivalent* to the history: folding it gives the
  same model as folding every event ever routed to the key. Compaction is
  the loader's job (latest-state tables yield only the latest; completed
  executions are absent because scheduled-then-completed folds to absence),
  and every event is filled in full, exactly as the emit site would fill
  it.
  """

  alias Coflux.Events.{
    ExecutionAssigned,
    ExecutionScheduled,
    ExecutionWaiting,
    InputActivated,
    ManifestRegistered,
    RunCreated,
    RunOutcome,
    TokenCreated
  }

  alias Coflux.Orchestration.{
    Gates,
    Ids,
    Inputs,
    Manifests,
    Principals,
    Results,
    Runs,
    TagSets,
    Workspaces
  }

  alias Coflux.Store.{Epochs, Index}

  @target_runs_archive_depth 5

  def load(state, {:queue, workspace_external_id}, _opts) do
    with {:ok, workspace_id} <- resolve_workspace(state, workspace_external_id) do
      gates = Gates.for_workspace(state, workspace_id)

      events =
        state
        |> active_executions(workspace_id)
        |> Enum.flat_map(fn {scheduled, assigned} ->
          [scheduled | List.wrap(assigned)] ++ waiting(scheduled, gates)
        end)

      {:ok, events}
    end
  end

  def load(state, {:modules, workspace_external_id}, _opts) do
    with {:ok, workspace_id} <- resolve_workspace(state, workspace_external_id) do
      manifests = manifest_events(state.db, workspace_id, workspace_external_id)

      executions =
        state
        |> active_executions(workspace_id)
        |> Enum.flat_map(fn {scheduled, assigned} -> [scheduled | List.wrap(assigned)] end)

      {:ok, manifests ++ executions}
    end
  end

  def load(state, {:workflow, module, target, workspace_external_id}, opts) do
    max_runs = Keyword.fetch!(opts, :max_runs)

    with {:ok, workspace_id} <- resolve_workspace(state, workspace_external_id) do
      manifest = manifest_event(state.db, workspace_id, workspace_external_id, module)
      runs = target_runs(state, module, target, workspace_id, max_runs)

      if (is_nil(manifest) or not Map.has_key?(manifest.workflows, target)) and runs == [] do
        {:error, :not_found}
      else
        run_events =
          Enum.flat_map(runs, fn {run, created_at, user_ext_id, token_ext_id, outcome} ->
            created = %RunCreated{
              run: run,
              workspace: workspace_external_id,
              root_module: module,
              root_target: target,
              type: :workflow,
              created_at: created_at,
              created_by: principal(user_ext_id, token_ext_id)
            }

            if outcome do
              [
                created,
                %RunOutcome{
                  run: run,
                  workspace: workspace_external_id,
                  root_module: module,
                  root_target: target,
                  outcome: outcome
                }
              ]
            else
              [created]
            end
          end)

        executions =
          state
          |> active_executions(workspace_id, {module, target})
          |> Enum.flat_map(fn {scheduled, assigned} -> [scheduled | List.wrap(assigned)] end)

        {:ok, List.wrap(manifest) ++ run_events ++ executions}
      end
    end
  end

  def load(state, {:targets, workspace_external_id}, _opts) do
    with {:ok, workspace_id} <- resolve_workspace(state, workspace_external_id) do
      manifests = manifest_events(state.db, workspace_id, workspace_external_id)
      {:ok, rows} = Runs.get_latest_executions_for_workspace(state.db, workspace_id)
      tag_sets = tag_sets(state.db, rows)
      {:ok, manifests ++ Enum.map(rows, &scheduled_event(&1, tag_sets))}
    end
  end

  def load(state, {:manifests, workspace_external_id}, _opts) do
    with {:ok, workspace_id} <- resolve_workspace(state, workspace_external_id) do
      {:ok, manifest_events(state.db, workspace_id, workspace_external_id)}
    end
  end

  # Every unanswered input an in-flight execution is waiting on.
  def load(state, {:inputs, workspace_external_id}, _opts) do
    with {:ok, workspace_id} <- resolve_workspace(state, workspace_external_id) do
      {:ok, rows} = Inputs.get_inputs_for_workspace(state.db, workspace_id)

      events =
        Enum.map(rows, fn {_id, run_external_id, number, _workspace_id, _key, _prompt_id,
                           _schema_id, created_at, title, requires_tag_set_id, _response_type} ->
          %InputActivated{
            workspace: workspace_external_id,
            input: Ids.input(run_external_id, number),
            run: run_external_id,
            created_at: created_at,
            title: title,
            requires: tag_set(state.db, requires_tag_set_id)
          }
        end)

      {:ok, events}
    end
  end

  # Revoked tokens are absent: created-then-revoked folds to absence.
  def load(state, :tokens, _opts) do
    {:ok, tokens} = Principals.list_tokens(state.db)

    events =
      tokens
      |> Enum.reject(& &1.revoked_at)
      |> Enum.map(fn token ->
        %TokenCreated{
          token: token.external_id,
          id: token.id,
          name: token.name,
          workspaces: token.workspaces,
          created_at: token.created_at,
          expires_at: token.expires_at,
          created_by: token.created_by
        }
      end)

    {:ok, events}
  end

  # ---------------------------------------------------------------------------
  # Executions

  # Every execution in the workspace without a completion, as the events
  # that describe it: `{scheduled, assigned | nil}`. With a target, only
  # the executions of runs that started there - narrowed in SQL, since a
  # workspace can hold far more in flight than one workflow's share.
  defp active_executions(state, workspace_id, target \\ nil) do
    {:ok, rows} =
      case target do
        nil ->
          Runs.get_active_executions(state.db, workspace_id)

        {module, target} ->
          Runs.get_active_executions_for_target(state.db, workspace_id, module, target)
      end

    tag_sets = tag_sets(state.db, rows)

    Enum.map(rows, fn row ->
      {scheduled_event(row, tag_sets), if(row.assigned_at, do: struct(ExecutionAssigned, row))}
    end)
  end

  defp scheduled_event(row, tag_sets) do
    requires =
      tag_sets
      |> Map.get(row.run_requires_tag_set_id, %{})
      |> Map.merge(Map.get(tag_sets, row.step_requires_tag_set_id, %{}))
      |> Map.reject(fn {_key, values} -> values == [] end)

    struct(ExecutionScheduled, Map.put(row, :requires, requires))
  end

  defp waiting(scheduled, gates) do
    case Map.get(gates, scheduled.execution, []) do
      [] ->
        []

      gates ->
        [
          %ExecutionWaiting{
            execution: scheduled.execution,
            run: scheduled.run,
            workspace: scheduled.workspace,
            gates: gates
          }
        ]
    end
  end

  defp tag_sets(db, rows) do
    rows
    |> Enum.flat_map(&[&1.run_requires_tag_set_id, &1.step_requires_tag_set_id])
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Map.new(fn tag_set_id ->
      {:ok, tag_set} = TagSets.get_tag_set(db, tag_set_id)
      {tag_set_id, tag_set}
    end)
  end

  # ---------------------------------------------------------------------------
  # Manifests

  defp manifest_events(db, workspace_id, workspace_external_id) do
    {:ok, manifests} = Manifests.get_latest_manifests(db, workspace_id)
    instructions = instructions(db, Map.values(manifests))

    Enum.map(manifests, fn {module, workflows} ->
      %ManifestRegistered{
        workspace: workspace_external_id,
        module: module,
        workflows: with_instructions(workflows, instructions)
      }
    end)
  end

  defp manifest_event(db, workspace_id, workspace_external_id, module) do
    case Manifests.get_latest_manifest(db, workspace_id, module) do
      {:ok, nil} ->
        nil

      {:ok, workflows} ->
        %ManifestRegistered{
          workspace: workspace_external_id,
          module: module,
          workflows: with_instructions(workflows, instructions(db, [workflows]))
        }
    end
  end

  # The content of every instruction the manifests reference, in one
  # query rather than one per workflow.
  defp instructions(db, manifests) do
    ids =
      manifests
      |> Enum.flat_map(&Enum.map(&1, fn {_name, workflow} -> workflow.instruction_id end))
      |> Enum.reject(&is_nil/1)

    {:ok, contents} = Manifests.get_instructions(db, ids)
    contents
  end

  # The shape the API registers: instruction content in place of the stored id.
  defp with_instructions(workflows, instructions) do
    Map.new(workflows, fn {name, workflow} ->
      {instruction_id, workflow} = Map.pop(workflow, :instruction_id)
      content = if instruction_id, do: Map.get(instructions, instruction_id)
      {name, Map.put(workflow, :instruction, content)}
    end)
  end

  # ---------------------------------------------------------------------------
  # Runs

  # The workspace's most recent runs of a workflow, newest first, reaching
  # into archived epochs when the active one has fewer than `limit`.
  defp target_runs(state, module, target, workspace_id, limit) do
    {:ok, live_runs} =
      Runs.get_target_runs(state.db, module, target, :workflow, workspace_id, limit)

    runs = resolve_run_outcomes(state.db, live_runs)

    if length(runs) < limit do
      workspace_external_id = state.workspaces[workspace_id].external_id

      unindexed_map =
        state.epochs
        |> Epochs.unindexed_dbs()
        |> Map.new()

      indexed_epoch_ids = Index.indexed_epoch_ids(state.epoch_index, "runs")

      archives =
        (Map.keys(unindexed_map) ++ indexed_epoch_ids)
        |> Enum.sort(:desc)
        |> Enum.take(@target_runs_archive_depth)
        |> Enum.map(fn epoch_id ->
          case Map.fetch(unindexed_map, epoch_id) do
            {:ok, db} -> {:open, db}
            :error -> {:closed, epoch_id}
          end
        end)

      seen = MapSet.new(runs, &elem(&1, 0))

      archives
      |> Enum.reduce_while({runs, seen}, fn archive, {acc, seen} ->
        remaining = limit - length(acc)

        archive_runs =
          archive_target_runs(state, archive, module, target, workspace_external_id, remaining)

        new_runs = Enum.reject(archive_runs, &MapSet.member?(seen, elem(&1, 0)))
        new_seen = MapSet.union(seen, MapSet.new(new_runs, &elem(&1, 0)))
        combined = acc ++ new_runs

        if length(combined) >= limit do
          {:halt, {Enum.take(combined, limit), new_seen}}
        else
          {:cont, {combined, new_seen}}
        end
      end)
      |> elem(0)
    else
      runs
    end
  end

  defp archive_target_runs(_state, {:open, db}, module, target, workspace_external_id, limit) do
    query_archive_target_runs(db, module, target, workspace_external_id, limit)
  end

  defp archive_target_runs(
         state,
         {:closed, epoch_id},
         module,
         target,
         workspace_external_id,
         limit
       ) do
    path = Epochs.archive_path(state.epochs, epoch_id)

    case Exqlite.Sqlite3.open(path) do
      {:ok, db} ->
        try do
          query_archive_target_runs(db, module, target, workspace_external_id, limit)
        after
          Exqlite.Sqlite3.close(db)
        end

      {:error, _} ->
        []
    end
  end

  defp query_archive_target_runs(db, module, target, workspace_external_id, limit) do
    case Workspaces.get_workspace_id(db, workspace_external_id) do
      {:ok, workspace_id} when not is_nil(workspace_id) ->
        {:ok, runs} = Runs.get_target_runs(db, module, target, :workflow, workspace_id, limit)
        resolve_run_outcomes(db, runs)

      {:ok, nil} ->
        []
    end
  end

  # Swaps each run's initial execution id for the outcome it resolved to.
  # Done per database, since the completion (and any successor it handed
  # off to) lives in the same epoch as the run.
  defp resolve_run_outcomes(db, runs) do
    Enum.map(runs, fn {external_id, created_at, user_ext_id, token_ext_id, initial_execution_id} ->
      {external_id, created_at, user_ext_id, token_ext_id,
       Results.run_outcome(db, initial_execution_id)}
    end)
  end

  defp tag_set(_db, nil), do: %{}

  defp tag_set(db, tag_set_id) do
    {:ok, tag_set} = TagSets.get_tag_set(db, tag_set_id)
    tag_set
  end

  defp principal(nil, nil), do: nil
  defp principal(user_external_id, nil), do: %{type: "user", external_id: user_external_id}
  defp principal(nil, token_external_id), do: %{type: "token", external_id: token_external_id}

  # ---------------------------------------------------------------------------

  defp resolve_workspace(state, external_id) do
    case Map.fetch(state.workspace_external_ids, external_id) do
      {:ok, workspace_id} -> {:ok, workspace_id}
      :error -> {:error, :workspace_invalid}
    end
  end
end
