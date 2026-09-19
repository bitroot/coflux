defmodule Coflux.Topics.Search do
  @moduledoc """
  Target search for a workspace. The topic has no value; it answers `query`
  from the targets it knows: every workflow in a registered manifest and
  every target that has run, with the latest execution of each.
  """

  alias Coflux.Orchestration
  alias Coflux.Orchestration.Ids
  alias Coflux.Topics.Search.Model

  use Topical.Topic, route: ["workspaces", :workspace_id, "search"]

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.subscribe(project_id, {:targets, workspace_id}, self()) do
      {:ok, events, ref} ->
        targets = Model.fold(Model.new(), events)
        {:ok, Topical.Topic.new(nil, %{targets: targets, ref: ref})}

      {:error, :workspace_invalid} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, events}, topic) do
    targets = Model.fold(topic.state.targets, events)
    {:ok, %{topic | state: %{topic.state | targets: targets}}}
  end

  def handle_execute("query", {query}, topic, _context) do
    query_parts = String.split(query)

    matches =
      if Enum.any?(query_parts) do
        topic.state.targets
        |> generate_candidates()
        |> Enum.map(fn {candidate, candidate_parts} ->
          {score_candidate(candidate_parts, query_parts), candidate}
        end)
        |> Enum.filter(fn {score, _match} -> score > 0 end)
        |> Enum.sort_by(fn {score, _match} -> score end, :desc)
        |> Enum.take(20)
        |> Enum.map(fn {score, match} ->
          Map.put(build_match(match), :score, score)
        end)
      else
        []
      end

    {:ok, matches, topic}
  end

  defp generate_candidates(targets) do
    Enum.flat_map(targets, fn {module_name, module} ->
      module_name_parts = [module_name | String.split(module_name, ["_", "."])]

      module
      |> Enum.map(fn {target_name, {target_type, latest_run}} ->
        target_name_parts = [target_name | String.split(target_name, "_")]

        target_parts =
          module_name_parts
          |> Enum.concat(target_name_parts)
          |> Enum.reject(&(&1 == ""))
          |> Enum.uniq()

        {{target_type, module_name, target_name, latest_run}, target_parts}
      end)
      |> Enum.concat([{{:module, module_name}, module_name_parts}])
    end)
  end

  defp score_part(candidate, query) do
    if String.starts_with?(candidate, query) do
      String.length(query) / String.length(candidate)
    else
      0
    end
  end

  defp score_candidate(candidate_parts, query_parts) do
    query_parts
    |> Enum.map(fn query_part ->
      candidate_parts
      |> Enum.map(&score_part(&1, query_part))
      |> Enum.max()
    end)
    |> Enum.product()
  end

  defp build_match(match) do
    case match do
      {:module, module_name} ->
        %{
          type: "module",
          name: module_name
        }

      {type, module_name, target_name, latest_run} when type in [:workflow, :task] ->
        run =
          case latest_run do
            {run_id, step_id, attempt} ->
              %{
                runId: run_id,
                stepId: Ids.step(run_id, step_id),
                stepNumber: step_id,
                attempt: attempt
              }

            nil ->
              nil
          end

        %{
          type: Atom.to_string(type),
          module: module_name,
          name: target_name,
          run: run
        }
    end
  end
end

defmodule Coflux.Topics.Search.Model do
  @moduledoc """
  The search index as a fold over events: `%{module => %{target => {type,
  latest}}}`, where `latest` is `{run, step, attempt}` of the most recently
  scheduled execution, or nil for a workflow that has only been registered.
  """

  import Kernel, except: [apply: 2]

  alias Coflux.Events.{ExecutionScheduled, ManifestRegistered, ModuleArchived}

  def new, do: %{}

  def fold(targets, events), do: Enum.reduce(events, targets, &apply(&2, &1))

  # A registered workflow that has already run keeps its latest execution.
  def apply(targets, %ManifestRegistered{} = e) do
    Enum.reduce(Map.keys(e.workflows), targets, fn target, targets ->
      case get_in(targets, [e.module, target]) do
        {:workflow, _latest} -> targets
        _ -> put_in(targets, [Access.key(e.module, %{}), target], {:workflow, nil})
      end
    end)
  end

  # Archiving forgets the module's workflows that never ran; anything that
  # has run stays searchable by its latest execution.
  def apply(targets, %ModuleArchived{} = e) do
    case Map.fetch(targets, e.module) do
      {:ok, module} ->
        case Map.reject(module, fn {_target, entry} -> entry == {:workflow, nil} end) do
          remaining when map_size(remaining) == 0 -> Map.delete(targets, e.module)
          remaining -> Map.put(targets, e.module, remaining)
        end

      :error ->
        targets
    end
  end

  def apply(targets, %ExecutionScheduled{} = e) do
    put_in(targets, [Access.key(e.module, %{}), e.target], {e.type, {e.run, e.step, e.attempt}})
  end
end
