defmodule Coflux.Topics.Search do
  alias Coflux.Orchestration

  use Topical.Topic, route: ["projects", :project_id, "search", :workspace_id]

  import Coflux.TopicUtils, only: [validate_project_access: 2]

  def connect(params, context) do
    namespace = Map.get(context, :namespace)

    with :ok <- validate_project_access(params.project_id, namespace) do
      {:ok, params}
    end
  end

  def init(params) do
    project_id = Map.fetch!(params, :project_id)
    workspace_id = Map.fetch!(params, :workspace_id)

    case Orchestration.subscribe_targets(project_id, workspace_id, self()) do
      {:ok, targets, _ref} ->
        topic = Topical.Topic.new(nil, %{targets: targets})
        {:ok, topic}
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification(&2, &1))
    {:ok, topic}
  end

  defp process_notification(topic, {:manifests, targets}) do
    update_in(topic.state.targets, fn existing ->
      Enum.reduce(targets, existing, fn {module_name, workflow_names}, existing ->
        Enum.reduce(workflow_names, existing, fn target_name, existing ->
          existing_target = get_in(existing, [module_name, target_name])

          if !existing_target || elem(existing_target, 0) != :workflow do
            put_in(
              existing,
              [Access.key(module_name, %{}), target_name],
              {:workflow, nil}
            )
          else
            existing
          end
        end)
      end)
    end)
  end

  defp process_notification(
         topic,
         {:step, module, target_name, target_type, external_run_id, external_step_id, attempt}
       ) do
    update_in(topic.state.targets, fn targets ->
      put_in(
        targets,
        [Access.key(module, %{}), target_name],
        {target_type, {external_run_id, external_step_id, attempt}}
      )
    end)
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
            {run_id, step_id, attempt} -> %{runId: run_id, stepId: step_id, attempt: attempt}
            nil -> nil
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
