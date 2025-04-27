defmodule Coflux.Topics.Search do
  alias Coflux.Orchestration
  use Topical.Topic, route: ["projects", :project_id, "search", :workspace_id]

  def init(params) do
    project_id = Keyword.fetch!(params, :project_id)
    workspace_id = Keyword.fetch!(params, :workspace_id)

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
      Enum.reduce(targets, existing, fn {repository_name, repository_targets}, existing ->
        Enum.reduce(
          %{workflows: :workflow, sensors: :sensor},
          existing,
          fn {key, target_type}, existing ->
            repository_targets
            |> Map.fetch!(key)
            |> Enum.reduce(existing, fn target_name, existing ->
              existing_target = get_in(existing, [repository_name, target_name])

              if !existing_target || elem(existing_target, 0) != target_type do
                put_in(
                  existing,
                  [Access.key(repository_name, %{}), target_name],
                  {target_type, nil}
                )
              else
                existing
              end
            end)
          end
        )
      end)
    end)
  end

  defp process_notification(
         topic,
         {:step, repository, target_name, target_type, external_run_id, external_step_id, attempt}
       ) do
    update_in(topic.state.targets, fn targets ->
      put_in(
        targets,
        [Access.key(repository, %{}), target_name],
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
    Enum.flat_map(targets, fn {repository_name, repository} ->
      repository_name_parts = [repository_name | String.split(repository_name, ["_", "."])]

      repository
      |> Enum.map(fn {target_name, {target_type, latest_run}} ->
        target_name_parts = [target_name | String.split(target_name, "_")]

        target_parts =
          repository_name_parts
          |> Enum.concat(target_name_parts)
          |> Enum.reject(&(&1 == ""))
          |> Enum.uniq()

        {{target_type, repository_name, target_name, latest_run}, target_parts}
      end)
      |> Enum.concat([{{:repository, repository_name}, repository_name_parts}])
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
      {:repository, repository_name} ->
        %{
          type: "repository",
          name: repository_name
        }

      {type, repository_name, target_name, latest_run} when type in [:workflow, :sensor, :task] ->
        run =
          case latest_run do
            {run_id, step_id, attempt} -> %{runId: run_id, stepId: step_id, attempt: attempt}
            nil -> nil
          end

        %{
          type: Atom.to_string(type),
          repository: repository_name,
          name: target_name,
          run: run
        }
    end
  end
end
