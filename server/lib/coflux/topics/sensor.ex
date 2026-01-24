defmodule Coflux.Topics.Sensor do
  use Topical.Topic,
    route: ["projects", :project_id, "sensors", :module, :target, :space_id]

  alias Coflux.Orchestration

  import Coflux.TopicUtils, only: [validate_project_access: 2]

  def connect(params, context) do
    namespace = Map.get(context, :namespace)

    with :ok <- validate_project_access(params.project_id, namespace) do
      {:ok, params}
    end
  end

  def init(params) do
    project_id = Map.fetch!(params, :project_id)
    module = Map.fetch!(params, :module)
    target_name = Map.fetch!(params, :target)
    space_id = String.to_integer(Map.fetch!(params, :space_id))

    case Orchestration.subscribe_sensor(
           project_id,
           module,
           target_name,
           space_id,
           self()
         ) do
      {:ok, sensor, instruction, runs, ref} ->
        value = %{
          parameters: if(sensor, do: build_parameters(sensor.parameters)),
          instruction: instruction,
          configuration: build_configuration(sensor),
          runs: build_runs(runs)
        }

        {:ok, Topic.new(value, %{ref: ref})}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification/2)
    {:ok, topic}
  end

  defp process_notification({:target, target}, topic) do
    topic
    |> Topic.set([:parameters], build_parameters(target.parameters))
    |> Topic.set([:instruction], target.instruction)
    |> Topic.set([:configuration], build_configuration(target))
  end

  defp process_notification({:run, external_run_id, created_at}, topic) do
    Topic.set(
      topic,
      [:runs, external_run_id],
      %{id: external_run_id, createdAt: created_at}
    )
  end

  defp build_parameters(parameters) do
    Enum.map(parameters, fn {name, default, annotation} ->
      %{name: name, default: default, annotation: annotation}
    end)
  end

  defp build_configuration(sensor) do
    if sensor do
      %{
        requires: sensor.requires
      }
    end
  end

  defp build_runs(runs) do
    Map.new(runs, fn {external_run_id, created_at} ->
      {external_run_id, %{id: external_run_id, createdAt: created_at}}
    end)
  end
end
