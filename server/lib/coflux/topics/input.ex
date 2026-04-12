defmodule Coflux.Topics.Input do
  use Topical.Topic, route: ["inputs", :input_id]

  alias Coflux.Orchestration

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    input_id = Map.fetch!(params, :input_id)

    case Orchestration.subscribe_input(project_id, input_id, self()) do
      {:ok, input, ref} ->
        placeholders =
          Map.new(input.placeholders, fn {placeholder, value} ->
            {placeholder, Coflux.TopicUtils.build_value(value)}
          end)

        response = build_response(input.response)

        initial =
          if input.initial do
            Jason.decode!(input.initial)
          end

        {:ok,
         Topic.new(
           %{
             key: input.key,
             template: input.template,
             placeholders: placeholders,
             schema: input.schema,
             initial: initial,
             title: input.title,
             actions: input.actions,
             createdAt: input.created_at,
             response: response,
             active: input.active
           },
           %{ref: ref}
         )}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, notifications}, topic) do
    topic = Enum.reduce(notifications, topic, &process_notification/2)
    {:ok, topic}
  end

  defp process_notification({:response, response}, topic) do
    Topic.set(topic, [:response], build_response(response))
  end

  defp process_notification({:active, active}, topic) do
    Topic.set(topic, [:active], active)
  end

  defp build_response(nil), do: nil

  defp build_response(%{type: type, value: value, created_at: created_at, created_by: created_by}) do
    resp = %{
      "type" => Atom.to_string(type),
      "createdAt" => created_at
    }

    resp = if value, do: Map.put(resp, "value", value), else: resp

    if created_by do
      Map.put(resp, "createdBy", %{
        "type" => created_by.type,
        "externalId" => created_by.external_id
      })
    else
      resp
    end
  end
end
