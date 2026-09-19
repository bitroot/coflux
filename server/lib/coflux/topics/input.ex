defmodule Coflux.Topics.Input do
  @moduledoc "One input: its prompt, its response if any, and whether anything is waiting on it."

  use Topical.Topic, route: ["inputs", :input_id]

  alias Coflux.Orchestration
  alias Coflux.Topics.Diff
  alias Coflux.Topics.Input.Model

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    input_id = Map.fetch!(params, :input_id)

    case Orchestration.subscribe(project_id, {:input, input_id}, self()) do
      {:ok, events, ref} ->
        model = Model.fold(Model.new(), events)
        {:ok, Topic.new(Model.project(model), %{model: model, ref: ref})}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, events}, topic) do
    model = Model.fold(topic.state.model, events)
    topic = Diff.apply(topic, [], topic.value, Model.project(model))
    {:ok, %{topic | state: %{topic.state | model: model}}}
  end
end

defmodule Coflux.Topics.Input.Model do
  @moduledoc false

  import Kernel, except: [apply: 2]

  alias Coflux.Events.{InputActivated, InputDeactivated, InputDetails, InputResponded}

  def new, do: %{details: nil, response: nil, active: false}

  def fold(model, events), do: Enum.reduce(events, model, &apply(&2, &1))

  def apply(model, %InputDetails{} = e), do: %{model | details: e}
  def apply(model, %InputResponded{} = e), do: %{model | response: e.response}
  def apply(model, %InputActivated{}), do: %{model | active: true}
  def apply(model, %InputDeactivated{}), do: %{model | active: false}

  def project(%{details: nil}), do: %{}

  def project(%{details: details} = model) do
    %{
      key: details.key,
      template: details.template,
      placeholders:
        Map.new(details.placeholders, fn {placeholder, value} ->
          {placeholder, Coflux.TopicUtils.build_value(value)}
        end),
      schema: details.schema,
      initial: if(details.initial, do: Jason.decode!(details.initial)),
      title: details.title,
      actions: details.actions,
      requires: details.requires,
      createdAt: details.created_at,
      response: build_response(model.response),
      active: model.active
    }
  end

  defp build_response(nil), do: nil

  defp build_response(%{type: type, value: value, created_at: created_at, created_by: created_by}) do
    response = %{"type" => Atom.to_string(type), "createdAt" => created_at}
    response = if value, do: Map.put(response, "value", value), else: response

    if created_by do
      Map.put(response, "createdBy", %{
        "type" => created_by.type,
        "externalId" => created_by.external_id
      })
    else
      response
    end
  end
end
