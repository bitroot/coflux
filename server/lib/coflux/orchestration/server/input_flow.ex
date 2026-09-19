defmodule Coflux.Orchestration.Server.InputFlow do
  @moduledoc """
  Asking a person a question mid-run, and checking the answer.

  An execution that needs a decision submits an input: a prompt template
  with placeholder values, and a JSON Schema the answer must satisfy. The
  execution then waits on it like any other dependency, and whoever
  answers - through Studio or the API - releases it.

  Two kinds of validation happen here, and they are separate concerns.
  When the input is *created*, the schema itself must be well-formed and
  any initial value must satisfy it, so a run cannot ask an unanswerable
  question. When it is *answered*, the response is checked against that
  same stored schema, so a bad answer is rejected rather than handed to
  the waiting execution.

  Inputs are keyed, and a key is reused: an execution asking the same
  question twice - a retry re-running the same step, say - joins the
  existing input rather than raising a second one.
  """

  alias Coflux.Orchestration.{Inputs, Principals}

  alias Coflux.Orchestration.Server.{Permissions, Resolve}

  def decode_input_response_type(nil), do: nil
  def decode_input_response_type(1), do: :value
  def decode_input_response_type(2), do: :dismissed
  def decode_input_response_type(3), do: :cancelled

  def build_input_response(db, input_id) do
    case Inputs.get_input_response(db, input_id) do
      {:ok, nil} ->
        nil

      {:ok, {:value, value, created_at, created_by_id}} ->
        principal =
          case Principals.get_principal(db, created_by_id) do
            {:ok, {type, external_id}} -> %{type: type, external_id: external_id}
            {:ok, nil} -> nil
          end

        %{type: :value, value: value, created_at: created_at, created_by: principal}

      {:ok, {:dismissed, created_at, created_by_id}} ->
        principal =
          case Principals.get_principal(db, created_by_id) do
            {:ok, {type, external_id}} -> %{type: type, external_id: external_id}
            {:ok, nil} -> nil
          end

        %{type: :dismissed, value: nil, created_at: created_at, created_by: principal}

      {:ok, {:cancelled, created_at, created_by_id}} ->
        principal =
          case Principals.get_principal(db, created_by_id) do
            {:ok, {type, external_id}} -> %{type: type, external_id: external_id}
            {:ok, nil} -> nil
          end

        %{type: :cancelled, value: nil, created_at: created_at, created_by: principal}
    end
  end

  def build_input_details(
        db,
        input_id,
        key,
        prompt_id,
        schema_id,
        title,
        actions,
        initial,
        requires_tag_set_id,
        created_at
      ) do
    {:ok, template, placeholder_values} = Inputs.get_input_prompt(db, prompt_id)

    schema =
      if schema_id do
        {:ok, s} = Inputs.get_input_schema(db, schema_id)
        s
      end

    response = build_input_response(db, input_id)

    parsed_actions =
      if actions do
        Jason.decode!(actions)
      end

    resolved_placeholders =
      Map.new(placeholder_values, fn {k, v} -> {k, Resolve.value(db, v)} end)

    requires = Resolve.tag_set(db, requires_tag_set_id)

    %{
      key: key,
      template: template,
      placeholders: resolved_placeholders,
      schema: schema,
      initial: initial,
      title: title,
      actions: parsed_actions,
      requires: requires,
      created_at: created_at,
      response: response
    }
  end

  def validate_and_prepare_input(state, schema_json, initial) do
    with {:ok, schema_id} <- validate_and_get_schema(state, schema_json),
         :ok <- validate_initial_value(initial, schema_json, schema_id) do
      {:ok, schema_id}
    end
  end

  def validate_and_get_schema(_state, nil), do: {:ok, nil}

  def validate_and_get_schema(state, schema_json) do
    case Coflux.JsonSchema.validate_schema(schema_json) do
      :ok ->
        {:ok, id} = Inputs.get_or_create_schema(state.db, schema_json)
        {:ok, id}

      {:error, reason} ->
        {:error, {:invalid_schema, reason}}
    end
  end

  def validate_initial_value(nil, _schema_json, _schema_id), do: :ok

  def validate_initial_value(_initial, nil, _schema_id),
    do: {:error, {:invalid_initial, "initial value requires a schema"}}

  def validate_initial_value(initial, schema_json, _schema_id) do
    case Jason.decode(initial) do
      {:ok, initial_value} ->
        case Coflux.JsonSchema.validate_partial(initial_value, schema_json) do
          :ok -> :ok
          {:error, reason} -> {:error, {:invalid_initial, reason}}
        end

      {:error, _} ->
        {:error, {:invalid_initial, "initial value is not valid JSON"}}
    end
  end

  def find_or_create_input(
        state,
        key,
        run_id,
        workspace_id,
        execution_id,
        prompt_id,
        schema_id,
        title,
        actions,
        initial,
        requires_tag_set_id,
        now
      ) do
    if key do
      workspace_ids = Permissions.get_cache_workspace_ids(state, workspace_id)

      case Inputs.find_input_by_key(state.db, run_id, workspace_ids, key) do
        {:ok,
         {existing_id, existing_number, existing_prompt_id, existing_schema_id, existing_title,
          _existing_actions, existing_requires_tag_set_id}} ->
          if existing_prompt_id == prompt_id && existing_schema_id == schema_id &&
               existing_requires_tag_set_id == requires_tag_set_id do
            Inputs.record_execution_input(state.db, execution_id, existing_id, now)
            {:ok, existing_number, existing_title, false}
          else
            {:error, :input_mismatch}
          end

        {:ok, nil} ->
          {:ok, _id, input_number} =
            Inputs.create_input(
              state.db,
              run_id,
              execution_id,
              workspace_id,
              prompt_id,
              schema_id,
              key,
              title,
              actions,
              initial,
              requires_tag_set_id,
              now
            )

          {:ok, input_number, title, true}
      end
    else
      {:ok, _id, input_number} =
        Inputs.create_input(
          state.db,
          run_id,
          execution_id,
          workspace_id,
          prompt_id,
          schema_id,
          nil,
          title,
          actions,
          initial,
          requires_tag_set_id,
          now
        )

      {:ok, input_number, title, true}
    end
  end

  def validate_input_response(_state, nil, _value), do: :ok

  def validate_input_response(state, schema_id, value) do
    {:ok, schema_json} = Inputs.get_input_schema(state.db, schema_id)

    case Coflux.JsonSchema.validate_value(value, schema_json) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
