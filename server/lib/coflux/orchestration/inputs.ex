defmodule Coflux.Orchestration.Inputs do
  import Coflux.Store

  alias Coflux.Orchestration.Values

  # Response types
  @type_value 1
  @type_dismissed 2

  def type_value, do: @type_value
  def type_dismissed, do: @type_dismissed

  # --- Schema deduplication ---

  def get_or_create_schema(db, schema_json) do
    hash = :crypto.hash(:sha256, schema_json)

    case query_one(db, "SELECT id FROM input_schemas WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {id}} ->
        {:ok, id}

      {:ok, nil} ->
        insert_one(db, :input_schemas, %{hash: {:blob, hash}, schema: schema_json})
    end
  end

  # --- Prompt deduplication ---

  defp hash_prompt(template, placeholder_value_ids) do
    data =
      [template]
      |> Enum.concat(
        placeholder_value_ids
        |> Enum.sort_by(fn {placeholder, _value_id} -> placeholder end)
        |> Enum.flat_map(fn {placeholder, value_id} ->
          [placeholder, Integer.to_string(value_id)]
        end)
      )
      |> Enum.intersperse(0)

    :crypto.hash(:sha256, data)
  end

  def get_or_create_prompt(db, template, placeholder_value_ids) do
    with_transaction(db, fn ->
      hash = hash_prompt(template, placeholder_value_ids)

      case query_one(db, "SELECT id FROM input_prompts WHERE hash = ?1", {{:blob, hash}}) do
        {:ok, {id}} ->
          {:ok, id}

        {:ok, nil} ->
          {:ok, prompt_id} =
            insert_one(db, :input_prompts, %{hash: {:blob, hash}, template: template})

          {:ok, _} =
            insert_many(
              db,
              :input_prompt_placeholders,
              {:prompt_id, :placeholder, :value_id},
              Enum.map(placeholder_value_ids, fn {placeholder, value_id} ->
                {prompt_id, placeholder, value_id}
              end)
            )

          {:ok, prompt_id}
      end
    end)
  end

  # --- Input CRUD ---

  defp get_next_input_number(db, run_id) do
    case query(
           db,
           """
           SELECT COALESCE(MAX(number), 0) + 1
           FROM inputs
           WHERE run_id = ?1
           """,
           {run_id}
         ) do
      {:ok, [{next_number}]} ->
        {:ok, next_number}
    end
  end

  def create_input(
        db,
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
      ) do
    with_transaction(db, fn ->
      {:ok, input_number} = get_next_input_number(db, run_id)

      {:ok, input_id} =
        insert_one(db, :inputs, %{
          run_id: run_id,
          number: input_number,
          workspace_id: workspace_id,
          key: key,
          prompt_id: prompt_id,
          schema_id: schema_id,
          title: title,
          actions: actions,
          initial: initial,
          requires_tag_set_id: requires_tag_set_id,
          created_at: now
        })

      {:ok, _} =
        insert_one(db, :execution_inputs, %{
          execution_id: execution_id,
          input_id: input_id,
          created_at: now
        })

      {:ok, input_id, input_number}
    end)
  end

  def record_execution_input(db, execution_id, input_id, now) do
    insert_one(
      db,
      :execution_inputs,
      %{execution_id: execution_id, input_id: input_id, created_at: now},
      on_conflict: "DO NOTHING"
    )
  end

  def find_input_by_key(db, run_id, workspace_ids, key) do
    placeholders =
      workspace_ids
      |> Enum.with_index(2)
      |> Enum.map_intersperse(", ", fn {_, i} -> "?#{i}" end)
      |> Enum.join()

    case query(
           db,
           """
           SELECT i.id, i.number, i.prompt_id, i.schema_id, i.title, i.actions,
                  i.requires_tag_set_id
           FROM inputs AS i
           WHERE i.run_id = ?1
             AND i.workspace_id IN (#{placeholders})
             AND i.key = ?#{length(workspace_ids) + 2}
           ORDER BY i.created_at DESC
           LIMIT 1
           """,
           List.to_tuple([run_id] ++ workspace_ids ++ [key])
         ) do
      {:ok, [{id, number, prompt_id, schema_id, title, actions, requires_tag_set_id}]} ->
        {:ok, {id, number, prompt_id, schema_id, title, actions, requires_tag_set_id}}

      {:ok, []} ->
        {:ok, nil}
    end
  end

  def record_input_dependency(db, execution_id, input_id, now) do
    case query_one(
           db,
           "SELECT 1 FROM input_dependencies WHERE execution_id = ?1 AND input_id = ?2",
           {execution_id, input_id}
         ) do
      {:ok, nil} ->
        insert_one(db, :input_dependencies, %{
          execution_id: execution_id,
          input_id: input_id,
          created_at: now
        })

        {:ok, true}

      {:ok, _} ->
        {:ok, false}
    end
  end

  def is_input_responded?(db, input_id) do
    case query_one(db, "SELECT 1 FROM input_responses WHERE input_id = ?1", {input_id}) do
      {:ok, nil} -> false
      {:ok, _} -> true
    end
  end

  def get_input_response(db, input_id) do
    case query_one(
           db,
           """
           SELECT ir.type, ir.value, ir.created_at, ir.created_by
           FROM input_responses AS ir
           WHERE ir.input_id = ?1
           """,
           {input_id}
         ) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, {@type_value, value_json, created_at, created_by}} ->
        {:ok, {:value, Jason.decode!(value_json), created_at, created_by}}

      {:ok, {@type_dismissed, _value, created_at, created_by}} ->
        {:ok, {:dismissed, created_at, created_by}}
    end
  end

  def record_input_response(db, input_id, type, value_json, now, created_by) do
    case query_one(db, "SELECT 1 FROM input_responses WHERE input_id = ?1", {input_id}) do
      {:ok, nil} ->
        insert_one(db, :input_responses, %{
          input_id: input_id,
          type: type,
          value: value_json,
          created_at: now,
          created_by: created_by
        })

        {:ok, true}

      {:ok, _} ->
        {:error, :already_responded}
    end
  end

  def get_input_by_run_and_number(db, run_external_id, input_number) do
    query_one(
      db,
      """
      SELECT i.id, i.workspace_id, i.key, i.prompt_id, i.schema_id,
             i.title, i.actions, i.initial, i.requires_tag_set_id, i.created_at, r.id AS run_id
      FROM inputs AS i
      INNER JOIN runs AS r ON r.id = i.run_id
      WHERE r.external_id = ?1 AND i.number = ?2
      """,
      {run_external_id, input_number}
    )
  end

  def get_input_by_id(db, id) do
    query_one(
      db,
      """
      SELECT r.external_id, i.number, i.key, i.prompt_id, i.schema_id, i.title, i.actions,
             i.requires_tag_set_id, i.created_at
      FROM inputs AS i
      INNER JOIN runs AS r ON r.id = i.run_id
      WHERE i.id = ?1
      """,
      {id}
    )
  end

  def get_input_run_and_number(db, id) do
    case query_one(
           db,
           """
           SELECT r.external_id, i.number
           FROM inputs AS i
           INNER JOIN runs AS r ON r.id = i.run_id
           WHERE i.id = ?1
           """,
           {id}
         ) do
      {:ok, {run_ext_id, number}} -> {:ok, run_ext_id, number}
      {:ok, nil} -> {:error, :not_found}
    end
  end

  def get_input_id_by_run_and_number(db, run_external_id, input_number) do
    case query_one(
           db,
           """
           SELECT i.id
           FROM inputs AS i
           INNER JOIN runs AS r ON r.id = i.run_id
           WHERE r.external_id = ?1 AND i.number = ?2
           """,
           {run_external_id, input_number}
         ) do
      {:ok, {id}} -> {:ok, id}
      {:ok, nil} -> {:error, :not_found}
    end
  end

  def get_input_prompt(db, prompt_id) do
    case query_one(db, "SELECT template FROM input_prompts WHERE id = ?1", {prompt_id}) do
      {:ok, {template}} ->
        {:ok, placeholders} =
          query(
            db,
            "SELECT placeholder, value_id FROM input_prompt_placeholders WHERE prompt_id = ?1",
            {prompt_id}
          )

        placeholder_values =
          Map.new(placeholders, fn {placeholder, value_id} ->
            {:ok, value} = Values.get_value_by_id(db, value_id)
            {placeholder, value}
          end)

        {:ok, template, placeholder_values}

      {:ok, nil} ->
        {:error, :not_found}
    end
  end

  def get_input_schema(db, schema_id) do
    case query_one(db, "SELECT schema FROM input_schemas WHERE id = ?1", {schema_id}) do
      {:ok, {schema_json}} -> {:ok, schema_json}
      {:ok, nil} -> {:error, :not_found}
    end
  end

  # --- Queries for topics ---

  def get_submitted_inputs_for_run(db, run_id) do
    query(
      db,
      """
      SELECT ei.execution_id, r.external_id, i.number, i.title,
             ir.type AS response_type
      FROM inputs AS i
      INNER JOIN execution_inputs AS ei ON ei.input_id = i.id
      INNER JOIN runs AS r ON r.id = i.run_id
      LEFT JOIN input_responses AS ir ON ir.input_id = i.id
      WHERE i.run_id = ?1
      """,
      {run_id}
    )
  end

  def get_inputs_for_run(db, run_id) do
    query(
      db,
      """
      SELECT i.id, r.external_id, i.number, i.workspace_id, i.key,
             i.prompt_id, i.schema_id, i.requires_tag_set_id, i.created_at,
             ir.type AS response_type, ir.value AS response_value, ir.created_at AS responded_at,
             ir.created_by
      FROM inputs AS i
      INNER JOIN runs AS r ON r.id = i.run_id
      LEFT JOIN input_responses AS ir ON ir.input_id = i.id
      WHERE i.run_id = ?1
      """,
      {run_id}
    )
  end

  def get_inputs_for_workspace(db, workspace_id) do
    query(
      db,
      """
      SELECT DISTINCT i.id, r.external_id AS run_external_id, i.number,
             i.workspace_id, i.key,
             i.prompt_id, i.schema_id, i.created_at, i.title,
             i.requires_tag_set_id,
             ir.type AS response_type
      FROM inputs AS i
      INNER JOIN runs AS r ON r.id = i.run_id
      LEFT JOIN input_responses AS ir ON ir.input_id = i.id
      INNER JOIN input_dependencies AS id ON id.input_id = i.id
      LEFT JOIN results AS dr ON dr.execution_id = id.execution_id
      WHERE i.workspace_id = ?1
        AND ir.input_id IS NULL
        AND (dr.execution_id IS NULL OR dr.successor_id IS NOT NULL)
      ORDER BY i.created_at DESC
      """,
      {workspace_id}
    )
  end

  def has_active_dependency?(db, input_id) do
    # An execution is "active" if it has no result, or if its result has a
    # successor (suspended/retried — the chain is still alive).
    case query_one(
           db,
           """
           SELECT 1
           FROM input_dependencies AS id
           LEFT JOIN results AS r ON r.execution_id = id.execution_id
           WHERE id.input_id = ?1
             AND (r.execution_id IS NULL OR r.successor_id IS NOT NULL)
           LIMIT 1
           """,
           {input_id}
         ) do
      {:ok, nil} -> false
      {:ok, _} -> true
    end
  end

  def get_dependent_run_external_ids(db, input_id) do
    query(
      db,
      """
      SELECT DISTINCT r.external_id
      FROM input_dependencies AS id
      INNER JOIN executions AS e ON e.id = id.execution_id
      INNER JOIN steps AS s ON s.id = e.step_id
      INNER JOIN runs AS r ON r.id = s.run_id
      WHERE id.input_id = ?1
      """,
      {input_id}
    )
  end

  def get_input_dependencies_for_execution(db, execution_id) do
    query(
      db,
      "SELECT input_id FROM input_dependencies WHERE execution_id = ?1",
      {execution_id}
    )
  end

  def get_input_dependencies_for_run(db, run_id) do
    query(
      db,
      """
      SELECT id.execution_id, r.external_id, i.number, i.key, i.prompt_id, i.title, i.created_at,
             ir.type AS response_type, ir.value AS response_value, ir.created_at AS responded_at,
             ir.created_by
      FROM input_dependencies AS id
      INNER JOIN inputs AS i ON i.id = id.input_id
      INNER JOIN runs AS r ON r.id = i.run_id
      INNER JOIN executions AS e ON e.id = id.execution_id
      INNER JOIN steps AS s ON s.id = e.step_id
      LEFT JOIN input_responses AS ir ON ir.input_id = i.id
      WHERE s.run_id = ?1
      """,
      {run_id}
    )
  end
end
