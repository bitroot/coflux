defmodule Coflux.Orchestration.Observations do
  import Coflux.Store

  alias Coflux.Orchestration.Values

  def record_logs(db, execution_id, messages) do
    with_transaction(db, fn ->
      now = current_timestamp()

      Enum.each(messages, fn {timestamp, level, template, values} ->
        {:ok, template_id} =
          if template do
            get_or_create_template(db, template)
          else
            {:ok, nil}
          end

        {:ok, message_id} =
          insert_one(db, :messages, %{
            execution_id: execution_id,
            timestamp: timestamp,
            level: encode_level(level),
            template_id: template_id,
            created_at: now
          })

        {:ok, _} =
          insert_many(
            db,
            :message_values,
            {:message_id, :label_id, :value_id},
            Enum.map(values, fn {label, value} ->
              {:ok, label_id} = get_or_create_label(db, label)
              {:ok, value_id} = Values.get_or_create_value(db, value)
              {message_id, label_id, value_id}
            end)
          )
      end)
    end)
  end

  def get_messages_for_run(db, run_id) do
    case query(
           db,
           """
           SELECT m.id, m.execution_id, m.timestamp, m.level, m.template_id
           FROM messages AS m
           INNER JOIN executions AS e ON e.id = m.execution_id
           INNER JOIN steps AS s ON s.id = e.step_id
           WHERE s.run_id = ?1
           ORDER BY m.timestamp
           """,
           {run_id}
         ) do
      {:ok, rows} ->
        messages =
          Enum.map(rows, fn {message_id, execution_id, timestamp, level, template_id} ->
            # TODO: batch?
            {:ok, template} =
              if template_id do
                get_template_by_id(db, template_id)
              else
                {:ok, nil}
              end

            {:ok, values} = get_values_for_message(db, message_id)
            {execution_id, timestamp, decode_level(level), template, values}
          end)

        {:ok, messages}
    end
  end

  def get_counts_for_run(db, run_id) do
    case query(
           db,
           """
           SELECT m.execution_id, count(*)
           FROM messages AS m
           INNER JOIN executions AS e ON e.id = m.execution_id
           INNER JOIN steps AS s ON s.id = e.step_id
           WHERE s.run_id = ?1
           GROUP BY m.execution_id
           """,
           {run_id}
         ) do
      {:ok, rows} -> {:ok, Map.new(rows)}
    end
  end

  defp get_template_by_id(db, template_id) do
    case query_one(db, "SELECT template FROM message_templates WHERE id = ?1", {template_id}) do
      {:ok, {template}} -> {:ok, template}
    end
  end

  defp get_values_for_message(db, message_id) do
    case query(
           db,
           """
           SELECT ml.label, mv.value_id
           FROM message_values AS mv
           INNER JOIN message_labels AS ml ON ml.id = mv.label_id
           WHERE mv.message_id = ?1
           """,
           {message_id}
         ) do
      {:ok, rows} ->
        {:ok,
         Map.new(rows, fn {label, value_id} ->
           {:ok, value} = Values.get_value_by_id(db, value_id)
           {label, value}
         end)}
    end
  end

  defp get_or_create_template(db, template) do
    case query_one(db, "SELECT id FROM message_templates WHERE template = ?1", {template}) do
      {:ok, {template_id}} ->
        {:ok, template_id}

      {:ok, nil} ->
        case insert_one(db, :message_templates, %{template: template}) do
          {:ok, template_id} -> {:ok, template_id}
        end
    end
  end

  defp get_or_create_label(db, label) do
    case query_one(db, "SELECT id FROM message_labels WHERE label = ?1", {label}) do
      {:ok, {label_id}} ->
        {:ok, label_id}

      {:ok, nil} ->
        case insert_one(db, :message_labels, %{label: label}) do
          {:ok, label_id} -> {:ok, label_id}
        end
    end
  end

  defp encode_level(level) do
    case level do
      :debug -> 0
      :stdout -> 1
      :info -> 2
      :stderr -> 3
      :warning -> 4
      :error -> 5
    end
  end

  defp decode_level(level) do
    case level do
      0 -> :debug
      1 -> :stdout
      2 -> :info
      3 -> :stderr
      4 -> :warning
      5 -> :error
    end
  end

  defp current_timestamp() do
    System.os_time(:millisecond)
  end
end
