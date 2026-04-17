defmodule Coflux.JsonSchema do
  @moduledoc """
  JSON Schema validation utilities.

  Validates schema documents themselves (meta-validation) and
  validates data against schemas.
  """

  @doc """
  Validate that a JSON string is a valid JSON Schema document.

  Returns :ok or {:error, reason}.
  """
  def validate_schema(schema_json) when is_binary(schema_json) do
    case Jason.decode(schema_json) do
      {:ok, schema} when is_map(schema) ->
        case JSV.build(schema) do
          {:ok, _} -> :ok
          {:error, _} -> {:error, "invalid JSON Schema"}
        end

      {:ok, _} ->
        {:error, "schema must be a JSON object"}

      {:error, _} ->
        {:error, "invalid JSON"}
    end
  end

  @doc """
  Validate a value against a JSON Schema (given as a JSON string).

  Returns :ok or {:error, reason}.
  """
  def validate_value(value, schema_json) when is_binary(schema_json) do
    case Jason.decode(schema_json) do
      {:ok, schema} ->
        case JSV.build(schema) do
          {:ok, root} ->
            case JSV.validate(value, root) do
              {:ok, _} -> :ok
              {:error, error} -> {:error, format_errors(error)}
            end

          {:error, _} ->
            {:error, "invalid schema"}
        end

      {:error, _} ->
        {:error, "invalid schema JSON"}
    end
  end

  @doc """
  Validate a value against a JSON Schema with `required` constraints removed.

  Useful for validating partial/initial values where missing fields are expected.
  Note: this does not resolve `$ref` — required constraints inside referenced
  definitions will not be stripped.
  Returns :ok or {:error, reason}.
  """
  def validate_partial(value, schema_json) when is_binary(schema_json) do
    case Jason.decode(schema_json) do
      {:ok, schema} ->
        relaxed = strip_required(schema)

        case JSV.build(relaxed) do
          {:ok, root} ->
            case JSV.validate(value, root) do
              {:ok, _} -> :ok
              {:error, error} -> {:error, format_errors(error)}
            end

          {:error, _} ->
            {:error, "invalid schema"}
        end

      {:error, _} ->
        {:error, "invalid schema JSON"}
    end
  end

  defp strip_required(schema) when is_map(schema) do
    schema
    |> Map.delete("required")
    |> Map.new(fn {k, v} -> {k, strip_required(v)} end)
  end

  defp strip_required(list) when is_list(list) do
    Enum.map(list, &strip_required/1)
  end

  defp strip_required(other), do: other

  defp format_errors(%JSV.ValidationError{errors: errors}) do
    errors
    |> Enum.map(&format_error/1)
    |> Enum.join("; ")
  end

  defp format_errors(other), do: inspect(other)

  defp format_error(%JSV.Validator.Error{kind: kind, data_path: data_path}) do
    path = Enum.join(data_path, "/")

    if path != "" do
      "/#{path}: #{kind}"
    else
      "#{kind}"
    end
  end

  defp format_error(other), do: inspect(other)
end
