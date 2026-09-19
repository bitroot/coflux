defmodule Coflux.Orchestration.Ids do
  @moduledoc """
  The external id formats topics and clients speak. Internal ids never
  leave the server; these are what an event carries instead.
  """

  def execution(run_external_id, step_number, attempt) do
    "#{run_external_id}:#{step_number}:#{attempt}"
  end

  def step(run_external_id, step_number) do
    "#{run_external_id}:#{step_number}"
  end

  def input(run_external_id, input_number) do
    "#{run_external_id}/i#{input_number}"
  end

  # `<run>:<step>_<index>`. Run ids are alphanumeric and step numbers are
  # integers, so the last `_` unambiguously separates the index.
  def stream(run_external_id, step_number, index) do
    "#{run_external_id}:#{step_number}_#{index}"
  end

  def catalog_version(path, number), do: "#{path}@#{number}"

  # A wait is for whatever comes after `number`, so it is keyed apart from
  # a read of that version.
  def catalog_wait(path, number), do: "#{path}@#{number}+"

  def parse_execution(external_id) do
    case String.split(external_id, ":") do
      [run_external_id, step_number, attempt] ->
        with {step_number, ""} <- Integer.parse(step_number),
             {attempt, ""} <- Integer.parse(attempt) do
          {:ok, run_external_id, step_number, attempt}
        else
          _ -> {:error, :invalid_format}
        end

      _ ->
        {:error, :invalid_format}
    end
  end

  def parse_step(step_id) do
    case String.split(step_id, ":", parts: 2) do
      [run_external_id, step_number] ->
        case Integer.parse(step_number) do
          {step_number, ""} -> {:ok, run_external_id, step_number}
          _ -> {:error, :invalid}
        end

      _ ->
        {:error, :invalid}
    end
  end

  # The index is separated by the last `_`, so a run id containing one is
  # still unambiguous.
  def parse_stream(id) when is_binary(id) do
    case String.split(id, "_") do
      parts when length(parts) >= 2 ->
        {index, prefix_parts} = List.pop_at(parts, -1)

        with {index, ""} when index >= 0 <- Integer.parse(index),
             {:ok, run_external_id, step_number} <- parse_step(Enum.join(prefix_parts, "_")) do
          {:ok, run_external_id, step_number, index}
        else
          _ -> {:error, :invalid_format}
        end

      _ ->
        {:error, :invalid_format}
    end
  end

  def parse_stream(_), do: {:error, :invalid_format}

  def parse_input(external_id) do
    case String.split(external_id, "/i", parts: 2) do
      [run_external_id, number] ->
        case Integer.parse(number) do
          {number, ""} -> {:ok, run_external_id, number}
          _ -> :error
        end

      _ ->
        :error
    end
  end
end
