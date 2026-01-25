defmodule Coflux.Orchestration.Utils do
  def encode_params_list(params) do
    case params do
      true -> ""
      false -> nil
      nil -> nil
      params -> Enum.map_join(params, ",", &Integer.to_string/1)
    end
  end

  def decode_params_list(value) do
    case value do
      nil -> false
      "" -> true
      value -> value |> String.split(",") |> Enum.map(&String.to_integer/1)
    end
  end

  def encode_params_set(indexes) do
    Enum.reduce(indexes, 0, &Bitwise.bor(&2, Bitwise.bsl(1, &1)))
  end

  def decode_params_set(value) do
    value
    |> Integer.digits(2)
    |> Enum.reverse()
    |> Enum.with_index()
    |> Enum.filter(fn {v, _} -> v == 1 end)
    |> Enum.map(fn {_, i} -> i end)
  end

  def encode_step_type(type) do
    case type do
      :task -> 0
      :workflow -> 1
    end
  end

  def decode_step_type(value) do
    case value do
      0 -> :task
      1 -> :workflow
    end
  end
end
