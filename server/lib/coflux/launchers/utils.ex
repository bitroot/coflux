defmodule Coflux.Launchers.Utils do
  @doc """
  Truncates a string to at most `max_bytes` bytes, taken from the end.

  When truncation occurs, the (potentially broken) first partial line is
  stripped so the result always starts at a line boundary.
  """
  def truncate_bytes(string, max_bytes) when byte_size(string) <= max_bytes, do: string

  def truncate_bytes(string, max_bytes) do
    truncated =
      string
      |> binary_part(byte_size(string) - max_bytes, max_bytes)
      |> String.replace(~r/^[^\n]*\n/, "")

    if String.valid?(truncated), do: truncated, else: nil
  end
end
