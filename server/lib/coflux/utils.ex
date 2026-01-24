defmodule Coflux.Utils do
  alias Coflux.Config

  @id_chars String.codepoints("bcdfghjklmnpqrstvwxyzBCDFGHJKLMNPQRSTVWXYZ23456789")

  def generate_id(length, prefix \\ "") do
    prefix <> Enum.map_join(0..length, fn _ -> Enum.random(@id_chars) end)
  end

  def data_path(path) do
    path = Path.join(Config.data_dir(), path)

    dir = Path.dirname(path)
    File.mkdir_p!(dir)

    path
  end
end
