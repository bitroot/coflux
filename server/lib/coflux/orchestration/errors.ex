defmodule Coflux.Orchestration.Errors do
  @moduledoc """
  Shared helpers for deduping errors in the `errors` + `error_frames` tables.
  Used by `Results` (execution errors) and `Streams` (stream closure errors).
  """

  import Coflux.Store

  # Inserts or returns an existing error matching (type, message, frames).
  # Returns the error id as an integer.
  def get_or_create(db, type, message, frames) do
    hash = hash(type, message, frames)

    case query_one(db, "SELECT id FROM errors WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {id}} ->
        id

      {:ok, nil} ->
        {:ok, error_id} =
          insert_one(db, :errors, %{
            hash: {:blob, hash},
            type: type,
            message: message
          })

        {:ok, _} =
          insert_many(
            db,
            :error_frames,
            {:error_id, :depth, :file, :line, :name, :code},
            frames
            |> Enum.with_index()
            |> Enum.map(fn {{file, line, name, code}, index} ->
              {error_id, index, file, line, name, code}
            end)
          )

        error_id
    end
  end

  # Returns `{:ok, {type, message, frames}}`.
  def get_by_id(db, error_id) do
    {:ok, {type, message}} =
      query_one!(db, "SELECT type, message FROM errors WHERE id = ?1", {error_id})

    {:ok, frames} =
      query(
        db,
        "SELECT file, line, name, code FROM error_frames WHERE error_id = ?1 ORDER BY depth",
        {error_id}
      )

    {:ok, {type, message, frames}}
  end

  defp hash(type, message, frames) do
    frame_parts =
      Enum.flat_map(frames, fn {file, line, name, code} ->
        [file, Integer.to_string(line), name || 0, code || 0]
      end)

    parts = Enum.concat([type, message], frame_parts)
    :crypto.hash(:sha256, Enum.intersperse(parts, 0))
  end
end
