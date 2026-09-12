defmodule Coflux.Orchestration.Catalog do
  @moduledoc """
  The catalog: paths holding versioned values.

  See the comments on `catalog_versions` in the migration for the model. In
  short: a publish appends a row pointing at a `values_` row; `id` is the
  project-wide clock; `number` is the per-path ordinal; a reader in
  workspace `W` sees every version published in `W` or any of its bases and
  takes the newest by `id`.

  Every function here takes the reader's workspace chain explicitly
  (nearest-first, as `Workspaces.get_workspace_chain/2` returns it) rather
  than a single workspace id, since visibility is a property of the chain
  and the caller may be resolving against an archived epoch whose ids are
  remapped.
  """

  import Coflux.Store

  alias Coflux.Orchestration.Results

  @path_segment ~r/^[A-Za-z0-9_.-]+$/
  @max_path_length 512

  @columns "id, path, workspace_id, number, value_id, execution_ref_id, created_by, created_at"

  @doc """
  Validates a catalog path: slash-separated segments of `[A-Za-z0-9_.-]`,
  no leading or trailing slash, no empty segment, no `.` or `..` segment.
  `@` and `:` are excluded so `path@6` can be an identifier.
  """
  def validate_path(path) when is_binary(path) do
    segments = String.split(path, "/")

    cond do
      path == "" or byte_size(path) > @max_path_length ->
        {:error, :invalid_path}

      Enum.any?(segments, &(&1 in ["", ".", ".."])) ->
        {:error, :invalid_path}

      Enum.any?(segments, &(!Regex.match?(@path_segment, &1))) ->
        {:error, :invalid_path}

      true ->
        :ok
    end
  end

  def validate_path(_), do: {:error, :invalid_path}

  @doc """
  Publishes `value_id` at `path` into `workspace_id`.

  `chain` is the publishing workspace's chain, used for the dedup check:
  if the head of the path as seen from the publisher already points at the
  same value, no row is written and the existing version is returned.
  Values are content-hashed, so this is exact, and it is what makes a
  publish safe to re-run after a suspension.

  Returns `{:ok, version, created?}`.
  """
  def publish(db, path, workspace_id, chain, value_id, execution_ref_id, created_by) do
    with_transaction(db, fn ->
      {:ok, head} = get_head(db, path, chain, nil, nil)

      if head && head.value_id == value_id do
        {:ok, head, false}
      else
        now = current_timestamp()

        {:ok, {number}} =
          query_one!(
            db,
            "SELECT COALESCE(MAX(number), 0) + 1 FROM catalog_versions WHERE path = ?1",
            {path}
          )

        {:ok, id} =
          insert_one(db, :catalog_versions, %{
            path: path,
            workspace_id: workspace_id,
            number: number,
            value_id: value_id,
            execution_ref_id: execution_ref_id,
            created_by: created_by,
            created_at: now
          })

        {:ok, version} = get_by_id(db, id)
        {:ok, version, true}
      end
    end)
  end

  @doc """
  The latest version at `path` visible from `chain`, as of `pin`.

  `pin` bounds the read to versions with `id <= pin`; `nil` means no bound.
  `run_external_id`, when given, exempts versions published by that run
  from the pin (read-your-run's-writes). The pin is applied before the
  chain: a version newer than the snapshot is invisible whichever workspace
  it is in, and the nearest-by-recency rule runs over what's left.

  Returns `{:ok, version | nil}`.
  """
  def get_head(db, path, chain, pin, run_external_id) do
    {placeholders, params} = chain_placeholders(chain, 1)
    pin_index = length(chain) + 2
    run_index = length(chain) + 3

    query_one(
      db,
      """
      SELECT #{@columns}
      FROM catalog_versions
      WHERE path = ?1
        AND workspace_id IN (#{placeholders})
        AND (
          ?#{pin_index} IS NULL
          OR id <= ?#{pin_index}
          OR execution_ref_id IN (
            SELECT id FROM execution_refs WHERE run_external_id = ?#{run_index}
          )
        )
      ORDER BY id DESC
      LIMIT 1
      """,
      List.to_tuple([path | params] ++ [pin, run_external_id]),
      &build_version/1
    )
  end

  @doc """
  The version numbered `number` at `path`, wherever it was published.
  Visibility is the caller's to check (`visible?/2`): a number is allocated
  once per path, so an invisible one can never become visible.
  """
  def get_version(db, path, number) do
    query_one(
      db,
      "SELECT #{@columns} FROM catalog_versions WHERE path = ?1 AND number = ?2",
      {path, number},
      &build_version/1
    )
  end

  def get_by_id(db, id) do
    query_one(
      db,
      "SELECT #{@columns} FROM catalog_versions WHERE id = ?1",
      {id},
      &build_version/1
    )
  end

  def visible?(version, chain), do: version.workspace_id in chain

  @doc """
  The smallest visible version at `path` numbered above `after_number`.
  Deliberately ignores the pin: this is the wait side, and seeing past the
  snapshot is the point.
  """
  def get_next(db, path, chain, after_number) do
    {placeholders, params} = chain_placeholders(chain, 2)

    query_one(
      db,
      """
      SELECT #{@columns}
      FROM catalog_versions
      WHERE path = ?1
        AND number > ?2
        AND workspace_id IN (#{placeholders})
      ORDER BY number ASC
      LIMIT 1
      """,
      List.to_tuple([path, after_number | params]),
      &build_version/1
    )
  end

  @doc """
  The head of every path visible from `chain` under `prefix` (a path prefix
  match; `nil` or `""` for everything), ordered by path.
  """
  def list_heads(db, chain, prefix) do
    {placeholders, params} = chain_placeholders(chain, 1)
    pattern = escape_like(prefix || "") <> "%"

    query(
      db,
      """
      SELECT #{@columns}
      FROM catalog_versions
      WHERE id IN (
        SELECT MAX(id)
        FROM catalog_versions
        WHERE path LIKE ?1 ESCAPE '\\'
          AND workspace_id IN (#{placeholders})
        GROUP BY path
      )
      ORDER BY path
      """,
      List.to_tuple([pattern | params]),
      &build_version/1
    )
  end

  @doc """
  Versions at `path` visible from `chain`, newest first. `before` bounds the
  listing to numbers strictly below it (for paging); `nil` starts at the
  head.
  """
  def list_versions(db, path, chain, limit, before \\ nil) do
    {placeholders, params} = chain_placeholders(chain, 2)

    query(
      db,
      """
      SELECT #{@columns}
      FROM catalog_versions
      WHERE path = ?1
        AND (?2 IS NULL OR number < ?2)
        AND workspace_id IN (#{placeholders})
      ORDER BY number DESC
      LIMIT ?#{length(chain) + 3}
      """,
      List.to_tuple([path, before | params] ++ [limit]),
      &build_version/1
    )
  end

  @doc "The catalog clock: the highest version id, or 0 when empty."
  def current_sequence(db) do
    {:ok, {sequence}} = query_one!(db, "SELECT COALESCE(MAX(id), 0) FROM catalog_versions", {})
    {:ok, sequence}
  end

  @doc """
  The snapshot `execution_id` takes at assignment, as a `catalog_versions.id`.

  An override on the execution wins. Otherwise an execution gated on a
  catalog wait — the successor of a `next()` — takes the clock: it exists
  to see past its predecessor. Otherwise the pin of the attempt before it,
  so a retry, a re-run, or the resumption of a suspension sees what that
  attempt saw — unless that attempt recurred, since each iteration of a
  recurrent target is meant to start afresh. Otherwise the run's override,
  and failing that the clock.
  """
  def resolve_pin(db, execution_id) do
    {:ok, {step_id, attempt, override}} =
      query_one!(
        db,
        "SELECT step_id, attempt, catalog_sequence FROM executions WHERE id = ?1",
        {execution_id}
      )

    cond do
      override ->
        {:ok, override}

      gated?(db, execution_id) ->
        current_sequence(db)

      true ->
        case previous_pin(db, step_id, attempt) do
          :recurred ->
            current_sequence(db)

          pin when is_integer(pin) ->
            {:ok, pin}

          nil ->
            case run_override(db, step_id) do
              nil -> current_sequence(db)
              sequence -> {:ok, sequence}
            end
        end
    end
  end

  defp gated?(db, execution_id) do
    {:ok, waits} = get_waits(db, execution_id)
    waits != []
  end

  # The pin of the most recent earlier attempt that was assigned: `:recurred`
  # if that attempt recurred, nil if there is none (or it was assigned
  # before the catalog existed, and so has no pin to pass on).
  defp previous_pin(db, step_id, attempt) do
    case query_one(
           db,
           """
           SELECT a.catalog_sequence, c.kind
           FROM executions AS e
           JOIN assignments AS a ON a.execution_id = e.id
           LEFT JOIN completions AS c ON c.execution_id = e.id
           WHERE e.step_id = ?1 AND e.attempt < ?2
           ORDER BY e.attempt DESC
           LIMIT 1
           """,
           {step_id, attempt}
         ) do
      {:ok, nil} ->
        nil

      {:ok, {pin, kind}} ->
        if kind && Results.kind_atom(kind) == :recurred, do: :recurred, else: pin
    end
  end

  defp run_override(db, step_id) do
    {:ok, {sequence}} =
      query_one!(
        db,
        "SELECT r.catalog_sequence FROM runs AS r JOIN steps AS s ON s.run_id = r.id WHERE s.id = ?1",
        {step_id}
      )

    sequence
  end

  @doc "The pin recorded when `execution_id` was assigned, or nil."
  def get_pin(db, execution_id) do
    case query_one(
           db,
           "SELECT catalog_sequence FROM assignments WHERE execution_id = ?1",
           {execution_id}
         ) do
      {:ok, {sequence}} -> {:ok, sequence}
      {:ok, nil} -> {:ok, nil}
    end
  end

  # --- Lineage ---

  @doc "Records that `execution_id` resolved `version_id`. Returns whether the row is new."
  def record_read(db, execution_id, version_id) do
    {:ok, id} =
      insert_one(
        db,
        :catalog_reads,
        %{execution_id: execution_id, version_id: version_id, created_at: current_timestamp()},
        on_conflict: "DO NOTHING"
      )

    {:ok, !is_nil(id)}
  end

  @doc "Versions read by executions of `run_id`, as `[{execution_id, version}]`."
  def get_reads_for_run(db, run_id) do
    query(
      db,
      """
      SELECT r.execution_id, #{columns("v")}
      FROM catalog_reads AS r
      INNER JOIN catalog_versions AS v ON v.id = r.version_id
      INNER JOIN executions AS e ON e.id = r.execution_id
      INNER JOIN steps AS s ON s.id = e.step_id
      WHERE s.run_id = ?1
      ORDER BY r.created_at
      """,
      {run_id},
      fn row ->
        {execution_id, rest} = Map.pop(row, :execution_id)
        {execution_id, build_version(rest)}
      end
    )
  end

  @doc """
  Versions published by executions of the run `run_external_id`, as
  `[{{step_number, attempt}, version}]`.
  """
  def get_publishes_for_run(db, run_external_id) do
    query(
      db,
      """
      SELECT er.step_number, er.attempt, #{columns("v")}
      FROM catalog_versions AS v
      INNER JOIN execution_refs AS er ON er.id = v.execution_ref_id
      WHERE er.run_external_id = ?1
      ORDER BY v.id
      """,
      {run_external_id},
      fn row ->
        {step_number, rest} = Map.pop(row, :step_number)
        {attempt, rest} = Map.pop(rest, :attempt)
        {{step_number, attempt}, build_version(rest)}
      end
    )
  end

  # --- Waits ---

  def record_wait(db, execution_id, path, number) do
    {:ok, _} =
      insert_one(
        db,
        :catalog_waits,
        %{
          execution_id: execution_id,
          path: path,
          number: number,
          created_at: current_timestamp()
        },
        on_conflict: "(execution_id, path) DO UPDATE SET number = excluded.number"
      )

    :ok
  end

  def get_waits(db, execution_id) do
    query(
      db,
      "SELECT path, number FROM catalog_waits WHERE execution_id = ?1",
      {execution_id}
    )
  end

  @doc "Waits recorded by executions of `run_id`, as `[{execution_id, path, number}]`."
  def get_waits_for_run(db, run_id) do
    query(
      db,
      """
      SELECT w.execution_id, w.path, w.number
      FROM catalog_waits AS w
      INNER JOIN executions AS e ON e.id = w.execution_id
      INNER JOIN steps AS s ON s.id = e.step_id
      WHERE s.run_id = ?1
      """,
      {run_id}
    )
  end

  # --- Helpers ---

  defp columns(alias_) do
    @columns
    |> String.split(", ")
    |> Enum.map_join(", ", &"#{alias_}.#{&1}")
  end

  defp build_version(row) do
    %{
      id: row.id,
      path: row.path,
      workspace_id: row.workspace_id,
      number: row.number,
      value_id: row.value_id,
      execution_ref_id: row.execution_ref_id,
      created_by: row.created_by,
      created_at: row.created_at
    }
  end

  # Placeholders `?(offset+1) … ?(offset+n)` for the n workspaces of a chain,
  # so a query with `offset` positional parameters ahead of them binds the
  # chain as the next n.
  defp chain_placeholders(chain, offset) do
    placeholders =
      chain
      |> Enum.with_index(offset + 1)
      |> Enum.map_join(", ", fn {_id, index} -> "?#{index}" end)

    {placeholders, chain}
  end

  defp escape_like(value) do
    value
    |> String.replace("\\", "\\\\")
    |> String.replace("%", "\\%")
    |> String.replace("_", "\\_")
  end

  defp current_timestamp() do
    System.os_time(:millisecond)
  end
end
