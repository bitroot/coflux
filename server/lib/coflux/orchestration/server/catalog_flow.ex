defmodule Coflux.Orchestration.Server.CatalogFlow do
  @moduledoc """
  Reading and publishing catalog versions, and releasing whatever was
  waiting for one.

  The catalog is a versioned key-value space shared across a workspace and
  everything based on it. A run reads it through a *snapshot*: the
  `catalog` option fixes a point in the version clock, and every read the
  run makes sees the catalog as it was then - except for versions the run
  published itself, which it always sees.

  An execution can also wait for what comes *after* a version it has
  already seen. Those waits are held as dependencies, and a publish that
  lands at the path releases them here. A wait for a version that can
  never arrive - one already superseded when it was asked for - is refused
  rather than parked, so an execution cannot block on the past.
  """

  alias Coflux.Events.{CatalogPublished, CatalogRead}

  alias Coflux.Orchestration.{Catalog, Ids, Runs}

  alias Coflux.Orchestration.Server.{Archives, Dependencies, Effects, Resolve, State, Waiters}

  # Resolves a version for an execution: by number, or the head as of the
  # execution's snapshot when `number` is nil.
  # The `catalog` option of `start_run` and `rerun_step`, as a snapshot:
  # `"latest"` is the clock now; `"path@n"` is that version's place in the
  # clock, so a run or attempt started from it sees the catalog as it was
  # when that version was published. It has to exist and be visible from
  # the workspace the run is in.
  def resolve_catalog_option(_state, _workspace_id, nil), do: {:ok, nil}

  def resolve_catalog_option(state, _workspace_id, "latest"),
    do: Catalog.current_sequence(state.db)

  def resolve_catalog_option(state, workspace_id, ref) when is_binary(ref) do
    with {:ok, path, number} <- parse_catalog_ref(ref),
         {:ok, %{} = version} <- Catalog.get_version(state.db, path, number) do
      if Catalog.visible?(version, State.workspace_chain(state, workspace_id)),
        do: {:ok, version.id},
        else: {:error, :catalog_invisible}
    else
      {:ok, nil} -> {:error, :catalog_not_found}
      {:error, _} -> {:error, :catalog_invalid}
    end
  end

  def resolve_catalog_option(_state, _workspace_id, _other), do: {:error, :catalog_invalid}

  # `path@n`. A path can't contain `@`, so the split is unambiguous.
  def parse_catalog_ref(ref) do
    with [path, number] <- String.split(ref, "@"),
         :ok <- Catalog.validate_path(path),
         {n, ""} when n > 0 <- Integer.parse(number) do
      {:ok, path, n}
    else
      _ -> {:error, :invalid}
    end
  end

  def lookup_catalog_version(state, execution_id, path, number) do
    {:ok, workspace_id} = Runs.get_workspace_id_for_execution(state.db, execution_id)
    chain = State.workspace_chain(state, workspace_id)

    if number do
      # A named version is an immutable reference, so the pin doesn't
      # apply. A number is allocated once per path, so one published
      # outside the caller's chain can never become visible.
      case Catalog.get_version(state.db, path, number) do
        {:ok, nil} ->
          {:ok, nil}

        {:ok, version} ->
          if Catalog.visible?(version, chain),
            do: {:ok, version},
            else: {:error, :invisible}
      end
    else
      {:ok, pin} = Catalog.get_pin(state.db, execution_id)

      {:ok, {run_external_id}} =
        Runs.get_external_run_id_for_execution(state.db, execution_id)

      Catalog.get_head(state.db, path, chain, pin, run_external_id)
    end
  end

  # A version as topics and the API see it, with its value resolved for
  # rendering.
  def record_catalog_read(state, execution_id, execution_external_id, version) do
    case Catalog.record_read(state.db, execution_id, version.id) do
      {:ok, true} ->
        {:ok, {run_external_id}} =
          Runs.get_external_run_id_for_execution(state.db, execution_id)

        Effects.emit(state, %CatalogRead{
          run: run_external_id,
          execution: execution_external_id,
          version: Resolve.catalog_version(state.db, version)
        })

      {:ok, false} ->
        state
    end
  end

  # A new version landed. Routing tells the publishing run and every
  # workspace that can see it (its own and every descendant); anything
  # waiting on the path is woken here.
  def notify_catalog_version(state, version) do
    state = Effects.emit(state, catalog_published(state, version))
    wake_catalog_waiters(state, version)
  end

  # The publisher comes from the built version, which has already resolved
  # the version's execution ref - there is nothing to resolve a second time.
  def catalog_published(state, version) do
    built = Resolve.catalog_version(state.db, version)

    run =
      if built.published_by do
        {:ok, run_ext_id, _step, _attempt} = Ids.parse_execution(built.published_by)
        run_ext_id
      end

    %CatalogPublished{run: run, execution: built.published_by, version: built}
  end

  # A version landed at `version.path`. Release every gated successor and
  # serve every running select that was waiting on the path from a
  # workspace that can see it. Both sets are keyed by path and position;
  # the gates carry the waiter's workspace by internal id (they are rebuilt
  # on rotation), the selects by external id (they are carried across it).
  # Publishes are rare enough that scanning the keys beats keeping an index.
  def wake_catalog_waiters(state, version) do
    woken? = fn path, number, workspace_id ->
      path == version.path and number < version.number and
        version.workspace_id in State.workspace_chain(state, workspace_id)
    end

    state =
      state.dependency_waiters
      |> Map.keys()
      |> Enum.filter(fn
        {:catalog, workspace_id, path, number} -> woken?.(path, number, workspace_id)
        _key -> false
      end)
      |> Enum.reduce(state, &Dependencies.clear_dependency_key(&2, &1))

    state.waiting
    |> Map.keys()
    |> Enum.flat_map(fn
      {:catalog, workspace_external_id, path, number} = key ->
        # A workspace is never removed, so the id resolves; the check is
        # against the shape of the key, not its age.
        case Map.fetch(state.workspace_external_ids, workspace_external_id) do
          {:ok, workspace_id} ->
            if woken?.(path, number, workspace_id), do: [{key, workspace_id}], else: []

          :error ->
            []
        end

      _key ->
        []
    end)
    |> Enum.reduce(state, fn {{:catalog, _, path, number} = key, workspace_id}, state ->
      chain = State.workspace_chain(state, workspace_id)
      # Whatever is next from here — this version, unless one landed in
      # between — is what the waiter gets, and is recorded as its read.
      {:ok, next} = Catalog.get_next(state.db, path, chain, number)

      state =
        state.waiting
        |> Map.get(key, [])
        |> Enum.reduce(state, fn entry, state ->
          case Archives.resolve_internal_execution_id(state, entry.from_ext_id) do
            {:ok, execution_id} ->
              record_catalog_read(state, execution_id, entry.from_ext_id, next)

            {:error, :not_found} ->
              state
          end
        end)

      Waiters.notify_select_waiters(state, key, {:value, {:raw, next.number, []}})
    end)
  end

  # A suspend request can name a catalog path for the successor to be gated
  # on. One that isn't a valid path can't gate anything, and an ungated
  # successor would run at once and ask again, so the execution fails
  # instead — as it would for any other request the server can't honour.
  # Checked as the result arrives, so the rest of the pipeline sees an
  # ordinary error. Returns the result to record, and whether the worker
  # has to be told to stop the execution: one that has asked to suspend is
  # waiting to be aborted, which a suspension does and an error wouldn't.
  # The official adapter validates paths up front, so this is for others.
  def refuse_invalid_catalog_wait({:suspended, _execute_after, dependency_keys} = result) do
    case Enum.find(dependency_keys, &match?({:catalog, _path}, &1)) do
      {:catalog, path} ->
        case Catalog.validate_path(path) do
          :ok ->
            {result, false}

          {:error, :invalid_path} ->
            message = "invalid catalog path: #{inspect(path)}"
            {{:error, "InvalidCatalogPath", message, [], false}, true}
        end

      nil ->
        {result, false}
    end
  end

  def refuse_invalid_catalog_wait(result), do: {result, false}

  # A catalog wait from a suspend request names only the path. Its position
  # is the suspending execution's own view of that path — the head as of
  # its pin, plus its run's writes — so "newer" means newer than anything
  # it could see, and its own publish can never wake it. (A wait from a
  # suspended select already carries its position.) An invalid path was
  # refused before the result was recorded; dropping one here rather than
  # gating on it forever is only a backstop.
  def resolve_catalog_waits(state, execution_id, dependency_keys) do
    Enum.flat_map(dependency_keys, fn
      {:catalog, path} ->
        case Catalog.validate_path(path) do
          :ok ->
            number =
              case lookup_catalog_version(state, execution_id, path, nil) do
                {:ok, nil} -> 0
                {:ok, version} -> version.number
              end

            [{:catalog, path, number}]

          {:error, :invalid_path} ->
            []
        end

      other ->
        [other]
    end)
  end
end
