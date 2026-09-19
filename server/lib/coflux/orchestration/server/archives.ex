defmodule Coflux.Orchestration.Server.Archives do
  @moduledoc """
  Reading, and reaching back into, the epochs this project has rotated
  through.

  The active database holds recent history; older rows live in archived
  epochs. A run, an input or a cacheable execution may be in any of them,
  so a lookup that misses in the active epoch searches backwards - guided
  by the Bloom index, so an epoch that cannot hold the id is never opened.

  Two kinds of answer come back. A *read* (`find_run/3`, `query_epoch/3`)
  runs against whichever database holds the row and returns what it found,
  which is why everything it calls must work against an archive's database
  as well as the active one. A *copy* (`ensure_run_in_active_epoch/2`,
  `find_and_copy_input_from_archives/2`) brings the rows forward first,
  because what happens next will write against them, and writes only ever
  go to the active epoch.
  """

  require Logger

  alias Coflux.Store.{Epochs, Index}

  alias Coflux.Orchestration.{Epoch, Ids, Inputs, Results, Runs, Streams, Workspaces}

  alias Coflux.Orchestration.Server.{State}

  # How long a client idempotency key keeps matching an existing run.
  @idempotency_ttl_ms 24 * 60 * 60 * 1000

  def resolve_internal_execution_id(state, external_id) do
    case Map.fetch(state.execution_ids, external_id) do
      {:ok, id} ->
        {:ok, id}

      :error ->
        with {:ok, run_ext_id, step_num, attempt} <- Ids.parse_execution(external_id) do
          # First check active epoch
          case Runs.get_execution_id(state.db, run_ext_id, step_num, attempt) do
            {:ok, {id}} when not is_nil(id) ->
              {:ok, id}

            _ ->
              # Not in active epoch - search archived epochs and copy forward
              case find_and_copy_run_from_archives(state, run_ext_id) do
                {:ok, _remap} ->
                  # Run was copied to active epoch, now resolve again
                  case Runs.get_execution_id(state.db, run_ext_id, step_num, attempt) do
                    {:ok, {id}} when not is_nil(id) -> {:ok, id}
                    _ -> {:error, :not_found}
                  end

                :not_found ->
                  {:error, :not_found}
              end
          end
        else
          _ -> {:error, :not_found}
        end
    end
  end

  # Runs `fun.(db, run)` against the database the run lives in: the active
  # epoch, or an archived one found through the epoch index.
  def find_run(state, external_run_id, fun) do
    case Runs.get_run_by_external_id(state.db, external_run_id) do
      {:ok, run} when not is_nil(run) ->
        {:ok, fun.(state.db, run)}

      {:ok, nil} ->
        query_fn = fn archive_db ->
          case Runs.get_run_by_external_id(archive_db, external_run_id) do
            {:ok, run} when not is_nil(run) ->
              {:found, fun.(archive_db, run)}

            {:ok, nil} ->
              :not_found
          end
        end

        bloom_fn = fn epoch_index ->
          Index.find_epochs(epoch_index, "runs", external_run_id)
        end

        case search_archived_epochs(state, query_fn, bloom_fn) do
          {:found, result} -> {:ok, result}
          :not_found -> :not_found
        end
    end
  end

  # The run's structure: every step and attempt with its status, the links
  # between them, and groups — but none of the per-execution detail
  # (results, dependencies, checkpoints, assets, inputs, metrics) or the
  # per-step arguments and streams. Those are loaded for the parts a topic
  # shows, through `build_run_details/3`. Nothing here is resolved per
  # execution: external ids come from the run, step number and attempt.
  # The run snapshot is built here rather than in `Snapshots`: it is
  # assembled from the same helpers that build values, results and assets
  # for the live emits, and reaches into archived epochs through `find_run`.
  def ensure_run_in_active_epoch(state, run_external_id) do
    case Runs.get_run_by_external_id(state.db, run_external_id) do
      {:ok, run} when not is_nil(run) ->
        {:ok, run}

      {:ok, nil} ->
        case find_and_copy_run_from_archives(state, run_external_id) do
          {:ok, _remap} ->
            Runs.get_run_by_external_id(state.db, run_external_id)

          :not_found ->
            {:ok, nil}
        end
    end
  end

  def maybe_find_idempotent_run(_state, nil, _ws_ext_id), do: :miss

  def maybe_find_idempotent_run(state, client_key, ws_ext_id) do
    hashed_key = Runs.build_idempotency_key(ws_ext_id, client_key)
    created_after = System.os_time(:millisecond) - @idempotency_ttl_ms

    # Tier 0: Check active epoch
    case Runs.find_run_by_idempotency_key(state.db, hashed_key, created_after) do
      {:ok, {ext_run_id, step_number, attempt}} ->
        {:hit, ext_run_id, step_number, attempt}

      {:ok, nil} ->
        # Search archived epochs
        query_fn = fn archive_db ->
          case Runs.find_run_by_idempotency_key(archive_db, hashed_key, created_after) do
            {:ok, {ext_run_id, step_number, attempt}} ->
              {:found, {:hit, ext_run_id, step_number, attempt}}

            {:ok, nil} ->
              :not_found
          end
        end

        bloom_fn = fn epoch_index ->
          Index.find_epochs(epoch_index, "idempotency_keys", hashed_key, created_after)
        end

        case search_archived_epochs(state, query_fn, bloom_fn) do
          {:found, result} -> result
          :not_found -> :miss
        end
    end
  end

  # Read-only: returns {db, input_id, key, prompt_id, schema_id, title, actions, initial, requires_tag_set_id, created_at}
  # where `db` is the DB handle the data lives in (active or archive).
  # Does NOT copy data to the active epoch.
  def read_input_from_active_or_archives(state, input_external_id) do
    with {:ok, run_ext_id, input_number} <- Ids.parse_input(input_external_id) do
      case Inputs.get_input_by_run_and_number(state.db, run_ext_id, input_number) do
        {:ok, nil} ->
          query_fn = fn archive_db ->
            case Inputs.get_input_by_run_and_number(archive_db, run_ext_id, input_number) do
              {:ok, nil} ->
                :not_found

              {:ok,
               {input_id, workspace_id, key, prompt_id, schema_id, title, actions, initial,
                requires_tag_set_id, created_at, _run_id}} ->
                {:found,
                 {archive_db, input_id, workspace_id, key, prompt_id, schema_id, title, actions,
                  initial, requires_tag_set_id, created_at}}
            end
          end

          bloom_fn = fn epoch_index ->
            Index.find_epochs(epoch_index, "runs", run_ext_id)
          end

          case search_archived_epochs(state, query_fn, bloom_fn) do
            {:found, result} -> {:ok, result}
            :not_found -> {:ok, nil}
          end

        {:ok,
         {input_id, workspace_id, key, prompt_id, schema_id, title, actions, initial,
          requires_tag_set_id, created_at, _run_id}} ->
          {:ok,
           {state.db, input_id, workspace_id, key, prompt_id, schema_id, title, actions, initial,
            requires_tag_set_id, created_at}}
      end
    else
      :error -> {:ok, nil}
    end
  end

  # Write path: copies the input to the active epoch if found in an archive.
  # Returns the full input tuple from the active DB.
  def find_and_copy_input_from_archives(state, input_external_id) do
    with {:ok, run_ext_id, input_number} <- Ids.parse_input(input_external_id) do
      case Inputs.get_input_by_run_and_number(state.db, run_ext_id, input_number) do
        {:ok, nil} ->
          # Search archived epochs using the runs Bloom filter
          query_fn = fn archive_db ->
            case Inputs.get_input_by_run_and_number(archive_db, run_ext_id, input_number) do
              {:ok, nil} ->
                :not_found

              {:ok, _result} ->
                # Found in archive — copy the run to active epoch
                {:ok, _remap} = Epoch.copy_run(archive_db, state.db, run_ext_id)

                # Re-read from the active DB (now copied)
                case Inputs.get_input_by_run_and_number(state.db, run_ext_id, input_number) do
                  {:ok, nil} -> :not_found
                  {:ok, active_result} -> {:found, active_result}
                end
            end
          end

          bloom_fn = fn epoch_index ->
            Index.find_epochs(epoch_index, "runs", run_ext_id)
          end

          case search_archived_epochs(state, query_fn, bloom_fn) do
            {:found, result} -> {:ok, result}
            :not_found -> {:ok, nil}
          end

        result ->
          result
      end
    else
      :error -> {:ok, nil}
    end
  end

  def find_and_copy_run_from_archives(state, run_external_id) do
    query_fn = fn archive_db ->
      if Runs.run_exists?(archive_db, run_external_id) do
        {:ok, remap} = Epoch.copy_run(archive_db, state.db, run_external_id)
        {:found, {:ok, remap}}
      else
        :not_found
      end
    end

    bloom_fn = fn epoch_index ->
      Index.find_epochs(epoch_index, "runs", run_external_id)
    end

    case search_archived_epochs(state, query_fn, bloom_fn) do
      {:found, result} -> result
      :not_found -> :not_found
    end
  end

  def find_cached_execution_across_epochs(
        state,
        cache_workspace_ids,
        step_id,
        cache_key,
        recorded_after
      ) do
    # Tier 0: Check active epoch
    case Runs.find_cached_execution(
           state.db,
           cache_workspace_ids,
           step_id,
           cache_key,
           recorded_after
         ) do
      {:ok, cached_execution_id} when not is_nil(cached_execution_id) ->
        {:in_epoch, cached_execution_id}

      {:ok, nil} ->
        workspace_external_ids =
          Enum.map(cache_workspace_ids, &State.workspace_external_id(state, &1))

        query_fn = fn archive_db ->
          archive_workspace_ids =
            Enum.flat_map(workspace_external_ids, fn ext_id ->
              case Workspaces.get_workspace_id(archive_db, ext_id) do
                {:ok, id} when not is_nil(id) -> [id]
                {:ok, nil} -> []
              end
            end)

          if archive_workspace_ids == [] do
            :not_found
          else
            case Runs.find_cached_execution(
                   archive_db,
                   archive_workspace_ids,
                   nil,
                   cache_key,
                   recorded_after
                 ) do
              {:ok, archive_exec_id} when not is_nil(archive_exec_id) ->
                # Instead of copying the entire run, resolve the result value
                # from the archive and create an execution_ref
                case Results.resolve(archive_db, archive_exec_id) do
                  {:ok, {:value, value}} ->
                    # Create execution ref for the cached execution itself
                    {:ok, {run_ext, step_num, attempt, module, target}} =
                      Runs.get_run_by_execution(archive_db, archive_exec_id)

                    {:ok, ref_id} =
                      Runs.get_or_create_execution_ref(
                        state.db,
                        run_ext,
                        step_num,
                        attempt,
                        module,
                        target
                      )

                    {:found, {:resolved, ref_id, value}}

                  {:ok, _other} ->
                    :not_found

                  {:pending, _} ->
                    :not_found
                end

              {:ok, nil} ->
                :not_found
            end
          end
        end

        bloom_fn = fn epoch_index ->
          if is_binary(cache_key) do
            Index.find_epochs(epoch_index, "cache_keys", cache_key)
          else
            []
          end
        end

        case search_archived_epochs(state, query_fn, bloom_fn) do
          {:found, result} -> result
          :not_found -> nil
        end
    end
  end

  # Searches archived epochs across both tiers (unindexed, then indexed via Bloom).
  # `query_fn` receives an archive DB handle and returns `{:found, result}` or `:not_found`.
  # `bloom_fn` receives the epoch index and returns candidate epoch IDs.
  # Query one archived epoch, treating an unreadable file as a miss.
  #
  # A corrupt archive opens cleanly — SQLite only reads the schema when a
  # statement is prepared — so the failure lands inside the query, where
  # `Store` matches on success. Without this, one bad file takes the
  # project's orchestration server down on every lookup that reaches the
  # archives, rather than costing that lookup one epoch's worth of rows.
  def query_epoch(state, epoch_id, archive_db, query_fn) do
    query_fn.(archive_db)
  rescue
    error ->
      Logger.error(
        "Couldn't read archived epoch #{epoch_id} in project #{state.project_id}: " <>
          Exception.message(error)
      )

      :not_found
  end

  def search_archived_epochs(state, query_fn, bloom_fn) do
    # Tier 1: Check unindexed DBs (always open, newest first)
    unindexed = Epochs.unindexed_dbs(state.epochs)

    result =
      Enum.reduce_while(unindexed, :not_found, fn {epoch_id, archive_db}, :not_found ->
        case query_epoch(state, epoch_id, archive_db, query_fn) do
          {:found, _} = found -> {:halt, found}
          :not_found -> {:cont, :not_found}
        end
      end)

    case result do
      {:found, _} ->
        result

      :not_found ->
        # Tier 2: Consult Bloom index for indexed epochs, open/query/close on demand
        unindexed_ids = MapSet.new(unindexed, fn {id, _db} -> id end)

        candidate_epoch_ids =
          bloom_fn.(state.epoch_index)
          |> Enum.reject(&MapSet.member?(unindexed_ids, &1))

        Enum.reduce_while(candidate_epoch_ids, :not_found, fn epoch_id, :not_found ->
          path = Epochs.archive_path(state.epochs, epoch_id)

          case Exqlite.Sqlite3.open(path) do
            {:ok, archive_db} ->
              try do
                case query_epoch(state, epoch_id, archive_db, query_fn) do
                  {:found, _} = found -> {:halt, found}
                  :not_found -> {:cont, :not_found}
                end
              after
                Exqlite.Sqlite3.close(archive_db)
              end

            {:error, _} ->
              {:cont, :not_found}
          end
        end)
    end
  end

  # Build the map passed to workers in the :execute message, describing
  # the execution's default stream config. Returns nil when no config was
  # set (NULL columns) — keeps the wire message compact for the common
  # case. When a config exists, the buffer key is ALWAYS present (nil =
  # unbounded, from the -1 column sentinel) so the adapter can distinguish
  # an explicit opt-out of backpressure from an unset buffer.

  # Resolve a stream's external id to its row in the active epoch, copying
  # its run forward from an archived epoch if that's where it lives.
  def resolve_stream_id(state, external_id) do
    with {:ok, run_ext_id, step_number, index} <- Ids.parse_stream(external_id) do
      case Streams.get_stream_id_by_key(state.db, run_ext_id, step_number, index) do
        {:ok, id} ->
          {:ok, id}

        {:error, :not_found} ->
          case find_and_copy_run_from_archives(state, run_ext_id) do
            {:ok, _remap} ->
              Streams.get_stream_id_by_key(state.db, run_ext_id, step_number, index)

            :not_found ->
              {:error, :not_found}
          end
      end
    else
      {:error, :invalid_format} -> {:error, :not_found}
    end
  end
end
