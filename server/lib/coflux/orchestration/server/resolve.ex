defmodule Coflux.Orchestration.Server.Resolve do
  @moduledoc """
  Stored rows resolved into the shapes that leave the server.

  A row holds internal ids and unresolved references; an event, a worker
  message or an API reply holds external ids and the detail a reader needs
  without a further lookup. Everything that crosses that boundary in the
  outward direction is here, so there is one answer to "what does a
  resolved value/result/execution look like" rather than one per call
  site.

  Every function takes `db` first and is a pure read: no server state, no
  events, nothing to flush. That is what makes it safe to call from the
  emit sites, the snapshot loaders and the archived-epoch searches alike -
  including against an archive's database rather than the active one.

  The inward direction lives elsewhere: `Coflux.Orchestration.Values`
  normalises an incoming value's references into the stored form.
  """

  alias Coflux.Orchestration.{
    Assets,
    CacheConfigs,
    Ids,
    Principals,
    Results,
    Runs,
    Streams,
    TagSets,
    Values,
    Workspaces
  }

  # ---------------------------------------------------------------------------
  # Values and references

  @doc "A stored value with every reference it carries resolved."
  def value(db, {:raw, data, refs}), do: {:raw, data, references(db, refs)}
  def value(db, {:blob, key, size, refs}), do: {:blob, key, size, references(db, refs)}

  @doc """
  References resolved: an execution gains its external id and its step's
  module and target, an asset its summary, a fragment passes through.
  """
  def references(db, references) do
    Enum.map(references, fn
      {:fragment, format, blob_key, size, metadata} ->
        {:fragment, format, blob_key, size, metadata}

      {:execution, run_ext, step_num, attempt} ->
        ext_id = Ids.execution(run_ext, step_num, attempt)

        {module, target} =
          case Runs.get_module_target(db, run_ext, step_num, attempt) do
            {:ok, {m, t}} -> {m, t}
            {:ok, nil} -> {nil, nil}
          end

        {:execution, ext_id, {module, target}}

      {:asset, external_id} ->
        {:ok, asset_id} = Assets.get_asset_id(db, external_id)
        {^external_id, name, total_count, total_size, entry} = asset(db, asset_id)
        {:asset, external_id, {name, total_count, total_size, entry}}

      {:input, external_id} ->
        {:input, external_id}
    end)
  end

  @doc "An asset's summary: `{external_id, name, total_count, total_size, entry}`."
  def asset(db, asset_id) do
    case Assets.get_asset_summary(db, asset_id) do
      {:ok, external_id, name, total_count, total_size, entry} ->
        {external_id, name, total_count, total_size, entry}
    end
  end

  @doc """
  Checkpoint values resolved. Checkpoints carry references in the same
  form as arguments, so they need the same resolution before going out to
  a worker or a topic.
  """
  def checkpoints(db, checkpoints) do
    Map.new(checkpoints, fn {name, v} -> {name, value(db, v)} end)
  end

  # ---------------------------------------------------------------------------
  # Executions and results

  @doc "An execution as `{external_id, module, target}`, by internal id."
  def execution(db, execution_id) do
    {:ok, {run_ext, step_num, attempt, module, target}} =
      Runs.get_run_by_execution(db, execution_id)

    {Ids.execution(run_ext, step_num, attempt), module, target}
  end

  @doc "The same, through an `execution_refs` row - which may name an execution in another epoch."
  def execution_ref(db, ref_id) do
    {:ok, {run_ext, step_num, attempt, module, target}} = Runs.get_execution_ref(db, ref_id)
    {Ids.execution(run_ext, step_num, attempt), module, target}
  end

  @doc """
  A result with its nested detail resolved: a value's references, and the
  execution a retry or hand-off points at (recursively, for a hand-off
  that has itself resolved).
  """
  def result(db, result) do
    case result do
      {:error, type, message, frames, retry_id, retryable} ->
        {:error, type, message, frames, maybe_execution(db, retry_id), retryable}

      {:error, type, message, frames, retry_id} ->
        {:error, type, message, frames, maybe_execution(db, retry_id)}

      {:value, v} ->
        {:value, value(db, v)}

      {:abandoned, retry_id} ->
        {:abandoned, maybe_execution(db, retry_id)}

      {:crashed, retry_id} ->
        {:crashed, maybe_execution(db, retry_id)}

      :cancelled ->
        :cancelled

      {:timeout, retry_id} ->
        {:timeout, maybe_execution(db, retry_id)}

      {:suspended, successor_id} ->
        {:suspended, maybe_execution(db, successor_id)}

      {:recurred, successor_id} ->
        {:recurred, maybe_execution(db, successor_id)}

      # In-flight successor: the id is an internal execution id, and its
      # own result may not have landed yet.
      {type, execution_id}
      when type in [:deferred, :cached, :spawned] and is_integer(execution_id) ->
        inner =
          case Results.resolve(db, execution_id) do
            {:ok, inner} -> inner
            {:pending, _execution_id} -> nil
          end

        {type, execution(db, execution_id), result(db, inner)}

      # Resolved ref form: the ref and the value are both already stored.
      {type, ref_id, v} when type in [:deferred, :cached, :spawned] ->
        {type, execution_ref(db, ref_id), {:value, value(db, v)}}

      nil ->
        nil
    end
  end

  @doc """
  Whether a result stands: nothing retries or supersedes it. An execution
  that handed off shows its successor's result nested, so the hand-off
  itself is not final until that resolves.
  """
  def final_result?(result) do
    case result do
      {:error, _, _, _, retry_id, _retryable} -> is_nil(retry_id)
      {:error, _, _, _, retry_id} -> is_nil(retry_id)
      {:value, _} -> true
      {:abandoned, retry_id} -> is_nil(retry_id)
      {:crashed, retry_id} -> is_nil(retry_id)
      :cancelled -> true
      {:timeout, retry_id} -> is_nil(retry_id)
      {:suspended, _} -> false
      {:recurred, _} -> false
      {:deferred, _} -> false
      {:cached, _} -> false
      {:spawned, _} -> false
      # Resolved ref forms are final: the value is already resolved.
      {:deferred, _, _} -> true
      {:cached, _, _} -> true
      {:spawned, _, _} -> true
    end
  end

  @doc "A successor, as the run topics show it."
  def successor(nil), do: nil

  def successor({run_ext, step_number, attempt}) do
    %{type: "execution", id: Ids.execution(run_ext, step_number, attempt)}
  end

  # ---------------------------------------------------------------------------
  # Catalog

  @doc "A catalog version as topics and the API see it, with its value resolved for rendering."
  def catalog_version(db, version) do
    {:ok, stored_value} = Values.get_value_by_id(db, version.value_id)

    published_by =
      if version.execution_ref_id do
        {ext_id, _module, _target} = execution_ref(db, version.execution_ref_id)
        ext_id
      end

    created_by =
      case Principals.get_principal(db, version.created_by) do
        {:ok, {type, external_id}} -> %{type: type, external_id: external_id}
        {:ok, nil} -> nil
      end

    {:ok, workspace_external_id} = Workspaces.get_workspace_external_id(db, version.workspace_id)

    %{
      path: version.path,
      number: version.number,
      sequence: version.id,
      value: value(db, stored_value),
      created_at: version.created_at,
      workspace_id: workspace_external_id,
      published_by: published_by,
      created_by: created_by
    }
  end

  # ---------------------------------------------------------------------------
  # Streams

  @doc "The external id of a stream's current producer, or nil if it has none."
  def stream_producer(db, stream_id) do
    with {:ok, execution_id} <- Streams.get_producer(db, stream_id),
         {:ok, {r, s, a}} <- Runs.get_execution_key(db, execution_id) do
      Ids.execution(r, s, a)
    else
      _ -> nil
    end
  end

  @doc """
  A stream closure as consumers see it, as `{reason, error}`.

  A closure stored as `:lifecycle` says only that the producer stopped;
  what actually happened is on the producing execution's completion, so it
  is derived here (see `lifecycle_info/2`). Every other reason is stored
  with whatever error belongs to it.
  """
  def closure_reason(db, :lifecycle, _stored_error, closed_by), do: lifecycle_info(db, closed_by)
  def closure_reason(_db, reason, stored_error, _closed_by), do: {reason, stored_error}

  @doc """
  What a lifecycle closure means, from the producing execution's
  completion kind: `{reason, error}`, where `reason` is the shape of the
  ending rather than a fabricated exception, and `error` is non-nil only
  for `:errored`. Clients decide how to represent each reason.

  `:recurred` is reported distinctly from `:abandoned`: the producer
  didn't fail, it finished a recurrent iteration, and the next one opens
  its own streams. (A suspend never closes a stream, so it never appears
  here.)
  """
  def lifecycle_info(db, execution_id) do
    case Results.get_completion(db, execution_id) do
      {:ok, {:cancelled, _, _, _, _}} ->
        {:cancelled, nil}

      {:ok, {:abandoned, _, _, _, _}} ->
        {:abandoned, nil}

      {:ok, {:crashed, _, _, _, _}} ->
        {:crashed, nil}

      {:ok, {:timeout, _, _, _, _}} ->
        {:timeout, nil}

      {:ok, {:recurred, _, _, _, _}} ->
        {:recurred, nil}

      {:ok, {:errored, _, _, _, _}} ->
        # The error payload lives on the results row - pull it so
        # consumers see the producer's actual exception.
        case Results.get_result_payload(db, execution_id) do
          {:ok, {:error, type, message, frames, _}} -> {:errored, {type, message, frames}}
          _ -> {:errored, nil}
        end

      _ ->
        {nil, nil}
    end
  end

  # ---------------------------------------------------------------------------
  # Odds and ends

  @doc "A tag set by id, or an empty one for nil."
  def tag_set(_db, nil), do: %{}

  def tag_set(db, tag_set_id) do
    case TagSets.get_tag_set(db, tag_set_id) do
      {:ok, tag_set} -> tag_set
    end
  end

  @doc "The cache config of each of `steps` that has one, keyed by id."
  def cache_configs(db, steps) do
    steps
    |> Enum.map(& &1.cache_config_id)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Map.new(fn cache_config_id ->
      case CacheConfigs.get_cache_config(db, cache_config_id) do
        {:ok, cache_config} -> {cache_config_id, cache_config}
      end
    end)
  end

  defp maybe_execution(_db, nil), do: nil
  defp maybe_execution(db, execution_id), do: execution(db, execution_id)
end
