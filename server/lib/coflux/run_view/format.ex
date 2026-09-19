defmodule Coflux.RunView.Format do
  @moduledoc """
  Converts what a `Coflux.RunView` holds (orchestration's tuples and maps)
  into the shape the run topics publish.
  """

  import Coflux.TopicUtils

  alias Coflux.Orchestration.Ids

  @branch_statuses [:assigning, :running, :completed, :errored, :aborted, :suspended]

  defdelegate step_key(run_external_id, step_number), to: Ids, as: :step

  def child(%{step: step, attempt: attempt, group_id: group_id}, run_external_id) do
    %{stepId: step_key(run_external_id, step), attempt: attempt, groupId: group_id}
  end

  def value(value), do: build_value(value)

  def values(values), do: Map.new(values, fn {name, value} -> {name, build_value(value)} end)

  def principal(principal), do: build_principal(principal)

  def asset(asset), do: build_asset(asset)

  def catalog_version(version), do: build_catalog_version(version)

  # How a resolved version and a wait are keyed among an execution's
  # dependencies (and its publishes): the same keys the orchestration uses.
  defdelegate catalog_version_key(path, number), to: Ids, as: :catalog_version
  defdelegate catalog_wait_key(path, number), to: Ids, as: :catalog_wait

  def branch_status(status), do: Atom.to_string(status)

  @doc """
  The summary of a group's members: how many there are, and how many are
  in each branch status. Every status is present so the shape is stable.
  """
  def members(counts) do
    by_status = Map.new(@branch_statuses, fn status -> {status, Map.get(counts, status, 0)} end)
    %{total: Enum.sum(Map.values(by_status)), byStatus: by_status}
  end

  def cache_config(nil), do: nil

  def cache_config(cache_config) do
    %{
      params: cache_config.params,
      maxAge: cache_config.max_age,
      namespace: cache_config.namespace,
      version: cache_config.version
    }
  end

  def concurrency(%{concurrency_key: nil}), do: nil

  def concurrency(step) do
    %{limit: step.concurrency_limit, key: key(step.concurrency_key)}
  end

  # The group limit the step counts against, if any. The key is readable
  # as it stands ("<parent execution>/<group id>"), so no hex prefix.
  def group(%{group_key: nil}), do: nil

  def group(step) do
    %{limit: step.group_limit, key: step.group_key}
  end

  def retries(nil), do: nil

  def retries(%{limit: limit, backoff_min: backoff_min, backoff_max: backoff_max}) do
    %{limit: limit, backoffMin: backoff_min, backoffMax: backoff_max}
  end

  def key(key, length \\ 10)
  def key(nil, _length), do: nil

  def key(key, length) do
    key
    |> Base.encode16(case: :lower)
    |> String.slice(0, length)
  end

  def metric(definition) do
    %{
      group: definition.group,
      groupUnits: definition.group_units,
      groupLower: definition.group_lower,
      groupUpper: definition.group_upper,
      scale: definition.scale,
      units: definition.units,
      progress: definition.progress,
      lower: definition.lower,
      upper: definition.upper
    }
  end

  # `pending` marks a dependency the execution is still waiting on. It's
  # carried per entry rather than as a count so the graph can point at the
  # one that's holding a step back.
  def dependencies(dependencies, pending) do
    Map.new(dependencies, fn
      {id, {:result, execution}} ->
        {id,
         %{
           type: "result",
           execution: build_execution(execution),
           pending: MapSet.member?(pending, id)
         }}

      {id, {:input, title, status}} ->
        {id,
         %{
           type: "input",
           inputId: id,
           title: title,
           status: status,
           pending: MapSet.member?(pending, id)
         }}

      {id, {:asset, asset}} ->
        {id,
         %{
           type: "asset",
           assetId: id,
           asset: build_asset(asset),
           pending: MapSet.member?(pending, id)
         }}

      {id, {:stream, stream_id, module, target}} ->
        {id,
         %{
           type: "stream",
           streamId: stream_id,
           module: module,
           target: target,
           pending: MapSet.member?(pending, id)
         }}

      {id, {:catalog, version}} ->
        {id,
         %{
           type: "catalog",
           path: version.path,
           number: version.number,
           version: build_catalog_version(version),
           pending: false
         }}

      # A wait is for whatever comes after `number`: no version yet.
      {id, {:catalog_wait, path, number}} ->
        {id,
         %{
           type: "catalog",
           path: path,
           number: number,
           version: nil,
           pending: MapSet.member?(pending, id)
         }}
    end)
  end

  def frames(frames) do
    Enum.map(frames, fn {file, line, name, code} ->
      %{file: file, line: line, name: name, code: code}
    end)
  end

  def result(result, created_by \\ nil) do
    created_by = build_principal(created_by)

    case result do
      {:error, type, message, frames, retry, retryable} ->
        %{
          type: "error",
          createdBy: created_by,
          error: %{type: type, message: message, frames: frames(frames)},
          retry: if(retry, do: execution_attempt(retry)),
          retryable: retryable
        }

      {:error, type, message, frames, retry} ->
        %{
          type: "error",
          createdBy: created_by,
          error: %{type: type, message: message, frames: frames(frames)},
          retry: if(retry, do: execution_attempt(retry)),
          retryable: nil
        }

      {:value, value} ->
        %{type: "value", createdBy: created_by, value: build_value(value)}

      {:abandoned, retry} ->
        %{
          type: "abandoned",
          createdBy: created_by,
          retry: if(retry, do: execution_attempt(retry))
        }

      {:crashed, retry} ->
        %{
          type: "crashed",
          createdBy: created_by,
          retry: if(retry, do: execution_attempt(retry))
        }

      :cancelled ->
        %{type: "cancelled", createdBy: created_by}

      {:timeout, retry} ->
        %{
          type: "timeout",
          createdBy: created_by,
          retry: if(retry, do: execution_attempt(retry))
        }

      {:suspended, successor} ->
        %{
          type: "suspended",
          createdBy: created_by,
          successor: if(successor, do: execution_attempt(successor))
        }

      {:recurred, successor} ->
        %{
          type: "recurred",
          createdBy: created_by,
          successor: if(successor, do: execution_attempt(successor))
        }

      {:deferred, execution, result} ->
        %{
          type: "deferred",
          createdBy: created_by,
          execution: build_execution(execution),
          result: result(result)
        }

      {:cached, execution, result} ->
        %{
          type: "cached",
          createdBy: created_by,
          execution: build_execution(execution),
          result: result(result)
        }

      {:spawned, execution, result} ->
        %{
          type: "spawned",
          createdBy: created_by,
          execution: build_execution(execution),
          result: result(result)
        }

      nil ->
        nil
    end
  end

  # Checkpoints are reported as a pair so an attempt can be read as a
  # transition: `before` is what it was handed when it started, `after` what
  # it ended up holding. They're equal for an attempt that didn't write.
  def checkpoints(before, after_) do
    %{before: values(before), after: values(after_)}
  end

  # Streams belong to the step. Only those in a workspace the topic is
  # showing are included — a re-run in another workspace opens its own.
  def streams(streams, workspace_ids) do
    streams
    |> Enum.filter(fn {_index, stream} -> stream.workspace_id in workspace_ids end)
    |> Map.new(fn {index, stream} -> {Integer.to_string(index), stream(stream)} end)
  end

  def stream(stream) do
    %{
      id: stream.id,
      index: stream.index,
      position: stream.position,
      workspaceId: stream.workspace_id,
      buffer: stream.buffer,
      timeoutMs: stream.timeout_ms,
      openedAt: stream.opened_at,
      attempts: stream.attempts,
      closedAt: stream.closed_at,
      closedBy: stream.closed_by,
      reason: stream.reason,
      error: stream.error
    }
  end

  def stream_error(nil), do: nil

  def stream_error({type, message, frames}) do
    %{type: type, message: message, frames: frames(frames)}
  end

  defp execution_attempt({ext_id, _module, _target}) do
    ext_id |> String.split(":") |> List.last() |> String.to_integer()
  end
end
