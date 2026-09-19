defmodule Coflux.Topics.Pool do
  @moduledoc "One pool: its definition and the workers launched for it."

  use Topical.Topic, route: ["workspaces", :workspace_id, "pools", :pool_name]

  alias Coflux.Orchestration
  alias Coflux.Topics.Diff
  alias Coflux.Topics.Pool.Model

  def connect(params, context) do
    {:ok, Map.put(params, :project, context.project)}
  end

  def init(params) do
    project_id = Map.fetch!(params, :project)
    workspace_id = Map.fetch!(params, :workspace_id)
    pool_name = Map.fetch!(params, :pool_name)

    case Orchestration.subscribe(project_id, {:pool, workspace_id, pool_name}, self()) do
      {:ok, events, ref} ->
        model = Model.fold(Model.new(pool_name), events)
        {:ok, Topic.new(Model.project(model), %{model: model, ref: ref})}

      {:error, :not_found} ->
        {:error, :not_found}

      {:error, :workspace_invalid} ->
        {:error, :not_found}
    end
  end

  def handle_info({:topic, _ref, events}, topic) do
    model = Model.fold(topic.state.model, events)
    topic = Diff.apply(topic, [], topic.value, Model.project(model))
    {:ok, %{topic | state: %{topic.state | model: model}}}
  end

  @doc "The wire shape of a pool definition, shared with the pools topic."
  def build_pool(nil), do: nil

  def build_pool(pool) do
    %{
      modules: pool.modules,
      provides: pool.provides,
      accepts: Map.get(pool, :accepts, %{}),
      launcher: if(pool.launcher, do: build_launcher(pool.launcher)),
      state: to_string(Map.get(pool, :state, :active))
    }
  end

  defp build_launcher(launcher) do
    base =
      case launcher.type do
        :docker ->
          %{type: "docker", image: launcher.image}
          |> maybe_put(:dockerHost, Map.get(launcher, :docker_host))
          |> maybe_put(:networkMode, Map.get(launcher, :network_mode))

        :process ->
          %{type: "process", directory: launcher.directory}

        # `token` is deliberately absent: this shape is delivered to every
        # subscriber of the topic, and the launcher's credentials are not
        # part of what a pool looks like. `caCert` is a path on the
        # server's host rather than a credential, so it stays. Everything
        # else a pool is configured with belongs here too, or `pools get`
        # shows less than `pools export` does.
        # As above: the secret access key and session token are credentials,
        # so they stay out. The key ID says which credentials without being
        # one, and is how a pool's access is recognised, so it stays.
        :ecs ->
          %{
            type: "ecs",
            cluster: launcher.cluster,
            taskDefinition: launcher.task_definition,
            region: launcher.region
          }
          |> maybe_put(:containerName, Map.get(launcher, :container_name))
          |> maybe_put(:launchType, Map.get(launcher, :launch_type))
          |> maybe_put(:capacityProvider, Map.get(launcher, :capacity_provider))
          |> maybe_put(:subnets, Map.get(launcher, :subnets))
          |> maybe_put(:securityGroups, Map.get(launcher, :security_groups))
          |> maybe_put(:assignPublicIp, Map.get(launcher, :assign_public_ip))
          |> maybe_put(:platformVersion, Map.get(launcher, :platform_version))
          |> maybe_put(:accessKeyId, Map.get(launcher, :access_key_id))
          |> maybe_put(:endpoint, Map.get(launcher, :endpoint))

        :kubernetes ->
          %{type: "kubernetes", image: launcher.image}
          |> maybe_put(:namespace, Map.get(launcher, :namespace))
          |> maybe_put(:apiServer, Map.get(launcher, :api_server))
          |> maybe_put(:serviceAccount, Map.get(launcher, :service_account))
          |> maybe_put(:caCert, Map.get(launcher, :ca_cert))
          |> maybe_put(:insecure, Map.get(launcher, :insecure))
          |> maybe_put(:imagePullPolicy, Map.get(launcher, :image_pull_policy))
          |> maybe_put(:nodeSelector, Map.get(launcher, :node_selector))
          |> maybe_put(:tolerations, Map.get(launcher, :tolerations))
          |> maybe_put(:imagePullSecrets, Map.get(launcher, :image_pull_secrets))
          |> maybe_put(:hostAliases, Map.get(launcher, :host_aliases))
          |> maybe_put(:labels, Map.get(launcher, :labels))
          |> maybe_put(:annotations, Map.get(launcher, :annotations))
          |> maybe_put(:activeDeadlineSeconds, Map.get(launcher, :active_deadline_seconds))
          |> maybe_put(:volumes, Map.get(launcher, :volumes))
          |> maybe_put(:volumeMounts, Map.get(launcher, :volume_mounts))
          |> maybe_put(:resources, Map.get(launcher, :resources))
      end

    base
    |> maybe_put(:serverHost, Map.get(launcher, :server_host))
    |> maybe_put(:serverSecure, Map.get(launcher, :server_secure))
    |> maybe_put(:adapter, Map.get(launcher, :adapter))
    |> maybe_put(:concurrency, Map.get(launcher, :concurrency))
    |> maybe_put(:idleTimeout, Map.get(launcher, :idle_timeout))
    |> maybe_put(:env, Map.get(launcher, :env))
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end

defmodule Coflux.Topics.Pool.Model do
  @moduledoc false

  import Kernel, except: [apply: 2]

  alias Coflux.Events.{
    PoolStateChanged,
    PoolUpdated,
    SessionConnected,
    SessionExecutions,
    SessionUpdated,
    WorkerCreated,
    WorkerDeactivated,
    WorkerLaunchResult,
    WorkerStateChanged,
    WorkerStopping,
    WorkerStopResult
  }

  alias Coflux.Topics.Pool

  def new(name), do: %{name: name, pool: nil, workers: %{}}

  def fold(model, events), do: Enum.reduce(events, model, &apply(&2, &1))

  def apply(model, %PoolUpdated{} = e), do: %{model | pool: e.definition}

  def apply(%{pool: nil} = model, %PoolStateChanged{}), do: model

  def apply(model, %PoolStateChanged{} = e),
    do: %{model | pool: Map.put(model.pool, :state, e.state)}

  def apply(model, %WorkerCreated{} = e) do
    worker = %{
      starting_at: e.created_at,
      started_at: nil,
      start_error: nil,
      stopping_at: nil,
      stopped_at: nil,
      stop_error: nil,
      deactivated_at: nil,
      error: nil,
      logs: nil,
      state: :active,
      session: e.session,
      connected: false,
      executions: 0
    }

    %{model | workers: Map.put(model.workers, e.worker, worker)}
  end

  def apply(model, %WorkerLaunchResult{} = e),
    do: update(model, e.worker, &%{&1 | started_at: e.started_at, start_error: e.error})

  def apply(model, %WorkerStopping{} = e),
    do: update(model, e.worker, &%{&1 | stopping_at: e.stopping_at})

  def apply(model, %WorkerStopResult{} = e),
    do: update(model, e.worker, &%{&1 | stopped_at: e.stopped_at, stop_error: e.error})

  def apply(model, %WorkerDeactivated{} = e),
    do:
      update(
        model,
        e.worker,
        &%{&1 | deactivated_at: e.deactivated_at, error: e.error, logs: e.logs}
      )

  def apply(model, %WorkerStateChanged{} = e),
    do: update(model, e.worker, &%{&1 | state: e.state})

  def apply(model, %SessionExecutions{worker: nil}), do: model

  def apply(model, %SessionExecutions{} = e),
    do: update(model, e.worker, &%{&1 | executions: e.executions})

  # Session events name a session, not a worker, so they land on whichever
  # worker was launched with it - and on none, for a session that isn't a
  # pool worker's.
  def apply(model, %SessionUpdated{} = e),
    do: update_by_session(model, e.session, &%{&1 | connected: e.connected})

  def apply(model, %SessionConnected{} = e),
    do: update_by_session(model, e.session, &%{&1 | connected: e.connected})

  defp update(model, worker, fun) do
    case Map.fetch(model.workers, worker) do
      {:ok, entry} -> %{model | workers: Map.put(model.workers, worker, fun.(entry))}
      :error -> model
    end
  end

  defp update_by_session(model, session, fun) do
    case Enum.find(model.workers, fn {_id, worker} -> worker.session == session end) do
      {worker_id, _} -> update(model, worker_id, fun)
      nil -> model
    end
  end

  def project(model) do
    %{
      pool: Pool.build_pool(model.pool),
      workers:
        Map.new(model.workers, fn {id, worker} ->
          {id,
           %{
             startingAt: worker.starting_at,
             startedAt: worker.started_at,
             startError: worker.start_error,
             stoppingAt: worker.stopping_at,
             stoppedAt: worker.stopped_at,
             stopError: worker.stop_error,
             deactivatedAt: worker.deactivated_at,
             error: worker.error,
             logs: worker.logs,
             state: worker.state,
             sessionId: worker.session,
             connected: worker.connected,
             executions: worker.executions
           }}
        end)
    }
  end
end
