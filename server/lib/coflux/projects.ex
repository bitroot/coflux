defmodule Coflux.Projects do
  use GenServer

  alias Coflux.Utils

  def start_link(opts) do
    GenServer.start_link(__MODULE__, {}, opts)
  end

  def create_project(server, project_name, namespace \\ nil) do
    GenServer.call(server, {:create_project, project_name, namespace})
  end

  @doc """
  Gets a project by ID, optionally validating namespace access.

  Returns:
    - {:ok, project} if project exists and namespace matches (if provided)
    - :error if project doesn't exist or belongs to a different namespace
  """
  def get_project_by_id(server, project_id, namespace \\ nil) do
    GenServer.call(server, {:get_project_by_id, project_id, namespace})
  end

  def subscribe(server, pid, namespace \\ nil) do
    GenServer.call(server, {:subscribe, pid, namespace})
  end

  def unsubscribe(server, ref) do
    GenServer.cast(server, {:unsubscribe, ref})
  end

  def init({}) do
    path = get_path()

    projects =
      if File.exists?(path) do
        path
        |> File.read!()
        |> Jason.decode!()
        |> Map.new(fn {project_id, project} ->
          {project_id, build_project(project)}
        end)
      else
        %{}
      end

    {:ok, %{projects: projects, subscribers: %{}}}
  end

  def handle_call({:create_project, project_name, namespace}, _from, state) do
    existing_project_names =
      state.projects
      |> Map.values()
      |> Enum.filter(&(&1.namespace == namespace))
      |> MapSet.new(& &1.name)

    errors =
      Map.reject(
        %{
          project_name: validate_project_name(project_name, existing_project_names)
        },
        fn {_key, value} -> value == :ok end
      )

    if Enum.any?(errors) do
      {:reply, {:error, errors}, state}
    else
      project_id = generate_id(state)

      state =
        put_in(
          state.projects[project_id],
          %{name: project_name, namespace: namespace}
        )

      save_projects(state)
      notify_subscribers(state, project_id)
      {:reply, {:ok, project_id}, state}
    end
  end

  def handle_call({:get_project_by_id, project_id, namespace}, _from, state) do
    result =
      case Map.fetch(state.projects, project_id) do
        {:ok, project} ->
          if project.namespace == namespace do
            {:ok, project}
          else
            :error
          end

        :error ->
          :error
      end

    {:reply, result, state}
  end

  def handle_call({:subscribe, pid, namespace}, _from, state) do
    ref = Process.monitor(pid)
    state = put_in(state.subscribers[ref], {pid, namespace})

    filtered_projects =
      state.projects
      |> Enum.filter(fn {_id, project} -> project.namespace == namespace end)
      |> Map.new()

    {:reply, {ref, filtered_projects}, state}
  end

  def handle_cast({:unsubscribe, ref}, state) do
    state = remove_subscriber(state, ref)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    state = remove_subscriber(state, ref)
    {:noreply, state}
  end

  defp get_path() do
    Utils.data_path("projects.json")
  end

  defp remove_subscriber(state, ref) do
    Map.update!(state, :subscribers, &Map.delete(&1, ref))
  end

  defp notify_subscribers(state, project_id) do
    project = Map.fetch!(state.projects, project_id)

    Enum.each(state.subscribers, fn {ref, {pid, namespace}} ->
      if project.namespace == namespace do
        send(pid, {:project, ref, project_id, project})
      end
    end)
  end

  defp save_projects(state) do
    projects_for_json =
      Map.new(state.projects, fn {id, project} ->
        json_project =
          if project.namespace do
            %{"name" => project.name, "namespace" => project.namespace}
          else
            %{"name" => project.name}
          end

        {id, json_project}
      end)

    content = Jason.encode!(projects_for_json)
    path = get_path()
    :ok = File.mkdir_p!(Path.dirname(path))
    File.write!(path, content)
  end

  def generate_id(state, length \\ 5) do
    id = Utils.generate_id(length, "p")

    if Map.has_key?(state.projects, id) do
      generate_id(state, length + 1)
    else
      id
    end
  end

  defp build_project(project) do
    %{
      name: Map.fetch!(project, "name"),
      namespace: Map.get(project, "namespace")
    }
  end

  defp validate_project_name(name, existing) do
    cond do
      # TODO: stricter validation?
      !name -> :invalid
      Enum.member?(existing, name) -> :exists
      true -> :ok
    end
  end
end
