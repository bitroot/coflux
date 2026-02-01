defmodule Coflux.ProjectsStore do
  @moduledoc """
  Configuration store for projects.

  Projects are loaded from $COFLUX_DATA_DIR/config/projects.json at startup and
  automatically reloaded when the file changes. If the file is invalid,
  a warning is logged and the old configuration is kept.

  If COFLUX_PROJECT is set, that project is added automatically.

  Project file format:
  ```json
  {
    "test1": {},
    "test2": {}
  }
  ```

  - Key: project name (string)
  """

  use Coflux.ConfigStore,
    filename: "projects.json",
    table_key: :coflux_projects_table

  alias Coflux.Config

  @impl Coflux.ConfigStore
  def parse_entries(data) do
    projects = Map.new(data, fn {name, _config} -> {name, %{}} end)
    add_env_project(projects)
  end

  @doc """
  Checks if a project exists. Reads directly from ETS.

  Returns `true` if project exists, `false` otherwise.
  """
  def exists?(project), do: has_key?(project)

  defp add_env_project(projects) do
    case Config.project() do
      nil -> projects
      project_id -> Map.put_new(projects, project_id, %{})
    end
  end
end
