defmodule Coflux.Auth do
  @moduledoc """
  Handles token authentication.

  Token access is configured in $COFLUX_DATA_DIR/tokens.json:
  ```json
  {
    "e3044207e9a26a59388c7224fc3f3c01...": {
      "projects": ["test1", "test2"],
      "workspaces": ["production"]
    },
    "479ccb088f9a5dce29dbff65996beac6...": {
      "projects": ["test1"],
      "workspaces": ["staging", "development/*"]
    }
  }
  ```

  Authentication mode is controlled by COFLUX_AUTH_MODE:
  - "token" (default): All requests require valid token
  - "none": No authentication required
  """

  alias Coflux.{Config, TokensStore}

  @doc """
  Checks if the given token is authorized for the project and optional workspace.

  Returns `:ok` when access is allowed.
  Returns `{:error, :unauthorized}` otherwise.
  """
  def check(token, project_id, workspace_name \\ nil) do
    case Config.auth_mode() do
      :none ->
        :ok

      :token ->
        check_token(token, project_id, workspace_name)
    end
  end

  @doc """
  Resolves the allowed workspace IDs for a token.

  Takes the token, project_id, and a map of workspaces `%{workspace_id => %{name: name, ...}}`.

  Returns:
  - `{:ok, :all}` if auth is disabled
  - `{:ok, MapSet.t()}` with allowed workspace IDs
  - `{:error, :unauthorized}` if token is invalid or project not allowed
  """
  def resolve_allowed_workspaces(token, project_id, workspaces) do
    case Config.auth_mode() do
      :none ->
        {:ok, :all}

      :token ->
        resolve_token_workspaces(token, project_id, workspaces)
    end
  end

  @doc """
  Checks if a workspace ID is in the allowed set.

  Returns `true` if allowed, `false` otherwise.
  """
  def workspace_allowed?(_workspace_id, :all), do: true

  def workspace_allowed?(workspace_id, allowed_ids) when is_struct(allowed_ids, MapSet) do
    MapSet.member?(allowed_ids, workspace_id)
  end

  @doc """
  Checks if a workspace name matches the allowed patterns.

  Returns `true` if the name matches any pattern.
  """
  def name_matches_patterns?(workspace_name, patterns) do
    Enum.any?(patterns, fn pattern ->
      match_workspace_pattern?(workspace_name, pattern)
    end)
  end

  # Private functions

  defp check_token(nil, _project_id, _workspace_name), do: {:error, :unauthorized}

  defp check_token(token, project_id, workspace_name) do
    token_hash = hash_token(token)

    case TokensStore.get_token_config(token_hash) do
      {:ok, config} ->
        with :ok <- check_project(project_id, config.projects),
             :ok <- check_workspace(workspace_name, config.workspaces) do
          :ok
        end

      :error ->
        {:error, :unauthorized}
    end
  end

  defp resolve_token_workspaces(nil, _project_id, _workspaces), do: {:error, :unauthorized}

  defp resolve_token_workspaces(token, project_id, workspaces) do
    token_hash = hash_token(token)

    case TokensStore.get_token_config(token_hash) do
      {:ok, config} ->
        case check_project(project_id, config.projects) do
          :ok ->
            allowed_ids =
              workspaces
              |> Enum.filter(fn {_id, ws} ->
                name_matches_patterns?(ws.name, config.workspaces)
              end)
              |> Enum.map(fn {id, _ws} -> id end)
              |> MapSet.new()

            {:ok, allowed_ids}

          error ->
            error
        end

      :error ->
        {:error, :unauthorized}
    end
  end

  defp check_project(project_id, allowed_projects) do
    if project_id in allowed_projects do
      :ok
    else
      {:error, :unauthorized}
    end
  end

  defp check_workspace(nil, _allowed_workspaces), do: :ok

  defp check_workspace(workspace_name, allowed_workspaces) do
    if name_matches_patterns?(workspace_name, allowed_workspaces) do
      :ok
    else
      {:error, :unauthorized}
    end
  end

  defp match_workspace_pattern?(workspace_name, pattern) do
    cond do
      # Match all workspaces
      pattern == "*" ->
        true

      # Exact match
      workspace_name == pattern ->
        true

      # Wildcard pattern: "staging/*" matches "staging", "staging/feature1", etc.
      String.ends_with?(pattern, "/*") ->
        prefix = String.slice(pattern, 0..-3//1)
        workspace_name == prefix or String.starts_with?(workspace_name, prefix <> "/")

      true ->
        false
    end
  end

  defp hash_token(token) do
    :crypto.hash(:sha256, token)
    |> Base.encode16(case: :lower)
  end
end
