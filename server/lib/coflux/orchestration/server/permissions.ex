defmodule Coflux.Orchestration.Server.Permissions do
  @moduledoc """
  Who may do what, and to which workspace.

  Every operation that names a workspace resolves it here, and resolution
  and authorisation are the same step: `require_workspace/3` either hands
  back the workspace or says why not, so there is no way to act on a
  workspace without having been allowed to.

  Access arrives as the caller's grant - a token's workspace patterns, or
  a studio session's - and `nil` means an internal caller with no
  restriction. A pattern grants a scope of the workspace naming
  hierarchy, which `Coflux.Scopes` defines. There is one level of it: a
  grant covering a workspace allows everything in that workspace, from
  submitting a run to editing its pools. "Operator" here means only that
  - holding the workspace - not a second, higher kind of grant.

  Also here: which workspaces a cache lookup may reach into, which is the
  same question of visibility asked of the workspace graph rather than of
  a caller.
  """

  alias Coflux.Orchestration.{Sessions}
  alias Coflux.Scopes

  def require_workspace(state, workspace_external_id, access \\ nil) do
    case Map.fetch(state.workspace_external_ids, workspace_external_id) do
      {:ok, workspace_id} ->
        workspace = Map.fetch!(state.workspaces, workspace_id)

        cond do
          workspace.state == :archived ->
            {:error, :workspace_invalid}

          access != nil and not operator?(access[:workspaces], workspace.name) ->
            {:error, :forbidden}

          true ->
            {:ok, workspace_id, workspace}
        end

      :error ->
        {:error, :workspace_invalid}
    end
  end

  def resolve_workspace_external_id(state, workspace_external_id) do
    case Map.fetch(state.workspace_external_ids, workspace_external_id) do
      {:ok, workspace_id} -> {:ok, workspace_id}
      :error -> {:error, :workspace_invalid}
    end
  end

  def resolve_optional_workspace(_state, nil), do: {:ok, nil}

  def resolve_optional_workspace(state, external_id) do
    case Map.fetch(state.workspace_external_ids, external_id) do
      {:ok, workspace_id} -> {:ok, workspace_id}
      :error -> {:error, %{base_id: "invalid"}}
    end
  end

  def operator?(scopes, workspace), do: Scopes.covers_any?(scopes, workspace)

  def check_operator_access(nil, _name), do: :ok

  def check_operator_access(access, name) do
    if operator?(access[:workspaces], name), do: :ok, else: {:error, :name_restricted}
  end

  def check_rename_allowed(_access, nil), do: :ok
  def check_rename_allowed(nil, _name), do: :ok

  def check_rename_allowed(access, new_name) do
    if operator?(access[:workspaces], new_name), do: :ok, else: {:error, :name_restricted}
  end

  def verify_session_secret(secret, secret_hash) do
    if Sessions.verify_secret(secret, secret_hash), do: :ok, else: {:error, :session_invalid}
  end

  def require_workspace_match(workspace_id, expected_workspace_id) do
    if workspace_id == expected_workspace_id, do: :ok, else: {:error, :workspace_mismatch}
  end

  def is_workspace_ancestor?(state, maybe_ancestor_id, workspace_id) do
    # TODO: avoid cycle?
    workspace = Map.fetch!(state.workspaces, workspace_id)

    cond do
      !workspace.base_id ->
        false

      workspace.base_id == maybe_ancestor_id ->
        true

      true ->
        is_workspace_ancestor?(state, maybe_ancestor_id, workspace.base_id)
    end
  end

  # The workspace inheritance chain ordered nearest-first (the workspace
  # itself, then its bases). Checkpoint reads walk this and stop at the first
  # workspace that has any state, so the nearest scope wins outright.

  def get_cache_workspace_ids(state, workspace_id, ids \\ []) do
    workspace = Map.fetch!(state.workspaces, workspace_id)

    if workspace.base_id do
      get_cache_workspace_ids(state, workspace.base_id, [workspace_id | ids])
    else
      [workspace_id | ids]
    end
  end
end
