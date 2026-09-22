defmodule Coflux.Orchestration.Principals do
  @moduledoc """
  Who did something. A principal names a user or a token by external id,
  and everything with a `created_by` points at a row here.

  Tokens themselves live in the admin store (`Coflux.Admin.Tokens`), which
  isn't rotated, so a token's principal is made here when the token is
  first seen in an epoch rather than when the token is created.
  """

  alias Coflux.Store

  @doc """
  Looks up or creates a user principal by external_id (JWT sub claim).
  Returns {:ok, principal_id}.
  """
  def ensure_user(db, external_id), do: ensure(db, :user_external_id, external_id)

  @doc """
  Looks up or creates a token principal by the token's external_id.
  Returns {:ok, principal_id}.
  """
  def ensure_token(db, external_id), do: ensure(db, :token_external_id, external_id)

  @doc """
  Looks up or creates the principal for an identity as `build/2` describes
  it. Nobody (nil) has no principal.
  """
  def ensure_identity(_db, nil), do: {:ok, nil}
  def ensure_identity(db, %{type: "user", external_id: id}), do: ensure_user(db, id)
  def ensure_identity(db, %{type: "token", external_id: id}), do: ensure_token(db, id)

  defp ensure(db, column, external_id) do
    case Store.query_one(db, "SELECT id FROM principals WHERE #{column} = ?1", {external_id}) do
      {:ok, {id}} ->
        {:ok, id}

      {:ok, nil} ->
        Store.insert_one(db, :principals, %{column => external_id})
    end
  end

  @doc """
  Who did something, from the external ids of the user and token a
  `principals` row holds. At most one is set; neither means nobody - a
  run the server started on its own.
  """
  def build(nil, nil), do: nil
  def build(user_external_id, nil), do: %{type: "user", external_id: user_external_id}
  def build(nil, token_external_id), do: %{type: "token", external_id: token_external_id}

  @doc """
  Gets the type and external_id for a principal.
  Returns {:ok, {type, external_id}} or {:ok, nil} if not found.
  """
  def get_principal(_db, nil), do: {:ok, nil}

  def get_principal(db, principal_id) do
    case Store.query_one(
           db,
           "SELECT user_external_id, token_external_id FROM principals WHERE id = ?1",
           {principal_id}
         ) do
      {:ok, {user_external_id, nil}} ->
        {:ok, {"user", user_external_id}}

      {:ok, {nil, token_external_id}} ->
        {:ok, {"token", token_external_id}}

      {:ok, nil} ->
        {:ok, nil}
    end
  end
end
