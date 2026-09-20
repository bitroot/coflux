defmodule Coflux.Scopes do
  @moduledoc """
  The workspace naming hierarchy, and the one relation asked of it:
  whether a scope covers a workspace.

  A scope is a workspace name, or a prefix of one along the `/` in the
  name - `development` covers `development/joe` and `development/joe/x` -
  and the root scope, `""`, covers every workspace. Names are compared a
  segment at a time, so `development` doesn't cover `development-2`.

  Two things are defined in terms of this: which workspaces a secret set
  for a scope applies to, and which workspaces a token's grant allows. A
  grant is written as a pattern - `*` for the root, and `development` or
  `development/*` for a subtree, which mean the same thing - and
  `from_pattern/1` turns one into the scope it grants.

  Coverage is closed downward: a scope that covers a workspace covers
  everything under that workspace too. That is what makes it sound to
  authorise a whole scope - setting a secret for one, say - by asking
  only whether a grant covers the scope's own name.
  """

  @typedoc """
  A scope, or `:never` - the scope of a pattern that grants nothing,
  which covers no workspace at all.
  """
  @type t :: String.t() | :never

  @doc """
  Whether `scope` covers `name`: the workspace is the scope itself, or
  lies under it.
  """
  @spec covers?(t(), String.t()) :: boolean()
  def covers?(:never, _name), do: false
  def covers?("", _name), do: true

  def covers?(scope, name) when is_binary(scope) and is_binary(name),
    do: name == scope or String.starts_with?(name, scope <> "/")

  @doc """
  The scope a grant pattern grants.

  `*` is the root scope, and a trailing `/*` is optional, so
  `development/*` and `development` both grant the `development`
  subtree - the workspace itself and everything under it.

  Anything that names nothing - an empty pattern, or a bare `/*` - grants
  `:never` rather than the root, so a stored pattern that means nothing
  can't come to mean everything.
  """
  @spec from_pattern(term()) :: t()
  def from_pattern("*"), do: ""

  def from_pattern(pattern) when is_binary(pattern) do
    case String.replace_suffix(pattern, "/*", "") do
      "" -> :never
      scope -> scope
    end
  end

  def from_pattern(_pattern), do: :never

  @doc """
  Whether a pattern names a scope - false for one that grants nothing.
  """
  @spec valid_pattern?(term()) :: boolean()
  def valid_pattern?(pattern), do: from_pattern(pattern) != :never
end
