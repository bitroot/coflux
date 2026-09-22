defmodule Coflux.Scopes do
  @moduledoc """
  Scopes: the patterns that select workspaces.

  A workspace name identifies one workspace - `development/joe`. A scope
  selects a set of them:

    * `*` - every workspace
    * `development/*` - every workspace under `development/`, but not
      `development` itself
    * `development` - that workspace, and nothing else

  The `*` is not a glob. `development/*` reaches `development/joe` and
  `development/joe/feature-1` alike - it is a prefix of the name, not one
  level of it. To select a workspace *and* everything under it, name both:
  `development,development/*`.

  Several scopes given together - a token's grant, or the workspaces a
  secret is set for - mean the union of what each selects.

  Two questions get asked of a scope, and they are not the same question:

    * `covers?/2` - does this scope select this workspace? Asked when a
      caller acts on a workspace, and when a secret is resolved for one.

    * `contains?/2` - does this scope select everything that one does?
      Asked when a caller hands authority on: setting a secret for a
      scope, or minting a token for one. Selecting *part* of a scope is
      not enough to give it away, and asking `covers?/2` there - treating
      the scope as though it were a workspace name - is how a grant over
      one workspace came to authorise a scope reaching others.
  """

  @scope_regex ~r/^[a-z0-9][a-z0-9_\/-]{0,99}$/i

  @doc """
  Whether `scope` selects the workspace called `name`.
  """
  @spec covers?(String.t(), String.t()) :: boolean()
  def covers?("*", _name), do: true

  def covers?(scope, name) when is_binary(scope) and is_binary(name) do
    case prefix_base(scope) do
      nil -> name == scope
      base -> String.starts_with?(name, base <> "/")
    end
  end

  @doc """
  Whether `outer` selects everything `inner` selects.

  A scope contains itself, `*` contains everything, and `development/*`
  contains `development/joe` and `development/joe/*` - but not
  `development`, which it doesn't select.
  """
  @spec contains?(String.t(), String.t()) :: boolean()
  def contains?("*", _inner), do: true
  def contains?(_outer, "*"), do: false

  def contains?(outer, inner) when is_binary(outer) and is_binary(inner) do
    case {prefix_base(outer), prefix_base(inner)} do
      # An exact scope selects one workspace, so it can only contain the
      # scope that selects the same one.
      {nil, _} -> outer == inner
      {base, nil} -> String.starts_with?(inner, base <> "/")
      {base, inner_base} -> String.starts_with?(inner_base <> "/", base <> "/")
    end
  end

  @doc "Whether any of `scopes` selects the workspace called `name`."
  @spec covers_any?([String.t()], String.t()) :: boolean()
  def covers_any?(scopes, name), do: Enum.any?(scopes, &covers?(&1, name))

  @doc "Whether any of `scopes` contains `inner` whole."
  @spec contains_any?([String.t()], String.t()) :: boolean()
  def contains_any?(scopes, inner), do: Enum.any?(scopes, &contains?(&1, inner))

  @doc """
  Whether this is a scope: `*`, a workspace name, or one with `/*`.
  """
  @spec valid?(term()) :: boolean()
  def valid?("*"), do: true

  def valid?(scope) when is_binary(scope) do
    case prefix_base(scope) do
      nil -> Regex.match?(@scope_regex, scope) and not String.ends_with?(scope, "/")
      base -> base != "" and Regex.match?(@scope_regex, base)
    end
  end

  def valid?(_scope), do: false

  @doc """
  How specific a scope is, for picking between those that all cover the
  same workspace: an exact scope beats a prefix, a longer prefix beats a
  shorter one, and `*` loses to everything.

  Only meaningful between scopes covering the same workspace - which is
  why it is a number and not an ordering of scopes in general.
  """
  @spec specificity(String.t()) :: {non_neg_integer(), non_neg_integer()}
  def specificity("*"), do: {0, 0}

  def specificity(scope) do
    case prefix_base(scope) do
      nil -> {2, byte_size(scope)}
      base -> {1, byte_size(base)}
    end
  end

  # The name before a trailing `/*`, or nil when the scope has none.
  defp prefix_base(scope) do
    if String.ends_with?(scope, "/*") do
      binary_part(scope, 0, byte_size(scope) - 2)
    end
  end
end
