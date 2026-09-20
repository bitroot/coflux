defmodule Coflux.Admin.Secrets do
  @moduledoc """
  Secrets, in the admin store: values a pool needs that must never appear
  in its configuration - API keys for workers, credentials for launchers.

  Each is encrypted with a key derived from the server secret, and bound
  to what it is: a ciphertext moved to another row won't decrypt. There's
  one current value per scope and name. Setting it again replaces the
  value and bumps the version; deleting it removes it. Nothing older is
  kept, so a rotated or deleted value is gone from here.

  A secret is set for a scope, as `Coflux.Scopes` defines one: a secret
  for `development` applies to `development/joe`, and the nearest scope
  wins. The root scope is the whole project. This follows the naming
  hierarchy rather than the base-workspace chain: a workspace that
  inherits results from `production` doesn't inherit its secrets.

  Pools name secrets in fields that take nothing else - `tokenSecret`,
  `credentialsSecret`, `envSecrets` - and the values are resolved for a
  launcher just before it is called, never stored with it.
  """

  alias Coflux.Store

  @key_id "hkdf-sha256-v1"
  @nonce_bytes 12
  @tag_bytes 16
  @max_value_bytes 65_536

  @name_regex ~r/^[a-z0-9][a-z0-9_-]{0,63}$/i
  @scope_regex ~r/^[a-z0-9][a-z0-9_\/-]{0,99}$/i

  def valid_name?(name), do: is_binary(name) and Regex.match?(@name_regex, name)

  def valid_scope?(""), do: true

  def valid_scope?(scope),
    do:
      is_binary(scope) and Regex.match?(@scope_regex, scope) and
        not String.ends_with?(scope, "/")

  def valid_value?(value), do: is_binary(value) and byte_size(value) <= @max_value_bytes

  @doc """
  Sets a secret's value, replacing whatever it had. `updated_by` is an
  identity as `Principals.build/2` describes it, or nil.

  Returns `{:ok, secret}` with the secret's metadata (never its value), or
  `{:error, :no_secret}` when the server has no secret to encrypt with.
  """
  def set(db, project_id, scope, name, value, updated_by) do
    with {:ok, key} <- encryption_key() do
      now = System.system_time(:second)

      {existing_id, created_at, version} =
        case Store.query_one(
               db,
               "SELECT id, created_at, version FROM secrets WHERE scope = ?1 AND name = ?2",
               {scope, name}
             ) do
          {:ok, {id, created_at, version}} -> {id, created_at, version + 1}
          {:ok, nil} -> {nil, now, 1}
        end

      nonce = :crypto.strong_rand_bytes(@nonce_bytes)

      {ciphertext, tag} =
        :crypto.crypto_one_time_aead(
          :aes_256_gcm,
          key,
          nonce,
          value,
          aad(project_id, scope, name, version),
          @tag_bytes,
          true
        )

      updated_by_type = updated_by && updated_by.type
      updated_by_external_id = updated_by && updated_by.external_id

      if existing_id do
        {:ok, _} =
          Store.query(
            db,
            """
            UPDATE secrets
            SET version = ?1, key_id = ?2, nonce = ?3, ciphertext = ?4, updated_at = ?5,
                updated_by_type = ?6, updated_by_external_id = ?7
            WHERE id = ?8
            """,
            {version, @key_id, {:blob, nonce}, {:blob, ciphertext <> tag}, now, updated_by_type,
             updated_by_external_id, existing_id}
          )
      else
        {:ok, _} =
          Store.insert_one(db, :secrets, %{
            scope: scope,
            name: name,
            version: version,
            key_id: @key_id,
            nonce: {:blob, nonce},
            ciphertext: {:blob, ciphertext <> tag},
            created_at: created_at,
            updated_at: now,
            updated_by_type: updated_by_type,
            updated_by_external_id: updated_by_external_id
          })
      end

      {:ok,
       %{
         scope: scope,
         name: name,
         version: version,
         created_at: created_at,
         updated_at: now,
         updated_by: updated_by
       }}
    end
  end

  def delete(db, scope, name) do
    case Store.query_one(
           db,
           "SELECT id FROM secrets WHERE scope = ?1 AND name = ?2",
           {scope, name}
         ) do
      {:ok, {id}} ->
        {:ok, _} = Store.query(db, "DELETE FROM secrets WHERE id = ?1", {id})
        :ok

      {:ok, nil} ->
        {:error, :not_found}
    end
  end

  @doc "Every secret's metadata - never a value."
  def list(db) do
    {:ok, rows} =
      Store.query(
        db,
        """
        SELECT scope, name, version, created_at, updated_at, updated_by_type, updated_by_external_id
        FROM secrets
        ORDER BY scope, name
        """,
        {}
      )

    {:ok,
     Enum.map(rows, fn {scope, name, version, created_at, updated_at, type, external_id} ->
       %{
         scope: scope,
         name: name,
         version: version,
         created_at: created_at,
         updated_at: updated_at,
         updated_by: if(type, do: %{type: type, external_id: external_id})
       }
     end)}
  end

  @doc """
  The value of a secret as seen from a workspace: the one in the nearest
  scope that covers the workspace.

  Returns `{:ok, value}`, `{:error, :not_found}`, `{:error, :undecryptable}`
  (the server secret isn't the one it was encrypted with, or the row was
  tampered with), or `{:error, :no_secret}`.
  """
  def resolve(db, project_id, workspace_name, name) do
    with {:ok, key} <- encryption_key(),
         {:ok, {scope, version, nonce, blob}} <- find(db, workspace_name, name) do
      decrypt(key, project_id, scope, name, version, nonce, blob)
    end
  end

  def exists?(db, workspace_name, name), do: match?({:ok, _}, find(db, workspace_name, name))

  @doc "Whether a secret in `scope` is one a workspace of this name sees."
  defdelegate scope_applies?(scope, workspace_name), to: Coflux.Scopes, as: :covers?

  # --- Launcher configs ---

  @doc "The names of the secrets a launcher config refers to."
  def references(launcher) when is_map(launcher) do
    env_secrets = Map.get(launcher, :env_secrets) || %{}

    [Map.get(launcher, :token_secret), Map.get(launcher, :credentials_secret)]
    |> Enum.concat(Map.values(env_secrets))
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
  end

  def references(_launcher), do: []

  @doc """
  Checks that every secret a launcher config refers to exists for a
  workspace, so a pool that could never launch is refused when it is
  saved rather than found out when it is used.
  """
  def check_references(db, workspace_name, launcher) do
    case Enum.reject(references(launcher), &exists?(db, workspace_name, &1)) do
      [] -> :ok
      missing -> {:error, {:secrets_not_found, missing}}
    end
  end

  @doc """
  Gives a launcher config the values of the secrets it refers to, for the
  launcher to use and then forget: `token_secret` becomes `token`,
  `credentials_secret` becomes the AWS key fields, and `env_secrets` are
  merged into `env`.

  Returns `{:ok, launcher}`, `{:error, {:secret_not_found, name}}`,
  `{:error, {:secret_invalid, name}}` (it can't be decrypted, or isn't in
  the shape the field needs), or `{:error, :no_secret}`.
  """
  def resolve_launcher(db, project_id, workspace_name, launcher) do
    with {:ok, launcher} <-
           resolve_field(db, project_id, workspace_name, launcher, :token_secret, fn l, value ->
             {:ok, Map.put(l, :token, value)}
           end),
         {:ok, launcher} <-
           resolve_field(
             db,
             project_id,
             workspace_name,
             launcher,
             :credentials_secret,
             &put_aws_credentials/2
           ) do
      resolve_env(db, project_id, workspace_name, launcher)
    end
  end

  defp resolve_field(db, project_id, workspace_name, launcher, key, put) do
    case Map.get(launcher, key) do
      name when is_binary(name) ->
        with {:ok, value} <- resolve_named(db, project_id, workspace_name, name),
             {:ok, launcher} <- put_or_invalid(put.(launcher, value), name) do
          {:ok, launcher}
        end

      _ ->
        {:ok, launcher}
    end
  end

  defp resolve_env(db, project_id, workspace_name, launcher) do
    env_secrets = Map.get(launcher, :env_secrets) || %{}

    Enum.reduce_while(env_secrets, {:ok, launcher}, fn {var, name}, {:ok, launcher} ->
      case resolve_named(db, project_id, workspace_name, name) do
        {:ok, value} ->
          env = Map.put(Map.get(launcher, :env) || %{}, var, value)
          {:cont, {:ok, Map.put(launcher, :env, env)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  defp resolve_named(db, project_id, workspace_name, name) do
    case resolve(db, project_id, workspace_name, name) do
      {:ok, value} -> {:ok, value}
      {:error, :not_found} -> {:error, {:secret_not_found, name}}
      {:error, :undecryptable} -> {:error, {:secret_invalid, name}}
      {:error, :no_secret} -> {:error, :no_secret}
    end
  end

  defp put_or_invalid({:ok, launcher}, _name), do: {:ok, launcher}
  defp put_or_invalid(:error, name), do: {:error, {:secret_invalid, name}}

  # AWS credentials as JSON, in the shape `aws configure export-credentials`
  # and credential processes produce (`AccessKeyId`, `SecretAccessKey`,
  # `SessionToken`), or the same keys in camelCase.
  defp put_aws_credentials(launcher, value) do
    with {:ok, %{} = creds} <- Jason.decode(value),
         access_key_id when is_binary(access_key_id) <-
           creds["AccessKeyId"] || creds["accessKeyId"],
         secret_access_key when is_binary(secret_access_key) <-
           creds["SecretAccessKey"] || creds["secretAccessKey"] do
      launcher =
        launcher
        |> Map.put(:access_key_id, access_key_id)
        |> Map.put(:secret_access_key, secret_access_key)

      case creds["SessionToken"] || creds["sessionToken"] do
        token when is_binary(token) and token != "" ->
          {:ok, Map.put(launcher, :session_token, token)}

        _ ->
          {:ok, launcher}
      end
    else
      _ -> :error
    end
  end

  # --- Storage and crypto ---

  # Nearest scope wins: the longest that covers the workspace.
  defp find(db, workspace_name, name) do
    {:ok, rows} =
      Store.query(
        db,
        "SELECT scope, version, nonce, ciphertext FROM secrets WHERE name = ?1",
        {name}
      )

    rows
    |> Enum.filter(fn {scope, _, _, _} -> scope_applies?(scope, workspace_name) end)
    |> Enum.max_by(fn {scope, _, _, _} -> byte_size(scope) end, fn -> nil end)
    |> case do
      nil -> {:error, :not_found}
      row -> {:ok, row}
    end
  end

  defp decrypt(key, project_id, scope, name, version, nonce, blob)
       when byte_size(blob) >= @tag_bytes do
    ciphertext_size = byte_size(blob) - @tag_bytes
    <<ciphertext::binary-size(ciphertext_size), tag::binary>> = blob

    case :crypto.crypto_one_time_aead(
           :aes_256_gcm,
           key,
           nonce,
           ciphertext,
           aad(project_id, scope, name, version),
           tag,
           false
         ) do
      :error -> {:error, :undecryptable}
      value -> {:ok, value}
    end
  end

  defp decrypt(_key, _project_id, _scope, _name, _version, _nonce, _blob),
    do: {:error, :undecryptable}

  # Binding the ciphertext to its row means the row can't be repurposed:
  # a value can't be moved to another name, scope, or project.
  defp aad(project_id, scope, name, version),
    do: Enum.join(["coflux", "secret", project_id, scope, name, Integer.to_string(version)], "\n")

  # The server secret is the root; the key is derived from it for this
  # purpose alone, so it is never the same bytes that sign anything.
  defp encryption_key do
    case Coflux.Config.secret() do
      nil -> {:error, :no_secret}
      secret -> {:ok, hkdf_sha256(secret, "coflux-secrets", "aes-256-gcm", 32)}
    end
  end

  # RFC 5869, for outputs of one hash block or less.
  defp hkdf_sha256(ikm, salt, info, length) when length <= 32 do
    prk = :crypto.mac(:hmac, :sha256, salt, ikm)
    okm = :crypto.mac(:hmac, :sha256, prk, info <> <<1>>)
    binary_part(okm, 0, length)
  end
end
