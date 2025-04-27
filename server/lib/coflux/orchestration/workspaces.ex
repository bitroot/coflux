defmodule Coflux.Orchestration.Workspaces do
  alias Coflux.Orchestration.TagSets

  import Coflux.Store

  def get_all_workspaces(db) do
    case query(
           db,
           """
           SELECT
             w.id,
             (SELECT ws.state
               FROM workspace_states AS ws
               WHERE ws.workspace_id = w.id
               ORDER BY ws.created_at DESC
               LIMIT 1) AS state,
             (SELECT wn.name
               FROM workspace_names AS wn
               WHERE wn.workspace_id = w.id
               ORDER BY wn.created_at DESC
               LIMIT 1) AS name,
             (SELECT wb.base_id
               FROM workspace_bases AS wb
               WHERE wb.workspace_id = w.id
               ORDER BY wb.created_at DESC
               LIMIT 1) AS base_id
           FROM workspaces AS w
           """
         ) do
      {:ok, rows} ->
        workspaces =
          Enum.reduce(rows, %{}, fn {workspace_id, state, name, base_id}, result ->
            Map.put(result, workspace_id, %{
              name: name,
              base_id: base_id,
              state: decode_state(state)
            })
          end)

        {:ok, workspaces}
    end
  end

  def get_workspace_pools(db, workspace_id) do
    case query(
           db,
           """
           SELECT p.id, p.name, p.pool_definition_id
           FROM pools AS p
           JOIN (
               SELECT name, MAX(created_at) AS created_at
               FROM pools
               WHERE workspace_id = ?1
               GROUP BY name
           ) latest ON p.name = latest.name AND p.created_at = latest.created_at
           WHERE p.workspace_id = ?1 AND p.pool_definition_id IS NOT NULL
           """,
           {workspace_id}
         ) do
      {:ok, rows} ->
        {:ok,
         Map.new(rows, fn {pool_id, pool_name, pool_definition_id} ->
           {:ok, pool_definition} = get_pool_definition(db, pool_definition_id)
           {pool_name, Map.put(pool_definition, :id, pool_id)}
         end)}
    end
  end

  defp workspace_name_used?(db, workspace_name) do
    # TODO: neater way to do this?
    case query(
           db,
           """
           SELECT
             (SELECT ws.state
               FROM workspace_states AS ws
               WHERE ws.workspace_id = w.id
               ORDER BY ws.created_at DESC
               LIMIT 1) AS state,
             (SELECT wn.name
               FROM workspace_names AS wn
               WHERE wn.workspace_id = w.id
               ORDER BY wn.created_at DESC
               LIMIT 1) AS name
           FROM workspaces AS w
           """
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.any?(rows, fn {state, name} ->
           name == workspace_name && decode_state(state) != :archived
         end)}
    end
  end

  defp has_active_child_workspaces?(db, workspace_id) do
    # TODO: neater way to do this?
    case query(
           db,
           """
           SELECT
             (SELECT ws.state
               FROM workspace_states AS ws
               WHERE ws.workspace_id = w.id
               ORDER BY ws.created_at DESC
               LIMIT 1) AS state,
             (SELECT wb.base_id
               FROM workspace_bases AS wb
               WHERE wb.workspace_id = w.id
               ORDER BY wb.created_at DESC
               LIMIT 1) AS base_id
           FROM workspaces AS w
           """
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.any?(rows, fn {state, base_id} ->
           base_id == workspace_id && decode_state(state) != :archived
         end)}
    end
  end

  # TODO: change to 'get_active_workspace_by_id'?
  defp get_workspace_by_id(db, workspace_id) do
    case query_one(
           db,
           """
           SELECT
             (SELECT ws.state
               FROM workspace_states AS ws
               WHERE ws.workspace_id = w.id
               ORDER BY ws.created_at DESC
               LIMIT 1) AS state,
             (SELECT wn.name
               FROM workspace_names AS wn
               WHERE wn.workspace_id = w.id
               ORDER BY wn.created_at DESC
               LIMIT 1) AS name,
             (SELECT wb.base_id
               FROM workspace_bases AS wb
               WHERE wb.workspace_id = w.id
               ORDER BY wb.created_at DESC
               LIMIT 1) AS base_id
           FROM workspaces AS w
           WHERE w.id = ?1
           """,
           {workspace_id}
         ) do
      {:ok, {state, name, base_id}} ->
        {:ok, %{state: decode_state(state), name: name, base_id: base_id}}

      {:ok, nil} ->
        {:ok, nil}
    end
  end

  def create_workspace(db, name, base_id) do
    with_transaction(db, fn ->
      workspace = %{
        state: :active,
        name: name,
        base_id: base_id
      }

      {workspace, errors} =
        validate(
          workspace,
          name: &validate_name(&1, db),
          base_id: &validate_base_id(&1, db)
        )

      if Enum.any?(errors) do
        {:error, errors}
      else
        now = current_timestamp()
        {:ok, workspace_id} = insert_one(db, :workspaces, %{})
        {:ok, _} = insert_workspace_state(db, workspace_id, workspace.state, now)
        {:ok, _} = insert_workspace_name(db, workspace_id, workspace.name, now)
        {:ok, _} = insert_workspace_base(db, workspace_id, workspace.base_id, now)

        {:ok, workspace_id, workspace}
      end
    end)
  end

  def update_workspace(db, workspace_id, updates) do
    with_transaction(db, fn ->
      case get_workspace_by_id(db, workspace_id) do
        {:ok, nil} ->
          {:error, :not_found}

        {:ok, %{state: :archived}} ->
          {:error, :not_found}

        {:ok, workspace} ->
          {updates, errors} =
            validate(
              updates,
              name: &validate_name(&1, db),
              base_id: &validate_base_id(&1, db, workspace_id)
            )

          if Enum.any?(errors) do
            {:error, errors}
          else
            now = current_timestamp()

            if Map.has_key?(updates, :name) && updates.name != workspace.name do
              {:ok, _} = insert_workspace_name(db, workspace_id, updates.name, now)
            end

            if Map.has_key?(updates, :base_id) && updates.base_id != workspace.base_id do
              {:ok, _} = insert_workspace_base(db, workspace_id, updates.base_id, now)
            end

            # TODO: don't return workspace - move this to separate function?
            {:ok, workspace} = get_workspace_by_id(db, workspace_id)

            {:ok, workspace}
          end
      end
    end)
  end

  def pause_workspace(db, workspace_id) do
    with_transaction(db, fn ->
      case get_workspace_by_id(db, workspace_id) do
        {:ok, nil} ->
          {:error, :not_found}

        {:ok, %{state: :archived}} ->
          {:error, :not_found}

        {:ok, %{state: :active}} ->
          {:ok, _} = insert_workspace_state(db, workspace_id, :paused, current_timestamp())
          :ok

        {:ok, %{state: :paused}} ->
          :ok
      end
    end)
  end

  def resume_workspace(db, workspace_id) do
    with_transaction(db, fn ->
      case get_workspace_by_id(db, workspace_id) do
        {:ok, nil} ->
          {:error, :not_found}

        {:ok, %{state: :archived}} ->
          {:error, :not_found}

        {:ok, %{state: :paused}} ->
          {:ok, _} = insert_workspace_state(db, workspace_id, :active, current_timestamp())
          :ok

        {:ok, %{state: :active}} ->
          :ok
      end
    end)
  end

  def archive_workspace(db, workspace_id) do
    with_transaction(db, fn ->
      case get_workspace_by_id(db, workspace_id) do
        {:ok, nil} ->
          {:error, :not_found}

        {:ok, %{state: :archived}} ->
          {:error, :not_found}

        {:ok, _} ->
          case has_active_child_workspaces?(db, workspace_id) do
            {:ok, true} ->
              {:error, :descendants}

            {:ok, false} ->
              {:ok, _} =
                insert_workspace_state(db, workspace_id, :archived, current_timestamp())

              :ok
          end
      end
    end)
  end

  def update_pool(db, workspace_id, pool_name, pool) do
    # TODO: validate pool

    with_transaction(db, fn ->
      now = current_timestamp()

      pool_definition_id =
        if pool do
          {:ok, pool_definition_id} = get_or_create_pool_definition(db, pool)
          pool_definition_id
        end

      {existing_pool_id, existing_pool_definition_id} =
        case get_latest_pool(db, workspace_id, pool_name) do
          {:ok, {existing_pool_id, existing_pool_definition_id}} ->
            {existing_pool_id, existing_pool_definition_id}

          {:ok, nil} ->
            {nil, nil}
        end

      if pool_definition_id != existing_pool_definition_id do
        insert_workspace_pool(db, workspace_id, pool_name, pool_definition_id, now)
      else
        {:ok, existing_pool_id}
      end
    end)
  end

  defp is_valid_name?(name) do
    is_binary(name) && Regex.match?(~r/^[a-z0-9_-]+(\/[a-z0-9_-]+)*$/i, name)
  end

  defp validate_name(name, db) do
    if is_valid_name?(name) do
      case workspace_name_used?(db, name) do
        {:ok, false} -> :ok
        {:ok, true} -> {:error, :exists}
      end
    else
      {:error, :invalid}
    end
  end

  defp get_ancestor_ids(db, workspace_id, ancestor_ids \\ []) do
    case get_workspace_by_id(db, workspace_id) do
      {:ok, %{base_id: nil}} ->
        {:ok, ancestor_ids}

      {:ok, %{base_id: base_id}} ->
        get_ancestor_ids(db, base_id, [workspace_id | ancestor_ids])
    end
  end

  defp validate_base_id(base_id, db, workspace_id \\ nil) do
    if is_nil(base_id) do
      :ok
    else
      case get_workspace_by_id(db, base_id) do
        {:ok, base} ->
          if !base || base.state == :archived do
            {:error, :invalid}
          else
            if workspace_id do
              case get_ancestor_ids(db, base_id) do
                {:ok, ancestor_ids} ->
                  if workspace_id in ancestor_ids do
                    {:error, :invalid}
                  else
                    :ok
                  end
              end
            else
              :ok
            end
          end
      end
    end
  end

  defp validate(updates, validators) do
    Enum.reduce(validators, {updates, %{}}, fn {field, validator}, {updates, errors} ->
      if Map.has_key?(updates, field) do
        case validator.(Map.fetch!(updates, field)) do
          :ok ->
            {updates, errors}

          {:ok, value} ->
            updates = Map.put(updates, field, value)
            {updates, errors}

          {:error, error} ->
            {updates, Map.put(errors, field, error)}
        end
      else
        {updates, errors}
      end
    end)
  end

  defp hash_launcher(type, config) do
    # TODO: better hashing? (recursively sort config)
    data = [Atom.to_string(type), 0, Jason.encode!(config)]
    :crypto.hash(:sha256, data)
  end

  defp get_or_create_launcher(db, launcher) do
    type = Map.fetch!(launcher, :type)
    config = Map.delete(launcher, :type)
    hash = hash_launcher(type, config)

    case query_one(db, "SELECT id FROM launchers WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {id}} ->
        {:ok, id}

      {:ok, nil} ->
        insert_one(db, :launchers, %{
          hash: {:blob, hash},
          type: encode_launcher_type(type),
          config: Jason.encode!(config)
        })
    end
  end

  defp hash_pool_definition(launcher_id, provides_tag_set_id, repositories) do
    data =
      Enum.intersperse(
        [
          if(launcher_id, do: Integer.to_string(launcher_id), else: ""),
          Integer.to_string(provides_tag_set_id),
          Enum.join(Enum.sort(repositories), "\n")
        ],
        0
      )

    :crypto.hash(:sha256, data)
  end

  defp get_or_create_pool_definition(db, pool) do
    repositories = Map.get(pool, :repositories, [])
    provides = Map.get(pool, :provides, %{})
    launcher = Map.get(pool, :launcher)

    launcher_id =
      if launcher do
        case get_or_create_launcher(db, launcher) do
          {:ok, launcher_id} -> launcher_id
        end
      end

    provides_tag_set_id =
      if provides && Enum.any?(provides) do
        case TagSets.get_or_create_tag_set_id(db, provides) do
          {:ok, tag_set_id} -> tag_set_id
        end
      end

    hash = hash_pool_definition(launcher_id, provides_tag_set_id, repositories)

    case query_one(db, "SELECT id FROM pool_definitions WHERE hash = ?1", {{:blob, hash}}) do
      {:ok, {id}} ->
        {:ok, id}

      {:ok, nil} ->
        {:ok, pool_definition_id} =
          insert_one(db, :pool_definitions, %{
            hash: {:blob, hash},
            provides_tag_set_id: provides_tag_set_id,
            launcher_id: launcher_id
          })

        {:ok, _} =
          insert_many(
            db,
            :pool_definition_repositories,
            {:pool_definition_id, :pattern},
            Enum.map(repositories, fn pattern ->
              {pool_definition_id, pattern}
            end)
          )

        {:ok, pool_definition_id}
    end
  end

  defp get_launcher(db, launcher_id) do
    case query_one(db, "SELECT type, config FROM launchers WHERE id = ?1", {launcher_id}) do
      {:ok, {type, config}} ->
        {:ok, build_launcher(type, config)}
    end
  end

  defp build_launcher(type, config) do
    config = Jason.decode!(config, keys: :atoms)
    type = decode_launcher_type(type)
    Map.put(config, :type, type)
  end

  def get_launcher_for_pool(db, pool_id) do
    case query_one(
           db,
           """
           SELECT l.type, l.config
           FROM pools AS p
           INNER JOIN pool_definitions AS pd ON pd.id = p.pool_definition_id
           INNER JOIN launchers AS l ON l.id = pd.launcher_id
           WHERE p.id = ?1
           """,
           {pool_id}
         ) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, {type, config}} ->
        {:ok, build_launcher(type, config)}
    end
  end

  defp get_latest_pool(db, workspace_id, pool_name) do
    query_one(
      db,
      """
      SELECT id, pool_definition_id
      FROM pools
      WHERE workspace_id = ?1 AND name = ?2
      ORDER BY created_at DESC
      LIMIT 1
      """,
      {workspace_id, pool_name}
    )
  end

  defp get_pool_definition(db, pool_definition_id) do
    case query_one(
           db,
           "SELECT launcher_id, provides_tag_set_id FROM pool_definitions WHERE id = ?1",
           {pool_definition_id}
         ) do
      {:ok, {launcher_id, provides_tag_set_id}} ->
        provides =
          if provides_tag_set_id do
            case TagSets.get_tag_set(db, provides_tag_set_id) do
              {:ok, tag_set} ->
                tag_set
            end
          else
            %{}
          end

        repositories =
          case query(
                 db,
                 """
                 SELECT pattern
                 FROM pool_definition_repositories
                 WHERE pool_definition_id = ?1
                 """,
                 {pool_definition_id}
               ) do
            {:ok, rows} -> Enum.map(rows, fn {pattern} -> pattern end)
          end

        {:ok, launcher} = get_launcher(db, launcher_id)

        {:ok,
         %{
           provides: provides,
           repositories: repositories,
           launcher: launcher
         }}

      {:ok, nil} ->
        {:error, :not_found}
    end
  end

  defp insert_workspace_state(db, workspace_id, state, created_at) do
    insert_one(db, :workspace_states, %{
      workspace_id: workspace_id,
      state: encode_state(state),
      created_at: created_at
    })
  end

  defp insert_workspace_name(db, workspace_id, name, created_at) do
    insert_one(db, :workspace_names, %{
      workspace_id: workspace_id,
      name: name,
      created_at: created_at
    })
  end

  defp insert_workspace_base(db, workspace_id, base_id, created_at) do
    insert_one(db, :workspace_bases, %{
      workspace_id: workspace_id,
      base_id: base_id,
      created_at: created_at
    })
  end

  defp insert_workspace_pool(db, workspace_id, pool_name, pool_definition_id, created_at) do
    insert_one(db, :pools, %{
      workspace_id: workspace_id,
      name: pool_name,
      pool_definition_id: pool_definition_id,
      created_at: created_at
    })
  end

  defp encode_state(state) do
    case state do
      :active -> 0
      :paused -> 1
      :archived -> 2
    end
  end

  defp decode_state(value) do
    case value do
      0 -> :active
      1 -> :paused
      2 -> :archived
    end
  end

  defp encode_launcher_type(type) do
    case type do
      :docker -> 0
    end
  end

  defp decode_launcher_type(value) do
    case value do
      0 -> :docker
    end
  end

  defp current_timestamp() do
    System.os_time(:millisecond)
  end
end
