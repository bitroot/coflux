defmodule Coflux.Orchestration.Spaces do
  alias Coflux.Orchestration.TagSets

  import Coflux.Store

  def get_all_spaces(db) do
    case query(
           db,
           """
           SELECT
             w.id,
             (SELECT ws.state
               FROM space_states AS ws
               WHERE ws.space_id = w.id
               ORDER BY ws.created_at DESC
               LIMIT 1) AS state,
             (SELECT wn.name
               FROM space_names AS wn
               WHERE wn.space_id = w.id
               ORDER BY wn.created_at DESC
               LIMIT 1) AS name,
             (SELECT wb.base_id
               FROM space_bases AS wb
               WHERE wb.space_id = w.id
               ORDER BY wb.created_at DESC
               LIMIT 1) AS base_id
           FROM spaces AS w
           """
         ) do
      {:ok, rows} ->
        spaces =
          Enum.reduce(rows, %{}, fn {space_id, state, name, base_id}, result ->
            Map.put(result, space_id, %{
              name: name,
              base_id: base_id,
              state: decode_state(state)
            })
          end)

        {:ok, spaces}
    end
  end

  def get_space_pools(db, space_id) do
    case query(
           db,
           """
           SELECT p.id, p.name, p.pool_definition_id
           FROM pools AS p
           JOIN (
               SELECT name, MAX(created_at) AS created_at
               FROM pools
               WHERE space_id = ?1
               GROUP BY name
           ) latest ON p.name = latest.name AND p.created_at = latest.created_at
           WHERE p.space_id = ?1 AND p.pool_definition_id IS NOT NULL
           """,
           {space_id}
         ) do
      {:ok, rows} ->
        {:ok,
         Map.new(rows, fn {pool_id, pool_name, pool_definition_id} ->
           {:ok, pool_definition} = get_pool_definition(db, pool_definition_id)
           {pool_name, Map.put(pool_definition, :id, pool_id)}
         end)}
    end
  end

  defp space_name_used?(db, space_name) do
    # TODO: neater way to do this?
    case query(
           db,
           """
           SELECT
             (SELECT ws.state
               FROM space_states AS ws
               WHERE ws.space_id = w.id
               ORDER BY ws.created_at DESC
               LIMIT 1) AS state,
             (SELECT wn.name
               FROM space_names AS wn
               WHERE wn.space_id = w.id
               ORDER BY wn.created_at DESC
               LIMIT 1) AS name
           FROM spaces AS w
           """
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.any?(rows, fn {state, name} ->
           name == space_name && decode_state(state) != :archived
         end)}
    end
  end

  defp has_active_child_spaces?(db, space_id) do
    # TODO: neater way to do this?
    case query(
           db,
           """
           SELECT
             (SELECT ws.state
               FROM space_states AS ws
               WHERE ws.space_id = w.id
               ORDER BY ws.created_at DESC
               LIMIT 1) AS state,
             (SELECT wb.base_id
               FROM space_bases AS wb
               WHERE wb.space_id = w.id
               ORDER BY wb.created_at DESC
               LIMIT 1) AS base_id
           FROM spaces AS w
           """
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.any?(rows, fn {state, base_id} ->
           base_id == space_id && decode_state(state) != :archived
         end)}
    end
  end

  # TODO: change to 'get_active_space_by_id'?
  defp get_space_by_id(db, space_id) do
    case query_one(
           db,
           """
           SELECT
             (SELECT ws.state
               FROM space_states AS ws
               WHERE ws.space_id = w.id
               ORDER BY ws.created_at DESC
               LIMIT 1) AS state,
             (SELECT wn.name
               FROM space_names AS wn
               WHERE wn.space_id = w.id
               ORDER BY wn.created_at DESC
               LIMIT 1) AS name,
             (SELECT wb.base_id
               FROM space_bases AS wb
               WHERE wb.space_id = w.id
               ORDER BY wb.created_at DESC
               LIMIT 1) AS base_id
           FROM spaces AS w
           WHERE w.id = ?1
           """,
           {space_id}
         ) do
      {:ok, {state, name, base_id}} ->
        {:ok, %{state: decode_state(state), name: name, base_id: base_id}}

      {:ok, nil} ->
        {:ok, nil}
    end
  end

  def create_space(db, name, base_id) do
    with_transaction(db, fn ->
      space = %{
        state: :active,
        name: name,
        base_id: base_id
      }

      {space, errors} =
        validate(
          space,
          name: &validate_name(&1, db),
          base_id: &validate_base_id(&1, db)
        )

      if Enum.any?(errors) do
        {:error, errors}
      else
        now = current_timestamp()
        {:ok, space_id} = insert_one(db, :spaces, %{})
        {:ok, _} = insert_space_state(db, space_id, space.state, now)
        {:ok, _} = insert_space_name(db, space_id, space.name, now)
        {:ok, _} = insert_space_base(db, space_id, space.base_id, now)

        {:ok, space_id, space}
      end
    end)
  end

  def update_space(db, space_id, updates) do
    with_transaction(db, fn ->
      case get_space_by_id(db, space_id) do
        {:ok, nil} ->
          {:error, :not_found}

        {:ok, %{state: :archived}} ->
          {:error, :not_found}

        {:ok, space} ->
          {updates, errors} =
            validate(
              updates,
              name: &validate_name(&1, db),
              base_id: &validate_base_id(&1, db, space_id)
            )

          if Enum.any?(errors) do
            {:error, errors}
          else
            now = current_timestamp()

            if Map.has_key?(updates, :name) && updates.name != space.name do
              {:ok, _} = insert_space_name(db, space_id, updates.name, now)
            end

            if Map.has_key?(updates, :base_id) && updates.base_id != space.base_id do
              {:ok, _} = insert_space_base(db, space_id, updates.base_id, now)
            end

            # TODO: don't return space - move this to separate function?
            {:ok, space} = get_space_by_id(db, space_id)

            {:ok, space}
          end
      end
    end)
  end

  def pause_space(db, space_id) do
    with_transaction(db, fn ->
      case get_space_by_id(db, space_id) do
        {:ok, nil} ->
          {:error, :not_found}

        {:ok, %{state: :archived}} ->
          {:error, :not_found}

        {:ok, %{state: :active}} ->
          {:ok, _} = insert_space_state(db, space_id, :paused, current_timestamp())
          :ok

        {:ok, %{state: :paused}} ->
          :ok
      end
    end)
  end

  def resume_space(db, space_id) do
    with_transaction(db, fn ->
      case get_space_by_id(db, space_id) do
        {:ok, nil} ->
          {:error, :not_found}

        {:ok, %{state: :archived}} ->
          {:error, :not_found}

        {:ok, %{state: :paused}} ->
          {:ok, _} = insert_space_state(db, space_id, :active, current_timestamp())
          :ok

        {:ok, %{state: :active}} ->
          :ok
      end
    end)
  end

  def archive_space(db, space_id) do
    with_transaction(db, fn ->
      case get_space_by_id(db, space_id) do
        {:ok, nil} ->
          {:error, :not_found}

        {:ok, %{state: :archived}} ->
          {:error, :not_found}

        {:ok, _} ->
          case has_active_child_spaces?(db, space_id) do
            {:ok, true} ->
              {:error, :descendants}

            {:ok, false} ->
              {:ok, _} =
                insert_space_state(db, space_id, :archived, current_timestamp())

              :ok
          end
      end
    end)
  end

  def update_pool(db, space_id, pool_name, pool) do
    # TODO: validate pool (check launcher is specified)

    with_transaction(db, fn ->
      now = current_timestamp()

      pool_definition_id =
        if pool do
          {:ok, pool_definition_id} = get_or_create_pool_definition(db, pool)
          pool_definition_id
        end

      {existing_pool_id, existing_pool_definition_id} =
        case get_latest_pool(db, space_id, pool_name) do
          {:ok, {existing_pool_id, existing_pool_definition_id}} ->
            {existing_pool_id, existing_pool_definition_id}

          {:ok, nil} ->
            {nil, nil}
        end

      if pool_definition_id != existing_pool_definition_id do
        insert_space_pool(db, space_id, pool_name, pool_definition_id, now)
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
      case space_name_used?(db, name) do
        {:ok, false} -> :ok
        {:ok, true} -> {:error, :exists}
      end
    else
      {:error, :invalid}
    end
  end

  defp get_ancestor_ids(db, space_id, ancestor_ids \\ []) do
    case get_space_by_id(db, space_id) do
      {:ok, %{base_id: nil}} ->
        {:ok, ancestor_ids}

      {:ok, %{base_id: base_id}} ->
        get_ancestor_ids(db, base_id, [space_id | ancestor_ids])
    end
  end

  defp validate_base_id(base_id, db, space_id \\ nil) do
    if is_nil(base_id) do
      :ok
    else
      case get_space_by_id(db, base_id) do
        {:ok, base} ->
          if !base || base.state == :archived do
            {:error, :invalid}
          else
            if space_id do
              case get_ancestor_ids(db, base_id) do
                {:ok, ancestor_ids} ->
                  if space_id in ancestor_ids do
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

  defp hash_pool_definition(launcher_id, provides_tag_set_id, modules) do
    data =
      Enum.intersperse(
        [
          if(launcher_id, do: Integer.to_string(launcher_id), else: ""),
          Integer.to_string(provides_tag_set_id),
          Enum.join(Enum.sort(modules), "\n")
        ],
        0
      )

    :crypto.hash(:sha256, data)
  end

  defp get_or_create_pool_definition(db, pool) do
    modules = Map.get(pool, :modules, [])
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

    hash = hash_pool_definition(launcher_id, provides_tag_set_id, modules)

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
            :pool_definition_modules,
            {:pool_definition_id, :pattern},
            Enum.map(modules, fn pattern ->
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

  defp get_latest_pool(db, space_id, pool_name) do
    query_one(
      db,
      """
      SELECT id, pool_definition_id
      FROM pools
      WHERE space_id = ?1 AND name = ?2
      ORDER BY created_at DESC
      LIMIT 1
      """,
      {space_id, pool_name}
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

        modules =
          case query(
                 db,
                 """
                 SELECT pattern
                 FROM pool_definition_modules
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
           modules: modules,
           launcher: launcher
         }}

      {:ok, nil} ->
        {:error, :not_found}
    end
  end

  defp insert_space_state(db, space_id, state, created_at) do
    insert_one(db, :space_states, %{
      space_id: space_id,
      state: encode_state(state),
      created_at: created_at
    })
  end

  defp insert_space_name(db, space_id, name, created_at) do
    insert_one(db, :space_names, %{
      space_id: space_id,
      name: name,
      created_at: created_at
    })
  end

  defp insert_space_base(db, space_id, base_id, created_at) do
    insert_one(db, :space_bases, %{
      space_id: space_id,
      base_id: base_id,
      created_at: created_at
    })
  end

  defp insert_space_pool(db, space_id, pool_name, pool_definition_id, created_at) do
    insert_one(db, :pools, %{
      space_id: space_id,
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
