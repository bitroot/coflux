defmodule Coflux.Orchestration.Models do
  alias Coflux.Orchestration.Utils

  defmodule Run do
    defstruct [
      :id,
      :external_id,
      :parent_id,
      :idempotency_key,
      :created_at,
      :created_by
    ]

    def prepare(fields) do
      {user_external_id, fields} = Keyword.pop(fields, :created_by_user_external_id)
      {token_external_id, fields} = Keyword.pop(fields, :created_by_token_external_id)

      created_by =
        case {user_external_id, token_external_id} do
          {nil, nil} -> nil
          {user_ext_id, nil} -> %{type: "user", external_id: user_ext_id}
          {nil, token_ext_id} -> %{type: "token", external_id: token_ext_id}
        end

      Keyword.put(fields, :created_by, created_by)
    end
  end

  defmodule Step do
    alias Utils

    defstruct [
      :id,
      :number,
      :run_id,
      :parent_id,
      :module,
      :target,
      :type,
      :priority,
      :wait_for,
      :cache_config_id,
      :cache_key,
      :defer_key,
      :memo_key,
      :retry_limit,
      :retry_delay_min,
      :retry_delay_max,
      :recurrent,
      :delay,
      :requires_tag_set_id,
      :created_at
    ]

    def prepare(fields) do
      fields
      |> Keyword.update!(:type, &Utils.decode_step_type/1)
      |> Keyword.update!(:wait_for, &Utils.decode_params_set/1)
    end
  end

  defmodule UnassignedExecution do
    defstruct [
      :execution_id,
      :step_id,
      :run_id,
      :run_external_id,
      :step_number,
      :module,
      :target,
      :type,
      :wait_for,
      :cache_key,
      :cache_config_id,
      :defer_key,
      :parent_id,
      :requires_tag_set_id,
      :retry_limit,
      :retry_delay_min,
      :retry_delay_max,
      :workspace_id,
      :execute_after,
      :attempt,
      :created_at
    ]

    def prepare(fields) do
      fields
      |> Keyword.update!(:type, &Utils.decode_step_type/1)
      |> Keyword.update!(:wait_for, &Utils.decode_params_set/1)
    end
  end
end
