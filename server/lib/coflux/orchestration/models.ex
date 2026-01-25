defmodule Coflux.Orchestration.Models do
  alias Coflux.Orchestration.Utils

  defmodule Run do
    defstruct [
      :id,
      :external_id,
      :parent_id,
      :idempotency_key,
      :created_at
    ]
  end

  defmodule Step do
    alias Utils

    defstruct [
      :id,
      :external_id,
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
      :space_id,
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
