import Badge from "./Badge";
import * as models from "../models";

export type Props = {
  execution: models.Execution;
  step: models.Step;
};

export default function ExecutionStatus({ execution, step }: Props) {
  return execution.result?.type == "cached" ? (
    <Badge intent="none" label="Cached" />
  ) : execution.result?.type == "spawned" ? (
    <Badge intent="none" label="Spawned" />
  ) : execution.result?.type == "deferred" ? (
    <Badge intent="none" label="Deferred" />
  ) : execution.result?.type == "value" ? (
    <Badge intent="success" label="Completed" />
  ) : execution.result?.type == "error" ? (
    <Badge intent="danger" label="Failed" />
  ) : execution.result?.type == "abandoned" ? (
    <Badge intent="warning" label="Abandoned" />
  ) : execution.result?.type == "suspended" ? (
    <Badge intent="none" label="Suspended" />
  ) : execution.result?.type == "cancelled" ? (
    <Badge
      intent="warning"
      label={step.type == "sensor" ? "Stoppped" : "Cancelled"}
    />
  ) : !execution.assignedAt ? (
    <Badge intent="info" label="Assigning" />
  ) : !execution.result ? (
    <Badge intent="info" label="Running" />
  ) : null;
}
