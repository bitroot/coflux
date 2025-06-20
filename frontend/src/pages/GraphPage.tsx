import { useParams, useSearchParams } from "react-router-dom";
import * as models from "../models";
import { minBy } from "lodash";

import RunGraph from "../components/RunGraph";
import { useContext } from "../layouts/RunLayout";

function getRunSpaceId(run: models.Run) {
  const initialStepId = minBy(
    Object.keys(run.steps).filter((id) => !run.steps[id].parentId),
    (stepId) => run.steps[stepId].createdAt,
  )!;
  return run.steps[initialStepId].executions[1].spaceId;
}

export default function GraphPage() {
  const { run, width, height } = useContext();
  const { project: projectId, run: runId } = useParams();
  const [searchParams] = useSearchParams();
  const activeStepId = searchParams.get("step") || undefined;
  const activeAttempt = searchParams.has("attempt")
    ? parseInt(searchParams.get("attempt")!)
    : undefined;
  return (
    <RunGraph
      projectId={projectId!}
      run={run}
      width={width}
      height={height}
      runId={runId!}
      activeStepId={activeStepId}
      activeAttempt={activeAttempt}
      runSpaceId={getRunSpaceId(run)}
    />
  );
}
