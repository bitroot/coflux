import { useParams, useSearchParams } from "react-router-dom";
import { DateTime } from "luxon";
import { findKey } from "lodash";

import * as models from "../models";
import { useContext } from "../layouts/RunLayout";
import RunLogs from "../components/RunLogs";
import StepLink from "../components/StepLink";
import { useSpaces, useLogs } from "../topics";

type StepIdentifierProps = {
  runId: string;
  run: models.Run;
  executionId: string;
};

function StepIdentifier({ runId, run, executionId }: StepIdentifierProps) {
  const stepId = Object.keys(run.steps).find((id) =>
    Object.values(run.steps[id].executions).some(
      (e) => e.executionId == executionId,
    ),
  );
  const step = stepId && run.steps[stepId];
  const attempt =
    step &&
    Object.keys(step.executions).find(
      (n) => step.executions[n].executionId == executionId,
    );
  if (step && attempt) {
    return (
      <StepLink
        runId={runId}
        stepId={stepId}
        attempt={parseInt(attempt, 10)}
        className="block truncate w-40 max-w-full rounded-sm text-sm ring-offset-1"
        activeClassName="ring-2 ring-cyan-400"
        hoveredClassName="ring-2 ring-slate-300"
      >
        <span className="font-mono">{step.target}</span>{" "}
        <span className="text-slate-500 text-sm">({step.module})</span>
      </StepLink>
    );
  } else {
    return null;
  }
}

export default function LogsPage() {
  const { run } = useContext();
  const { project: projectId, run: runId } = useParams();
  const [searchParams] = useSearchParams();
  const activeSpaceName = searchParams.get("space") || undefined;
  const spaces = useSpaces(projectId);
  const activeSpaceId = findKey(
    spaces,
    (e) => e.name == activeSpaceName && e.state != "archived",
  );
  const logs = useLogs(projectId, runId, activeSpaceId);
  if (runId && logs) {
    return (
      <div className="p-5">
        <RunLogs
          startTime={DateTime.fromMillis(run.createdAt)}
          logs={logs}
          stepIdentifier={(executionId: string) => (
            <StepIdentifier runId={runId} run={run} executionId={executionId} />
          )}
          projectId={projectId!}
        />
      </div>
    );
  } else {
    return null;
  }
}
