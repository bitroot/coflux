import { useCallback, useState } from "react";
import { IconCpu, IconPlayerPlay } from "@tabler/icons-react";
import { useNavigate } from "react-router-dom";

import * as api from "../api";
import RunSelector from "./RunSelector";
import Button from "./common/Button";
import { buildUrl } from "../utils";
import RunDialog from "./RunDialog";
import { useRun, useSensor } from "../topics";
import { maxBy } from "lodash";

type StopResumeButtonProps = {
  isRunning?: boolean;
  onStop: () => void;
  onResume: () => void;
};

function StopResumeButton({
  isRunning,
  onStop,
  onResume,
}: StopResumeButtonProps) {
  const handleStopClick = useCallback(() => {
    if (confirm("Are you sure you want to stop this sensor?")) {
      onStop();
    }
  }, [onStop]);
  return isRunning ? (
    <Button
      onClick={handleStopClick}
      outline={true}
      variant="warning"
      size="sm"
    >
      Stop
    </Button>
  ) : (
    <Button onClick={onResume} outline={true} size="sm">
      Resume
    </Button>
  );
}

type Props = {
  module: string | undefined;
  target: string | undefined;
  projectId: string;
  runId?: string;
  activeSpaceId: string | undefined;
  activeSpaceName: string | undefined;
};

export default function SensorHeader({
  module,
  target,
  projectId,
  runId,
  activeSpaceId,
  activeSpaceName,
}: Props) {
  const navigate = useNavigate();
  const [runDialogOpen, setRunDialogOpen] = useState(false);
  const sensor = useSensor(projectId, module, target, activeSpaceId);
  const run = useRun(projectId, runId, activeSpaceId);
  const handleRunSubmit = useCallback(
    (arguments_: ["json", string][]) => {
      const configuration = sensor!.configuration!;
      return api
        .startSensor(
          projectId,
          module!,
          target!,
          activeSpaceName!,
          arguments_,
          {
            requires: configuration.requires,
          },
        )
        .then(({ runId }) => {
          setRunDialogOpen(false);
          navigate(
            buildUrl(`/projects/${projectId}/runs/${runId}`, {
              space: activeSpaceName,
            }),
          );
        });
    },
    [navigate, projectId, module, target, activeSpaceName, sensor],
  );
  const initialStepId =
    run && Object.keys(run.steps).find((stepId) => !run.steps[stepId].parentId);
  const latestAttempt =
    run &&
    maxBy(Object.keys(run.steps[initialStepId!].executions), (attempt) =>
      parseInt(attempt, 10),
    );
  const latestExecutionId =
    run && run.steps[initialStepId!].executions[latestAttempt!].executionId;
  const handleStop = useCallback(() => {
    if (latestExecutionId) {
      return api.cancelExecution(projectId, latestExecutionId);
    }
  }, [projectId, latestExecutionId]);
  const handleResume = useCallback(() => {
    api.rerunStep(projectId, initialStepId!, activeSpaceName!);
  }, [projectId, initialStepId, activeSpaceName]);
  const handleStartClick = useCallback(() => {
    setRunDialogOpen(true);
  }, []);
  const handleRunDialogClose = useCallback(() => setRunDialogOpen(false), []);
  const isRunning =
    run &&
    Object.values(run.steps).some((s) =>
      Object.values(s.executions).some((e) => !e.result),
    );
  return (
    <div className="p-5 flex justify-between gap-2 items-start">
      <div className="flex flex-col gap-2">
        <div className="flex items-baseline gap-1">
          <span className="text-slate-400">{module}</span>
          <span className="text-slate-400">/</span>
          <IconCpu
            size={26}
            strokeWidth={1.5}
            className="text-slate-400 shrink-0 self-start"
          />
          <h1 className="text-lg font-bold font-mono">{target}</h1>
        </div>

        {runId && (
          <div className="flex items-center gap-2">
            {sensor && (
              <RunSelector
                runs={sensor.runs}
                projectId={projectId}
                runId={runId}
                activeSpaceName={activeSpaceName}
              />
            )}
            <StopResumeButton
              isRunning={isRunning}
              onStop={handleStop}
              onResume={handleResume}
            />
          </div>
        )}
      </div>
      <div className="flex items-center gap-2">
        {sensor && (
          <>
            <Button
              onClick={handleStartClick}
              left={<IconPlayerPlay size={16} />}
              disabled={!activeSpaceId || !sensor.parameters}
            >
              Start...
            </Button>
            {activeSpaceId && sensor.parameters && (
              <RunDialog
                projectId={projectId}
                module={module}
                target={target}
                parameters={sensor.parameters}
                instruction={sensor.instruction}
                activeSpaceId={activeSpaceId}
                open={runDialogOpen}
                onRun={handleRunSubmit}
                onClose={handleRunDialogClose}
              />
            )}
          </>
        )}
      </div>
    </div>
  );
}
