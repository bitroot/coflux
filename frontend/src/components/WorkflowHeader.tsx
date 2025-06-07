import { useCallback, useState } from "react";
import { IconBolt, IconSubtask } from "@tabler/icons-react";
import { useNavigate } from "react-router-dom";

import * as api from "../api";
import RunSelector from "./RunSelector";
import Button from "./common/Button";
import { buildUrl } from "../utils";
import RunDialog from "./RunDialog";
import WorkspaceLabel from "./WorkspaceLabel";
import { useRun, useWorkflow } from "../topics";
import { maxBy, minBy } from "lodash";

type CancelButtonProps = {
  onCancel: () => void;
};

function CancelButton({ onCancel }: CancelButtonProps) {
  const handleCancelClick = useCallback(() => {
    if (confirm("Are you sure you want to cancel this run?")) {
      onCancel();
    }
  }, [onCancel]);
  return (
    <Button
      onClick={handleCancelClick}
      outline={true}
      variant="warning"
      size="sm"
    >
      Cancel
    </Button>
  );
}

type Props = {
  module: string | undefined;
  target: string | undefined;
  projectId: string;
  runId?: string;
  activeWorkspaceId: string | undefined;
  activeWorkspaceName: string | undefined;
};

export default function WorkflowHeader({
  module,
  target,
  projectId,
  runId,
  activeWorkspaceId,
  activeWorkspaceName,
}: Props) {
  const navigate = useNavigate();
  const [runDialogOpen, setRunDialogOpen] = useState(false);
  const workflow = useWorkflow(projectId, module, target, activeWorkspaceId);
  const run = useRun(projectId, runId, activeWorkspaceId);
  const handleRunSubmit = useCallback(
    (arguments_: ["json", string][]) => {
      const configuration = workflow!.configuration!;
      const executeAfter = configuration.delay
        ? new Date().getTime() + configuration.delay * 1000
        : null;
      return api
        .submitWorkflow(
          projectId,
          module!,
          target!,
          activeWorkspaceName!,
          arguments_,
          {
            waitFor: configuration.waitFor,
            cache: configuration.cache,
            defer: configuration.defer,
            executeAfter: executeAfter,
            retries: configuration.retries,
            requires: configuration.requires,
          },
        )
        .then(({ runId }) => {
          setRunDialogOpen(false);
          navigate(
            buildUrl(`/projects/${projectId}/runs/${runId}`, {
              workspace: activeWorkspaceName,
            }),
          );
        });
    },
    [navigate, projectId, module, target, activeWorkspaceName, workflow],
  );
  const initialStepId =
    run &&
    minBy(
      Object.keys(run.steps).filter((id) => !run.steps[id].parentId),
      (stepId) => run.steps[stepId].createdAt,
    )!;
  const latestAttempt =
    run &&
    maxBy(Object.keys(run.steps[initialStepId!].executions), (attempt) =>
      parseInt(attempt, 10),
    );
  const latestExecutionId =
    run && run.steps[initialStepId!].executions[latestAttempt!].executionId;
  const handleCancel = useCallback(() => {
    if (latestExecutionId) {
      return api.cancelExecution(projectId, latestExecutionId);
    }
  }, [projectId, latestExecutionId]);
  const handleRunClick = useCallback(() => {
    setRunDialogOpen(true);
  }, []);
  const handleRunDialogClose = useCallback(() => setRunDialogOpen(false), []);
  const runWorkspaceId = run?.steps[initialStepId!].executions[1].workspaceId;
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
          <IconSubtask
            size={24}
            strokeWidth={1.5}
            className="text-slate-400 shrink-0 self-start"
          />
          <h1 className="text-lg font-bold font-mono">{target}</h1>
        </div>

        {runId && (
          <div className="flex items-center gap-2">
            <RunSelector
              runs={workflow?.runs}
              projectId={projectId}
              runId={runId}
              activeWorkspaceName={activeWorkspaceName}
            />

            {runWorkspaceId && runWorkspaceId != activeWorkspaceId && (
              <WorkspaceLabel
                projectId={projectId}
                workspaceId={runWorkspaceId}
                warning="This run is from a different workspace"
              />
            )}
            {isRunning && <CancelButton onCancel={handleCancel} />}
          </div>
        )}
      </div>
      <div className="flex items-center gap-2">
        {workflow && (
          <>
            <Button
              onClick={handleRunClick}
              left={<IconBolt size={16} />}
              disabled={!activeWorkspaceId || !workflow.parameters}
            >
              Run...
            </Button>
            {activeWorkspaceId && workflow.parameters && (
              <RunDialog
                projectId={projectId}
                module={module}
                target={target}
                parameters={workflow.parameters}
                instruction={workflow.instruction}
                activeWorkspaceId={activeWorkspaceId}
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
