import Dialog from "./common/Dialog";
import * as models from "../models";
import StepLink from "./StepLink";
import classNames from "classnames";
import { ComponentProps, useCallback } from "react";
import { groupBy, max, omit } from "lodash";
import { buildUrl } from "../utils";
import { useLocation, useNavigate, useSearchParams } from "react-router-dom";
import Value from "./Value";
import { getBranchStatus } from "../graph";
import Tabs, { Tab } from "./common/Tabs";
import Badge from "./Badge";

function findGroup(run: models.Run, identifier: string) {
  const parts = identifier.split("-");
  if (parts.length != 3) {
    return undefined;
  }
  const stepId = parts[0];
  const attempt = parseInt(parts[1], 10);
  const groupId = parseInt(parts[2], 10);
  const execution = run.steps[stepId]?.executions[attempt];
  if (!execution || !(groupId in execution.groups)) {
    return undefined;
  }
  const name = execution.groups[groupId];
  const steps = execution.children
    .filter((c) => c.groupId == groupId)
    .map((c) => c.stepId);
  return { name, steps };
}

type BranchStatus = ReturnType<typeof getBranchStatus>;

const statusLabels: Record<BranchStatus, string> = {
  errored: "Errored",
  aborted: "Aborted",
  suspended: "Suspended",
  deferred: "Deferred",
  completed: "Completed",
  running: "Running",
  assigning: "Assigning",
};

const statusIntents: Record<
  BranchStatus,
  ComponentProps<typeof Badge>["intent"]
> = {
  errored: "danger",
  aborted: "warning",
  suspended: "info",
  deferred: "info",
  completed: "none",
  running: "info",
  assigning: "info",
};

type StepsListProps = {
  run: models.Run;
  stepIds: string[];
  runId: string;
  projectId: string;
};

function StepsList({ run, stepIds, runId, projectId }: StepsListProps) {
  const stepsByStatus: Partial<Record<BranchStatus, string[]>> = groupBy(
    stepIds,
    (stepId) => getBranchStatus(run, stepId),
  );
  return (
    <Tabs>
      {Object.entries(stepsByStatus).map(([status, stepIds]) => (
        <Tab
          key={status}
          label={
            <>
              {statusLabels[status as BranchStatus]}{" "}
              <Badge
                label={stepIds.length.toString()}
                intent={statusIntents[status as BranchStatus]}
              />
            </>
          }
        >
          <ul className="pt-2 flex flex-col h-80 overflow-auto">
            {stepIds.map((stepId) => {
              const step = run.steps[stepId];
              const attempt = max(
                Object.keys(step.executions).map((a) => parseInt(a, 10)),
              )!;
              return (
                <li key={stepId} className="py-0.5">
                  <StepLink
                    runId={runId}
                    stepId={stepId}
                    attempt={attempt}
                    className={classNames(
                      "px-2 py-1 cursor-pointer rounded-sm flex flex-col data-active:bg-slate-100 hover:bg-slate-50",
                    )}
                    activeClassName="bg-slate-100"
                  >
                    <div className="flex items-center gap-1">
                      <div className="flex-1 flex items-baseline flex-wrap gap-1 leading-tight">
                        <div className="flex items-baseline gap-1">
                          <span className="text-slate-400 text-sm">
                            {step.module}
                          </span>
                          <span className="text-slate-400">/</span>
                        </div>
                        <span className="flex items-baseline gap-1">
                          <h2 className="font-mono">{step.target}</h2>
                        </span>
                      </div>
                      <div className="flex gap-1 items-center">#{attempt}</div>
                    </div>
                    <div>
                      {step.arguments.length > 0 && (
                        <ol className="list-decimal list-inside marker:text-slate-400 marker:text-xs space-y-1">
                          {step.arguments.map((argument, index) => (
                            <li key={index}>
                              <Value
                                value={argument}
                                projectId={projectId}
                                className="align-middle"
                              />
                            </li>
                          ))}
                        </ol>
                      )}
                    </div>
                  </StepLink>
                </li>
              );
            })}
          </ul>
        </Tab>
      ))}
    </Tabs>
  );
}

type Props = {
  runId: string;
  run: models.Run;
  identifier: string | null;
  projectId: string;
};

export default function GroupDialog({
  runId,
  run,
  identifier,
  projectId,
}: Props) {
  const [searchParams] = useSearchParams();
  const { pathname } = useLocation();
  const navigate = useNavigate();
  const group = identifier ? findGroup(run, identifier) : undefined;
  const handleDialogClose = useCallback(() => {
    navigate(
      buildUrl(pathname, omit(Object.fromEntries(searchParams), "group")),
    );
  }, [searchParams, pathname, navigate]);
  return (
    <Dialog
      open={!!identifier}
      title={group ? group.name || <em>Unnamed group</em> : undefined}
      onClose={handleDialogClose}
      size="lg"
      className="p-6"
    >
      {group ? (
        <StepsList
          run={run}
          stepIds={group.steps}
          runId={runId}
          projectId={projectId}
        />
      ) : identifier ? (
        <p>Unrecognised group</p>
      ) : null}
    </Dialog>
  );
}
