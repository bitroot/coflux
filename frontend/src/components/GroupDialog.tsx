import Dialog from "./common/Dialog";
import * as models from "../models";
import StepLink from "./StepLink";
import classNames from "classnames";
import ExecutionStatus from "./ExecutionStatus";
import { useCallback } from "react";
import { max, omit } from "lodash";
import { buildUrl } from "../utils";
import { useLocation, useNavigate, useSearchParams } from "react-router-dom";
import Value from "./Value";

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
    .map((c) => c.stepId)
    .reduce<Record<string, number>>(
      (acc, sId) => ({
        ...acc,
        [sId]: max(
          Object.keys(run.steps[sId].executions).map((a) => parseInt(a, 10)),
        )!,
      }),
      {},
    );
  return { name, steps };
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
  const group = identifier && findGroup(run, identifier);
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
      size="md"
      className="p-6"
    >
      {group ? (
        <ul className="flex flex-col overflow-auto divide-y">
          {Object.entries(group.steps).map(([stepId, attempt]) => {
            const step = run.steps[stepId];
            const execution = step.executions[attempt];
            return (
              <li key={stepId} className="py-1">
                <StepLink
                  runId={runId}
                  stepId={stepId}
                  attempt={attempt}
                  className={classNames(
                    "p-1 cursor-pointer rounded-sm flex flex-col data-active:bg-slate-100 hover:bg-slate-50",
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
                    <div className="flex gap-1 items-center">
                      #{attempt}
                      <ExecutionStatus execution={execution} step={step} />
                    </div>
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
      ) : identifier ? (
        <p>Unrecognised group</p>
      ) : null}
    </Dialog>
  );
}
