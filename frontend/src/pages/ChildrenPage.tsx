import { Fragment } from "react";
import { useParams } from "react-router-dom";
import { DateTime } from "luxon";

import * as models from "../models";
import { useContext } from "../layouts/RunLayout";
import StepLink from "../components/StepLink";
import { sortBy } from "lodash";
import { IconArrowUpRight } from "@tabler/icons-react";

function findSpawned(
  run: models.Run,
  execution: models.Execution,
): (readonly [string, number])[] {
  return Object.keys(run.steps)
    .filter((stepId) => run.steps[stepId].parentId == execution.executionId)
    .flatMap((stepId) =>
      Object.entries(run.steps[stepId].executions).flatMap(([attempt, e]) => {
        return e.result?.type == "spawned"
          ? [[stepId, parseInt(attempt, 10)] as const]
          : findSpawned(run, e);
      }),
    );
}

export default function ChildrenPage() {
  const { run } = useContext();
  const { run: runId } = useParams();
  const initialStepId = Object.keys(run.steps).find(
    (stepId) => !run.steps[stepId].parentId,
  )!;
  const executions = run.steps[initialStepId].executions;
  const hasChildren = Object.values(run.steps).some((s) =>
    Object.values(s.executions).some((e) => e.result?.type == "spawned"),
  );
  return (
    <div className="p-5">
      {hasChildren ? (
        <table className="w-full">
          <tbody>
            {Object.keys(executions)
              .map((a) => parseInt(a, 10))
              .sort((a, b) => a - b)
              .map((attempt) => {
                const spawned = findSpawned(run, executions[attempt]);
                return (
                  <Fragment key={attempt}>
                    <tr>
                      <td colSpan={3}>
                        <h3 className="text-xs text-slate-400 uppercase font-semibold mt-3 mb-1">
                          <StepLink
                            runId={runId!}
                            stepId={initialStepId}
                            attempt={attempt}
                            className="rounded ring-offset-1 px-1"
                            activeClassName="ring-2 ring-cyan-400"
                            hoveredClassName="ring-2 ring-slate-300"
                          >
                            Iteration #{attempt}
                          </StepLink>
                        </h3>
                      </td>
                    </tr>
                    {spawned.length ? (
                      <Fragment>
                        {sortBy(
                          spawned,
                          ([stepId, attempt]) =>
                            run.steps[stepId].executions[attempt].completedAt,
                        ).map(([stepId, attempt]) => {
                          const step = run.steps[stepId];
                          const execution = step.executions[attempt];
                          const createdAt = DateTime.fromMillis(
                            execution.completedAt!,
                          );
                          const child =
                            execution.result?.type == "spawned"
                              ? execution.result.execution
                              : undefined;
                          return (
                            <tr key={execution.executionId}>
                              <td>
                                <span className="text-sm text-slate-400">
                                  {createdAt.toLocaleString(
                                    DateTime.DATETIME_SHORT_WITH_SECONDS,
                                  )}
                                </span>
                              </td>
                              <td>
                                <StepLink
                                  runId={runId!}
                                  stepId={stepId}
                                  attempt={attempt}
                                  className="rounded text-sm ring-offset-1 px-1"
                                  activeClassName="ring-2 ring-cyan-400"
                                  hoveredClassName="ring-2 ring-slate-300"
                                >
                                  <span className="font-mono">
                                    {step.target}
                                  </span>{" "}
                                  <span className="text-slate-500 text-xs">
                                    ({step.module})
                                  </span>
                                </StepLink>
                              </td>
                              <td>
                                {child && (
                                  <StepLink
                                    runId={child.runId}
                                    stepId={child.stepId}
                                    attempt={child.attempt}
                                    className="inline-flex items-center pl-1 pr-2 border border-slate-300 text-sm rounded-full ring-offset-1"
                                    hoveredClassName="ring-2 ring-slate-400"
                                  >
                                    <IconArrowUpRight
                                      size={18}
                                      className="text-slate-400"
                                    />
                                    <span className="text-slate-500 flex-1 text-end">
                                      {child.runId}
                                    </span>
                                  </StepLink>
                                )}
                              </td>
                            </tr>
                          );
                        })}
                      </Fragment>
                    ) : null}
                  </Fragment>
                );
              })}
          </tbody>
        </table>
      ) : (
        <p className="italic">None</p>
      )}
    </div>
  );
}
