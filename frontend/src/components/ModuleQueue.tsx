import { Link } from "react-router-dom";

import * as models from "../models";
import { buildUrl } from "../utils";
import { DateTime } from "luxon";
import classNames from "classnames";

type Props = {
  projectId: string;
  workspaceName: string;
  title: string;
  titleClassName?: string;
  executions: Record<string, models.QueuedExecution>;
  now: DateTime;
  emptyText: string;
};

export default function ModuleQueue({
  projectId,
  workspaceName,
  title,
  titleClassName,
  executions,
  now,
  emptyText,
}: Props) {
  return (
    <div className="flex-1 py-2 flex flex-col gap-2">
      <div className="shadow-inner bg-slate-50 rounded px-3 py-4">
        <h1
          className={classNames(
            "uppercase font-bold text-xs text-slate-400",
            titleClassName,
          )}
        >
          {title}
        </h1>
        <p className="text-slate-600 text-3xl">
          {Object.keys(executions).length}
        </p>
      </div>
      <div className="flex-1 shadow-inner bg-slate-50 rounded overflow-auto p-3">
        {Object.keys(executions).length ? (
          <table className="w-full">
            <tbody>
              {Object.entries(executions).map(([executionId, execution]) => {
                const time = DateTime.fromMillis(
                  execution.assignedAt ||
                    execution.executeAfter ||
                    execution.createdAt,
                );
                const diff = now.diff(time, [
                  "days",
                  "hours",
                  "minutes",
                  "seconds",
                ]);
                return (
                  <tr key={executionId}>
                    <td>
                      <Link
                        to={buildUrl(
                          `/projects/${projectId}/runs/${execution.runId}/graph`,
                          {
                            workspace: workspaceName,
                            step: execution.stepId,
                            attempt: execution.attempt,
                          },
                        )}
                      >
                        <span className="font-mono">{execution.target}</span>
                      </Link>
                    </td>
                    <td className="text-right">
                      <span className="text-slate-400">
                        {diff.rescale().toHuman({ unitDisplay: "narrow" })}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        ) : (
          <p className="italic text-slate-300 text-lg">{emptyText}</p>
        )}
      </div>
    </div>
  );
}
