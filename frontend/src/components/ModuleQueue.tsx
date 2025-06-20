import { Link } from "react-router-dom";

import * as models from "../models";
import { buildUrl } from "../utils";
import { DateTime } from "luxon";
import classNames from "classnames";

type Props = {
  projectId: string;
  spaceName: string;
  title: string;
  titleClassName?: string;
  executions: Record<string, models.QueuedExecution>;
  now: DateTime;
  emptyText: string;
};

export default function ModuleQueue({
  projectId,
  spaceName,
  title,
  titleClassName,
  executions,
  now,
  emptyText,
}: Props) {
  return (
    <div className="flex-1 flex flex-col gap-2 border border-slate-200 rounded-md">
      <div className="px-3 py-4 border-b border-slate-200">
        <h1
          className={classNames(
            "uppercase font-semibold text-xs text-slate-400",
            titleClassName,
          )}
        >
          {title}
        </h1>
        <p className="text-slate-600 text-3xl">
          {Object.keys(executions).length}
        </p>
      </div>
      <div className="flex-1 overflow-auto p-3">
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
                            space: spaceName,
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
