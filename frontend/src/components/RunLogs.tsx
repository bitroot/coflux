import { Fragment, ReactNode } from "react";
import { omit, sortBy } from "lodash";
import { DateTime } from "luxon";
import classNames from "classnames";

import * as models from "../models";
import Value from "./Value";

function classForLevel(level: models.LogMessageLevel) {
  switch (level) {
    case 0:
      return "border-slate-200/30";
    case 1:
      return "border-slate-300/30 text-slate-600 font-mono text-sm";
    case 2:
      return "border-blue-400/30";
    case 3:
      return "border-red-300/30 text-red-800 font-mono text-sm";
    case 4:
      return "border-yellow-500/30";
    case 5:
      return "border-red-600/30";
  }
}

function substituteValues(
  template: string | null,
  values: Record<string, models.Value>,
  projectId: string,
): [ReactNode[] | null, Record<string, models.Value>] {
  if (template) {
    const parts = template.split(/\{(\w+)\}/g);
    const used = parts.filter((_, i) => i % 2 == 1);
    const extra = omit(values, used);
    return [
      parts.map((part, index) =>
        index % 2 == 0 ? (
          part
        ) : !(part in values) ? (
          `{${part}}`
        ) : (
          <Value key={index} value={values[part]} projectId={projectId} />
        ),
      ),
      extra,
    ];
  } else {
    return [null, values];
  }
}

type LogMessageProps = {
  level: models.LogMessageLevel;
  template: string | null;
  values: Record<string, models.Value>;
  projectId: string;
  className?: string;
};

function LogMessage({
  level,
  template,
  values,
  projectId,
  className,
}: LogMessageProps) {
  const [message, extra] = substituteValues(template, values, projectId);
  return (
    <div
      className={classNames(className, "border-l-4 pl-2", classForLevel(level))}
    >
      {message && (
        <span className="whitespace-pre-wrap text-sm">{message}</span>
      )}
      {Object.keys(extra).length > 0 && (
        <div className="flex flex-wrap gap-2 mt-1">
          {Object.entries(extra).map(([label, value]) => (
            <div key={label} className="flex items-start gap-1">
              <span className="bg-slate-400/20 rounded-sm text-slate-400 text-xs px-1 whitespace-nowrap my-0.5">
                <span className="text-slate-900">{label}</span>:
              </span>
              <Value value={value} projectId={projectId} />
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

type Props = {
  startTime: DateTime;
  logs: models.LogMessage[];
  darkerTimestampRule?: boolean;
  stepIdentifier?: (executionId: string) => ReactNode;
  projectId: string;
};

export default function RunLogs({
  startTime,
  logs,
  darkerTimestampRule,
  stepIdentifier,
  projectId,
}: Props) {
  let lastTimestamp = startTime;
  let lastExecutionId: string | undefined;
  if (logs.length) {
    return (
      <table className="w-full border-collapse">
        <tbody>
          {sortBy(logs, (l) => l[1]).map((message, index) => {
            const [executionId, timestamp, level, template, values] = message;
            const createdAt = DateTime.fromMillis(timestamp);
            const diff = createdAt.diff(lastTimestamp, [
              "days",
              "hours",
              "minutes",
              "seconds",
              "milliseconds",
            ]);
            const showTimestamp = index == 0 || diff.toMillis() > 950;
            if (showTimestamp) {
              lastTimestamp = createdAt;
            }
            const showExecution =
              !lastExecutionId ||
              lastExecutionId != executionId ||
              showTimestamp;
            lastExecutionId = executionId;
            return (
              <Fragment key={index}>
                {showTimestamp && (
                  <tr>
                    <td
                      className="py-2 select-none"
                      colSpan={stepIdentifier ? 2 : 1}
                    >
                      <div className="flex items-center">
                        <span
                          className="text-slate-400/70 relative pr-2 text-sm"
                          title={createdAt.toLocaleString(
                            DateTime.DATETIME_SHORT_WITH_SECONDS,
                          )}
                        >
                          +{diff.rescale().toHuman({ unitDisplay: "short" })}
                        </span>
                        <span className="flex-1">
                          <span
                            className={classNames(
                              "border-b  mt-1 block",
                              darkerTimestampRule
                                ? "border-slate-200"
                                : "border-slate-100",
                            )}
                          />
                        </span>
                      </div>
                    </td>
                  </tr>
                )}
                <tr>
                  {stepIdentifier && (
                    <td className="w-0 align-top px-1 select-none">
                      {showExecution && stepIdentifier(executionId)}
                    </td>
                  )}
                  <td className="align-top p-1">
                    <LogMessage
                      level={level}
                      template={template}
                      values={values}
                      projectId={projectId}
                    />
                  </td>
                </tr>
              </Fragment>
            );
          })}
        </tbody>
      </table>
    );
  } else {
    return <p className="italic">None</p>;
  }
}
