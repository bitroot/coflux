import { useParams, useSearchParams } from "react-router-dom";
import { IconBox } from "@tabler/icons-react";
import { DateTime } from "luxon";

import * as models from "../models";
import { useTitlePart } from "../components/TitleContext";
import Loading from "../components/Loading";
import ModuleQueue from "../components/ModuleQueue";
import useNow from "../hooks/useNow";
import { useSetActive } from "../layouts/ProjectLayout";
import { useWorkspaces, useExecutions } from "../topics";
import { findKey } from "lodash";

function splitExecutions(
  executions: Record<string, models.QueuedExecution>,
  now: DateTime,
) {
  return Object.entries(executions).reduce(
    ([executing, overdue, scheduled], [executionId, execution]) => {
      if (execution.assignedAt) {
        return [{ ...executing, [executionId]: execution }, overdue, scheduled];
      } else {
        const executeAt = DateTime.fromMillis(
          execution.executeAfter || execution.createdAt,
        );
        const dueDiff = executeAt.diff(now);
        if (dueDiff.toMillis() < 0) {
          return [
            executing,
            { ...overdue, [executionId]: execution },
            scheduled,
          ];
        } else {
          return [
            executing,
            overdue,
            { ...scheduled, [executionId]: execution },
          ];
        }
      }
    },
    [{}, {}, {}],
  );
}

export default function ModulePage() {
  const { project: projectId, module: moduleName } = useParams();
  const [searchParams] = useSearchParams();
  const workspaceName = searchParams.get("workspace") || undefined;
  const workspaces = useWorkspaces(projectId);
  const workspaceId = findKey(
    workspaces,
    (e) => e.name == workspaceName && e.state != "archived",
  );
  const executions = useExecutions(projectId, moduleName, workspaceId);
  useTitlePart(moduleName);
  useSetActive(moduleName ? ["module", moduleName] : undefined);
  const now = useNow(500);
  if (!executions) {
    return <Loading />;
  } else {
    const [executing, overdue, scheduled] = splitExecutions(executions, now);
    return (
      <div className="flex-1 p-4 flex flex-col min-h-0">
        <div className="flex py-1 mb-4">
          <h1 className="flex items-baseline gap-1">
            <IconBox
              size={24}
              strokeWidth={1.5}
              className="text-slate-400 shrink-0 self-start"
            />
            <span className="text-xl font-bold font-mono">{moduleName}</span>
          </h1>
        </div>
        <div className="flex-1 flex gap-4 min-h-0">
          <ModuleQueue
            projectId={projectId!}
            workspaceName={workspaceName!}
            title="Executing"
            executions={executing}
            now={now}
            emptyText="No executions running"
          />
          <ModuleQueue
            projectId={projectId!}
            workspaceName={workspaceName!}
            title="Due"
            executions={overdue}
            now={now}
            emptyText="No executions due"
          />
          <ModuleQueue
            projectId={projectId!}
            workspaceName={workspaceName!}
            title="Scheduled"
            executions={scheduled}
            now={now}
            emptyText="No executions scheduled"
          />
        </div>
      </div>
    );
  }
}
