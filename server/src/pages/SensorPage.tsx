import { findKey, maxBy } from "lodash";
import { Fragment } from "react";
import { Navigate, useParams, useSearchParams } from "react-router-dom";

import { useSetActive } from "../layouts/ProjectLayout";
import { buildUrl } from "../utils";
import Loading from "../components/Loading";
import { useWorkspaces, useSensor } from "../topics";
import { useTitlePart } from "../components/TitleContext";
import SensorHeader from "../components/SensorHeader";

export default function SensorPage() {
  const {
    project: projectId,
    module: moduleName,
    target: targetName,
  } = useParams();
  const [searchParams] = useSearchParams();
  const activeWorkspaceName = searchParams.get("workspace") || undefined;
  const workspaces = useWorkspaces(projectId);
  const activeWorkspaceId = findKey(
    workspaces,
    (e) => e.name == activeWorkspaceName && e.state != "archived",
  );
  const sensor = useSensor(
    projectId,
    moduleName,
    targetName,
    activeWorkspaceId,
  );
  useTitlePart(`${targetName} (${moduleName})`);
  useSetActive(
    moduleName && targetName ? ["target", moduleName, targetName] : undefined,
  );
  if (!sensor) {
    return <Loading />;
  } else {
    const latestRunId = maxBy(
      Object.keys(sensor.runs),
      (runId) => sensor.runs[runId].createdAt,
    );
    if (latestRunId) {
      return (
        <Navigate
          replace
          to={buildUrl(`/projects/${projectId}/runs/${latestRunId}`, {
            workspace: activeWorkspaceName,
          })}
        />
      );
    } else {
      return (
        <Fragment>
          <SensorHeader
            module={moduleName}
            target={targetName}
            projectId={projectId!}
            activeWorkspaceId={activeWorkspaceId}
            activeWorkspaceName={activeWorkspaceName}
          />
          <div className="p-5 flex-1 flex flex-col gap-2 items-center justify-center text-slate-300">
            <h1 className="text-2xl">This sensor hasn't been run yet</h1>
            <p className="text-sm">
              Click the 'start' button above to start an instance of this sensor
            </p>
          </div>
        </Fragment>
      );
    }
  }
}
