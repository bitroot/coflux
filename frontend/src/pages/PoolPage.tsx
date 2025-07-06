import { useCallback } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { useSpaces, usePool } from "../topics";
import { findKey, sortBy } from "lodash";
import { useSetActive } from "../layouts/ProjectLayout";
import { IconLayoutGrid, IconBrandDocker } from "@tabler/icons-react";
import TagSet from "../components/TagSet";
import Loading from "../components/Loading";
import Badge from "../components/Badge";
import Button from "../components/common/Button";
import * as models from "../models";
import * as api from "../api";
import { DateTime } from "luxon";

type WorkerRowProps = {
  projectId: string;
  spaceName: string;
  workerId: string;
  worker: models.Worker;
};

function WorkerRow({ projectId, spaceName, workerId, worker }: WorkerRowProps) {
  const handleStopClick = useCallback(() => {
    api.stopWorker(projectId, spaceName, workerId).catch(() => {
      alert("Failed to stop worker. Please try again.");
    });
  }, [projectId, spaceName, workerId]);
  const handleResumeClick = useCallback(() => {
    api.resumeWorker(projectId, spaceName, workerId).catch(() => {
      alert("Failed to resume worker. Please try again.");
    });
  }, [projectId, spaceName, workerId]);
  const startingAt = DateTime.fromMillis(worker.startingAt);
  return (
    <tr className="border-b border-slate-100">
      <td>{startingAt.toLocaleString(DateTime.DATETIME_SHORT_WITH_SECONDS)}</td>
      <td>
        {worker.startError ? (
          <Badge intent="danger" label="Start error" />
        ) : worker.stopError ? (
          <Badge intent="danger" label="Stop error" />
        ) : !worker.startedAt && !worker.deactivatedAt ? (
          <Badge intent="info" label="Starting..." />
        ) : worker.stoppedAt || worker.deactivatedAt ? (
          <Badge intent="none" label="Stopped" />
        ) : worker.stoppingAt ? (
          <Badge intent="info" label="Stopping" />
        ) : worker.state == "paused" ? (
          <Badge intent="info" label="Paused" />
        ) : worker.state == "draining" ? (
          <Badge intent="info" label="Draining" />
        ) : worker.connected === null ? (
          <Badge intent="none" label="Connecting..." />
        ) : worker.connected ? (
          <Badge intent="success" label="Connected" />
        ) : (
          <Badge intent="warning" label="Disconnected" />
        )}
      </td>
      <td>
        {worker.startedAt &&
          !worker.stoppingAt &&
          !worker.deactivatedAt &&
          (worker.state == "active" ? (
            <Button
              onClick={handleStopClick}
              size="sm"
              variant="secondary"
              outline={true}
            >
              Stop
            </Button>
          ) : (
            <Button onClick={handleResumeClick} size="sm">
              Resume
            </Button>
          ))}
      </td>
    </tr>
  );
}

type WorkersTableProps = {
  projectId: string;
  spaceName: string;
  title: string;
  workers: Record<string, models.Worker>;
};

function WorkersTable({
  projectId,
  spaceName,
  title,
  workers,
}: WorkersTableProps) {
  return (
    <div>
      <h1 className="text-xl font-semibold text-slate-700 my-1">{title}</h1>
      {Object.keys(workers).length ? (
        <table className="w-full table-fixed">
          <thead className="[&_th]:py-1">
            <tr className="border-b border-slate-100">
              {["Created at", "Status"].map((title, index) => (
                <th
                  key={index}
                  className="text-left text-sm text-slate-400 font-normal"
                >
                  {title}
                </th>
              ))}
              <th></th>
            </tr>
          </thead>
          <tbody className="[&_td]:py-1">
            {sortBy(
              Object.entries(workers),
              ([, worker]) => -worker.startingAt,
            ).map(([workerId, worker]) => (
              <WorkerRow
                key={workerId}
                projectId={projectId}
                spaceName={spaceName}
                workerId={workerId}
                worker={worker}
              />
            ))}
          </tbody>
        </table>
      ) : (
        <p className="italic">None</p>
      )}
    </div>
  );
}

type LauncherTypeProps = {
  launcher: models.Pool["launcher"];
};

function LauncherType({ launcher }: LauncherTypeProps) {
  switch (launcher?.type) {
    case "docker":
      return (
        <span className="rounded-md bg-blue-400/20 text-xs text-slate-600 inline-flex gap-1 px-1 py-px items-center">
          <IconBrandDocker size={16} strokeWidth={1.5} />
          Docker
        </span>
      );
    case undefined:
      return <span className="italic text-xs text-slate-400">Unmanaged</span>;
  }
}

export default function PoolPage() {
  const { project: projectId, pool: poolName } = useParams();
  const [searchParams] = useSearchParams();
  const spaceName = searchParams.get("space") || undefined;
  const spaces = useSpaces(projectId);
  const spaceId = findKey(
    spaces,
    (e) => e.name == spaceName && e.state != "archived",
  );

  const pool = usePool(projectId, spaceId, poolName);
  useSetActive(poolName ? ["pool", poolName] : undefined);

  if (!pool) {
    return <Loading />;
  } else {
    return (
      <>
        <div className="flex-1 flex flex-col min-h-0">
          <div className="p-5 flex items-baseline gap-1 border-b border-slate-200">
            <IconLayoutGrid
              size={24}
              strokeWidth={1.5}
              className="text-slate-400 shrink-0 self-start"
            />
            <h1 className="text-lg font-bold font-mono">{poolName}</h1>
          </div>
          <div className="flex-1 flex min-h-0">
            <div className="p-5 flex-1 flex flex-col gap-6 overflow-auto">
              <WorkersTable
                projectId={projectId!}
                spaceName={spaceName!}
                title="Workers"
                workers={pool.workers}
              />
            </div>
            {pool.pool && (
              <div className="p-5 max-w-[400px] min-w-[200px] w-[30%] border-l border-slate-200 flex flex-col gap-3">
                <div>
                  <h3 className="uppercase text-sm font-bold text-slate-400">
                    Modules
                  </h3>
                  {pool.pool.modules.length ? (
                    <ul className="list-disc ml-5 marker:text-slate-600">
                      {pool.pool.modules.map((module) => (
                        <li key={module}>{module}</li>
                      ))}
                    </ul>
                  ) : (
                    <p className="italic">None</p>
                  )}
                </div>
                <div>
                  <h3 className="uppercase text-sm font-bold text-slate-400">
                    Provides
                  </h3>
                  {Object.keys(pool.pool.provides).length ? (
                    <TagSet tagSet={pool.pool.provides} />
                  ) : (
                    <p className="italic">None</p>
                  )}
                </div>
                {pool.pool.launcher && (
                  <div>
                    <h3 className="uppercase text-sm font-bold text-slate-400">
                      Launcher
                    </h3>
                    <div className="my-1">
                      <LauncherType launcher={pool.pool.launcher} />
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </>
    );
  }
}
