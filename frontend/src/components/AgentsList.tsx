import { useState, useCallback, Fragment } from "react";
import { Link } from "react-router-dom";
import classNames from "classnames";
import {
  IconLayoutGrid,
  IconChevronUp,
  IconChevronDown,
} from "@tabler/icons-react";

import * as models from "../models";
import Badge from "./Badge";
import { pluralise, buildUrl } from "../utils";

type CountersProps = {
  sessions: models.Session[] | undefined;
};

function Counters({ sessions }: CountersProps) {
  const connected = sessions && sessions.filter((s) => s.connected);
  const activeCount = connected && connected.filter((s) => s.executions).length;
  const idleCount = connected && connected.filter((s) => !s.executions).length;
  return (
    <span className="flex gap-1">
      {idleCount || activeCount ? (
        <Fragment>
          {idleCount ? (
            <Badge
              label={`${idleCount}`}
              title={`${pluralise(idleCount, "idle agent")}`}
            />
          ) : null}
          {activeCount ? (
            <Badge
              label={`${activeCount}`}
              title={`${pluralise(activeCount, "active agent")}`}
              intent="info"
            />
          ) : null}
        </Fragment>
      ) : (
        <span className="text-slate-300" title="None">
          &ndash;
        </span>
      )}
    </span>
  );
}

type PoolItemProps = {
  poolName: string;
  projectId: string;
  spaceName: string;
  activePool: string | undefined;
  sessions: models.Session[];
};

function PoolItem({
  poolName,
  projectId,
  spaceName,
  activePool,
  sessions,
}: PoolItemProps) {
  return (
    <li>
      <Link
        to={buildUrl(`/projects/${projectId}/pools/${poolName}`, {
          space: spaceName,
        })}
        className={classNames(
          "flex gap-1 items-center pl-1 pr-2 py-0.5 rounded-md my-0.5",
          poolName == activePool ? "bg-slate-200" : "hover:bg-slate-200/50",
        )}
      >
        <IconLayoutGrid
          size={20}
          strokeWidth={1}
          className="text-slate-500 shrink-0"
        />
        <span className="flex-1 flex items-center justify-between">
          <span className="font-mono text-sm text-slate-800">{poolName}</span>
          <Counters sessions={sessions} />
        </span>
      </Link>
    </li>
  );
}

type UnmanagedItemProps = {
  sessions: models.Session[];
};

function UnmanagedItem({ sessions }: UnmanagedItemProps) {
  return (
    <li>
      <div className="flex gap-1 items-center p-1 rounded-md my-0.5">
        <span className="flex-1 flex justify-between">
          <span className="text-sm text-slate-600 italic">
            Unmanaged agents
          </span>
          <Counters sessions={sessions} />
        </span>
      </div>
    </li>
  );
}

type Props = {
  pools: models.Pools | undefined;
  projectId: string;
  spaceName: string;
  activePool: string | undefined;
  sessions: Record<string, models.Session> | undefined;
};

export default function AgentsList({
  pools,
  projectId,
  spaceName,
  activePool,
  sessions,
}: Props) {
  // TODO: remember expanded
  const [expanded, setExpanded] = useState<boolean>();
  const expand = expanded === undefined ? !!activePool : expanded;
  const handleToggleClick = useCallback(() => setExpanded(!expand), [expand]);
  const connected =
    sessions && Object.values(sessions).filter((s) => s.connected);
  return (
    <div className="my-2">
      <div className="px-3">
        <button
          className="w-full flex-1 flex justify-between items-center rounded-sm p-1 hover:bg-slate-50"
          onClick={handleToggleClick}
        >
          <div className="flex items-center gap-1">
            <h1 className="text-sm text-slate-500">Pools</h1>
            {expand ? (
              <IconChevronDown size={16} className="text-slate-400" />
            ) : (
              <IconChevronUp size={16} className="text-slate-400" />
            )}
          </div>
          {!expand && <Counters sessions={connected} />}
        </button>
      </div>
      {expand && pools && sessions ? (
        <div className="flex-1 overflow-auto min-h-0 px-3">
          {Object.keys(pools).length || Object.keys(sessions).length ? (
            <div className="flex flex-col divide-y">
              {Object.keys(pools).length ? (
                <ul className="flex flex-col py-1">
                  {Object.keys(pools)
                    .sort()
                    .map((name) => (
                      <PoolItem
                        key={name}
                        poolName={name}
                        projectId={projectId}
                        spaceName={spaceName}
                        sessions={Object.values(sessions).filter(
                          (s) => s.poolName == name,
                        )}
                        activePool={activePool}
                      />
                    ))}
                </ul>
              ) : null}
              {Object.values(sessions).some((s) => !s.poolName) ? (
                <ul className="py-1">
                  <UnmanagedItem
                    sessions={Object.values(sessions).filter(
                      (s) => !s.poolName,
                    )}
                  />
                </ul>
              ) : null}
            </div>
          ) : (
            <div className="px-1 py-2 italic text-sm text-slate-400">None</div>
          )}
        </div>
      ) : null}
    </div>
  );
}
