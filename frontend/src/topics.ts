import { useTopic } from "@topical/react";

import * as models from "./models";

export function useProjects() {
  const [projects] = useTopic<Record<string, models.Project>>("projects");
  return projects;
}

export function useSpaces(projectId: string | undefined) {
  const [spaces] = useTopic<Record<string, models.Space>>(
    "projects",
    projectId,
    "spaces",
  );
  return spaces;
}

export function useModules(
  projectId: string | undefined,
  spaceId: string | undefined,
) {
  const [modules] = useTopic<Record<string, models.Module>>(
    "projects",
    projectId,
    "modules",
    spaceId,
  );
  return modules;
}

export function usePools(
  projectId: string | undefined,
  spaceId: string | undefined,
) {
  const [pools] = useTopic<models.Pools>(
    "projects",
    projectId,
    "pools",
    spaceId,
  );
  return pools;
}

export function usePool(
  projectId: string | undefined,
  spaceId: string | undefined,
  poolName: string | undefined,
) {
  const [pool] = useTopic<{
    pool: models.Pool | null;
    agents: Record<string, models.Agent>;
  }>("projects", projectId, "pools", spaceId, poolName);
  return pool;
}

export function useSessions(
  projectId: string | undefined,
  spaceId: string | undefined,
) {
  const [sessions] = useTopic<Record<string, models.Session>>(
    "projects",
    projectId,
    "sessions",
    spaceId,
  );
  return sessions;
}

export function useWorkflow(
  projectId: string | undefined,
  moduleName: string | undefined,
  targetName: string | undefined,
  spaceId: string | undefined,
) {
  const [target] = useTopic<models.Workflow>(
    "projects",
    projectId,
    "workflows",
    moduleName,
    targetName,
    spaceId,
  );
  return target;
}

export function useSensor(
  projectId: string | undefined,
  module: string | undefined,
  targetName: string | undefined,
  spaceId: string | undefined,
) {
  const [target] = useTopic<models.Sensor>(
    "projects",
    projectId,
    "sensors",
    module,
    targetName,
    spaceId,
  );
  return target;
}

export function useExecutions(
  projectId: string | undefined,
  moduleName: string | undefined,
  spaceId: string | undefined,
) {
  const [executions] = useTopic<Record<string, models.QueuedExecution>>(
    "projects",
    projectId,
    "modules",
    moduleName,
    spaceId,
  );
  return executions;
}

export function useRun(
  projectId: string | undefined,
  runId: string | undefined,
  spaceId: string | undefined,
) {
  const [run, { loading }] = useTopic<models.Run>(
    "projects",
    projectId,
    "runs",
    runId,
    spaceId,
  );
  return loading ? undefined : run;
}

export function useLogs(
  projectId: string | undefined,
  runId: string | undefined,
  spaceId: string | undefined,
) {
  const [logs] = useTopic<models.LogMessage[]>(
    "projects",
    projectId,
    "runs",
    runId,
    "logs",
    spaceId,
  );
  return logs;
}
