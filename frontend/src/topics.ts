import { useTopic } from "@topical/react";

import * as models from "./models";

export function useProjects() {
  const [projects] = useTopic<Record<string, models.Project>>("projects");
  return projects;
}

export function useWorkspaces(projectId: string | undefined) {
  const [workspaces] = useTopic<Record<string, models.Workspace>>(
    "projects",
    projectId,
    "workspaces",
  );
  return workspaces;
}

export function useModules(
  projectId: string | undefined,
  workspaceId: string | undefined,
) {
  const [modules] = useTopic<Record<string, models.Module>>(
    "projects",
    projectId,
    "modules",
    workspaceId,
  );
  return modules;
}

export function usePools(
  projectId: string | undefined,
  workspaceId: string | undefined,
) {
  const [pools] = useTopic<models.Pools>(
    "projects",
    projectId,
    "pools",
    workspaceId,
  );
  return pools;
}

export function usePool(
  projectId: string | undefined,
  workspaceId: string | undefined,
  poolName: string | undefined,
) {
  const [pool] = useTopic<{
    pool: models.Pool | null;
    agents: Record<string, models.Agent>;
  }>("projects", projectId, "pools", workspaceId, poolName);
  return pool;
}

export function useSessions(
  projectId: string | undefined,
  workspaceId: string | undefined,
) {
  const [sessions] = useTopic<Record<string, models.Session>>(
    "projects",
    projectId,
    "sessions",
    workspaceId,
  );
  return sessions;
}

export function useWorkflow(
  projectId: string | undefined,
  moduleName: string | undefined,
  targetName: string | undefined,
  workspaceId: string | undefined,
) {
  const [target] = useTopic<models.Workflow>(
    "projects",
    projectId,
    "workflows",
    moduleName,
    targetName,
    workspaceId,
  );
  return target;
}

export function useSensor(
  projectId: string | undefined,
  module: string | undefined,
  targetName: string | undefined,
  workspaceId: string | undefined,
) {
  const [target] = useTopic<models.Sensor>(
    "projects",
    projectId,
    "sensors",
    module,
    targetName,
    workspaceId,
  );
  return target;
}

export function useExecutions(
  projectId: string | undefined,
  moduleName: string | undefined,
  workspaceId: string | undefined,
) {
  const [executions] = useTopic<Record<string, models.QueuedExecution>>(
    "projects",
    projectId,
    "modules",
    moduleName,
    workspaceId,
  );
  return executions;
}

export function useRun(
  projectId: string | undefined,
  runId: string | undefined,
  workspaceId: string | undefined,
) {
  const [run, { loading }] = useTopic<models.Run>(
    "projects",
    projectId,
    "runs",
    runId,
    workspaceId,
  );
  return loading ? undefined : run;
}

export function useLogs(
  projectId: string | undefined,
  runId: string | undefined,
  workspaceId: string | undefined,
) {
  const [logs] = useTopic<models.LogMessage[]>(
    "projects",
    projectId,
    "runs",
    runId,
    "logs",
    workspaceId,
  );
  return logs;
}
