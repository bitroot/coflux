import { toPairs } from "lodash";
import * as models from "./models";

export class RequestError extends Error {
  readonly code: string;
  readonly details: Record<string, string>;

  constructor(code: string, details: Record<string, string>) {
    super(`request error (${code})`);
    this.code = code;
    this.details = details;
  }
}

async function handleResponse(res: Response) {
  if (res.status == 200) {
    return await res.json();
  } else if (res.status == 204) {
    return;
  } else if (res.status == 400) {
    const data = await res.json();
    throw new RequestError(data.error, data.details);
  } else {
    throw new Error(`request failed (${res.status})`);
  }
}

async function post(name: string, data: Record<string, any>) {
  const res = await fetch(`/api/${name}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
  return await handleResponse(res);
}

async function get(name: string, params?: Record<string, any>) {
  const queryString =
    params &&
    toPairs(params)
      .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
      .join("&");
  const res = await fetch(
    `/api/${name}${queryString ? `?${queryString}` : ""}`,
  );
  return await handleResponse(res);
}

export function createProject(projectName: string) {
  return post("create_project", { projectName });
}

export function createWorkspace(
  projectId: string,
  name: string,
  baseId: string | null,
) {
  return post("create_workspace", { projectId, name, baseId });
}

export function pauseWorkspace(projectId: string, workspaceId: string) {
  return post("pause_workspace", { projectId, workspaceId });
}

export function resumeWorkspace(projectId: string, workspaceId: string) {
  return post("resume_workspace", { projectId, workspaceId });
}

export function archiveModule(
  projectId: string,
  workspaceName: string,
  moduleName: string,
) {
  return post("archive_module", {
    projectId,
    workspaceName,
    moduleName,
  });
}

export function stopAgent(
  projectId: string,
  workspaceName: string,
  agentId: string,
) {
  return post("stop_agent", {
    projectId,
    workspaceName,
    agentId,
  });
}

export function resumeAgent(
  projectId: string,
  workspaceName: string,
  agentId: string,
) {
  return post("resume_agent", {
    projectId,
    workspaceName,
    agentId,
  });
}

export function submitWorkflow(
  projectId: string,
  module: string,
  target: string,
  workspaceName: string,
  arguments_: ["json", string][],
  options?: Partial<{
    waitFor: number[];
    cache: {
      params: number[] | true;
      maxAge: number | null;
      namespace: string | null;
      version: string | null;
    } | null;
    defer: {
      params: number[] | true;
    } | null;
    executeAfter: number | null;
    retries: {
      limit: number;
      delayMin?: number;
      delayMax?: number;
    } | null;
    requires: Record<string, string[]>;
  }>,
) {
  return post("submit_workflow", {
    ...options,
    projectId,
    module,
    target,
    workspaceName,
    arguments: arguments_,
  });
}

export function startSensor(
  projectId: string,
  module: string,
  target: string,
  workspaceName: string,
  arguments_: ["json", string][],
  options?: Partial<{
    requires: Record<string, string[]>;
  }>,
) {
  return post("start_sensor", {
    ...options,
    projectId,
    module,
    target,
    workspaceName,
    arguments: arguments_,
  });
}

export function rerunStep(
  projectId: string,
  stepId: string,
  workspaceName: string,
) {
  return post("rerun_step", { projectId, stepId, workspaceName });
}

export function cancelExecution(projectId: string, executionId: string) {
  return post("cancel_execution", { projectId, executionId });
}

export function search(projectId: string, workspaceId: string, query: string) {
  return get("search", { project: projectId, workspaceId, query });
}

export function getWorkspaces(
  projectId: string,
): Promise<Record<string, Pick<models.Workspace, "name" | "baseId">>> {
  return get("get_workspaces", { project: projectId });
}
