import { toPairs } from "lodash";
import * as models from "./models";

const API_VERSION = "0.8";

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

async function post(name: string, data: Record<string, unknown>) {
  const res = await fetch(`/api/${name}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-API-Version": API_VERSION,
    },
    body: JSON.stringify(data),
  });
  return await handleResponse(res);
}

async function get(name: string, params?: Record<string, string>) {
  const queryString =
    params &&
    toPairs(params)
      .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
      .join("&");
  const res = await fetch(
    `/api/${name}${queryString ? `?${queryString}` : ""}`,
    {
      headers: {
        "X-API-Version": API_VERSION,
      },
    },
  );
  return await handleResponse(res);
}

export function createProject(projectName: string) {
  return post("create_project", { projectName });
}

export function createSpace(
  projectId: string,
  name: string,
  baseId: string | null,
) {
  return post("create_space", { projectId, name, baseId });
}

export function pauseSpace(projectId: string, spaceId: string) {
  return post("pause_space", { projectId, spaceId });
}

export function resumeSpace(projectId: string, spaceId: string) {
  return post("resume_space", { projectId, spaceId });
}

export function archiveModule(
  projectId: string,
  spaceName: string,
  moduleName: string,
) {
  return post("archive_module", {
    projectId,
    spaceName,
    moduleName,
  });
}

export function stopWorker(
  projectId: string,
  spaceName: string,
  workerId: string,
) {
  return post("stop_worker", {
    projectId,
    spaceName,
    workerId,
  });
}

export function resumeWorker(
  projectId: string,
  spaceName: string,
  workerId: string,
) {
  return post("resume_worker", {
    projectId,
    spaceName,
    workerId,
  });
}

export function submitWorkflow(
  projectId: string,
  module: string,
  target: string,
  spaceName: string,
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
    spaceName,
    arguments: arguments_,
  });
}

export function startSensor(
  projectId: string,
  module: string,
  target: string,
  spaceName: string,
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
    spaceName,
    arguments: arguments_,
  });
}

export function rerunStep(
  projectId: string,
  stepId: string,
  spaceName: string,
): Promise<{ attempt: number }> {
  return post("rerun_step", { projectId, stepId, spaceName });
}

export function cancelExecution(projectId: string, executionId: string) {
  return post("cancel_execution", { projectId, executionId });
}

export function search(projectId: string, spaceId: string, query: string) {
  return get("search", { project: projectId, spaceId, query });
}

export function getSpaces(
  projectId: string,
): Promise<Record<string, Pick<models.Space, "name" | "baseId">>> {
  return get("get_spaces", { project: projectId });
}

export function getAsset(
  projectId: string,
  assetId: string,
): Promise<models.Asset> {
  return get("get_asset", { project: projectId, asset: assetId });
}
