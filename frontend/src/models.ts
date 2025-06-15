export type Project = {
  name: string;
};

export type Workspace = {
  name: string;
  baseId: string | null;
  state: "active" | "paused" | "archived";
};

export type TagSet = Record<string, string[]>;

export type Parameter = {
  name: string;
  default: string | null;
  annotation: string;
};

export type CacheConfig = {
  params: number[] | true;
  maxAge: number | null;
  namespace: string | null;
  version: string | null;
};

export type Workflow = {
  parameters: Parameter[] | null;
  instruction: string | null;
  configuration: {
    waitFor: number[];
    cache: CacheConfig | null;
    defer: {
      params: number[] | true;
    } | null;
    delay: number;
    retries: {
      limit: number;
      delayMin?: number;
      delayMax?: number;
    } | null;
    requires: TagSet;
  } | null;
  runs: Record<string, Pick<Run, "createdAt">>;
};

export type Sensor = {
  parameters: Parameter[] | null;
  instruction: string | null;
  configuration: {
    requires: TagSet;
  } | null;
  runs: Record<string, Pick<Run, "createdAt">>;
};

export type Module = {
  workflows: string[];
  sensors: string[];
  executing: number;
  nextDueAt: number | null;
  scheduled: number;
};

export type ExecutionReference = {
  runId: string;
  stepId: string;
  attempt: number;
  module: string;
  target: string;
};

export type AssetEntry = {
  path: string;
  blobKey: string;
  size: number;
  metadata: Record<string, unknown>;
};

export type Asset = {
  name: string | null;
  entries: Record<string, Omit<AssetEntry, "path">>;
};

export type AssetSummary = Omit<Asset, "entries"> & {
  totalCount: number;
  totalSize: number;
  entry: AssetEntry | null;
};

export type Reference =
  | {
      type: "fragment";
      format: string;
      blobKey: string;
      size: number;
      metadata: Record<string, unknown>;
    }
  | {
      type: "execution";
      execution: ExecutionReference;
    }
  | {
      type: "asset";
      assetId: string;
      asset: AssetSummary;
    };

export type Data =
  | number
  | boolean
  | null
  | string
  | Data[]
  | { type: "dict"; items: Data[] }
  | { type: "set"; items: Data[] }
  | { type: "tuple"; items: Data[] }
  | { type: "ref"; index: number };

export type Value = (
  | {
      type: "raw";
      data: Data;
    }
  | {
      type: "blob";
      key: string;
      size: number;
    }
) & {
  references: Reference[];
};

export type ErrorFrame = {
  file: string;
  line: number;
  name: string;
  code: string | null;
};

export type Error = {
  type: string;
  message: string;
  frames: ErrorFrame[];
};

export type Result =
  | { type: "value"; value: Value }
  | { type: "error"; error: Error; retry: number | null }
  | { type: "abandoned"; retry: number | null }
  | { type: "cancelled" }
  | { type: "suspended"; successor: number }
  | {
      type: "deferred" | "cached" | "spawned";
      execution: ExecutionReference;
      result?: Result;
    };

// TODO: combine with `Execution`?
export type QueuedExecution = {
  target: string;
  runId: string;
  stepId: string;
  attempt: number;
  executeAfter: number | null;
  createdAt: number;
  assignedAt: number | null;
};

export type Dependency = {
  execution: ExecutionReference;
};

export type Child = {
  stepId: string;
  attempt: number;
  groupId: number | null;
};

export type Execution = {
  executionId: string;
  workspaceId: string;
  createdAt: number;
  executeAfter: number | null;
  assignedAt: number | null;
  completedAt: number | null;
  groups: Record<string, string>;
  dependencies: Record<string, Dependency>;
  children: Child[];
  result: Result | null;
  assets: Record<string, AssetSummary>;
  logCount: number;
};

export type Step = {
  module: string;
  target: string;
  type: "task" | "workflow" | "sensor";
  parentId: string | null;
  cacheConfig: CacheConfig | null;
  cacheKey: string | null;
  memoKey: string | null;
  createdAt: number;
  // TODO: index by execution id?
  executions: Record<string, Execution>;
  arguments: Value[];
  requires: TagSet;
};

export type Run = {
  createdAt: number;
  parent: ExecutionReference | null;
  steps: Record<string, Step>;
};

export type LogMessageLevel = 0 | 1 | 2 | 3 | 4 | 5;

export type LogMessage = [
  string,
  number,
  LogMessageLevel,
  string | null,
  Record<string, Value>,
];

export type Agent = {
  startingAt: number;
  startedAt?: number;
  startError?: unknown;
  stoppingAt?: number;
  stoppedAt?: number;
  stopError?: unknown;
  deactivatedAt?: number;
  state: "active" | "paused" | "draining";
  connected: boolean | null;
};

// TODO: rename 'PoolDefinition'?
export type Pool = {
  modules: string[];
  provides: TagSet;
  launcher: { type: "docker"; image: string } | null;
};

export type Pools = Record<string, Pool>;

export type Session = {
  connected: boolean;
  executions: number;
  poolName: string | null;
};
