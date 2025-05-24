import ELK, { ElkNode } from "elkjs/lib/elk.bundled.js";
import { isNil, max, minBy, uniq } from "lodash";

import * as models from "./models";
import { truncatePath } from "./utils";

type BaseGroup = {
  id: string;
  name: string | null;
  steps: Record<string, number>;
  activeStepId: string;
};

export type Group = BaseGroup & {
  width: number;
  height: number;
  x: number;
  y: number;
};

type BaseNode = (
  | {
      type: "step";
      step: models.Step;
      stepId: string;
      attempt: number;
    }
  | {
      type: "parent";
      parent: models.ExecutionReference | null;
    }
  | {
      type: "child";
      child: models.ExecutionReference;
    }
  | {
      type: "asset";
      stepId: string;
      assetId: string;
      asset: models.Asset;
    }
  | {
      type: "assets";
      stepId: string;
      assetIds: string[];
    }
) & {
  width: number;
  height: number;
  groupId: string | null;
};

export type Node = BaseNode & {
  x: number;
  y: number;
};

type BaseEdge = {
  from: string;
  to: string;
  type: "dependency" | "child" | "parent" | "asset";
};

export type Edge = BaseEdge & {
  path: { x: number; y: number }[];
};

export type Graph = {
  width: number;
  height: number;
  nodes: Record<string, Node>;
  edges: Record<string, Edge>;
  groups: Record<string, Group>;
};

function findParentStep(
  run: models.Run,
  stepId: string,
): [string, number] | undefined {
  const parentStepId = Object.keys(run.steps).find((sId) =>
    Object.values(run.steps[sId].executions).some((e) =>
      e.children.some((c) => c.stepId == stepId),
    ),
  );
  const attemptStr =
    parentStepId &&
    Object.keys(run.steps[parentStepId].executions).find((attempt) =>
      run.steps[parentStepId].executions[attempt].children.some(
        (c) => c.stepId == stepId,
      ),
    );
  if (parentStepId && attemptStr) {
    return [parentStepId, parseInt(attemptStr, 10)];
  } else {
    return undefined;
  }
}

function resolvePath(
  run: models.Run,
  activeStepId: string | undefined,
  activeAttempt: number | undefined,
): [Record<string, Record<string, string>>, Record<string, number>] {
  const groupSteps: Record<string, Record<string, string>> = {};
  const stepAttempts: Record<string, number> = {};
  if (activeStepId) {
    if (activeAttempt) {
      stepAttempts[activeStepId] = activeAttempt;
    }
    const process = (stepId: string) => {
      const parent = findParentStep(run, stepId);
      if (parent) {
        const [parentStepId, parentAttempt] = parent;
        stepAttempts[parentStepId] = parentAttempt;
        const child = run.steps[parentStepId].executions[
          parentAttempt
        ].children.find((c) => c.stepId == stepId);
        if (child && !isNil(child.groupId)) {
          if (!groupSteps[parentStepId]) {
            groupSteps[parentStepId] = {};
          }
          groupSteps[parentStepId][child.groupId] = stepId;
        }
        process(parentStepId);
      }
    };
    process(activeStepId);
  }
  return [groupSteps, stepAttempts];
}

function getStepAttempt(
  run: models.Run,
  stepAttempts: Record<string, number | undefined>,
  stepId: string,
) {
  const step = run.steps[stepId];
  return (
    stepAttempts[stepId] ||
    max(Object.keys(step.executions).map((a) => parseInt(a, 10)))
  );
}

function traverseRun(
  run: models.Run,
  groupSteps: Record<string, Record<string, string>>,
  stepAttempts: Record<string, number | undefined>,
  stepId: string,
  callback: (
    stepId: string,
    attempt: number,
    children: models.Child[],
    group: BaseGroup | null,
  ) => void,
  group: BaseGroup | null = null,
  seen: Record<string, true> = {},
) {
  const attempt = getStepAttempt(run, stepAttempts, stepId);
  if (attempt) {
    const execution = run.steps[stepId].executions[attempt];
    if (execution) {
      const children: models.Child[] = [];
      Object.keys(execution.groups).forEach((groupIdStr) => {
        const groupId = parseInt(groupIdStr, 10);
        const groupStepId = groupSteps[stepId]?.[groupIdStr];
        const child = execution.children.find(
          (c) =>
            c.groupId === groupId && (!groupStepId || c.stepId == groupStepId),
        );
        if (child) {
          children.push(child);
        }
      });
      execution.children
        .filter((c) => isNil(c.groupId))
        .forEach((child) => {
          children.push(child);
        });

      callback(stepId, attempt, children, group);
      seen[stepId] = true;

      children.forEach((child) => {
        if (!(child.stepId in seen)) {
          const childGroup = !isNil(child.groupId)
            ? {
                id: `${stepId}-${child.groupId}`,
                name: execution.groups[child.groupId],
                steps: execution.children
                  .filter((c) => c.groupId === child.groupId)
                  .reduce(
                    (acc, c) => ({
                      ...acc,
                      [c.stepId]: max(
                        Object.keys(run.steps[c.stepId].executions).map((a) =>
                          parseInt(a, 10),
                        ),
                      ),
                    }),
                    {},
                  ),
                activeStepId: child.stepId,
              }
            : group;
          traverseRun(
            run,
            groupSteps,
            stepAttempts,
            child.stepId,
            callback,
            childGroup,
            seen,
          );
        }
      });
    }
  }
}

let canvas: HTMLCanvasElement | undefined;

function getTextWidth(text: string, font = "14px system-ui") {
  canvas = canvas || document.createElement("canvas");
  const context = canvas.getContext("2d")!;
  context.font = font;
  return context.measureText(text).width;
}

function truncateList<T>(array: T[], limit: number): [T[], T[]] {
  if (array.length <= limit) {
    return [array, []];
  } else {
    const adjustedLimit = Math.max(0, limit - 1);
    return [array.slice(0, adjustedLimit), array.slice(adjustedLimit)];
  }
}

function buildChildren(
  nodes: Record<string, Omit<Node, "x" | "y">>,
): ElkNode[] {
  const groupIds = uniq(
    Object.values(nodes)
      .map((n) => n.groupId)
      .filter((id) => id),
  );

  return [
    ...Object.entries(nodes)
      .filter(([, node]) => !node.groupId)
      .map(([id, { width, height }]) => ({ id, width, height })),
    ...groupIds.map((groupId) => ({
      id: groupId!,
      layoutOptions: {
        "elk.padding": "[left=15, top=40, right=15, bottom=15]",
        "elk.layered.spacing.nodeNodeBetweenLayers": "40",
      },
      children: Object.entries(nodes)
        .filter(([, node]) => node.groupId == groupId)
        .map(([id, { width, height }]) => ({ id, width, height })),
    })),
  ];
}

function extractNodes(
  graph: ElkNode,
  nodes: Record<string, Omit<Node, "x" | "y">>,
): Record<string, Node> {
  return (graph.children || []).reduce((result, child) => {
    const node = { ...nodes[child.id], x: child.x!, y: child.y! };
    return {
      ...result,
      ...extractNodes(child, nodes),
      [child.id]: node,
    };
  }, {});
}

function extractEdges(
  graph: ElkNode,
  edges: Record<string, Omit<Edge, "path">>,
) {
  return (graph.edges || []).reduce((result, edge) => {
    if (edge.sections) {
      // TODO: support multiple sections?
      const { startPoint, bendPoints, endPoint } = edge.sections[0];
      const path = [startPoint, ...(bendPoints || []), endPoint];
      const edge_ = { ...edges[edge.id], path };
      return { ...result, [edge.id]: edge_ };
    } else {
      // TODO: ?
      return result;
    }
  }, {});
}

function extractGroups(graph: ElkNode, groups: Record<string, BaseGroup>) {
  return Object.entries(groups).reduce((acc, [id, group]) => {
    const child = graph.children?.find((c) => c.id == id);
    if (child) {
      return {
        ...acc,
        [id]: {
          ...group,
          x: child.x,
          y: child.y,
          width: child.width,
          height: child.height,
        },
      };
    } else {
      return acc;
    }
  }, {});
}

export default function buildGraph(
  run: models.Run,
  runId: string,
  activeStepId: string | undefined,
  activeAttempt: number | undefined,
): Promise<Graph> {
  const [groupSteps, stepAttempts] = resolvePath(
    run,
    activeStepId,
    activeAttempt,
  );

  const initialStepId = minBy(
    Object.keys(run.steps).filter((id) => !run.steps[id].parentId),
    (stepId) => run.steps[stepId].createdAt,
  )!;

  const nodes: Record<string, BaseNode> = {};
  const edges: Record<string, BaseEdge> = {};
  const groups: Record<string, BaseGroup> = {};

  nodes[run.parent?.runId || "start"] = {
    type: "parent",
    parent: run.parent || null,
    groupId: null,
    width: run.parent ? 100 : 30,
    height: 30,
  };
  edges["start"] = {
    from: initialStepId,
    to: run.parent?.runId || "start",
    type: "parent",
  };

  traverseRun(
    run,
    groupSteps,
    stepAttempts,
    initialStepId,
    (
      stepId: string,
      attempt: number,
      children: models.Child[],
      group: BaseGroup | null,
    ) => {
      if (group && !(group.id in groups)) {
        groups[group.id] = group;
      }
      const step = run.steps[stepId];
      nodes[stepId] = {
        type: "step",
        step,
        stepId,
        attempt,
        groupId: group?.id || null,
        width: 160,
        height: 50,
      };
      const execution = step.executions[attempt];
      const [assets, rest] = truncateList(Object.entries(execution.assets), 3);
      assets.forEach(([assetId, asset]) => {
        const text = truncatePath(asset.path) + (asset.type == 1 ? "/" : "");
        nodes[`asset:${assetId}`] = {
          type: "asset",
          stepId,
          assetId,
          asset,
          groupId: group?.id || null,
          width: Math.min(getTextWidth(text) + 32, 140),
          height: 20,
        };
        edges[`${stepId}-asset:${assetId}`] = {
          from: stepId,
          to: `asset:${assetId}`,
          type: "asset",
        };
      });
      if (rest.length) {
        const nodeId = `assets:${rest.map(([id]) => id).join(",")}`;
        const text = `(+${rest.length} more)`;
        nodes[nodeId] = {
          type: "assets",
          stepId,
          assetIds: rest.map(([id]) => id),
          groupId: group?.id || null,
          width: Math.min(getTextWidth(text) + 14, 100),
          height: 20,
        };
        edges[`${stepId}-${nodeId}`] = {
          from: stepId,
          to: nodeId,
          type: "asset",
        };
      }

      Object.entries(execution.dependencies).forEach(([, dependency]) => {
        if (dependency.execution.runId == runId) {
          edges[`${dependency.execution.stepId}-${stepId}`] = {
            from: dependency.execution.stepId,
            to: stepId,
            type: "dependency",
          };
        } else {
          // TODO: ?
        }
      });
      children.forEach((child) => {
        const childExecution =
          run.steps[child.stepId].executions[child.attempt];
        if (childExecution) {
          if (
            !Object.values(execution.dependencies).some(
              (d) => d.execution.stepId == child.stepId,
            )
          ) {
            edges[`${child.stepId}-${stepId}`] = {
              from: child.stepId,
              to: stepId,
              type: "child",
            };
          }
        } else {
          // TODO
        }
      });
      const result = execution?.result;
      if (
        result?.type == "deferred" ||
        result?.type == "cached" ||
        result?.type == "spawned"
      ) {
        if (result.execution.runId != runId) {
          if (result.type == "spawned") {
            const childId = `${result.execution.runId}/${result.execution.stepId}`;
            nodes[childId] = {
              type: "child",
              child: result.execution,
              groupId: group?.id || null,
              width: 100,
              height: 30,
            };
            edges[`${childId}-${stepId}`] = {
              from: childId,
              to: stepId,
              type: "dependency",
            };
          }
        } else {
          const childStepId = result.execution.stepId;
          edges[`${childStepId}-${stepId}`] = {
            from: childStepId,
            to: stepId,
            type: "dependency",
          };
        }
      }
    },
  );

  return new ELK()
    .layout({
      id: "root",
      layoutOptions: {
        "elk.layered.spacing.nodeNodeBetweenLayers": "40",
        "elk.layered.considerModelOrder.strategy": "PREFER_NODES",
        "elk.layered.considerModelOrder.crossingCounterNodeInfluence": "1",
        "elk.layered.nodePlacement.strategy": "NETWORK_SIMPLEX",
        "elk.hierarchyHandling": "INCLUDE_CHILDREN",
        "elk.json.shapeCoords": "ROOT",
        "elk.json.edgeCoords": "ROOT",
      },
      children: buildChildren(nodes),
      edges: Object.entries(edges)
        .filter(([, { from, to }]) => from in nodes && to in nodes) // TODO: remove?
        .map(([id, { from, to }]) => {
          return {
            id,
            sources: [from],
            targets: [to],
          };
        }),
    })
    .then((graph) => {
      return {
        nodes: extractNodes(graph, nodes),
        edges: extractEdges(graph, edges),
        groups: extractGroups(graph, groups),
        width: graph.width!,
        height: graph.height!,
      };
    });
}
