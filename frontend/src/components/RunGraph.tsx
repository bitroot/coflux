import {
  useCallback,
  useState,
  PointerEvent as ReactPointerEvent,
  WheelEvent as ReactWheelEvent,
  useRef,
  useEffect,
} from "react";
import classNames from "classnames";
import {
  IconArrowUpLeft,
  IconBolt,
  IconClock,
  IconArrowDownRight,
  IconZzz,
  IconArrowBounce,
  IconPin,
  IconAlertCircle,
  IconStackPop,
  IconStackPush,
  IconSelector,
} from "@tabler/icons-react";

import * as models from "../models";
import StepLink from "./StepLink";
import { useHoverContext } from "./HoverContext";
import {
  buildGraph,
  Graph,
  Edge,
  getExecutionStatus,
  getBranchStatus,
} from "../graph";
import SpaceLabel from "./SpaceLabel";
import AssetIcon from "./AssetIcon";
import { buildUrl } from "../utils";
import AssetLink from "./AssetLink";
import { countBy, isEqual, max } from "lodash";
import { Link, useLocation, useSearchParams } from "react-router-dom";
import { getAssetName } from "../assets";

function resolveExecutionResult(
  run: models.Run,
  stepId: string,
  attempt: number,
): models.Value | undefined {
  const result = run.steps[stepId].executions[attempt]?.result;
  switch (result?.type) {
    case "value":
      return result.value;
    case "error":
    case "abandoned":
      return result.retry
        ? resolveExecutionResult(run, stepId, result.retry)
        : undefined;
    case "suspended":
      return resolveExecutionResult(run, stepId, result.successor);
    case "deferred":
    case "cached":
    case "spawned":
      return result.result?.type == "value" ? result.result.value : undefined;
    default:
      return undefined;
  }
}

function isStepStale(
  stepId: string,
  attempt: number,
  run: models.Run,
  activeStepId: string | undefined,
  activeAttempt: number | undefined,
): boolean {
  const execution = run.steps[stepId]?.executions[attempt];
  if (execution) {
    return Object.values(execution.children).some((child) => {
      const childStep = run.steps[child.stepId];
      const initialResult = resolveExecutionResult(
        run,
        child.stepId,
        child.attempt,
      );
      const latestAttempt =
        (child.stepId == activeStepId && activeAttempt) ||
        Math.max(
          ...Object.keys(childStep.executions).map((a) => parseInt(a, 10)),
        );
      const latestResult = resolveExecutionResult(
        run,
        child.stepId,
        latestAttempt,
      );
      return (
        (initialResult &&
          latestResult &&
          !isEqual(initialResult, latestResult)) ||
        isStepStale(
          child.stepId,
          latestAttempt,
          run,
          activeStepId,
          activeAttempt,
        )
      );
    });
  } else {
    return false;
  }
}

const stepNodeStatusClassNames: Record<
  ReturnType<typeof getExecutionStatus>,
  string
> = {
  deferred: "border-slate-200 bg-white",
  assigning: "border-blue-200 bg-blue-50",
  running: "border-blue-400 bg-blue-100",
  errored: "border-red-400 bg-red-100",
  aborted: "border-yellow-400 bg-yellow-100",
  suspended: "border-slate-200 bg-white",
  completed: "border-slate-400 bg-white",
};

type StepNodeProps = {
  projectId: string;
  stepId: string;
  step: models.Step;
  attempt: number;
  runId: string;
  isActive: boolean;
  isStale: boolean;
  runSpaceId: string;
};

function StepNode({
  projectId,
  stepId,
  step,
  attempt,
  runId,
  isActive,
  isStale,
  runSpaceId,
}: StepNodeProps) {
  const execution = step.executions[attempt];
  const { isHovered } = useHoverContext();
  const isDeferred =
    execution?.result?.type == "cached" ||
    execution?.result?.type == "deferred";
  const status = getExecutionStatus(execution);
  return (
    <div className="relative h-[50px]">
      {Object.keys(step.executions).length > 1 && (
        <div
          className={classNames(
            "absolute w-full h-full border border-slate-300 bg-white rounded-sm ring-offset-2",
            isActive || isHovered({ stepId, attempt })
              ? "-top-2 -right-2"
              : "-top-1 -right-1",
            isHovered({ stepId }) &&
              !isHovered({ stepId, attempt }) &&
              "ring-2 ring-slate-400",
          )}
        ></div>
      )}
      <StepLink
        runId={runId}
        stepId={stepId}
        attempt={attempt}
        className={classNames(
          "absolute w-full h-full flex-1 flex gap-2 items-center border rounded-sm px-2 py-1 ring-offset-2",
          execution && stepNodeStatusClassNames[status],
          isStale && "border-opacity-40",
        )}
        activeClassName="ring-3 ring-cyan-400"
        hoveredClassName="ring-3 ring-slate-400"
      >
        <span
          className={classNames(
            "flex-1 flex items-center overflow-hidden",
            isStale && "opacity-30",
          )}
        >
          <span className="flex-1 flex flex-col gap-0.5 overflow-hidden">
            <span
              className={classNames(
                "truncate text-xs",
                isDeferred ? "text-slate-300" : "text-slate-400",
              )}
            >
              {step.module} /
            </span>
            <span className="flex gap-1 items-center">
              <span
                className={classNames(
                  "flex-1 truncate text-sm font-mono",
                  !step.parentId && "font-bold",
                  isDeferred && "text-slate-500",
                )}
              >
                {step.target}
              </span>
              {execution && execution.spaceId != runSpaceId && (
                <SpaceLabel
                  projectId={projectId}
                  spaceId={execution.spaceId}
                  size="sm"
                  warning="This execution ran in a different space"
                  compact
                />
              )}
            </span>
          </span>
        </span>
        {isStale ? (
          <span title="Stale">
            <IconAlertCircle size={16} className="text-slate-500" />
          </span>
        ) : execution && !execution.result && !execution.assignedAt ? (
          <span title="Assigning...">
            <IconClock size={16} />
          </span>
        ) : execution?.result?.type == "cached" ? (
          <span title="Read from cache">
            <IconStackPop size={16} className="text-slate-300" />
          </span>
        ) : step.memoKey ? (
          <span title="Memoised">
            <IconPin size={16} className="text-slate-500" />
          </span>
        ) : step.cacheConfig ? (
          <span title="Written to cache">
            <IconStackPush size={16} className="text-slate-400" />
          </span>
        ) : execution?.result?.type == "suspended" ? (
          <span title="Suspended">
            <IconZzz size={16} className="text-slate-500" />
          </span>
        ) : execution?.result?.type == "deferred" ? (
          <span title="Deferred">
            <IconArrowBounce size={16} className="text-slate-400" />
          </span>
        ) : null}
      </StepLink>
    </div>
  );
}

type AssetNodeProps = {
  assetId: string;
  asset: models.AssetSummary;
};

function AssetNode({ assetId, asset }: AssetNodeProps) {
  return (
    <AssetLink
      assetId={assetId}
      asset={asset}
      className="h-full w-full flex gap-0.5 px-1.5 items-center bg-slate-50 rounded-full text-slate-700 text-sm ring-slate-400"
      hoveredClassName="ring-2"
    >
      <AssetIcon
        asset={asset}
        size={16}
        strokeWidth={1.5}
        className="shrink-0"
      />
      <span className="text-ellipsis overflow-hidden whitespace-nowrap">
        {asset.name || (
          <span className="italic text-slate-800">{getAssetName(asset)}</span>
        )}
      </span>
    </AssetLink>
  );
}

type MoreAssetsNodeProps = {
  assetIds: string[];
};

function MoreAssetsNode({ assetIds }: MoreAssetsNodeProps) {
  const { isHovered } = useHoverContext();

  return (
    <span
      className={classNames(
        "h-full w-full px-1.5 items-center bg-slate-50 rounded-full text-slate-400 text-sm ring-slate-400 text-ellipsis overflow-hidden whitespace-nowrap",
        assetIds.some((assetId) => isHovered({ assetId })) && "ring-2",
      )}
    >
      (+{assetIds.length} more)
    </span>
  );
}

type ParentNodeProps = {
  parent: models.ExecutionReference | null;
};

function ParentNode({ parent }: ParentNodeProps) {
  if (parent) {
    return (
      <StepLink
        runId={parent.runId}
        stepId={parent.stepId}
        attempt={parent.attempt}
        className="flex-1 w-full h-full flex items-center px-2 py-1 border border-slate-300 rounded-full bg-white ring-offset-2"
        hoveredClassName="ring-3 ring-slate-400"
      >
        <span className="text-slate-500 font-bold flex-1">{parent.runId}</span>
        <IconArrowDownRight size={20} className="text-slate-400" />
      </StepLink>
    );
  } else {
    return (
      <div
        className="flex-1 w-full h-full flex items-center justify-center border border-slate-300 rounded-full bg-white"
        title="Manual initialisation"
      >
        <IconBolt className="text-slate-500" size={20} />
      </div>
    );
  }
}

type ChildNodeProps = {
  child: models.ExecutionReference;
};

function ChildNode({ child }: ChildNodeProps) {
  return (
    <StepLink
      runId={child.runId}
      stepId={child.stepId}
      attempt={child.attempt}
      className="flex-1 w-full h-full flex items-center px-2 py-1 border border-slate-300 rounded-full bg-white ring-offset-2"
      hoveredClassName="ring-3 ring-slate-400"
    >
      <IconArrowUpLeft size={20} className="text-slate-400" />
      <span className="text-slate-500 font-bold flex-1 text-end">
        {child.runId}
      </span>
    </StepLink>
  );
}

const groupHeaderStatusClassNames = {
  deferred: "bg-slate-100 text-slate-500",
  assigning: "bg-blue-100 text-slate-500",
  running: "bg-blue-200 text-blue-800",
  errored: "bg-red-200 text-red-800",
  aborted: "bg-yellow-200 text-yellow-800",
  suspended: "bg-slate-100 text-slate-500",
  completed: "bg-green-200 text-green-800",
};

type GroupHeaderProps = {
  identifier: string;
  run: models.Run;
};

function GroupHeader({ identifier, run }: GroupHeaderProps) {
  const [searchParams] = useSearchParams();
  const { pathname } = useLocation();
  const parts = identifier.split("-");
  const stepId = parts[0];
  const attempt = parseInt(parts[1], 10);
  const groupId = parseInt(parts[2], 10);
  const execution = run.steps[stepId]?.executions[attempt];
  const groupName = execution.groups[groupId];
  const statuses = execution.children
    .filter((c) => c.groupId == groupId)
    .map((c) => getBranchStatus(run, c.stepId));
  const counts = countBy(statuses);
  return (
    <div className="flex items-center gap-2">
      <div className="flex min-w-0 overflow-hidden mr-auto">
        {groupName ? (
          <span
            className="block text-slate-600 text-sm overflow-hidden whitespace-nowrap text-ellipsis pointer-events-auto px-1 rounded-sm"
            title={groupName}
          >
            {groupName}
          </span>
        ) : null}
      </div>
      <Link
        to={buildUrl(pathname, {
          ...Object.fromEntries(searchParams),
          group: identifier,
        })}
        className="flex items-center gap-0.5 rounded-md p-0.5 bg-white border border-slate-200 hover:border-slate-400 pointer-events-auto"
      >
        {(
          [
            "assigning",
            "running",
            "completed",
            "deferred",
            "suspended",
            "aborted",
            "errored",
          ] as ReturnType<typeof getExecutionStatus>[]
        ).map(
          (status) =>
            !!counts[status] && (
              <span
                key={status}
                className={classNames(
                  "px-1 rounded text-sm",
                  groupHeaderStatusClassNames[status],
                )}
                title={`${counts[status]} ${status}`}
              >
                ×{counts[status]}
              </span>
            ),
        )}
        <IconSelector size={16} className="text-slate-500" />
      </Link>
    </div>
  );
}

function buildEdgePath(edge: Edge, offset: [number, number]): string {
  const formatPoint = ({ x, y }: { x: number; y: number }) =>
    `${x + offset[0]},${y + offset[1]}`;

  return [
    `M ${formatPoint(edge.path[0])}`,
    ...edge.path.slice(1).map((p) => `L ${formatPoint(p)}`),
  ].join(" ");
}

type EdgePathProps = {
  edge: Edge;
  offset: [number, number];
  highlight?: boolean;
};

function EdgePath({ edge, offset, highlight }: EdgePathProps) {
  return (
    <path
      className={highlight ? "stroke-slate-400" : "stroke-slate-300"}
      fill="none"
      strokeWidth={
        edge.type == "asset"
          ? 1.5
          : highlight || edge.type == "dependency"
            ? 3
            : 2
      }
      strokeDasharray={
        edge.type == "asset" ? "2" : edge.type != "dependency" ? "5" : undefined
      }
      d={buildEdgePath(edge, offset)}
    />
  );
}

function calculateMargins(
  containerWidth: number,
  containerHeight: number,
  graphWidth: number,
  graphHeight: number,
) {
  const aspect =
    containerWidth && containerHeight ? containerWidth / containerHeight : 1;
  const marginX =
    Math.max(
      100,
      containerWidth - graphWidth,
      (graphHeight + 100) * aspect - graphWidth,
    ) / 2;
  const marginY =
    Math.max(
      100,
      containerHeight - graphHeight,
      (graphWidth + 100) / aspect - graphHeight,
    ) / 2;
  return [marginX, marginY];
}

function useGraph(
  run: models.Run,
  runId: string,
  activeStepId: string | undefined,
  activeAttempt: number | undefined,
) {
  const [graph, setGraph] = useState<Graph>();
  useEffect(() => {
    buildGraph(run, runId, activeStepId, activeAttempt)
      .then(setGraph)
      .catch((e) => {
        setGraph(undefined);
        // TODO: handle error
        console.error(e);
      });
  }, [run, runId, activeStepId, activeAttempt]);
  // return graph?.runId == runId ? graph : undefined;
  return graph;
}

type Props = {
  projectId: string;
  runId: string;
  run: models.Run;
  width: number;
  height: number;
  activeStepId: string | undefined;
  activeAttempt: number | undefined;
  runSpaceId: string;
};

export default function RunGraph({
  projectId,
  runId,
  run,
  width: containerWidth,
  height: containerHeight,
  activeStepId,
  activeAttempt,
  runSpaceId,
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [offsetOverride, setOffsetOverride] = useState<[number, number]>();
  const [dragging, setDragging] = useState<[number, number]>();
  const [zoomOverride, setZoomOverride] = useState<number>();
  const { isHovered } = useHoverContext();
  const graph = useGraph(run, runId, activeStepId, activeAttempt);
  const graphWidth = graph?.width || 0;
  const graphHeight = graph?.height || 0;
  const [marginX, marginY] = calculateMargins(
    containerWidth,
    containerHeight,
    graphWidth,
    graphHeight,
  );
  const canvasWidth = Math.max(graphWidth + 2 * marginX, containerWidth);
  const canvasHeight = Math.max(graphHeight + 2 * marginY, containerHeight);
  const minZoom = Math.min(
    containerWidth / canvasWidth,
    containerHeight / canvasHeight,
  );
  const zoom = zoomOverride || Math.max(minZoom, 0.6);
  const maxDragX = -(canvasWidth * zoom - containerWidth);
  const maxDragY = -(canvasHeight * zoom - containerHeight);
  const [offsetX, offsetY] = offsetOverride || [maxDragX / 2, maxDragY / 2];
  const handlePointerDown = useCallback(
    (ev: ReactPointerEvent) => {
      const dragStart = [ev.screenX, ev.screenY];
      let dragging: [number, number] = [offsetX, offsetY];
      const handleMove = (ev: PointerEvent) => {
        dragging = [
          Math.min(0, Math.max(maxDragX, offsetX - dragStart[0] + ev.screenX)),
          Math.min(0, Math.max(maxDragY, offsetY - dragStart[1] + ev.screenY)),
        ];
        setDragging(dragging);
      };
      const handleUp = () => {
        setDragging(undefined);
        setOffsetOverride(dragging);
        window.removeEventListener("pointermove", handleMove);
        window.removeEventListener("pointerup", handleUp);
      };
      window.addEventListener("pointermove", handleMove);
      window.addEventListener("pointerup", handleUp);
    },
    [offsetX, offsetY, maxDragX, maxDragY],
  );
  const handleWheel = useCallback(
    (ev: ReactWheelEvent<HTMLDivElement>) => {
      const pointerX = ev.clientX - containerRef.current!.offsetLeft;
      const pointerY = ev.clientY - containerRef.current!.offsetTop;
      const canvasX = (pointerX - offsetX) / zoom;
      const canvasY = (pointerY - offsetY) / zoom;
      const newZoom = Math.max(
        minZoom,
        Math.min(1.5, zoom * (1 + ev.deltaY / -500)),
      );
      const delta = newZoom - zoom;
      setZoomOverride(newZoom);
      setOffsetOverride([
        Math.min(0, Math.max(maxDragX, offsetX - canvasX * delta)),
        Math.min(0, Math.max(maxDragY, offsetY - canvasY * delta)),
      ]);
    },
    [offsetX, offsetY, zoom, minZoom, maxDragX, maxDragY],
  );
  const [dx, dy] = dragging || [offsetX, offsetY];
  return (
    <div
      className="relative w-full h-full overflow-hidden"
      ref={containerRef}
      onWheel={handleWheel}
    >
      <div
        style={{
          transformOrigin: "0 0",
          transform: `translate(${dx}px, ${dy}px) scale(${zoom})`,
        }}
        className="relative will-change-transform"
      >
        <svg
          width={canvasWidth}
          height={canvasHeight}
          className={classNames(
            "absolute bg-slate-50/50",
            dragging
              ? "cursor-grabbing"
              : zoom > minZoom
                ? "cursor-grab"
                : undefined,
          )}
          onPointerDown={handlePointerDown}
        >
          <defs>
            <pattern
              id="grid"
              width={20}
              height={20}
              patternUnits="userSpaceOnUse"
            >
              <circle cx={10} cy={10} r={1} className="fill-slate-400/30" />
            </pattern>
          </defs>
          {graph &&
            Object.entries(graph.groups).map(([id, group]) => (
              <rect
                key={id}
                width={group.width}
                height={group.height - 12}
                x={group.x + marginX}
                y={group.y + marginY + 12}
                rx={5}
                className="fill-slate-100"
              />
            ))}
          <rect width="100%" height="100%" fill="url(#grid)" />
          {graph &&
            Object.entries(graph.edges).flatMap(([edgeId, edge]) => {
              const from = graph.nodes[edge.from];
              const to = graph.nodes[edge.to];
              const highlight =
                (from.type == "parent" &&
                  from.parent &&
                  isHovered({ runId: from.parent.runId })) ||
                (from.type == "child" &&
                  isHovered({ runId: from.child.runId })) ||
                (to.type == "child" && isHovered({ runId: to.child.runId })) ||
                (from.type == "step" && isHovered({ stepId: from.stepId })) ||
                (to.type == "step" && isHovered({ stepId: to.stepId })) ||
                (to.type == "asset" && isHovered({ assetId: to.assetId })) ||
                (to.type == "assets" &&
                  to.assetIds.some((assetId) => isHovered({ assetId })));
              return (
                <EdgePath
                  key={edgeId}
                  offset={[marginX, marginY]}
                  edge={edge}
                  highlight={highlight}
                />
              );
            })}
        </svg>
        <div className="absolute">
          {graph &&
            Object.entries(graph.groups).map(([id, group]) => (
              <div
                key={id}
                className="absolute flex flex-col px-3 pointer-events-none"
                style={{
                  left: group.x + marginX,
                  top: group.y + marginY,
                  width: group.width,
                  height: group.height,
                }}
              >
                <GroupHeader identifier={group.identifier} run={run} />
              </div>
            ))}
          {graph &&
            Object.entries(graph.nodes).map(([nodeId, node]) => {
              return (
                <div
                  key={nodeId}
                  className="absolute flex"
                  style={{
                    left: node.x + marginX,
                    top: node.y + marginY,
                    width: node.width,
                    height: node.height,
                  }}
                >
                  {node.type == "parent" ? (
                    <ParentNode parent={node.parent} />
                  ) : node.type == "step" ? (
                    <div className="flex-1 flex flex-col justify-center gap-2">
                      <StepNode
                        projectId={projectId}
                        stepId={node.stepId}
                        step={run.steps[node.stepId]}
                        attempt={node.attempt}
                        runId={runId}
                        isActive={node.stepId == activeStepId}
                        isStale={isStepStale(
                          node.stepId,
                          node.attempt,
                          run,
                          activeStepId,
                          activeAttempt,
                        )}
                        runSpaceId={runSpaceId}
                      />
                    </div>
                  ) : node.type == "asset" ? (
                    <AssetNode
                      assetId={node.assetId}
                      asset={
                        run.steps[node.stepId].executions[node.attempt].assets[
                          node.assetId
                        ]
                      }
                    />
                  ) : node.type == "assets" ? (
                    <MoreAssetsNode assetIds={node.assetIds} />
                  ) : node.type == "child" ? (
                    <ChildNode child={node.child} />
                  ) : undefined}
                </div>
              );
            })}
        </div>
      </div>
      <div className="absolute flex right-1 bottom-1 bg-white/90 rounded-xl px-2 py-1 mb-1">
        <input
          type="range"
          value={zoom}
          min={Math.floor(minZoom * 100) / 100}
          max={1.5}
          step={0.01}
          className="w-24 accent-cyan-500 cursor-pointer"
          onChange={(ev) => setZoomOverride(parseFloat(ev.target.value))}
        />
      </div>
    </div>
  );
}
