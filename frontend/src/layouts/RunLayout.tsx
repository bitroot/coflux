import classNames from "classnames";
import { Fragment, ReactNode, useCallback } from "react";
import {
  NavLink,
  Outlet,
  useOutletContext,
  useParams,
  useSearchParams,
} from "react-router-dom";
import { Transition } from "@headlessui/react";
import { findKey } from "lodash";

import * as models from "../models";
import * as api from "../api";
import { useSetActive } from "./ProjectLayout";
import StepDetail from "../components/StepDetail";
import usePrevious from "../hooks/usePrevious";
import { buildUrl } from "../utils";
import Loading from "../components/Loading";
import { useWorkspaces, useRun } from "../topics";
import HoverContext from "../components/HoverContext";
import { useTitlePart } from "../components/TitleContext";
import WorkflowHeader from "../components/WorkflowHeader";
import SensorHeader from "../components/SensorHeader";
import GroupDialog from "../components/GroupDialog";
import useSize from "../hooks/useSize";

type TabProps = {
  page: string | null;
  children: ReactNode;
};

function Tab({ page, children }: TabProps) {
  const { project: projectId, run: runId } = useParams();
  const [searchParams] = useSearchParams();
  return (
    <NavLink
      to={buildUrl(
        `/projects/${projectId}/runs/${runId}${page ? "/" + page : ""}`,
        Object.fromEntries(searchParams),
      )}
      end={true}
      className={({ isActive }) =>
        classNames(
          "px-2 py-2 text-sm",
          isActive && "inline-block border-b-2 border-cyan-500 font-semibold",
        )
      }
    >
      {children}
    </NavLink>
  );
}

type DetailPanelProps = {
  runId: string;
  stepId: string | undefined;
  attemptNumber: number | undefined;
  run: models.Run;
  projectId: string;
  activeWorkspaceId: string;
  className?: string;
  width: number;
  activeTab: number;
  maximised: boolean;
};

function DetailPanel({
  runId,
  stepId,
  attemptNumber,
  run,
  projectId,
  activeWorkspaceId,
  className,
  width,
  activeTab,
  maximised,
}: DetailPanelProps) {
  const previousStepId = usePrevious(stepId);
  const previousAttemptNumber = usePrevious(attemptNumber);
  const stepIdOrPrevious = stepId || previousStepId;
  const attemptNumberOrPrevious = attemptNumber || previousAttemptNumber;
  const handleRerunStep = useCallback(
    (stepId: string, workspaceName: string) => {
      return api.rerunStep(projectId, stepId, workspaceName);
    },
    [projectId],
  );
  return (
    <Transition
      as={Fragment}
      show={!!stepId}
      enter="transform transition ease-in-out duration-150"
      enterFrom="translate-x-full"
      enterTo="translate-x-0"
      leave="transform transition ease-in-out duration-300"
      leaveFrom="translate-x-0"
      leaveTo="translate-x-full"
    >
      <div
        className={classNames(className, "flex")}
        style={{ width: maximised ? undefined : width }}
      >
        <div
          className={classNames(
            "bg-slate-100 border border-slate-200 flex flex-1 max-w-full",
            maximised ? "rounded-lg" : "rounded-md",
          )}
        >
          {stepIdOrPrevious && run.steps[stepIdOrPrevious] && (
            <StepDetail
              runId={runId}
              stepId={stepIdOrPrevious}
              attempt={attemptNumberOrPrevious || 1}
              run={run}
              projectId={projectId}
              activeWorkspaceId={activeWorkspaceId}
              className="flex-1 w-full"
              onRerunStep={handleRerunStep}
              activeTab={activeTab}
              maximised={maximised}
            />
          )}
        </div>
      </div>
    </Transition>
  );
}

type OutletContext = {
  run: models.Run;
  width: number;
  height: number;
};

export default function RunLayout() {
  const { project: projectId, run: runId } = useParams();
  const [searchParams] = useSearchParams();
  const activeStepId = searchParams.get("step") || undefined;
  const activeAttemptNumber = searchParams.has("attempt")
    ? parseInt(searchParams.get("attempt")!, 10)
    : undefined;
  const activeWorkspaceName = searchParams.get("workspace") || undefined;
  const activeTab = parseInt(searchParams.get("tab") || "", 10) || 0;
  const maximised = !!searchParams.get("maximised");
  const activeGroupIdentifier = searchParams.get("group");
  const workspaces = useWorkspaces(projectId);
  const activeWorkspaceId = findKey(
    workspaces,
    (e) => e.name == activeWorkspaceName && e.state != "archived",
  );
  const run = useRun(projectId, runId, activeWorkspaceId);
  const initialStep = run && Object.values(run.steps).find((s) => !s.parentId);
  useTitlePart(initialStep && `${initialStep.target} (${initialStep.module})`);
  useSetActive(
    initialStep && ["target", initialStep.module, initialStep.target],
  );
  const [ref, size] = useSize<HTMLDivElement>();
  const detailWidth = Math.min(Math.max(window.innerWidth / 3, 400), 600);
  if (!run || !initialStep) {
    return <Loading />;
  } else {
    return (
      <HoverContext>
        <GroupDialog
          runId={runId!}
          run={run}
          identifier={activeGroupIdentifier}
          projectId={projectId!}
        />
        <div
          className="flex-1 flex flex-col relative"
          style={{ paddingRight: activeStepId && !maximised ? detailWidth : 0 }}
        >
          {initialStep.type == "sensor" ? (
            <SensorHeader
              module={initialStep.module}
              target={initialStep.target}
              projectId={projectId!}
              runId={runId}
              activeWorkspaceId={activeWorkspaceId}
              activeWorkspaceName={activeWorkspaceName}
            />
          ) : initialStep.type == "workflow" ? (
            <WorkflowHeader
              module={initialStep.module}
              target={initialStep.target}
              projectId={projectId!}
              runId={runId}
              activeWorkspaceId={activeWorkspaceId}
              activeWorkspaceName={activeWorkspaceName}
            />
          ) : null}
          <div className="grow flex flex-col">
            <div className="border-b px-5">
              {initialStep.type == "sensor" && (
                <Tab page="children">Children</Tab>
              )}
              {initialStep.type == "workflow" && <Tab page="graph">Graph</Tab>}
              {initialStep.type == "workflow" && (
                <Tab page="timeline">Timeline</Tab>
              )}
              <Tab page="logs">Logs</Tab>
              {initialStep.type == "workflow" && (
                <Tab page="assets">Assets</Tab>
              )}
            </div>
            <div className="flex-1 basis-0 overflow-auto" ref={ref}>
              <Outlet
                context={{
                  run,
                  width: size.width || 0,
                  height: size.height || 0,
                }}
              />
            </div>
          </div>
          <DetailPanel
            runId={runId!}
            stepId={activeStepId}
            attemptNumber={activeAttemptNumber}
            run={run}
            projectId={projectId!}
            activeWorkspaceId={activeWorkspaceId!}
            className={classNames(
              maximised
                ? "fixed inset-0 px-10 py-10 bg-black/40"
                : "absolute right-0 top-0 bottom-0 w-[400px] pt-2 pr-2 pb-2",
            )}
            width={detailWidth}
            activeTab={activeTab}
            maximised={maximised}
          />
        </div>
      </HoverContext>
    );
  }
}

export const useContext = useOutletContext<OutletContext>;
