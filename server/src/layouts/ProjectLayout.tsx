import { Fragment, useEffect, useState } from "react";
import {
  Outlet,
  useOutletContext,
  useParams,
  useSearchParams,
  useLocation,
  useNavigate,
} from "react-router-dom";
import { IconInfoSquareRounded } from "@tabler/icons-react";
import { findKey } from "lodash";

import TargetsList from "../components/TargetsList";
import { useTitlePart } from "../components/TitleContext";
import {
  useWorkspaces,
  usePools,
  useProjects,
  useRepositories,
  useSessions,
} from "../topics";
import Header from "../components/Header";
import AgentsList from "../components/AgentsList";
import * as api from "../api";
import { buildUrl } from "../utils";

type SidebarProps = {
  projectId: string;
  workspaceName: string;
  active: Active;
};

function Sidebar({ projectId, workspaceName, active }: SidebarProps) {
  const workspaces = useWorkspaces(projectId);
  const workspaceId = findKey(
    workspaces,
    (e) => e.name == workspaceName && e.state != "archived",
  );
  const repositories = useRepositories(projectId, workspaceId);
  const pools = usePools(projectId, workspaceId);
  const sessions = useSessions(projectId, workspaceId);
  return (
    <div className="w-[30%] max-w-[350px] min-w-[200px] bg-slate-100 text-slate-400 border-r border-slate-200 flex-none flex flex-col">
      <div className="flex-1 flex flex-col min-h-0">
        <div className="flex-1 flex flex-col min-h-0 divide-y divide-slate-200">
          <div className="flex-1 flex flex-col overflow-auto">
            {repositories ? (
              Object.keys(repositories).length ? (
                <div className="flex-1 overflow-auto min-h-0">
                  <TargetsList
                    projectId={projectId}
                    workspaceName={workspaceName}
                    activeRepository={
                      active?.[0] == "repository" || active?.[0] == "target"
                        ? active?.[1]
                        : undefined
                    }
                    activeTarget={
                      active?.[0] == "target" ? active?.[2] : undefined
                    }
                    repositories={repositories}
                  />
                </div>
              ) : (
                <div className="flex-1 flex flex-col gap-1 justify-center items-center">
                  <IconInfoSquareRounded
                    size={32}
                    strokeWidth={1.5}
                    className="text-slate-300/50"
                  />
                  <p className="text-slate-300 text-lg px-2 max-w-48 text-center leading-tight">
                    No workflows registered
                  </p>
                </div>
              )
            ) : null}
          </div>
          <div className="flex flex-col max-h-[1/3] overflow-auto">
            <AgentsList
              pools={pools}
              projectId={projectId}
              workspaceName={workspaceName}
              activePool={active?.[0] == "pool" ? active?.[1] : undefined}
              sessions={sessions}
            />
          </div>
        </div>
      </div>
    </div>
  );
}

type Active =
  | ["repository", string]
  | ["target", string, string]
  | ["pool", string]
  | undefined;

type OutletContext = {
  setActive: (active: Active) => void;
};

export default function ProjectLayout() {
  const { project: projectId } = useParams();
  const [searchParams] = useSearchParams();
  const workspaceName = searchParams.get("workspace") || undefined;
  const [active, setActive] = useState<Active>();
  const projects = useProjects();
  const project = (projectId && projects && projects[projectId]) || undefined;
  const navigate = useNavigate();
  const location = useLocation();
  useTitlePart(
    project && workspaceName && `${project.name} (${workspaceName})`,
  );
  useEffect(() => {
    if (projectId && !workspaceName) {
      // TODO: handle error
      api.getWorkspaces(projectId).then((workspaces) => {
        // TODO: better way to choose workspace
        const defaultWorkspaceName = Object.values(workspaces)[0].name;
        navigate(
          buildUrl(location.pathname, { workspace: defaultWorkspaceName }),
          { replace: true },
        );
      });
    }
  }, [navigate, location, projectId, workspaceName]);
  return (
    <Fragment>
      <Header projectId={projectId!} activeWorkspaceName={workspaceName} />
      <div className="flex-1 flex min-h-0 bg-white overflow-hidden">
        <Sidebar
          projectId={projectId!}
          workspaceName={workspaceName!}
          active={active}
        />
        <div className="flex-1 flex flex-col min-w-0">
          <Outlet context={{ setActive }} />
        </div>
      </div>
    </Fragment>
  );
}

export function useSetActive(active: Active) {
  const { setActive } = useOutletContext<OutletContext>();
  useEffect(() => {
    setActive(active);
    return () => setActive(undefined);
  }, [setActive, JSON.stringify(active)]);
}
