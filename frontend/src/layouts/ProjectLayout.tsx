import { Fragment, useCallback, useEffect, useState } from "react";
import {
  Outlet,
  useOutletContext,
  useParams,
  useSearchParams,
  useLocation,
  useNavigate,
} from "react-router-dom";
import {
  IconInfoSquareRounded,
  IconLayoutSidebarLeftCollapse,
  IconLayoutSidebarLeftExpand,
} from "@tabler/icons-react";
import { findKey } from "lodash";

import TargetsList from "../components/TargetsList";
import { useTitlePart } from "../components/TitleContext";
import {
  useSpaces,
  usePools,
  useProjects,
  useModules,
  useSessions,
} from "../topics";
import Header from "../components/Header";
import AgentsList from "../components/AgentsList";
import * as api from "../api";
import { buildUrl } from "../utils";
import Button from "../components/common/Button";
import useLocalStorage from "../hooks/useLocalStorage";
import { Transition } from "@headlessui/react";

type SidebarProps = {
  projectId: string;
  spaceName: string;
  active: Active;
  open: boolean;
};

function Sidebar({ projectId, spaceName, active, open }: SidebarProps) {
  const spaces = useSpaces(projectId);
  const spaceId = findKey(
    spaces,
    (e) => e.name == spaceName && e.state != "archived",
  );
  const modules = useModules(projectId, spaceId);
  const pools = usePools(projectId, spaceId);
  const sessions = useSessions(projectId, spaceId);
  return (
    <Transition
      as={Fragment}
      show={open}
      enter="transform transition ease-in-out duration-150"
      enterFrom="-translate-x-full"
      enterTo="translate-x-0"
      leave="transform transition ease-in-out duration-150"
      leaveFrom="translate-x-0"
      leaveTo="-translate-x-full"
    >
      <div className="w-[25%] max-w-[300px] min-w-[200px] bg-white text-slate-400 border-r border-slate-200 flex-none flex flex-col">
        <div className="flex-1 flex flex-col min-h-0">
          <div className="flex-1 flex flex-col min-h-0 divide-y divide-slate-200">
            <div className="flex-1 flex flex-col overflow-auto">
              {modules ? (
                Object.keys(modules).length ? (
                  <div className="flex-1 overflow-auto min-h-0">
                    <TargetsList
                      projectId={projectId}
                      spaceName={spaceName}
                      activeModule={
                        active?.[0] == "module" || active?.[0] == "target"
                          ? active?.[1]
                          : undefined
                      }
                      activeTarget={
                        active?.[0] == "target" ? active?.[2] : undefined
                      }
                      modules={modules}
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
                spaceName={spaceName}
                activePool={active?.[0] == "pool" ? active?.[1] : undefined}
                sessions={sessions}
              />
            </div>
          </div>
        </div>
      </div>
    </Transition>
  );
}

type Active =
  | ["module", string]
  | ["target", string, string]
  | ["pool", string]
  | undefined;

type OutletContext = {
  setActive: (active: Active) => void;
};

export default function ProjectLayout() {
  const { project: projectId } = useParams();
  const [searchParams] = useSearchParams();
  const spaceName = searchParams.get("space") || undefined;
  const [active, setActive] = useState<Active>();
  const projects = useProjects();
  const project = (projectId && projects && projects[projectId]) || undefined;
  const navigate = useNavigate();
  const location = useLocation();
  const [sidebarOpen, setSidebarOpen] = useLocalStorage("sidebar", true);
  const handleSidebarButtonClick = useCallback(
    () => setSidebarOpen(!sidebarOpen),
    [sidebarOpen, setSidebarOpen],
  );
  useTitlePart(project && spaceName && `${project.name} (${spaceName})`);
  useEffect(() => {
    if (projectId && !spaceName) {
      // TODO: handle error
      api.getSpaces(projectId).then((spaces) => {
        // TODO: better way to choose space
        const defaultSpaceName = Object.values(spaces)[0].name;
        navigate(buildUrl(location.pathname, { space: defaultSpaceName }), {
          replace: true,
        });
      });
    }
  }, [navigate, location, projectId, spaceName]);
  return (
    <Fragment>
      <Header projectId={projectId!} activeSpaceName={spaceName} />
      <div className="flex-1 flex min-h-0 overflow-hidden">
        <Sidebar
          projectId={projectId!}
          spaceName={spaceName!}
          active={active}
          open={sidebarOpen}
        />
        <div className="flex-1 flex flex-col min-w-0 relative">
          <Outlet context={{ setActive }} />
          <Button
            className="absolute bottom-2 left-2"
            variant="secondary"
            outline={true}
            onClick={handleSidebarButtonClick}
          >
            {sidebarOpen ? (
              <IconLayoutSidebarLeftCollapse size={20} />
            ) : (
              <IconLayoutSidebarLeftExpand size={20} />
            )}
          </Button>
        </div>
      </div>
    </Fragment>
  );
}

export function useSetActive(active: Active) {
  const { setActive } = useOutletContext<OutletContext>();
  useEffect(
    () => {
      setActive(active);
      return () => setActive(undefined);
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [setActive, JSON.stringify(active)],
  );
}
