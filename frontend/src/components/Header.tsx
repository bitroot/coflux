import { Fragment, useCallback, useState } from "react";
import { useWorkspaces, useProjects } from "../topics";
import { findKey } from "lodash";
import Logo from "./Logo";
import {
  IconChevronCompactRight,
  IconMinusVertical,
  IconSettings,
  IconPlayerPauseFilled,
  IconPlayerPlayFilled,
} from "@tabler/icons-react";
import ProjectSelector from "./ProjectSelector";
import WorkspaceSelector from "./WorkspaceSelector";
import ProjectSettingsDialog from "./ProjectSettingsDialog";
import SearchInput from "./SearchInput";
import * as api from "../api";
import * as models from "../models";

type PlayPauseButtonProps = {
  projectId: string;
  workspaceId: string;
  workspace: models.Workspace;
};

function PlayPauseButton({
  projectId,
  workspaceId,
  workspace,
}: PlayPauseButtonProps) {
  const { state } = workspace;
  const handleClick = useCallback(() => {
    // TODO: handle error
    if (state == "active") {
      api.pauseWorkspace(projectId, workspaceId);
    } else if (state == "paused") {
      api.resumeWorkspace(projectId, workspaceId);
    }
  }, [projectId, workspaceId, state]);
  return state == "active" ? (
    <button
      className="text-slate-100 bg-cyan-800/30 rounded-sm p-1 hover:bg-cyan-800/60"
      title="Pause workspace"
      onClick={handleClick}
    >
      <IconPlayerPauseFilled size={16} />
    </button>
  ) : state == "paused" ? (
    <button
      className="text-slate-100 bg-cyan-800/30 rounded-sm p-1 hover:bg-cyan-800/60"
      title="Resume workspace"
      onClick={handleClick}
    >
      <IconPlayerPlayFilled size={16} className="animate-pulse" />
    </button>
  ) : null;
}

type Props = {
  projectId?: string;
  activeWorkspaceName?: string;
};

export default function Header({ projectId, activeWorkspaceName }: Props) {
  const projects = useProjects();
  const workspaces = useWorkspaces(projectId);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const handleSettingsClose = useCallback(() => setSettingsOpen(false), []);
  const handleSettingsClick = useCallback(() => setSettingsOpen(true), []);
  const activeWorkspaceId = findKey(
    workspaces,
    (e) => e.name == activeWorkspaceName && e.state != "archived",
  );
  return (
    <div className="flex p-3 items-center justify-between gap-5 h-14">
      <div className="flex items-center gap-1">
        <Logo />
        {projects && (
          <Fragment>
            <IconChevronCompactRight
              size={16}
              className="text-white/40 shrink-0"
            />
            <div className="flex items-center gap-2">
              <ProjectSelector projects={projects} />
              <IconChevronCompactRight
                size={16}
                className="text-white/40 shrink-0"
              />
              {projectId && workspaces && (
                <Fragment>
                  <WorkspaceSelector
                    projectId={projectId}
                    workspaces={workspaces}
                    activeWorkspaceId={activeWorkspaceId}
                  />
                  {activeWorkspaceId && (
                    <PlayPauseButton
                      projectId={projectId}
                      workspaceId={activeWorkspaceId}
                      workspace={workspaces[activeWorkspaceId]}
                    />
                  )}
                </Fragment>
              )}
            </div>
          </Fragment>
        )}
      </div>
      <div className="flex items-center gap-1">
        {projectId && (
          <Fragment>
            {activeWorkspaceId && (
              <Fragment>
                <SearchInput
                  projectId={projectId}
                  workspaceId={activeWorkspaceId}
                />
                <IconMinusVertical
                  size={16}
                  className="text-white/40 shrink-0"
                />
              </Fragment>
            )}
            <ProjectSettingsDialog
              projectId={projectId}
              open={settingsOpen}
              onClose={handleSettingsClose}
            />
            <button
              className="text-slate-100 p-1 rounded-sm hover:bg-slate-100/10"
              title="Settings"
              onClick={handleSettingsClick}
            >
              <IconSettings size={24} strokeWidth={1.5} />
            </button>
          </Fragment>
        )}
      </div>
    </div>
  );
}
