import { Fragment, useCallback, useState } from "react";
import { useWorkspaces, useProjects } from "../topics";
import { findKey } from "lodash";
import Logo from "./Logo";
import {
  IconChevronCompactRight,
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
import Button from "./common/Button";

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
    <Button
      variant="secondary"
      size="sm"
      outline={true}
      title="Pause workspace"
      onClick={handleClick}
    >
      <IconPlayerPauseFilled size={16} />
    </Button>
  ) : state == "paused" ? (
    <Button
      size="sm"
      outline={true}
      title="Resume workspace"
      onClick={handleClick}
    >
      <IconPlayerPlayFilled size={16} className="animate-pulse" />
    </Button>
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
    <div className="flex p-4 items-center justify-between gap-5 h-14 border-b border-slate-300 bg-white">
      <div className="flex items-center gap-2">
        <Logo />
        {projects && projectId && (
          <Fragment>
            <IconChevronCompactRight
              size={16}
              className="text-slate-300 shrink-0"
            />
            <ProjectSelector projects={projects} />
            <IconChevronCompactRight
              size={16}
              className="text-slate-300 shrink-0"
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
          </Fragment>
        )}
      </div>
      <div className="flex items-center gap-1">
        {projectId && (
          <Fragment>
            {activeWorkspaceId && (
              <SearchInput
                projectId={projectId}
                workspaceId={activeWorkspaceId}
              />
            )}
            <ProjectSettingsDialog
              projectId={projectId}
              open={settingsOpen}
              onClose={handleSettingsClose}
            />
            <Button
              variant="secondary"
              outline={true}
              title="Settings"
              onClick={handleSettingsClick}
            >
              <IconSettings size={20} strokeWidth={1.5} />
            </Button>
          </Fragment>
        )}
      </div>
    </div>
  );
}
