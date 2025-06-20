import { Fragment, useCallback, useState } from "react";
import { useSpaces, useProjects } from "../topics";
import { findKey } from "lodash";
import Logo from "./Logo";
import {
  IconChevronCompactRight,
  IconSettings,
  IconPlayerPauseFilled,
  IconPlayerPlayFilled,
} from "@tabler/icons-react";
import ProjectSelector from "./ProjectSelector";
import SpaceSelector from "./SpaceSelector";
import ProjectSettingsDialog from "./ProjectSettingsDialog";
import SearchInput from "./SearchInput";
import * as api from "../api";
import * as models from "../models";
import Button from "./common/Button";

type PlayPauseButtonProps = {
  projectId: string;
  spaceId: string;
  space: models.Space;
};

function PlayPauseButton({ projectId, spaceId, space }: PlayPauseButtonProps) {
  const { state } = space;
  const handleClick = useCallback(() => {
    // TODO: handle error
    if (state == "active") {
      api.pauseSpace(projectId, spaceId);
    } else if (state == "paused") {
      api.resumeSpace(projectId, spaceId);
    }
  }, [projectId, spaceId, state]);
  return state == "active" ? (
    <Button
      variant="secondary"
      size="sm"
      outline={true}
      title="Pause space"
      onClick={handleClick}
    >
      <IconPlayerPauseFilled size={16} />
    </Button>
  ) : state == "paused" ? (
    <Button size="sm" outline={true} title="Resume space" onClick={handleClick}>
      <IconPlayerPlayFilled size={16} className="animate-pulse" />
    </Button>
  ) : null;
}

type Props = {
  projectId?: string;
  activeSpaceName?: string;
};

export default function Header({ projectId, activeSpaceName }: Props) {
  const projects = useProjects();
  const spaces = useSpaces(projectId);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const handleSettingsClose = useCallback(() => setSettingsOpen(false), []);
  const handleSettingsClick = useCallback(() => setSettingsOpen(true), []);
  const activeSpaceId = findKey(
    spaces,
    (e) => e.name == activeSpaceName && e.state != "archived",
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
            {projectId && spaces && (
              <Fragment>
                <SpaceSelector
                  projectId={projectId}
                  spaces={spaces}
                  activeSpaceId={activeSpaceId}
                />
                {activeSpaceId && (
                  <PlayPauseButton
                    projectId={projectId}
                    spaceId={activeSpaceId}
                    space={spaces[activeSpaceId]}
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
            {activeSpaceId && (
              <SearchInput projectId={projectId} spaceId={activeSpaceId} />
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
