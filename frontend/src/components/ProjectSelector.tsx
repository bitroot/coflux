import { Link, useParams } from "react-router-dom";
import classNames from "classnames";
import {
  Menu,
  MenuButton,
  MenuItem,
  MenuItems,
  MenuSeparator,
} from "@headlessui/react";
import { IconCheck, IconChevronDown } from "@tabler/icons-react";

import * as models from "../models";
import Button from "./common/Button";

type Props = {
  projects: Record<string, models.Project>;
};

export default function ProjectSelector({ projects }: Props) {
  const { project: activeProjectId } = useParams();
  const activeProject =
    projects && activeProjectId ? projects[activeProjectId] : undefined;
  return (
    <Menu as="div" className="relative">
      <MenuButton
        as={Button}
        variant="secondary"
        outline={true}
        right={<IconChevronDown size={16} className="opacity-40 mt-0.5" />}
      >
        {activeProject ? activeProject.name : "Select project..."}
      </MenuButton>
      <MenuItems
        transition
        anchor={{ to: "bottom start", gap: 4, padding: 20 }}
        className="bg-white flex flex-col overflow-y-scroll shadow-xl rounded-md origin-top transition duration-200 ease-out data-closed:scale-95 data-closed:opacity-0 outline-none"
      >
        {Object.entries(projects).map(([projectId, project]) => (
          <MenuItem key={projectId}>
            <Link
              to={`/projects/${projectId}`}
              className="flex items-center gap-1 m-1 pl-2 pr-3 py-1 rounded-sm whitespace-nowrap text-sm data-active:bg-slate-100"
            >
              {projectId == activeProjectId ? (
                <IconCheck size={16} className="mt-0.5" />
              ) : (
                <span className="w-[16px]" />
              )}
              {project.name}
            </Link>
          </MenuItem>
        ))}
        <MenuSeparator className="my-1 h-px bg-slate-100" />
        <MenuItem>
          <Link
            to="/projects"
            className="flex m-1 px-2 py-1 rounded-sm whitespace-nowrap text-sm data-active:bg-slate-100"
          >
            Manage projects...
          </Link>
        </MenuItem>
      </MenuItems>
    </Menu>
  );
}
