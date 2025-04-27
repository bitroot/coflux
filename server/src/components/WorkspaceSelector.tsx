import { Fragment, useCallback, useState } from "react";
import { Link, useLocation, useSearchParams } from "react-router-dom";
import {
  Menu,
  MenuButton,
  MenuItem,
  MenuItems,
  MenuSeparator,
} from "@headlessui/react";
import {
  IconCheck,
  IconChevronDown,
  IconCornerDownRight,
} from "@tabler/icons-react";

import { buildUrl } from "../utils";
import * as models from "../models";
import WorkspaceLabel from "./WorkspaceLabel";
import AddWorkspaceDialog from "./AddWorkspaceDialog";
import { times } from "lodash";

function traverseWorkspaces(
  workspaces: Record<string, models.Workspace>,
  parentId: string | null = null,
  depth: number = 0,
): [string, models.Workspace, number][] {
  return Object.entries(workspaces)
    .filter(([_, e]) => e.baseId == parentId && e.state != "archived")
    .flatMap(([workspaceId, workspace]) => [
      [workspaceId, workspace, depth],
      ...traverseWorkspaces(workspaces, workspaceId, depth + 1),
    ]);
}

type Props = {
  projectId: string;
  workspaces: Record<string, models.Workspace>;
  activeWorkspaceId: string | undefined;
};

export default function WorkspaceSelector({
  projectId,
  workspaces,
  activeWorkspaceId,
}: Props) {
  const location = useLocation();
  const [searchParams] = useSearchParams();
  const activeWorkspace = searchParams.get("workspace");
  const [addWorkspaceDialogOpen, setAddWorkspaceDialogOpen] = useState(false);
  const handleAddWorkspaceClick = useCallback(() => {
    setAddWorkspaceDialogOpen(true);
  }, []);
  const handleAddWorkspaceDialogClose = useCallback(() => {
    setAddWorkspaceDialogOpen(false);
  }, []);
  const noWorkspaces =
    Object.values(workspaces).filter((e) => e.state != "archived").length == 0;
  return (
    <Fragment>
      <Menu as="div" className="relative">
        <MenuButton className="flex items-center gap-1">
          {activeWorkspaceId ? (
            <WorkspaceLabel
              projectId={projectId}
              workspaceId={activeWorkspaceId}
              interactive={true}
              accessory={
                <IconChevronDown size={14} className="opacity-40 mt-0.5" />
              }
            />
          ) : (
            <span className="flex items-center gap-1 rounded px-2 py-0.5 text-slate-100 hover:bg-white/10 text-sm">
              Select workspace...
              <IconChevronDown size={14} className="opacity-40 mt-0.5" />
            </span>
          )}
        </MenuButton>
        <MenuItems
          transition
          anchor={{ to: "bottom start", gap: 4, padding: 20 }}
          className="bg-white flex flex-col overflow-y-scroll shadow-xl rounded-md origin-top transition duration-200 ease-out data-[closed]:scale-95 data-[closed]:opacity-0"
        >
          {Object.keys(workspaces).length > 0 && (
            <Fragment>
              <div className="p-1">
                {traverseWorkspaces(workspaces).map(
                  ([workspaceId, workspace, depth]) => (
                    <MenuItem key={workspaceId}>
                      <Link
                        to={buildUrl(location.pathname, {
                          workspace: workspace.name,
                        })}
                        className="flex items-center gap-1 pl-2 pr-3 py-1 rounded whitespace-nowrap text-sm data-[active]:bg-slate-100"
                      >
                        {workspace.name == activeWorkspace ? (
                          <IconCheck size={16} className="mt-0.5" />
                        ) : (
                          <span className="w-[16px]" />
                        )}
                        {times(depth).map((i) =>
                          i == depth - 1 ? (
                            <IconCornerDownRight
                              key={i}
                              size={16}
                              className="text-slate-300"
                            />
                          ) : (
                            <span key={i} className="w-2" />
                          ),
                        )}
                        {workspace.name}
                      </Link>
                    </MenuItem>
                  ),
                )}
              </div>
              <MenuSeparator className="my-1 h-px bg-slate-100" />
            </Fragment>
          )}
          <MenuItem>
            <button
              className="flex m-1 px-2 py-1 rounded whitespace-nowrap text-sm data-[active]:bg-slate-100"
              onClick={handleAddWorkspaceClick}
            >
              Add workspace...
            </button>
          </MenuItem>
        </MenuItems>
      </Menu>
      <AddWorkspaceDialog
        workspaces={workspaces}
        open={noWorkspaces || addWorkspaceDialogOpen}
        hideCancel={noWorkspaces}
        onClose={handleAddWorkspaceDialogClose}
      />
    </Fragment>
  );
}
