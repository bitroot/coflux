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
import SpaceLabel from "./SpaceLabel";
import AddSpaceDialog from "./AddSpaceDialog";
import { times } from "lodash";

function traverseSpaces(
  spaces: Record<string, models.Space>,
  parentId: string | null = null,
  depth: number = 0,
): [string, models.Space, number][] {
  return Object.entries(spaces)
    .filter(([, e]) => e.baseId == parentId && e.state != "archived")
    .flatMap(([spaceId, space]) => [
      [spaceId, space, depth],
      ...traverseSpaces(spaces, spaceId, depth + 1),
    ]);
}

type Props = {
  projectId: string;
  spaces: Record<string, models.Space>;
  activeSpaceId: string | undefined;
};

export default function SpaceSelector({
  projectId,
  spaces,
  activeSpaceId,
}: Props) {
  const location = useLocation();
  const [searchParams] = useSearchParams();
  const activeSpace = searchParams.get("space");
  const [addSpaceDialogOpen, setAddSpaceDialogOpen] = useState(false);
  const handleAddSpaceClick = useCallback(() => {
    setAddSpaceDialogOpen(true);
  }, []);
  const handleAddSpaceDialogClose = useCallback(() => {
    setAddSpaceDialogOpen(false);
  }, []);
  const noSpaces =
    Object.values(spaces).filter((e) => e.state != "archived").length == 0;
  return (
    <Fragment>
      <Menu as="div" className="relative">
        <MenuButton className="flex items-center gap-1 outline-none">
          {activeSpaceId ? (
            <SpaceLabel
              projectId={projectId}
              spaceId={activeSpaceId}
              interactive={true}
              accessory={
                <IconChevronDown size={14} className="opacity-40 mt-0.5" />
              }
            />
          ) : (
            <span className="flex items-center gap-1 rounded-sm px-2 py-0.5 text-slate-100 hover:bg-white/10 text-sm">
              Select space...
              <IconChevronDown size={14} className="opacity-40 mt-0.5" />
            </span>
          )}
        </MenuButton>
        <MenuItems
          transition
          anchor={{ to: "bottom start", gap: 4, padding: 20 }}
          className="bg-white flex flex-col overflow-y-scroll shadow-xl rounded-md origin-top transition duration-200 ease-out data-closed:scale-95 data-closed:opacity-0 outline-none"
        >
          {Object.keys(spaces).length > 0 && (
            <Fragment>
              <div className="p-1">
                {traverseSpaces(spaces).map(([spaceId, space, depth]) => (
                  <MenuItem key={spaceId}>
                    <Link
                      to={buildUrl(location.pathname, {
                        space: space.name,
                      })}
                      className="flex items-center gap-1 pl-2 pr-3 py-1 rounded-sm whitespace-nowrap text-sm data-active:bg-slate-100"
                    >
                      {space.name == activeSpace ? (
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
                      {space.name}
                    </Link>
                  </MenuItem>
                ))}
              </div>
              <MenuSeparator className="my-1 h-px bg-slate-100" />
            </Fragment>
          )}
          <MenuItem>
            <button
              className="flex m-1 px-2 py-1 rounded-sm whitespace-nowrap text-sm data-active:bg-slate-100"
              onClick={handleAddSpaceClick}
            >
              Add space...
            </button>
          </MenuItem>
        </MenuItems>
      </Menu>
      <AddSpaceDialog
        spaces={spaces}
        open={noSpaces || addSpaceDialogOpen}
        hideCancel={noSpaces}
        onClose={handleAddSpaceDialogClose}
      />
    </Fragment>
  );
}
