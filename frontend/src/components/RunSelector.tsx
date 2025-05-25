import { Fragment } from "react";
import { Menu, MenuButton, MenuItem, MenuItems } from "@headlessui/react";
import { sortBy } from "lodash";
import classNames from "classnames";
import { Link, useLocation } from "react-router-dom";
import { DateTime } from "luxon";
import {
  IconChevronDown,
  IconChevronLeft,
  IconChevronRight,
} from "@tabler/icons-react";

import * as models from "../models";
import { buildUrl } from "../utils";

function getRunUrl(
  projectId: string,
  runId: string,
  workspaceName: string | undefined,
  pathname: string,
) {
  // TODO: better way to determine page
  const parts = pathname.split("/");
  const page = parts.length == 6 ? parts[5] : undefined;
  return buildUrl(
    `/projects/${projectId}/runs/${runId}${page ? "/" + page : ""}`,
    { workspace: workspaceName },
  );
}

type OptionsProps = {
  runs: Record<string, Pick<models.Run, "createdAt">> | undefined;
  projectId: string | null;
  activeWorkspaceName: string | undefined;
  selectedRunId: string;
};

function Options({
  runs,
  projectId,
  activeWorkspaceName,
  selectedRunId,
}: OptionsProps) {
  const location = useLocation();
  if (!runs) {
    return <p className="p-2 italic text-sm">Loading...</p>;
  } else if (!Object.keys(runs).length) {
    return (
      <p className="p-2 italic whitespace-nowrap text-sm">
        No runs in this workspace
      </p>
    );
  } else {
    return (
      <Fragment>
        {sortBy(Object.keys(runs), (runId) => runs[runId].createdAt)
          .reverse()
          .map((runId) => {
            const createdAt = DateTime.fromMillis(runs[runId].createdAt);
            return (
              <MenuItem key={runId}>
                <Link
                  to={getRunUrl(
                    projectId!,
                    runId,
                    activeWorkspaceName,
                    location.pathname,
                  )}
                  className="flex items-baseline gap-1 text-sm p-1 data-active:bg-slate-100 rounded-sm"
                >
                  <span
                    className={classNames(
                      "font-mono",
                      runId == selectedRunId && "font-bold",
                    )}
                  >
                    {runId}
                  </span>
                  <span
                    className="text-xs text-slate-400 whitespace-nowrap"
                    title={createdAt.toLocaleString(
                      DateTime.DATETIME_SHORT_WITH_SECONDS,
                    )}
                  >
                    {createdAt.toRelative()}
                  </span>
                </Link>
              </MenuItem>
            );
          })}
      </Fragment>
    );
  }
}

function getNextPrevious(
  ids: string[],
  currentId: string,
  direction: "next" | "previous",
) {
  const index = ids.indexOf(currentId);
  if (index >= 0) {
    if (direction == "next") {
      if (index < ids.length - 1) {
        return ids[index + 1];
      }
    } else {
      if (index > 0) {
        return ids[index - 1];
      }
    }
  }

  return null;
}

type NextPreviousButtonProps = {
  projectId: string | null;
  activeWorkspaceName: string | undefined;
  runs: Record<string, Pick<models.Run, "createdAt">> | undefined;
  currentRunId: string;
  direction: "next" | "previous";
};

function NextPreviousButton({
  projectId,
  activeWorkspaceName,
  runs,
  currentRunId,
  direction,
}: NextPreviousButtonProps) {
  const location = useLocation();
  // TODO: move to parent?
  const runIds = runs
    ? sortBy(Object.keys(runs), (runId) => runs[runId].createdAt)
    : [];
  const runId = getNextPrevious(runIds, currentRunId, direction);
  const Icon = direction == "next" ? IconChevronRight : IconChevronLeft;
  const className = classNames(
    "p-1 bg-white border border-slate-300 flex items-center",
    runId ? "hover:bg-slate-50 text-slate-500" : "text-slate-200",
    direction == "next" ? "rounded-r-md -ml-px" : "rounded-l-md -mr-px",
  );
  if (runId) {
    return (
      <Link
        to={getRunUrl(
          projectId!,
          runId,
          activeWorkspaceName,
          location.pathname,
        )}
        className={className}
      >
        <Icon size={16} />
      </Link>
    );
  } else {
    return (
      <span className={className}>
        <Icon size={16} />
      </span>
    );
  }
}

type Props = {
  runs: Record<string, Pick<models.Run, "createdAt">> | undefined;
  projectId: string | null;
  runId: string;
  activeWorkspaceName: string | undefined;
  className?: string;
};

export default function RunSelector({
  runs,
  projectId,
  runId,
  activeWorkspaceName,
  className,
}: Props) {
  return (
    <div className={classNames(className, "flex shadow-xs")}>
      <NextPreviousButton
        direction="previous"
        projectId={projectId}
        activeWorkspaceName={activeWorkspaceName}
        runs={runs}
        currentRunId={runId}
      />
      <Menu>
        <MenuButton className="flex items-center w-full py-1 px-2 gap-1 bg-white border border-slate-300 hover:bg-slate-50">
          <span className="font-mono text-sm">{runId}</span>
          <span className="text-slate-500">
            <IconChevronDown size={16} />
          </span>
        </MenuButton>
        <MenuItems
          transition
          className="p-1 overflow-y-scroll bg-white shadow-xl rounded-md origin-top transition duration-200 ease-out data-closed:scale-95 data-closed:opacity-0"
          anchor={{ to: "bottom start", gap: 2, padding: 20 }}
        >
          <Options
            runs={runs}
            projectId={projectId}
            activeWorkspaceName={activeWorkspaceName}
            selectedRunId={runId}
          />
        </MenuItems>
      </Menu>
      <NextPreviousButton
        direction="next"
        projectId={projectId}
        activeWorkspaceName={activeWorkspaceName}
        runs={runs}
        currentRunId={runId}
      />
    </div>
  );
}
