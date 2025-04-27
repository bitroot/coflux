import classNames from "classnames";
import { ComponentType, useCallback } from "react";
import { Link } from "react-router-dom";
import {
  IconSubtask,
  IconCpu,
  IconProps,
  IconInnerShadowTopLeft,
  IconAlertCircle,
  IconClock,
  IconTrash,
  IconDotsVertical,
} from "@tabler/icons-react";
import { DateTime } from "luxon";
import { Menu, MenuButton, MenuItem, MenuItems } from "@headlessui/react";
import { sortBy } from "lodash";

import * as models from "../models";
import { buildUrl, pluralise } from "../utils";
import useNow from "../hooks/useNow";
import * as api from "../api";

type TargetProps = {
  url: string;
  icon: ComponentType<IconProps>;
  name: string;
  isActive: boolean;
};

function Target({ url, icon: Icon, name, isActive }: TargetProps) {
  return (
    <li>
      <Link
        to={url}
        className={classNames(
          "px-1 py-0.5 my-0.5 rounded-md flex gap-1 items-center text-slate-900",
          isActive ? "bg-slate-200" : "hover:bg-slate-200/50",
        )}
      >
        <Icon size={20} strokeWidth={1} className="text-slate-500 shrink-0" />
        <div className="font-mono flex-1 overflow-hidden text-sm text-ellipsis">
          {name}
        </div>
      </Link>
    </li>
  );
}

type RepositoryHeaderProps = {
  repositoryName: string;
  repository: models.Repository;
  isActive: boolean;
  projectId: string;
  workspaceName: string;
  now: DateTime<true>;
};

function RepositoryHeader({
  repositoryName,
  repository,
  isActive,
  projectId,
  workspaceName,
  now,
}: RepositoryHeaderProps) {
  const nextDueDiff = repository.nextDueAt
    ? DateTime.fromMillis(repository.nextDueAt).diff(now, [
        "days",
        "hours",
        "minutes",
        "seconds",
      ])
    : undefined;
  return (
    <Link
      to={buildUrl(
        `/projects/${projectId}/repositories/${encodeURIComponent(repositoryName)}`,
        { workspace: workspaceName },
      )}
      className={classNames(
        "flex-1 rounded-md",
        isActive ? "bg-slate-200" : "hover:bg-slate-200/50",
      )}
    >
      <div className="flex items-center py-1 px-1 gap-2">
        <h2 className="font-bold uppercase text-slate-400 text-sm">
          {repositoryName}
        </h2>
        {nextDueDiff && nextDueDiff.toMillis() < -1000 ? (
          <span
            title={`Executions overdue (${nextDueDiff.rescale().toHuman({
              unitDisplay: "short",
            })})`}
          >
            <IconAlertCircle
              size={16}
              className={
                nextDueDiff.toMillis() < -5000
                  ? "text-red-700"
                  : "text-yellow-600"
              }
            />
          </span>
        ) : repository.executing ? (
          <span
            title={`${pluralise(repository.executing, "execution")} running`}
          >
            <IconInnerShadowTopLeft
              size={16}
              className="text-cyan-400 animate-spin"
            />
          </span>
        ) : repository.scheduled ? (
          <span
            title={`${pluralise(repository.scheduled, "execution")} scheduled${
              nextDueDiff
                ? ` (${nextDueDiff.rescale().toHuman({ unitDisplay: "narrow" })})`
                : ""
            }`}
          >
            <IconClock size={16} className="text-slate-400" />
          </span>
        ) : undefined}
      </div>
    </Link>
  );
}

type RepositoryMenuProps = {
  projectId: string;
  workspaceName: string;
  repositoryName: string;
};

function RepositoryMenu({
  projectId,
  workspaceName,
  repositoryName,
}: RepositoryMenuProps) {
  const handleArchiveClick = useCallback(() => {
    if (
      confirm(
        `Are you sure you want to archive '${repositoryName}'? It will be hidden until it's re-registered.`,
      )
    ) {
      api.archiveRepository(projectId, workspaceName, repositoryName);
    }
  }, [projectId, workspaceName, repositoryName]);
  return (
    <Menu>
      <MenuButton className="text-slate-600 p-1 hover:bg-slate-200 rounded">
        <IconDotsVertical size={16} />
      </MenuButton>
      <MenuItems
        transition
        className="p-1 bg-white shadow-xl rounded-md origin-top transition duration-200 ease-out data-[closed]:scale-95 data-[closed]:opacity-0"
        anchor={{ to: "bottom end" }}
      >
        <MenuItem>
          <button
            className="text-sm p-1 rounded data-[active]:bg-slate-100 flex items-center gap-1"
            onClick={handleArchiveClick}
          >
            <span className="shrink-0 text-slate-400">
              <IconTrash size={16} strokeWidth={1.5} />
            </span>
            <span className="flex-1">Archive repository</span>
          </button>
        </MenuItem>
      </MenuItems>
    </Menu>
  );
}

type Props = {
  projectId: string;
  workspaceName: string;
  activeRepository: string | undefined;
  activeTarget: string | undefined;
  repositories: Record<string, models.Repository>;
};

export default function TargetsList({
  projectId,
  workspaceName,
  activeRepository,
  activeTarget,
  repositories,
}: Props) {
  const now = useNow(500);
  return (
    <div className="px-3 py-1">
      {sortBy(Object.entries(repositories), ([name, _]) => name).map(
        ([repositoryName, repository]) => (
          <div key={repositoryName} className="py-2">
            <div className="flex gap-1 sticky top-0 bg-slate-100 py-1">
              <RepositoryHeader
                repositoryName={repositoryName}
                repository={repository}
                isActive={activeRepository == repositoryName && !activeTarget}
                projectId={projectId}
                workspaceName={workspaceName}
                now={now}
              />
              <RepositoryMenu
                projectId={projectId}
                workspaceName={workspaceName}
                repositoryName={repositoryName}
              />
            </div>
            {repository.workflows.length || repository.sensors.length ? (
              <ul>
                {repository.workflows.toSorted().map((name) => {
                  const isActive =
                    activeRepository == repositoryName && activeTarget == name;
                  return (
                    <Target
                      key={name}
                      name={name}
                      icon={IconSubtask}
                      url={buildUrl(
                        `/projects/${projectId}/workflows/${encodeURIComponent(
                          repositoryName,
                        )}/${name}`,
                        { workspace: workspaceName },
                      )}
                      isActive={isActive}
                    />
                  );
                })}
                {repository.sensors.map((name) => {
                  const isActive =
                    activeRepository == repositoryName && activeTarget == name;
                  return (
                    <Target
                      key={name}
                      name={name}
                      icon={IconCpu}
                      url={buildUrl(
                        `/projects/${projectId}/sensors/${encodeURIComponent(
                          repositoryName,
                        )}/${name}`,
                        { workspace: workspaceName },
                      )}
                      isActive={isActive}
                    />
                  );
                })}
              </ul>
            ) : (
              <p className="text-slate-300 italic px-2 text-sm">No targets</p>
            )}
          </div>
        ),
      )}
    </div>
  );
}
