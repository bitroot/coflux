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
          "px-1 py-1 my-px rounded-md flex gap-1 items-center text-slate-700",
          isActive ? "bg-slate-100" : "hover:bg-slate-100/50",
        )}
      >
        <Icon size={20} strokeWidth={1} className="text-slate-400 shrink-0" />
        <div className="font-mono flex-1 overflow-hidden text-sm text-ellipsis">
          {name}
        </div>
      </Link>
    </li>
  );
}

type ModuleHeaderProps = {
  moduleName: string;
  module: models.Module;
  isActive: boolean;
  projectId: string;
  spaceName: string;
  now: DateTime<true>;
};

function ModuleHeader({
  moduleName,
  module,
  isActive,
  projectId,
  spaceName,
  now,
}: ModuleHeaderProps) {
  const nextDueDiff = module.nextDueAt
    ? DateTime.fromMillis(module.nextDueAt).diff(now, [
        "days",
        "hours",
        "minutes",
        "seconds",
      ])
    : undefined;
  return (
    <Link
      to={buildUrl(
        `/projects/${projectId}/modules/${encodeURIComponent(moduleName)}`,
        { space: spaceName },
      )}
      className={classNames(
        "flex-1 rounded-md",
        isActive ? "bg-slate-100" : "hover:bg-slate-100/50",
      )}
    >
      <div className="flex items-center py-1 px-1 gap-2">
        <h2 className="font-semibold uppercase text-slate-400 text-sm">
          {moduleName}
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
        ) : module.executing ? (
          <span title={`${pluralise(module.executing, "execution")} running`}>
            <IconInnerShadowTopLeft
              size={16}
              className="text-cyan-400 animate-spin"
            />
          </span>
        ) : module.scheduled ? (
          <span
            title={`${pluralise(module.scheduled, "execution")} scheduled${
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

type ModuleMenuProps = {
  projectId: string;
  spaceName: string;
  moduleName: string;
};

function ModuleMenu({ projectId, spaceName, moduleName }: ModuleMenuProps) {
  const handleArchiveClick = useCallback(() => {
    if (
      confirm(
        `Are you sure you want to archive '${moduleName}'? It will be hidden until it's re-registered.`,
      )
    ) {
      api.archiveModule(projectId, spaceName, moduleName);
    }
  }, [projectId, spaceName, moduleName]);
  return (
    <Menu>
      <MenuButton className="text-slate-600 p-1 hover:bg-slate-200 rounded-sm">
        <IconDotsVertical size={16} />
      </MenuButton>
      <MenuItems
        transition
        className="p-1 bg-white shadow-xl rounded-md origin-top transition duration-200 ease-out data-closed:scale-95 data-closed:opacity-0"
        anchor={{ to: "bottom end" }}
      >
        <MenuItem>
          <button
            className="text-sm p-1 rounded-sm data-active:bg-slate-100 flex items-center gap-1"
            onClick={handleArchiveClick}
          >
            <span className="shrink-0 text-slate-400">
              <IconTrash size={16} strokeWidth={1.5} />
            </span>
            <span className="flex-1">Archive module</span>
          </button>
        </MenuItem>
      </MenuItems>
    </Menu>
  );
}

type Props = {
  projectId: string;
  spaceName: string;
  activeModule: string | undefined;
  activeTarget: string | undefined;
  modules: Record<string, models.Module>;
};

export default function TargetsList({
  projectId,
  spaceName,
  activeModule,
  activeTarget,
  modules,
}: Props) {
  const now = useNow(500);
  return (
    <div className="px-3 py-1">
      {sortBy(Object.entries(modules), ([name]) => name).map(
        ([moduleName, module]) => (
          <div key={moduleName} className="py-2">
            <div className="flex gap-1 sticky top-0 bg-white py-1">
              <ModuleHeader
                moduleName={moduleName}
                module={module}
                isActive={activeModule == moduleName && !activeTarget}
                projectId={projectId}
                spaceName={spaceName}
                now={now}
              />
              <ModuleMenu
                projectId={projectId}
                spaceName={spaceName}
                moduleName={moduleName}
              />
            </div>
            {module.workflows.length || module.sensors.length ? (
              <ul>
                {module.workflows.toSorted().map((name) => {
                  const isActive =
                    activeModule == moduleName && activeTarget == name;
                  return (
                    <Target
                      key={name}
                      name={name}
                      icon={IconSubtask}
                      url={buildUrl(
                        `/projects/${projectId}/workflows/${encodeURIComponent(
                          moduleName,
                        )}/${name}`,
                        { space: spaceName },
                      )}
                      isActive={isActive}
                    />
                  );
                })}
                {module.sensors.map((name) => {
                  const isActive =
                    activeModule == moduleName && activeTarget == name;
                  return (
                    <Target
                      key={name}
                      name={name}
                      icon={IconCpu}
                      url={buildUrl(
                        `/projects/${projectId}/sensors/${encodeURIComponent(
                          moduleName,
                        )}/${name}`,
                        { space: spaceName },
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
