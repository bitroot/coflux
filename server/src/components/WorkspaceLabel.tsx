import { ReactNode } from "react";
import classNames from "classnames";
import { IconExclamationCircle } from "@tabler/icons-react";
import { useWorkspaces } from "../topics";

function classNameForWorkspace(
  name: string | undefined,
  interactive: boolean | undefined,
) {
  if (name?.startsWith("stag")) {
    return classNames(
      "bg-yellow-300/70 text-yellow-900",
      interactive && "hover:bg-yellow-300/60",
    );
  } else if (name?.startsWith("prod")) {
    return classNames(
      "bg-fuchsia-300/70 text-fuchsia-900",
      interactive && "hover:bg-fuchsia-300/60",
    );
  } else {
    return classNames(
      "bg-slate-300/70 text-slate-700",
      interactive && "hover:bg-slate-300/60",
    );
  }
}

type Props = {
  projectId: string;
  workspaceId: string;
  size?: "sm" | "md";
  interactive?: boolean;
  warning?: string;
  accessory?: ReactNode;
  compact?: boolean;
};

export default function WorkspaceLabel({
  projectId,
  workspaceId,
  size,
  interactive,
  warning,
  accessory,
  compact,
}: Props) {
  const workspaces = useWorkspaces(projectId);
  const workspace = workspaces?.[workspaceId];
  return (
    <span
      className={classNames(
        "flex items-center gap-0.5 overflow-hidden",
        size == "sm"
          ? "px-1 py-px rounded-md h-5"
          : "px-1.5 py-0.5 rounded-lg h-6",
        classNameForWorkspace(workspace?.name, interactive),
      )}
      title={warning}
    >
      {warning && (
        <IconExclamationCircle
          size={size == "sm" ? 13 : 15}
          className="shrink-0"
        />
      )}
      {!compact && (
        <span
          className={classNames(
            "whitespace-nowrap overflow-hidden text-ellipsis",
            size == "sm" ? "px-px text-xs" : "px-0.5 text-sm",
            workspace?.state == "archived" && "line-through opacity-50",
          )}
        >
          {workspace?.name}
        </span>
      )}
      {accessory}
    </span>
  );
}
