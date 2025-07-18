import classNames from "classnames";
import { Size } from "./common/types";
import { ReactNode } from "react";

type Intent = "success" | "danger" | "warning" | "info" | "none";

function classNameForIntent(intent: Intent) {
  switch (intent) {
    case "success":
      return "bg-green-100 text-green-800";
    case "danger":
      return "bg-red-100 text-red-800";
    case "warning":
      return "bg-yellow-100 text-yellow-800";
    case "info":
      return "bg-blue-100 text-blue-800";
    case "none":
      return "bg-slate-100 text-slate-800";
  }
}

function classNameForSize(size: Size) {
  switch (size) {
    case "sm":
      return "text-[10px]/[14px] font-normal";
    case "md":
      return "text-xs font-semibold";
    case "lg":
      return "text-semibold";
  }
}

type Props = {
  label: ReactNode;
  intent?: Intent;
  size?: Size;
  title?: string;
};

export default function Badge({
  label,
  intent = "none",
  size = "md",
  title,
}: Props) {
  return (
    <span
      className={classNames(
        "rounded-md px-1 py-px uppercase",
        classNameForIntent(intent),
        classNameForSize(size),
      )}
      title={title}
    >
      {label}
    </span>
  );
}
