import { ReactNode } from "react";
import { Size, Variant } from "./types";
import classNames from "classnames";
import { TablerIcon } from "@tabler/icons-react";

const variantStyles: Record<Variant, string> = {
  primary: "bg-sky-50 text-sky-900 border-sky-900/5",
  secondary: "bg-slate-50 text-slate-900 border-slate-900/5",
  success: "bg-green-50 text-green-900 border-green-900/5",
  warning: "bg-yellow-50 text-yellow-900 border-yellow-900/5",
  danger: "bg-red-50 text-red-900 border-red-900/5",
};

const sizeStyles: Record<Size, string> = {
  sm: "text-xs p-1",
  md: "text-sm p-2",
  lg: "text-base p-3",
};

type Props = {
  variant?: Variant;
  size?: Size;
  icon?: TablerIcon;
  className?: string;
  children: ReactNode;
};

export default function Alert({
  variant = "secondary",
  size = "md",
  icon: Icon,
  className,
  children,
}: Props) {
  return (
    <div
      className={classNames(
        "border rounded-sm flex gap-2",
        variantStyles[variant],
        sizeStyles[size],
        className,
      )}
    >
      {Icon && <Icon size={18} className="shrink-0" />}
      <div>{children}</div>
    </div>
  );
}
