import { ReactNode } from "react";
import { Size, Variant } from "./types";
import classNames from "classnames";
import { TablerIcon } from "@tabler/icons-react";

const variantStyles: Record<Variant, string> = {
  primary: "bg-cyan-50 text-cyan-700 border-cyan-600/10",
  secondary: "bg-slate-50 text-slate-700 border-slate-600/10",
  success: "bg-green-50 text-green-700 border-green-600/10",
  warning: "bg-yellow-50 text-yellow-700 border-yellow-600/10",
  danger: "bg-red-50 text-red-700 border-red-600/10",
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
        "border rounded-sm flex gap-1",
        variantStyles[variant],
        sizeStyles[size],
        className,
      )}
    >
      {Icon && <Icon size={16} />}
      <div>{children}</div>
    </div>
  );
}
