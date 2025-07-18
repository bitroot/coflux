import classNames from "classnames";
import { ComponentProps, ElementType, ReactNode } from "react";
import { Size, Variant } from "./types";

const outlineStyles = {
  true: "border bg-white",
  false: "text-white shadow-xs",
};

const variantOutlineStyles = {
  primary: {
    true: [
      "border-cyan-400/50 text-cyan-500 enabled:shadow-cyan-500/30",
      "hover:text-cyan-600 hover:border-cyan-500",
      "focus:ring-cyan-300/50",
      "disabled:border-cyan-500/20 disabled:text-cyan-500/30",
    ],
    false: [
      "bg-cyan-500 enabled:shadow-cyan-800/50",
      "hover:bg-cyan-600",
      "focus:ring-cyan-400/50",
      "disabled:bg-cyan-600/20",
    ],
  },
  secondary: {
    true: [
      "border-slate-400/50 text-slate-500 enabled:shadow-slate-500/30",
      "hover:text-slate-600 hover:border-slate-400",
      "focus:ring-slate-300/50",
      "disabled:border-slate-500/20 disabled:text-slate-500/30",
    ],
    false: [
      "bg-slate-500 enabled:shadow-slate-800/50",
      "hover:bg-slate-600",
      "focus:ring-slate-400/50",
      "disabled:bg-slate-600/20",
    ],
  },
  success: {
    true: [
      "border-green-400/50 text-green-500 enabled:shadow-green-500/30",
      "hover:text-green-600 hover:border-green-500",
      "focus:ring-green-300/50",
      "disabled:border-green-500/20 disabled:text-green-500/30",
    ],
    false: [
      "bg-green-500 enabled:shadow-green-800/50",
      "hover:bg-green-600",
      "focus:ring-green-400/50",
      "disabled:bg-green-600/20",
    ],
  },
  warning: {
    true: [
      "border-yellow-400/50 text-yellow-500 enabled:shadow-yellow-500/30",
      "hover:text-yellow-600 hover:border-yellow-500",
      "focus:ring-yellow-300/50",
      "disabled:border-yellow-500/20 disabled:text-yellow-500/30",
    ],
    false: [
      "bg-yellow-500 enabled:shadow-yellow-800/50",
      "hover:bg-yellow-600",
      "focus:ring-yellow-400/50",
      "disabled:bg-yellow-600/20",
    ],
  },
  danger: {
    true: [
      "border-red-400/50 text-red-500 enabled:shadow-red-500/30",
      "hover:text-red-600 hover:border-red-500",
      "focus:ring-red-300/50",
      "disabled:border-red-500/20 disabled:text-red-500/30",
    ],
    false: [
      "bg-red-500 enabled:shadow-red-800/50",
      "hover:bg-red-600",
      "focus:ring-red-400/50",
      "disabled:bg-red-600/20",
    ],
  },
};

const sizeStyles = {
  sm: "rounded-sm px-2 pt-1 pb-0.5 text-xs h-6",
  md: "rounded-md px-3 pt-1.5 pb-1 text-sm h-8",
  lg: "rounded-md px-4 pt-1.5 pb-1 text-base",
};

type Props<C extends ElementType = "button"> = {
  variant?: Variant;
  outline?: boolean;
  size?: Size;
  as?: C;
  left?: ReactNode;
  right?: ReactNode;
} & Omit<
  ComponentProps<C>,
  "variant" | "outline" | "size" | "as" | "left" | "right"
>;

export default function Button<C extends ElementType = "button">({
  variant = "primary",
  outline = false,
  size = "md",
  as,
  className,
  children,
  left,
  right,
  ...props
}: Props<C>) {
  const Component = as ?? "button";
  return (
    <Component
      className={classNames(
        "focus:ring-3 focus:outline-hidden font-medium text-center flex items-center gap-1",
        outlineStyles[outline ? "true" : "false"],
        variantOutlineStyles[variant][outline ? "true" : "false"],
        sizeStyles[size],
        className,
      )}
      {...props}
    >
      {left}
      {children}
      {right}
    </Component>
  );
}
