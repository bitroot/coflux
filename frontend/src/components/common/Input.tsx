import {
  ChangeEvent,
  InputHTMLAttributes,
  ReactNode,
  useCallback,
} from "react";
import classNames from "classnames";

import { Size, Variant } from "./types";
import { useField } from "./Field";

const variantStyles = {
  primary:
    "text-cyan-900 border-slate-300 ring-cyan-200/50 focus-within:border-cyan-300",
  secondary: "text-slate-900 border-slate-300 ring-slate-200/50",
  success: "text-green-900 border-green-500 ring-green-200/50",
  warning: "text-yellow-900 border-yellow-500 ring-yellow-300/50",
  danger: "text-red-900 border-red-500 ring-red-200/50",
};

const sizeStyles = {
  sm: "px-1",
  md: "px-2",
  lg: "px-3",
};

const inputSizeStyles = {
  sm: "py-0 text-xs",
  md: "py-1 text-sm",
  lg: "py-2 text-base",
};

type Props = Omit<
  InputHTMLAttributes<HTMLInputElement>,
  "onChange" | "size"
> & {
  variant?: Variant;
  size?: Size;
  left?: ReactNode;
  right?: ReactNode;
  onChange?: (value: string) => void;
};

export default function Input({
  variant,
  size = "md",
  left,
  right,
  className,
  onChange,
  ...props
}: Props) {
  const { id: fieldId, hasError } = useField();
  const handleChange = useCallback(
    (ev: ChangeEvent<HTMLInputElement>) => onChange?.(ev.target.value),
    [onChange],
  );
  const defaultVariant = hasError ? "warning" : "primary";
  return (
    <div
      className={classNames(
        "flex items-center bg-slate-50 rounded-md shadow-sm border focus-within:ring ",
        variantStyles[variant || defaultVariant],
        sizeStyles[size],
        className,
      )}
    >
      {left}
      <input
        className={classNames(
          "flex-1 p-0 border-none text-inherit bg-transparent focus:ring-0 w-full",
          inputSizeStyles[size],
        )}
        id={fieldId}
        onChange={handleChange}
        {...props}
      />
      {right}
    </div>
  );
}
