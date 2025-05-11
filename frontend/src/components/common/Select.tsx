import { ReactNode } from "react";
import {
  Listbox,
  ListboxButton,
  ListboxOption,
  ListboxOptions,
} from "@headlessui/react";
import { IconSelector } from "@tabler/icons-react";
import classNames from "classnames";

import { useField } from "./Field";
import { Size, Variant } from "./types";

const variantStyles = {
  primary:
    "text-cyan-900 border-slate-300 ring-cyan-200/50 focus:border-cyan-300",
  secondary: "text-slate-900 border-slate-300 ring-slate-200/50",
  success: "text-green-900 border-green-500 ring-green-200/50",
  warning: "text-yellow-900 border-yellow-500 ring-yellow-300/50",
  danger: "text-red-900 border-red-500 ring-red-200/50",
};

const sizeStyles = {
  sm: "px-1 py-0 text-xs",
  md: "px-2 py-1 text-sm",
  lg: "px-3 py-2 text-base",
};

type Props<T extends string> = {
  value: T | null;
  options: Record<T, ReactNode> | T[];
  variant?: Variant;
  size?: Size;
  empty?: string;
  className?: string;
  onChange: (value: T | null) => void;
};

export default function Select<T extends string>({
  value,
  options,
  variant,
  size = "md",
  empty,
  className,
  onChange,
}: Props<T>) {
  const { id: fieldId, hasError } = useField();
  const defaultVariant = hasError ? "warning" : "primary";
  const keys = [
    ...(empty ? [null] : []),
    ...(!Array.isArray(options) ? (Object.keys(options) as T[]) : options),
  ];
  return (
    <Listbox value={value} onChange={onChange}>
      <div className={classNames("relative", className)}>
        <ListboxButton
          id={fieldId}
          className={classNames(
            "w-full flex items-center bg-slate-50 rounded-md shadow-sm border focus:outline-none focus:ring",
            variantStyles[variant || defaultVariant],
            sizeStyles[size],
          )}
        >
          <span className={classNames("flex-1 text-start")}>
            {value
              ? (!Array.isArray(options) && options[value]) || value
              : empty || "Select..."}
          </span>
          <span className="pointer-events-none -mr-1">
            <IconSelector className="size-5 text-gray-400" aria-hidden="true" />
          </span>
        </ListboxButton>
        <ListboxOptions
          anchor="bottom"
          transition
          className="absolute mt-1 p-1 overflow-auto bg-white rounded-md shadow-lg max-h-60 w-[var(--button-width)] focus:outline-none border transition duration-100 ease-in data-[leave]:data-[closed]:opacity-0"
        >
          {keys.map((key) => (
            <ListboxOption
              key={key}
              value={key}
              className="flex items-center gap-2 cursor-default rounded text-sm p-1 data-[active]:bg-slate-100 data-[selected]:font-bold"
            >
              {key === null
                ? empty
                : Array.isArray(options)
                  ? key
                  : options[key]}
            </ListboxOption>
          ))}
        </ListboxOptions>
      </div>
    </Listbox>
  );
}
