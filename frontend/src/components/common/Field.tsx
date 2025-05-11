import { ReactNode, createContext, useContext, useId } from "react";
import { IconAlertTriangle } from "@tabler/icons-react";
import classNames from "classnames";

const Context = createContext<{ id: string; hasError: boolean }>({
  id: "",
  hasError: false,
});

type Props = {
  label: ReactNode;
  hint?: ReactNode;
  error?: string;
  className?: string;
  children: ReactNode;
};

export default function Field({
  label,
  hint,
  error,
  className,
  children,
}: Props) {
  const id = useId();
  return (
    <Context.Provider value={{ id, hasError: !!error }}>
      <div className={classNames("my-2", className)}>
        <div className="mb-1 flex items-center">
          <label htmlFor={id} className="text-sm">
            {label}
          </label>
          {hint && (
            <span className="text-slate-400 ml-1 text-xs">({hint})</span>
          )}
        </div>
        <div>{children}</div>
        {error && (
          <div className="flex items-center text-yellow-700 my-1 gap-1">
            <IconAlertTriangle size={16} stroke={1.5} />
            <p className="flex-1 text-sm">{error}</p>
          </div>
        )}
      </div>
    </Context.Provider>
  );
}

export function useField() {
  return useContext(Context);
}
