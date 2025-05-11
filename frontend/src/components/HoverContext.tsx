import {
  ReactNode,
  createContext,
  useCallback,
  useContext,
  useState,
} from "react";

type State =
  | {
      runId: string;
    }
  | {
      stepId: string;
      attempt?: number;
    }
  | {
      assetId: string;
    };

const Context = createContext<
  [State | undefined, (hovered: State | undefined) => void] | undefined
>(undefined);

export function useHoverContext() {
  const value = useContext(Context);
  if (!value) {
    throw new Error("not in hover context");
  }
  const [state, setState] = value;
  const isHovered = useCallback(
    (query: State) => {
      return (
        state &&
        (("runId" in query && "runId" in state && query.runId == state.runId) ||
          ("stepId" in query &&
            "stepId" in state &&
            query.stepId == state.stepId &&
            ("attempt" in query
              ? "attempt" in state && query.attempt == state.attempt
              : true)) ||
          ("assetId" in query &&
            "assetId" in state &&
            query.assetId == state.assetId))
      );
    },
    [state],
  );
  return { isHovered, setHovered: setState };
}

type Props = {
  children: ReactNode;
};

export default function HoverContext({ children }: Props) {
  const [state, setState] = useState<State>();
  return (
    <Context.Provider value={[state, setState]}>{children}</Context.Provider>
  );
}
